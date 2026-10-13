"""LangGraph multi-agent assistant.

guard (analysis) -> supervisor <-> {kpi_analyst, supplier_analyst, inventory_analyst, network_analyst, sql_analyst}
                   <-> tools -> composer
guard (lookup)   -> entity_resolver -> schema_retriever -> sql_writer -> sql_validator -> execute_sql -> result_check
                   -> composer          (validator error back to the writer, max 3 attempts; 0 rows: one retry)
composer -> checks -> judge -> finalize          (revise once, then safe fallback)
guard -> fixed answer (out of scope / clarification);  tools / SQL calls -> unavailable (data service down)

The LLM reaches data only through the six API tools (assistant/tools.py); network_analyst's
get_supplier_disruption_impact is the API's view of the supply-chain knowledge graph, and sql_analyst's run_sql_query
runs read-only SQL that the API validates (Gold tables only, no confidential columns, row limit) for ad-hoc lookups.
Lookup questions skip the supervisor: the lookup path resolves IDs in code, reads the schema from the API, lets the
LLM write one query, dry-runs it (POST /analytics/sql/validate) and repairs it with the exact error before it runs.
"""
import json
import operator
import os
from typing import Annotated, Any, TypedDict

import httpx
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage
from langgraph.graph import END, START, StateGraph

from assistant import prompts
from assistant.evaluation import deterministic_checks, evidence_json, fallback_answer
from assistant.lookup import resolve_entities, schema_text
from assistant.schemas import (
    THRESHOLDS,
    AssistantAnswer,
    Draft,
    Judgment,
    RouteDecision,
    SqlDraft,
    SupervisorDecision,
    judgment_passed,
)
from assistant.tools import SPECIALIST_TOOLS, call_tool, tool_definitions

MAX_SUPERVISOR_STEPS = 3
MAX_TOOL_CALLS_PER_SPECIALIST = 4
MAX_TOOL_CALLS = 8
RECURSION_LIMIT = 40
EUR_TOOLS = {'get_supplier_scorecard', 'get_product_exposure', 'get_delayed_order_lines'}
FX_LIMITATION = 'EUR values use placeholder FX rates until the business brief fx_rates values are provided.'
KG_TOOLS = {'get_supplier_disruption_impact'}
KG_LIMITATION = ('Supplier dependencies and open shipments come from the knowledge-graph snapshot of the Gold tables '
                 '(as of its last build), not from a live query.')
SQL_TOOLS = {'run_sql_query'}
SQL_LIMITATION = 'The SQL result was cut at the row limit; only the first rows are shown.'
MAX_SQL_ATTEMPTS = 3          # SQL writer attempts when the validator or the warehouse rejects the query
LOOKUP_ROWS = 50
EMPTY_RESULT_FEEDBACK = ('The query returned 0 rows. Check the filters: exact upper-case IDs, exact code values from '
                         'the schema, the right column (see synonyms), and no unnecessary conditions. If nothing can '
                         'match, return the same query.')


def default_llm():
    """Same construction as the instructor's demos; reads only the instructor's .env values."""
    from dotenv import load_dotenv
    from langchain_openai import AzureChatOpenAI

    load_dotenv()
    return AzureChatOpenAI(model=os.getenv('AZURE_OPENAI_DEPLOYMENT_NAME'), temperature=0)


class State(TypedDict, total=False):
    question: str
    route: str
    question_type: str
    entities: list[dict[str, str]]
    schema: dict[str, Any] | None
    sql_draft: dict[str, Any] | None
    sql_attempts: int
    sql_feedback: str | None
    empty_retry_done: bool
    clarifying_question: str | None
    active: str
    task: str
    agent_messages: list[BaseMessage]
    pending_tools: bool
    findings: Annotated[list[dict[str, Any]], operator.add]
    evidence: Annotated[list[dict[str, Any]], operator.add]
    supervisor_steps: int
    unavailable: str | None
    draft: dict[str, Any] | None
    feedback: list[str]
    revisions: int
    judgment: dict[str, Any] | None
    judge_failed: bool
    fallback_used: bool
    final: dict[str, Any]
    trace: Annotated[list[str], operator.add]


def _text(message: AIMessage) -> str:
    content = message.content
    if isinstance(content, list):
        return ' '.join(part.get('text', '') if isinstance(part, dict) else str(part) for part in content)
    return content or ''


def _ok_evidence(state: State) -> list[dict[str, Any]]:
    return [e for e in state.get('evidence', []) if e['status'] == 'ok']


def _is_lookup(state: State) -> bool:
    return state.get('question_type') == 'lookup'


def _answer_evidence(state: State) -> list[dict[str, Any]]:
    """What the answer is based on: all successful tool results, or for a lookup only its final SQL result."""
    ok = _ok_evidence(state)
    return ok[-1:] if _is_lookup(state) else ok


def _result_summary(tool: str, data: dict[str, Any]) -> dict[str, Any]:
    if tool == 'query_kpi':
        return {'kpi': data['kpi'], 'unit': data['unit'], 'results': data['results']}
    if tool in KG_TOOLS:
        return {k: data[k] for k in ('supplier_id', 'supplier_name', 'affected_product_count',
                                     'uncovered_product_count')} | {'affected_products': data['affected_products'][:5]}
    if tool in SQL_TOOLS:
        return {k: data[k] for k in ('sql', 'tables', 'columns', 'row_count', 'truncated')} | {'rows': data['rows'][:5]}
    rows_key = next(k for k in ('suppliers', 'products', 'order_lines') if k in data)
    return {'count': data['count'], rows_key: data[rows_key][:5]}


def build_graph(llm: Any, client: httpx.AsyncClient):
    """Compiles the graph for one request. `client` calls the API's business endpoints."""

    async def structured(schema, messages):
        return await llm.with_structured_output(schema, method='function_calling').ainvoke(messages)

    # ------------------------------------------------------------------ guard and router
    async def guard(state: State) -> State:
        decision = await structured(RouteDecision, [SystemMessage(prompts.ROUTER), HumanMessage(state['question'])])
        lookup = decision.route == 'answer' and decision.question_type == 'lookup'
        return {'route': decision.route, 'clarifying_question': decision.clarifying_question,
                'question_type': 'lookup' if lookup else 'analysis',
                'trace': [f'guard:{decision.route}' + (':lookup' if lookup else '')]}

    def fixed_answer(state: State) -> State:
        if state['route'] == 'clarify':
            status = 'clarification_needed'
            text = state.get('clarifying_question') or 'Could you say which measure, supplier, product or period?'
        else:
            status = 'out_of_scope'
            text = ('I can only answer read-only questions about delivery performance, suppliers, delayed orders, '
                    'stock-out exposure and the impact of a supplier that stops delivering. I cannot perform actions '
                    'or share confidential raw values.')
        return {'final': _final(state, status, text, measures=[], limitations=[], quality=None),
                'trace': ['fixed_answer']}

    # ------------------------------------------------------------------ supervisor
    async def supervisor(state: State) -> State:
        steps = state.get('supervisor_steps', 0)
        if steps >= MAX_SUPERVISOR_STEPS or len(state.get('evidence', [])) >= MAX_TOOL_CALLS:
            return {'active': 'finish', 'trace': ['supervisor:finish(limit)']}
        findings = '\n'.join(f"- {f['specialist']} ({f['task']}): {f['summary']}" for f in state.get('findings', []))
        decision = await structured(SupervisorDecision, [
            SystemMessage(prompts.SUPERVISOR),
            HumanMessage(f"Question: {state['question']}\n\nFindings so far:\n{findings or '(none)'}")])
        if decision.next == 'finish':
            return {'active': 'finish', 'trace': ['supervisor:finish']}
        return {'active': decision.next, 'task': decision.task, 'supervisor_steps': steps + 1,
                'agent_messages': [SystemMessage(prompts.SPECIALISTS[decision.next] + prompts.SPECIALIST_RULES),
                                   HumanMessage(f"User question: {state['question']}\nYour task: {decision.task}")],
                'trace': [f'supervisor->{decision.next}']}

    # ------------------------------------------------------------------ specialists
    def make_specialist(name: str):
        definitions = tool_definitions(SPECIALIST_TOOLS[name])

        async def specialist(state: State) -> State:
            messages = state['agent_messages']
            used_here = sum(isinstance(m, ToolMessage) for m in messages)
            budget = min(MAX_TOOL_CALLS_PER_SPECIALIST - used_here, MAX_TOOL_CALLS - len(state.get('evidence', [])))
            model = llm.bind_tools(definitions) if budget > 0 else llm
            reply = await model.ainvoke(messages)
            messages = messages + [reply]
            if reply.tool_calls and budget > 0:
                return {'agent_messages': messages, 'pending_tools': True,
                        'trace': [f"{name}:call {', '.join(c['name'] for c in reply.tool_calls)}"]}
            return {'agent_messages': messages, 'pending_tools': False, 'trace': [f'{name}:done'],
                    'findings': [{'specialist': name, 'task': state['task'], 'summary': _text(reply) or '(none)'}]}

        return specialist

    # ------------------------------------------------------------------ tool node
    async def tools(state: State) -> State:
        call = state['agent_messages'][-1]
        allowed = SPECIALIST_TOOLS[state['active']]
        remaining = MAX_TOOL_CALLS - len(state.get('evidence', []))
        results, evidence, trace, unavailable = [], [], [], None
        for i, tool_call in enumerate(call.tool_calls):
            name = tool_call['name']
            if name not in allowed:
                outcome_kind, message, data, args = 'invalid_arguments', f'Tool {name!r} is not available to you.', None, {}
            elif i >= remaining:
                outcome_kind, message, data, args = 'invalid_arguments', 'Tool-call limit reached.', None, {}
            else:
                outcome = await call_tool(client, name, tool_call.get('args'))
                outcome_kind, message, data, args = outcome.kind, outcome.message, outcome.data, outcome.arguments
            ok = outcome_kind == 'ok'
            content = json.dumps(data, default=str) if ok else message
            results.append(ToolMessage(content=content, tool_call_id=tool_call['id'], name=name,
                                       status='success' if ok else 'error'))
            evidence.append({'tool': name, 'arguments': args, 'status': 'ok' if ok else 'error',
                             'data': data, 'error': None if ok else message})
            trace.append(f"tool:{name}:{'ok' if ok else outcome_kind}")
            if outcome_kind == 'unavailable':
                unavailable = message
                break
        return {'agent_messages': state['agent_messages'] + results, 'evidence': evidence,
                'unavailable': unavailable, 'trace': trace}

    # ------------------------------------------------------------------ SQL lookup path
    def entity_resolver(state: State) -> State:
        entities = resolve_entities(state['question'])
        return {'entities': entities,
                'trace': ['entity_resolver:' + (', '.join(e['id'] for e in entities) or 'none')]}

    async def schema_retriever(state: State) -> State:
        outcome = await call_tool(client, 'get_sql_schema', {})
        if outcome.kind != 'ok':
            return {'unavailable': outcome.message or 'The SQL schema is unavailable.', 'trace': ['schema_retriever:error']}
        return {'schema': outcome.data, 'trace': ['schema_retriever']}

    async def sql_writer(state: State) -> State:
        entities = '\n'.join(f"- {e['id']}: {e['table']}.{e['column']}" for e in state.get('entities', []))
        content = (f"QUESTION: {state['question']}\n\nENTITIES:\n{entities or '(none)'}\n\n"
                   f"SCHEMA:\n{schema_text(state['schema'])}")
        if state.get('sql_feedback'):
            content += f"\n\nPREVIOUS ATTEMPT:\n{state['sql_draft']['sql']}\nPROBLEM: {state['sql_feedback']}"
        draft = await structured(SqlDraft, [SystemMessage(prompts.SQL_WRITER), HumanMessage(content)])
        return {'sql_draft': draft.model_dump(), 'sql_attempts': state.get('sql_attempts', 0) + 1,
                'sql_feedback': None, 'trace': [f'sql_writer:{draft.kind}']}

    async def sql_call(state: State, tool: str, step: str) -> State:
        """Validator and execution share one pattern: ok, rejected (feedback to the writer) or unavailable."""
        outcome = await call_tool(client, tool, {'sql': state['sql_draft']['sql'], 'limit': LOOKUP_ROWS})
        if outcome.kind == 'unavailable':
            return {'unavailable': outcome.message, 'trace': [f'{step}:error']}
        ok = outcome.kind == 'ok'
        record = tool == 'run_sql_query' or not ok          # successful dry runs are not evidence
        return {'sql_feedback': None if ok else outcome.message, 'trace': [f"{step}:{'ok' if ok else 'fail'}"],
                'evidence': [{'tool': tool, 'arguments': outcome.arguments, 'status': 'ok' if ok else 'error',
                              'data': outcome.data if ok else None, 'error': None if ok else outcome.message}]
                if record else []}

    async def sql_validator(state: State) -> State:
        return await sql_call(state, 'validate_sql_query', 'sql_validator')

    async def execute_sql(state: State) -> State:
        return await sql_call(state, 'run_sql_query', 'execute_sql')

    def result_check(state: State) -> State:
        result = _ok_evidence(state)[-1]['data']
        if result['row_count'] == 0 and not state.get('empty_retry_done'):
            return {'sql_feedback': EMPTY_RESULT_FEEDBACK, 'empty_retry_done': True, 'trace': ['result_check:empty']}
        status = 'empty' if result['row_count'] == 0 else 'truncated' if result['truncated'] else 'ok'
        return {'trace': [f'result_check:{status}']}

    def after_sql_call(next_step: str):
        def route(s: State) -> str:
            if s.get('unavailable'):
                return 'unavailable'
            if not s.get('sql_feedback'):
                return next_step
            return 'sql_writer' if s.get('sql_attempts', 0) < MAX_SQL_ATTEMPTS else 'composer'
        return route

    def unavailable_answer(state: State) -> State:
        return {'final': _final(state, 'data_unavailable', 'The data service is unavailable right now, so I cannot '
                                'answer from the Gold data. Please try again later.', measures=[],
                                limitations=[state['unavailable']], quality=None),
                'trace': ['unavailable']}

    # ------------------------------------------------------------------ composer and evaluation gate
    async def composer(state: State) -> State:
        evidence = _answer_evidence(state)
        feedback = state.get('feedback') or []
        revisions = state.get('revisions', 0) + (1 if feedback else 0)
        if not evidence:
            draft = Draft(status='no_data', answer='I could not retrieve any data for this question.', measures=[])
            return {'draft': draft.model_dump(), 'revisions': revisions, 'feedback': [], 'trace': ['composer:no_data']}
        findings = '\n'.join(f"- {f['specialist']}: {f['summary']}" for f in state.get('findings', []))
        content = (f"Question: {state['question']}\n\nEVIDENCE_JSON:\n{evidence_json(evidence)}\n\n"
                   f"Specialist findings:\n{findings}")
        if feedback:
            content += (f"\n\nYour previous answer was rejected:\n{state['draft']['answer']}\n"
                        'Fix these problems:\n' + '\n'.join(f'- {f}' for f in feedback))
        system = prompts.COMPOSER_LOOKUP if _is_lookup(state) else prompts.COMPOSER
        draft = await structured(Draft, [SystemMessage(system), HumanMessage(content)])
        return {'draft': draft.model_dump(), 'revisions': revisions, 'feedback': [],
                'trace': ['composer:revision' if feedback else 'composer']}

    def checks(state: State) -> State:
        errors = deterministic_checks(state['draft'], _answer_evidence(state), state['question'], lookup=_is_lookup(state))
        return {'feedback': errors, 'trace': ['checks:fail' if errors else 'checks:pass']}

    async def judge(state: State) -> State:
        content = (f"QUESTION:\n{state['question']}\n\nEVIDENCE:\n{evidence_json(_answer_evidence(state))}\n\n"
                   f"CANDIDATE ANSWER:\n{state['draft']['answer']}")
        rubric = prompts.JUDGE_LOOKUP if _is_lookup(state) else prompts.JUDGE
        try:
            judgment = await structured(Judgment, [SystemMessage(rubric), HumanMessage(content)])
        except Exception as exc:  # noqa: BLE001 - a failing judge must never let an unchecked answer through
            return {'judge_failed': True, 'trace': [f'judge:error({type(exc).__name__})']}
        if judgment_passed(judgment):
            return {'judgment': judgment.model_dump(), 'feedback': [], 'trace': ['judge:pass']}
        low = [f'{name} {getattr(judgment, name)} < {minimum}' for name, minimum in THRESHOLDS.items()
               if getattr(judgment, name) < minimum]
        return {'judgment': judgment.model_dump(), 'feedback': (judgment.issues or [judgment.reasoning]) + low,
                'trace': ['judge:fail']}

    def fallback(state: State) -> State:
        evidence = _answer_evidence(state)
        draft = {'status': 'answered' if evidence else 'no_data',
                 'answer': fallback_answer(evidence) if evidence else 'I could not retrieve any data for this question.',
                 'measures': sorted({e['tool'] for e in evidence}),
                 'limitations': ['Answer generated directly from the tool results after the quality check failed.']}
        return {'draft': draft, 'fallback_used': True, 'trace': ['fallback']}

    def finalize(state: State) -> State:
        draft, judgment = state['draft'], state.get('judgment')
        evidence = _answer_evidence(state)
        limitations = list(draft.get('limitations', []))
        if any(e['tool'] in EUR_TOOLS or (e['data'] or {}).get('unit') == 'EUR' for e in evidence):
            limitations.append(FX_LIMITATION)
        if any(e['tool'] in KG_TOOLS for e in evidence):
            limitations.append(KG_LIMITATION)
        if any(e['tool'] in SQL_TOOLS and e['data']['truncated'] for e in evidence):
            limitations.append(SQL_LIMITATION)
        fallback_used = state.get('fallback_used', False)
        if fallback_used:
            passed = False
        elif draft['status'] != 'answered':
            passed = True                      # 'no_data' answers carry no numbers; checks passed, judge not needed
        else:
            passed = judgment is not None and judgment_passed(Judgment(**judgment))
        quality = {'scores': {k: judgment[k] for k in THRESHOLDS} if judgment else None, 'passed': passed,
                   'revised': state.get('revisions', 0) > 0, 'fallback_used': fallback_used}
        return {'final': _final(state, draft['status'], draft['answer'], draft['measures'],
                                list(dict.fromkeys(limitations)), quality), 'trace': ['finalize']}

    # ------------------------------------------------------------------ wiring
    graph = StateGraph(State)
    for name, node in [('guard', guard), ('fixed_answer', fixed_answer), ('supervisor', supervisor),
                       ('tools', tools), ('unavailable', unavailable_answer), ('composer', composer),
                       ('checks', checks), ('judge', judge), ('fallback', fallback), ('finalize', finalize),
                       ('entity_resolver', entity_resolver), ('schema_retriever', schema_retriever),
                       ('sql_writer', sql_writer), ('sql_validator', sql_validator), ('execute_sql', execute_sql),
                       ('result_check', result_check)]:
        graph.add_node(name, node)
    for name in SPECIALIST_TOOLS:
        graph.add_node(name, make_specialist(name))

    graph.add_edge(START, 'guard')
    graph.add_conditional_edges('guard', lambda s: 'fixed_answer' if s['route'] != 'answer'
                                else 'entity_resolver' if _is_lookup(s) else 'supervisor')
    graph.add_edge('entity_resolver', 'schema_retriever')
    graph.add_conditional_edges('schema_retriever', lambda s: 'unavailable' if s.get('unavailable') else 'sql_writer')
    graph.add_edge('sql_writer', 'sql_validator')
    graph.add_conditional_edges('sql_validator', after_sql_call('execute_sql'))
    graph.add_conditional_edges('execute_sql', after_sql_call('result_check'))
    graph.add_conditional_edges('result_check', lambda s: 'sql_writer' if s.get('sql_feedback') else 'composer')
    graph.add_conditional_edges('supervisor', lambda s: s['active'] if s['active'] in SPECIALIST_TOOLS else 'composer')
    for name in SPECIALIST_TOOLS:
        graph.add_conditional_edges(name, lambda s: 'tools' if s.get('pending_tools') else 'supervisor')
    graph.add_conditional_edges('tools', lambda s: 'unavailable' if s.get('unavailable') else s['active'])
    graph.add_edge('composer', 'checks')
    graph.add_conditional_edges('checks', lambda s: 'judge' if not s['feedback'] and s['draft']['status'] == 'answered'
                                else 'finalize' if not s['feedback']
                                else 'composer' if s.get('revisions', 0) == 0 else 'fallback')
    graph.add_conditional_edges('judge', lambda s: 'fallback' if s.get('judge_failed')
                                else 'finalize' if not s['feedback']
                                else 'composer' if s.get('revisions', 0) == 0 else 'fallback')
    graph.add_edge('fallback', 'finalize')
    for end in ('fixed_answer', 'unavailable', 'finalize'):
        graph.add_edge(end, END)
    return graph.compile()


def _final(state: State, status: str, answer: str, measures: list[str], limitations: list[str],
           quality: dict[str, Any] | None) -> dict[str, Any]:
    evidence = state.get('evidence', [])
    ok = [e for e in evidence if e['status'] == 'ok']
    filters: dict[str, Any] = {}
    for e in ok:
        filters.update(e['data'].get('filters', {}))
    return {
        'status': status, 'answer': answer,
        'evidence': [{'tool': e['tool'], 'arguments': e['arguments'], 'status': e['status'],
                      'result_summary': _result_summary(e['tool'], e['data']) if e['status'] == 'ok'
                      else {'error': e['error']}} for e in evidence],
        'filters': filters, 'measures': measures,
        'as_of_date': next((e['data'].get('as_of_date') for e in ok if e['data'].get('as_of_date')), None),
        'limitations': limitations, 'quality': quality, 'trace': state.get('trace', []),
    }


async def run_assistant(question: str, client: httpx.AsyncClient, llm: Any) -> AssistantAnswer:
    graph = build_graph(llm, client)
    state = await graph.ainvoke({'question': question, 'trace': [], 'findings': [], 'evidence': []},
                                config={'recursion_limit': RECURSION_LIMIT})
    final = state['final']
    final['trace'] = state['trace']
    return AssistantAnswer(**final)
