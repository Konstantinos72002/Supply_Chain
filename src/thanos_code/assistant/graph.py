"""LangGraph multi-agent assistant.

guard -> supervisor <-> {kpi_analyst, supplier_analyst, inventory_analyst, network_analyst, data_analyst} <-> tools
      -> composer -> checks -> judge -> finalize          (revise once, then safe fallback)
guard -> fixed answer (out of scope / clarification);  tools -> unavailable (data service down)

The LLM reaches data only through the six API tools (assistant/tools.py); network_analyst's
get_supplier_disruption_impact is the API's view of the supply-chain knowledge graph, and data_analyst's
run_sql_query runs one guarded read-only SELECT (assistant/sql_guard.py) for look-ups no KPI tool covers.
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
from assistant.schemas import (
    THRESHOLDS,
    AssistantAnswer,
    Draft,
    Judgment,
    RouteDecision,
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
SQL_LIMITATION = 'Look-up answered with an ad-hoc read-only SQL query over the Gold tables (see the evidence).'


def default_llm():
    """Same construction as the instructor's demos; reads only the instructor's .env values."""
    from dotenv import load_dotenv
    from langchain_openai import AzureChatOpenAI

    load_dotenv()
    return AzureChatOpenAI(model=os.getenv('AZURE_OPENAI_DEPLOYMENT_NAME'), temperature=0)


class State(TypedDict, total=False):
    question: str
    route: str
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


def _result_summary(tool: str, data: dict[str, Any]) -> dict[str, Any]:
    if tool == 'query_kpi':
        return {'kpi': data['kpi'], 'unit': data['unit'], 'results': data['results']}
    if tool in KG_TOOLS:
        return {k: data[k] for k in ('supplier_id', 'supplier_name', 'affected_product_count',
                                     'uncovered_product_count')} | {'affected_products': data['affected_products'][:5]}
    if tool in SQL_TOOLS:
        return {'sql': data['sql'], 'count': data['count'], 'truncated': data.get('truncated', False),
                'rows': data['rows'][:5]}
    rows_key = next(k for k in ('suppliers', 'products', 'order_lines') if k in data)
    return {'count': data['count'], rows_key: data[rows_key][:5]}


def build_graph(llm: Any, client: httpx.AsyncClient):
    """Compiles the graph for one request. `client` calls the API's business endpoints."""

    async def structured(schema, messages):
        return await llm.with_structured_output(schema, method='function_calling').ainvoke(messages)

    # ------------------------------------------------------------------ guard and router
    async def guard(state: State) -> State:
        decision = await structured(RouteDecision, [SystemMessage(prompts.ROUTER), HumanMessage(state['question'])])
        return {'route': decision.route, 'clarifying_question': decision.clarifying_question,
                'trace': [f'guard:{decision.route}']}

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

    def unavailable_answer(state: State) -> State:
        return {'final': _final(state, 'data_unavailable', 'The data service is unavailable right now, so I cannot '
                                'answer from the Gold data. Please try again later.', measures=[],
                                limitations=[state['unavailable']], quality=None),
                'trace': ['unavailable']}

    # ------------------------------------------------------------------ composer and evaluation gate
    async def composer(state: State) -> State:
        evidence = _ok_evidence(state)
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
        draft = await structured(Draft, [SystemMessage(prompts.COMPOSER), HumanMessage(content)])
        return {'draft': draft.model_dump(), 'revisions': revisions, 'feedback': [],
                'trace': ['composer:revision' if feedback else 'composer']}

    def checks(state: State) -> State:
        errors = deterministic_checks(state['draft'], _ok_evidence(state), state['question'])
        return {'feedback': errors, 'trace': ['checks:fail' if errors else 'checks:pass']}

    async def judge(state: State) -> State:
        content = (f"QUESTION:\n{state['question']}\n\nEVIDENCE:\n{evidence_json(_ok_evidence(state))}\n\n"
                   f"CANDIDATE ANSWER:\n{state['draft']['answer']}")
        try:
            judgment = await structured(Judgment, [SystemMessage(prompts.JUDGE), HumanMessage(content)])
        except Exception as exc:  # noqa: BLE001 - a failing judge must never let an unchecked answer through
            return {'judge_failed': True, 'trace': [f'judge:error({type(exc).__name__})']}
        if judgment_passed(judgment):
            return {'judgment': judgment.model_dump(), 'feedback': [], 'trace': ['judge:pass']}
        low = [f'{name} {getattr(judgment, name)} < {minimum}' for name, minimum in THRESHOLDS.items()
               if getattr(judgment, name) < minimum]
        return {'judgment': judgment.model_dump(), 'feedback': (judgment.issues or [judgment.reasoning]) + low,
                'trace': ['judge:fail']}

    def fallback(state: State) -> State:
        evidence = _ok_evidence(state)
        draft = {'status': 'answered' if evidence else 'no_data',
                 'answer': fallback_answer(evidence) if evidence else 'I could not retrieve any data for this question.',
                 'measures': sorted({e['tool'] for e in evidence}),
                 'limitations': ['Answer generated directly from the tool results after the quality check failed.']}
        return {'draft': draft, 'fallback_used': True, 'trace': ['fallback']}

    def finalize(state: State) -> State:
        draft, judgment = state['draft'], state.get('judgment')
        evidence = _ok_evidence(state)
        limitations = list(draft.get('limitations', []))
        if any(e['tool'] in EUR_TOOLS or (e['data'] or {}).get('unit') == 'EUR' for e in evidence):
            limitations.append(FX_LIMITATION)
        if any(e['tool'] in KG_TOOLS for e in evidence):
            limitations.append(KG_LIMITATION)
        if any(e['tool'] in SQL_TOOLS for e in evidence):
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
                       ('checks', checks), ('judge', judge), ('fallback', fallback), ('finalize', finalize)]:
        graph.add_node(name, node)
    for name in SPECIALIST_TOOLS:
        graph.add_node(name, make_specialist(name))

    graph.add_edge(START, 'guard')
    graph.add_conditional_edges('guard', lambda s: 'supervisor' if s['route'] == 'answer' else 'fixed_answer')
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
