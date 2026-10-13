"""Structured outputs of the LLM nodes and the response of POST /assistant/query."""
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

Specialist = Literal['kpi_analyst', 'supplier_analyst', 'inventory_analyst', 'network_analyst', 'sql_analyst']


class AssistantQuery(BaseModel):
    model_config = ConfigDict(extra='forbid')
    question: str = Field(min_length=3, max_length=1000, description='Business question in natural language.')


class RouteDecision(BaseModel):
    route: Literal['answer', 'clarify', 'out_of_scope'] = Field(
        description="'answer' = in scope and answerable with the tools; 'clarify' = in scope but too vague to pick "
                    "any measure; 'out_of_scope' = unrelated, an action request, or confidential raw data.")
    question_type: Literal['lookup', 'analysis'] = Field(
        'analysis', description="Only for 'answer': 'lookup' = attributes of a named entity, a filtered list, a "
                                "count or a sample from the Gold tables; 'analysis' = KPIs, rankings, exposure, "
                                "delays or supplier what-ifs.")
    reason: str = Field(description='One sentence explaining the decision.')
    clarifying_question: str | None = Field(None, description="Only for 'clarify': one short question to the user.")


class SupervisorDecision(BaseModel):
    next: Literal['kpi_analyst', 'supplier_analyst', 'inventory_analyst', 'network_analyst', 'sql_analyst',
                  'finish'] = Field(
        description='Specialist to run next, or finish when the findings answer every part of the question.')
    task: str = Field('', description='Precise instruction for the specialist; empty when finishing.')


class SqlDraft(BaseModel):
    sql: str = Field(description='One read-only SELECT (or WITH ... SELECT) on the Gold tables.')
    kind: Literal['single', 'list', 'count', 'sample'] = Field(
        description="'single' = attributes of one entity, 'list' = filtered rows, 'count' = a number, "
                    "'sample' = random row(s).")


class Draft(BaseModel):
    status: Literal['answered', 'no_data'] = Field(description="'no_data' when the tools returned no rows.")
    answer: str = Field(description='Answer in plain business English, using only numbers from the evidence.')
    measures: list[str] = Field(description='Business brief KPIs or tools the answer is based on.')
    limitations: list[str] = Field(default_factory=list, description='Caveats the user must know.')


class Judgment(BaseModel):
    correctness: int = Field(ge=0, le=100, description='Numbers, entities, units and dates match the evidence')
    groundedness: int = Field(ge=0, le=100, description='Every claim is supported by the tool evidence')
    faithfulness: int = Field(ge=0, le=100, description='No hallucination; caveats such as the as-of date are kept')
    relevance: int = Field(ge=0, le=100, description='Answers the question asked (measure, filters, period)')
    completeness: int = Field(ge=0, le=100, description='Covers every part of the question')
    clarity: int = Field(ge=0, le=100, description='Plain business language with units')
    issues: list[str] = Field(description='Concrete problems to fix; empty if none')
    reasoning: str = Field(description='One sentence explaining the scores')


THRESHOLDS = {'correctness': 80, 'groundedness': 80, 'faithfulness': 80,
              'relevance': 70, 'completeness': 60, 'clarity': 60}


def judgment_passed(judgment: Judgment) -> bool:
    """Decided in code from the scores, never by the judge itself."""
    return all(getattr(judgment, name) >= minimum for name, minimum in THRESHOLDS.items())


class Evidence(BaseModel):
    tool: str
    arguments: dict[str, Any]
    status: Literal['ok', 'error']
    result_summary: dict[str, Any]


class Quality(BaseModel):
    scores: dict[str, int] | None = Field(description='LLM judge scores (0-100); null when the judge did not run.')
    passed: bool = Field(description='True when the answer passed the deterministic checks and the judge.')
    revised: bool = Field(description='True when the answer was revised once after a failed check.')
    fallback_used: bool = Field(description='True when the answer was built directly from tool results.')


class AssistantAnswer(BaseModel):
    status: Literal['answered', 'clarification_needed', 'out_of_scope', 'no_data', 'data_unavailable']
    answer: str
    evidence: list[Evidence]
    filters: dict[str, Any]
    measures: list[str]
    as_of_date: str | None
    limitations: list[str]
    quality: Quality | None
    trace: list[str] = Field(description='Execution path: graph nodes and tool calls, in order.')
