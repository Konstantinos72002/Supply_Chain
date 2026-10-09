"""Evaluation gate before an answer is returned.

Stage 1 (deterministic, free): every number in the answer must come from the tool evidence.
Stage 2 (LLM judge): rubric scores; pass/fail is decided in code from THRESHOLDS (see schemas.py).
If both stages keep failing, a safe fallback answer is built directly from the evidence.
"""
import json
import re
from typing import Any

ID_OR_DATE = re.compile(r'\b[A-Za-z]+-[A-Za-z0-9-]+\b|\b\d{4}-\d{2}(?:-\d{2})?\b')
NUMBER = re.compile(r'(?<![\w.])\d[\d,]*(?:\.\d+)?')
SCALES = (1, 100, 1e-3, 1e-6)          # as is, ratio -> percent, thousands, millions


def numeric_values(data: Any) -> list[float]:
    """All numbers found in a JSON-like structure."""
    if isinstance(data, bool):
        return []
    if isinstance(data, (int, float)):
        return [float(data)]
    if isinstance(data, dict):
        return [v for item in data.values() for v in numeric_values(item)]
    if isinstance(data, list):
        return [v for item in data for v in numeric_values(item)]
    return []


def _is_grounded(token: str, evidence_values: list[float]) -> bool:
    value = float(token.replace(',', ''))
    decimals = len(token.split('.')[1]) if '.' in token else 0
    tolerance = 0.5 * 10 ** -decimals + 1e-9
    return any(abs(v * scale - value) <= tolerance for v in evidence_values for scale in SCALES)


def ungrounded_numbers(answer: str, evidence: list[dict[str, Any]], question: str) -> list[str]:
    """Numbers in the answer that cannot be traced to the evidence.

    Ignored: IDs (SUP-0007, PO-2025-00208), ISO dates, numbers taken from the question, years, and small
    integers 0-12 (list positions, counts of listed items, months)."""
    values = [v for item in evidence for v in numeric_values(item.get('data'))]
    in_question = set(NUMBER.findall(ID_OR_DATE.sub(' ', question)))
    problems = []
    for token in NUMBER.findall(ID_OR_DATE.sub(' ', answer)):
        plain = token.replace(',', '')
        if token in in_question or (plain.isdigit() and (int(plain) <= 12 or 2000 <= int(plain) <= 2100)):
            continue
        if not _is_grounded(token, values):
            problems.append(token)
    return problems


def deterministic_checks(draft: dict[str, Any], evidence: list[dict[str, Any]], question: str) -> list[str]:
    errors = []
    if draft['status'] == 'answered' and not evidence:
        errors.append("Status is 'answered' but no tool returned data.")
    bad = ungrounded_numbers(draft['answer'], evidence, question)
    if bad:
        errors.append(f"These numbers are not in the evidence: {', '.join(bad)}. Use only values the tools returned.")
    return errors


def compact(data: Any, max_items: int = 25) -> Any:
    """Shortens long result lists for prompts (the checks always use the full data)."""
    if isinstance(data, dict):
        return {k: compact(v, max_items) for k, v in data.items()}
    if isinstance(data, list) and len(data) > max_items:
        return [compact(v, max_items) for v in data[:max_items]] + [f'... {len(data) - max_items} more rows']
    if isinstance(data, list):
        return [compact(v, max_items) for v in data]
    return data


def evidence_json(evidence: list[dict[str, Any]]) -> str:
    return json.dumps([{'tool': e['tool'], 'arguments': e['arguments'], 'result': compact(e['data'])}
                       for e in evidence], default=str, indent=1)


# --------------------------------------------------------------------------- safe fallback
KEY_FIELDS = {
    'suppliers': ['supplier_id', 'supplier_name', 'on_time_delivery_rate', 'supplier_fill_rate', 'average_delay_days'],
    'products': ['product_id', 'product_name', 'criticality', 'days_of_cover', 'days_until_arrival',
                 'exposed_value_eur'],
    'order_lines': ['po_id', 'po_line_number', 'product_id', 'latest_eta_date', 'delayed_qty'],
}


def fallback_answer(evidence: list[dict[str, Any]]) -> str:
    """Answer built only from tool results, used when generated answers fail the evaluation gate."""
    lines = []
    for e in evidence:
        data = e['data']
        if e['tool'] == 'query_kpi':
            values = ', '.join(f"{r['group']}: {r['value']}" for r in data['results'])
            lines.append(f"{data['kpi']} ({data['unit']}): {values}")
            continue
        key = next(k for k in KEY_FIELDS if k in data)
        rows = data[key][:5]
        lines.append(f"{e['tool']}: {data['count']} rows returned; first {len(rows)}:")
        lines += ['  - ' + ', '.join(f'{f}={r.get(f)}' for f in KEY_FIELDS[key]) for r in rows]
    return 'Results retrieved for your question:\n' + '\n'.join(lines)
