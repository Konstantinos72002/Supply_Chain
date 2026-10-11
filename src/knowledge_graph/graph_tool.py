"""LangChain tool exposing the graph query to the assistant."""
from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path

from langchain_core.tools import tool
from pydantic import BaseModel, Field

from knowledge_graph.graph import DEFAULT_GRAPH_PATH, load_graph, supplier_alternatives


class SupplierAlternativesArgs(BaseModel):
    supplier_id: str = Field(
        ..., pattern=r"^SUP-\d{3,4}$",
        description="Supplier that may stop delivering, e.g. SUP-0009",
    )
    critical_only: bool = Field(
        True, description="Only return critical products (default true)",
    )


@lru_cache(maxsize=1)
def _graph():
    return load_graph(Path(os.getenv("KG_PATH", str(DEFAULT_GRAPH_PATH))))


@tool("supplier_alternatives_graph", args_schema=SupplierAlternativesArgs)
def supplier_alternatives_graph(supplier_id: str, critical_only: bool = True) -> str:
    """Knowledge-graph query: if a supplier stops delivering, list the affected
    (critical) products, their alternate suppliers and in-flight shipments.
    Use for 'what if supplier X fails / which products depend on X' questions."""
    try:
        result = supplier_alternatives(_graph(), supplier_id, critical_only)
    except FileNotFoundError:
        result = {"error": "NO_DATA", "detail": "knowledge graph file not built"}
    return json.dumps(result, default=str)
