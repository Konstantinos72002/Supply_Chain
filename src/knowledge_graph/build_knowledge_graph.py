"""Bonus: build the supply-chain knowledge graph from the Databricks Gold tables.

Reads Gold (business keys only), builds the networkx graph and saves it as JSON.
Re-running overwrites the file (idempotent).

Locally (reads .env, queries the SQL warehouse):
    uv run python -m knowledge_graph.build_knowledge_graph [SUPPLIER_ID]
In a Databricks notebook/job the active Spark session is used instead;
set KG_PATH to a Volume path, e.g. /Volumes/data2026acc123456/team4/artifacts/supply_graph.json.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

from knowledge_graph.databricks_source import fetch_gold_rows
from knowledge_graph.graph import DEFAULT_GRAPH_PATH, build_graph, save_graph, supplier_alternatives


def main(argv: list[str]) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("databricks").setLevel(logging.WARNING)
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    out = Path(os.getenv("KG_PATH", str(DEFAULT_GRAPH_PATH)))
    rows = fetch_gold_rows()
    g = build_graph(rows["suppliers"], rows["products"], rows["shipments"], rows["locations"])
    save_graph(g, out)
    print(f"Saved {g.number_of_nodes()} nodes / {g.number_of_edges()} edges to {out}")

    # Smoke test: the demo question.
    supplier_id = argv[0] if argv else "SUP-0009"
    print(json.dumps(supplier_alternatives(g, supplier_id), indent=2, default=str))


if __name__ == "__main__":
    main(sys.argv[1:])
