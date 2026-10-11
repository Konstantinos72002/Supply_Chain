"""Bonus (5%): supply-chain knowledge graph built from the Gold tables
(rows fetched by knowledge_graph.databricks_source).

Nodes : Supplier, Product, Location, Shipment
Edges : Supplier -SUPPLIES-> Product
        Product  -ALTERNATE_SUPPLIER-> Supplier
        Shipment -FOR_PRODUCT-> Product
        Shipment -DELIVERS_TO-> Location
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterable

import networkx as nx

logger = logging.getLogger(__name__)

DEFAULT_GRAPH_PATH = Path("data/graph/supply_graph.json")


def _sid(x: str) -> str:
    return f"supplier:{x}"


def _pid(x: str) -> str:
    return f"product:{x}"


def _lid(x: str) -> str:
    return f"location:{x}"


def _shid(x: str) -> str:
    return f"shipment:{x}"


def build_graph(
    suppliers: Iterable[dict],
    products: Iterable[dict],
    shipments: Iterable[dict],
    locations: Iterable[dict] = (),
) -> nx.MultiDiGraph:
    """Build the graph from rows of the Gold tables (business keys, not _sk).

    suppliers : supplier_id, supplier_name, is_active, risk_rating
    products  : product_id, product_name, criticality, is_critical
                (criticality = 'CRITICAL'), primary_supplier_id, alternate_supplier_id
    shipments : shipment_id, product_id, location_id, status_code
                (open shipments, latest status; location = PO line destination)
    locations : location_id, location_name, location_country, location_type
    """
    g = nx.MultiDiGraph()

    for s in suppliers:
        g.add_node(_sid(s["supplier_id"]), type="Supplier", **s)

    for loc in locations:
        g.add_node(_lid(loc["location_id"]), type="Location", **loc)

    for p in products:
        pid = _pid(p["product_id"])
        g.add_node(pid, type="Product", **p)
        if p.get("primary_supplier_id"):
            g.add_edge(_sid(p["primary_supplier_id"]), pid, key="SUPPLIES")
        if p.get("alternate_supplier_id"):
            g.add_edge(pid, _sid(p["alternate_supplier_id"]), key="ALTERNATE_SUPPLIER")

    for sh in shipments:
        shid = _shid(sh["shipment_id"])
        g.add_node(shid, type="Shipment", shipment_id=sh["shipment_id"],
                   status_code=sh.get("status_code"))
        if sh.get("product_id"):
            g.add_edge(shid, _pid(sh["product_id"]), key="FOR_PRODUCT")
        if sh.get("location_id"):
            lid = _lid(sh["location_id"])
            if lid not in g:
                g.add_node(lid, type="Location", location_id=sh["location_id"])
            g.add_edge(shid, lid, key="DELIVERS_TO")

    # Unknown FK targets (e.g. -1 members) become bare nodes; tag them.
    for n, data in g.nodes(data=True):
        data.setdefault("type", "Unknown")

    logger.info("knowledge graph built: %d nodes, %d edges",
                g.number_of_nodes(), g.number_of_edges())
    return g


def save_graph(g: nx.MultiDiGraph, path: Path = DEFAULT_GRAPH_PATH) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = nx.node_link_data(g, edges="edges")
    path.write_text(json.dumps(data, default=str, indent=2), encoding="utf-8")
    return path


def load_graph(path: Path = DEFAULT_GRAPH_PATH) -> nx.MultiDiGraph:
    data = json.loads(path.read_text(encoding="utf-8"))
    return nx.node_link_graph(data, directed=True, multigraph=True, edges="edges")


def _edges(g, node, key, out=True):
    edges = g.out_edges(node, keys=True) if out else g.in_edges(node, keys=True)
    return [(u, v) for u, v, k in edges if k == key]


def supplier_alternatives(g: nx.MultiDiGraph, supplier_id: str,
                          critical_only: bool = True) -> dict:
    """If `supplier_id` stops, which products are hit and who can replace it?"""
    node = _sid(supplier_id)
    if node not in g or g.nodes[node].get("type") != "Supplier":
        return {"error": "NOT_FOUND", "supplier_id": supplier_id}

    affected = []
    for _, prod in _edges(g, node, "SUPPLIES"):
        p = g.nodes[prod]
        if critical_only and not p.get("is_critical"):
            continue

        alternates = []
        for _, alt in _edges(g, prod, "ALTERNATE_SUPPLIER"):
            a = g.nodes[alt]
            if alt == node:
                continue  # alternate is the failing supplier itself
            alternates.append({
                "supplier_id": a.get("supplier_id", alt.split(":", 1)[1]),
                "supplier_name": a.get("supplier_name"),
                "is_active": bool(a.get("is_active", False)),
            })

        open_shipments = []
        for shp, _ in _edges(g, prod, "FOR_PRODUCT", out=False):
            s = g.nodes[shp]
            dest = [g.nodes[v].get("location_id") for _, v in _edges(g, shp, "DELIVERS_TO")]
            open_shipments.append({"shipment_id": s.get("shipment_id"),
                                   "status_code": s.get("status_code"),
                                   "delivers_to": dest[0] if dest else None})

        usable = [a for a in alternates if a["is_active"]]
        affected.append({
            "product_id": p.get("product_id"),
            "product_name": p.get("product_name"),
            "is_critical": bool(p.get("is_critical")),
            "alternate_suppliers": alternates,
            "has_usable_alternate": bool(usable),
            "shipments": open_shipments,
        })

    # Uncovered products first: they are the ones to act on.
    affected.sort(key=lambda r: (r["has_usable_alternate"], r["product_id"] or ""))
    return {
        "supplier_id": supplier_id,
        "critical_only": critical_only,
        "affected_product_count": len(affected),
        "uncovered_product_count": sum(not r["has_usable_alternate"] for r in affected),
        "affected_products": affected,
        "source": "knowledge graph built from gold_dim_supplier, gold_dim_product, "
                  "gold_dim_location, gold_fact_shipment_event, gold_fact_purchase_order_line",
    }
