import json

import pytest
from pydantic import ValidationError

from knowledge_graph.graph import build_graph, load_graph, save_graph, supplier_alternatives

SUPPLIERS = [
    {"supplier_id": "SUP-017", "supplier_name": "Acme", "is_active": True},
    {"supplier_id": "SUP-023", "supplier_name": "Beta", "is_active": True},
    {"supplier_id": "SUP-031", "supplier_name": "Gamma", "is_active": False},
]
PRODUCTS = [
    {"product_id": "PRD-0042", "product_name": "Valve", "is_critical": True,
     "primary_supplier_id": "SUP-017", "alternate_supplier_id": "SUP-023"},
    {"product_id": "PRD-0117", "product_name": "Pump", "is_critical": True,
     "primary_supplier_id": "SUP-017", "alternate_supplier_id": None},
    {"product_id": "PRD-0150", "product_name": "Seal", "is_critical": True,
     "primary_supplier_id": "SUP-017", "alternate_supplier_id": "SUP-031"},
    {"product_id": "PRD-0203", "product_name": "Bolt", "is_critical": False,
     "primary_supplier_id": "SUP-017", "alternate_supplier_id": "SUP-023"},
]
SHIPMENTS = [{"shipment_id": "SHP-00881", "product_id": "PRD-0117",
              "location_id": "WH-ATH", "status_code": "DELAYED"}]


@pytest.fixture
def g():
    return build_graph(SUPPLIERS, PRODUCTS, SHIPMENTS)


def test_edges_built(g):
    assert g.has_edge("supplier:SUP-017", "product:PRD-0042", key="SUPPLIES")
    assert g.has_edge("product:PRD-0042", "supplier:SUP-023", key="ALTERNATE_SUPPLIER")
    assert g.has_edge("shipment:SHP-00881", "product:PRD-0117", key="FOR_PRODUCT")
    assert g.has_edge("shipment:SHP-00881", "location:WH-ATH", key="DELIVERS_TO")


def test_demo_question(g):
    r = supplier_alternatives(g, "SUP-017")
    ids = [p["product_id"] for p in r["affected_products"]]
    assert "PRD-0203" not in ids                    # not critical
    assert r["supplier_name"] == "Acme" and r["supplier_is_active"] is True
    assert r["affected_product_count"] == 3
    assert r["uncovered_product_count"] == 2       # PRD-0117 none, PRD-0150 inactive alt
    first = r["affected_products"][0]
    assert first["has_usable_alternate"] is False   # uncovered sorted first


def test_shipment_context(g):
    r = supplier_alternatives(g, "SUP-017")
    pump = next(p for p in r["affected_products"] if p["product_id"] == "PRD-0117")
    assert pump["shipments"][0]["delivers_to"] == "WH-ATH"


def test_include_non_critical(g):
    assert supplier_alternatives(g, "SUP-017", critical_only=False)["affected_product_count"] == 4


def test_unknown_supplier(g):
    assert supplier_alternatives(g, "SUP-999") == {"error": "NOT_FOUND", "supplier_id": "SUP-999"}


def test_json_roundtrip(g, tmp_path):
    p = save_graph(g, tmp_path / "kg.json")
    g2 = load_graph(p)
    assert supplier_alternatives(g2, "SUP-017") == supplier_alternatives(g, "SUP-017")


def test_tool(g, tmp_path, monkeypatch):
    save_graph(g, tmp_path / "kg.json")
    monkeypatch.setenv("KG_PATH", str(tmp_path / "kg.json"))
    from knowledge_graph import graph_tool
    graph_tool._graph.cache_clear()
    out = json.loads(graph_tool.supplier_alternatives_graph.invoke({"supplier_id": "SUP-017"}))
    assert out["affected_product_count"] == 3
    with pytest.raises(ValidationError):
        graph_tool.supplier_alternatives_graph.invoke({"supplier_id": "acme"})
