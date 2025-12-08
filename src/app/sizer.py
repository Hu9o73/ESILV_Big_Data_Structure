from typing import Dict, Any, Tuple
from app.constants import (
    INT_B, NUMBER_B, STRING_B, DATE_B, LONGSTRING_B, KV_OVERHEAD_B,
    Stats,
)


# Map primitive "types" to byte sizes
PRIM_SIZE = {
    "int": INT_B,
    "number": NUMBER_B,
    "string": STRING_B,
    "longstring": LONGSTRING_B,
    "date": DATE_B,
}


def _size_of_schema(schema: Dict[str, Any]) -> int:
    """
    Recursively compute average document size (in bytes)
    for the given schema representation.
    """
    typ = schema["type"]

    if typ in PRIM_SIZE:
        return KV_OVERHEAD_B + PRIM_SIZE[typ]

    if typ == "object":
        total = 0
        for k, sub in schema["fields"].items():
            total += _size_of_schema(sub)
        return total

    if typ == "array":
        # Arrays: 12B + sum(items)
        avg_len = schema.get("avg_len", 0)
        item_size = _size_of_schema(schema["items"])
        return KV_OVERHEAD_B + avg_len * item_size

    # Security but shouldn't happen given the current state of my work
    # Guard for later on (maybe ...)
    raise ValueError(f"Unknown schema type: {typ}")


def pretty_gb(b: int) -> float:
    return round(b / (1024**3), 3)


def collection_cardinality(name: str, stats: Stats) -> int:
    """
    How many documents per collection (normalized base cardinalities).
    Denormalization affects schema shape, not the *count* of top-level docs
    (except cases where a normalized collection is removed/embedded).
    """
    if name == "Product":
        return stats.N_PRODUCTS
    if name == "Stock":
        return stats.N_STOCK
    if name == "Warehouse":
        return stats.N_WAREHOUSES
    if name == "OrderLine":
        return stats.N_ORDERLINES
    if name == "Client":
        return stats.N_CLIENTS
    # Fallback (should not happen with our DB1-DB5)
    return 0


def db_sizes(layout: Dict[str, Dict[str, Any]], stats: Stats) -> Tuple[int, Dict[str, int], Dict[str, int]]:
    """
    Returns:
      total_bytes, doc_size_bytes_by_collection, coll_bytes_by_collection
    """
    doc_sizes = {}
    coll_sizes = {}
    total = 0
    for coll, schema in layout.items():
        dsz = _size_of_schema(schema)
        card = collection_cardinality(coll, stats)
        csz = dsz * card
        doc_sizes[coll] = dsz
        coll_sizes[coll] = csz
        total += csz
    return total, doc_sizes, coll_sizes


def doc_size_bytes(schema: Dict[str, Any]) -> int:
    """
    Public helper that wraps the internal sizing routine so operators can reuse it.
    """
    return _size_of_schema(schema)


def projection_size(schema: Dict[str, Any], fields) -> int:
    """
    Approximate the size of a projected document by summing requested top-level fields.
    Unknown fields are ignored deliberately.
    """
    if schema["type"] != "object":
        return doc_size_bytes(schema)
    total = 0
    for f in fields:
        sub = schema["fields"].get(f)
        if sub:
            total += doc_size_bytes(sub)
    return total if total > 0 else doc_size_bytes(schema)
