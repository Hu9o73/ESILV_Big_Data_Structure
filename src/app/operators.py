from dataclasses import dataclass
from typing import List, Optional, Tuple

from app.constants import (
    READ_BW_BPS,
    NETWORK_LATENCY_S,
    CARBON_PER_GB,
    PRICE_PER_GB,
    KV_OVERHEAD_B,
    INT_B,
    Stats,
    Infra,
)
from app.schemas import DBLayout
from app.sizer import (
    collection_cardinality,
    doc_size_bytes,
    projection_size,
)


@dataclass(frozen=True)
class OperatorCost:
    name: str
    output_docs: float
    output_size_bytes: float
    scanned_bytes: float
    shards_touched: int
    time_s: float
    carbon_kg: float
    price_usd: float


def _costs_from_scan(scanned_bytes: float, shards_touched: int, parallelism: int) -> Tuple[float, float, float]:
    """
    Translate scanned volume + fan-out into coarse-grained time/carbon/price proxies.
    """
    time_s = scanned_bytes / max(parallelism, 1) / READ_BW_BPS
    time_s += shards_touched * NETWORK_LATENCY_S
    carbon = (scanned_bytes / (1024**3)) * CARBON_PER_GB
    price = (scanned_bytes / (1024**3)) * PRICE_PER_GB
    return time_s, carbon, price


def selectivity_for(collection: str, key: str, stats: Stats, value: Optional[str] = None) -> float:
    """
    Default selectivity assumptions (uniform) for the assignment's keys.
    """
    if key == "IDP":
        return 1 / stats.N_PRODUCTS
    if key == "IDW":
        return 1 / stats.N_WAREHOUSES
    if key == "IDC":
        return 1 / stats.N_CLIENTS
    if key == "brand":
        if value and value.lower() == "apple":
            return stats.N_APPLE_PRODUCTS / stats.N_PRODUCTS
        return 1 / stats.N_BRANDS
    if key == "date":
        return 1 / 365  # balanced over a year
    # Fallback: 1% selectivity
    return 0.01


def filter_operator(
    layout: DBLayout,
    stats: Stats,
    infra: Infra,
    collection: str,
    filter_key: str,
    selectivity: float,
    projected_fields: Optional[List[str]],
    sharded: bool,
    shard_aware: bool,
    indexed: bool,
) -> OperatorCost:
    total_docs = collection_cardinality(collection, stats)
    schema = layout.collections[collection]
    doc_sz = doc_size_bytes(schema)
    proj_sz = projection_size(schema, projected_fields or [])

    docs_per_shard = total_docs / infra.SERVERS if sharded else total_docs
    shards_touched = 1 if (sharded and shard_aware) else (infra.SERVERS if sharded else 1)
    scope_docs = docs_per_shard * shards_touched if sharded else total_docs

    raw_matched = total_docs * selectivity
    matched_docs = max(raw_matched, 1.0) if selectivity > 0 else 0.0

    docs_scanned = scope_docs if not indexed else scope_docs * selectivity
    docs_scanned = max(docs_scanned, matched_docs)  # at least the matches themselves
    docs_scanned = max(docs_scanned, 1.0) if selectivity > 0 else 0.0
    scanned_bytes = docs_scanned * doc_sz
    output_size = matched_docs * proj_sz

    parallelism = shards_touched if sharded else 1
    time_s, carbon, price = _costs_from_scan(scanned_bytes, shards_touched, parallelism)

    return OperatorCost(
        name="filter_sharded" if sharded else "filter_no_shard",
        output_docs=matched_docs,
        output_size_bytes=output_size,
        scanned_bytes=scanned_bytes,
        shards_touched=shards_touched,
        time_s=time_s,
        carbon_kg=carbon,
        price_usd=price,
    )


def _join_multiplicity(outer: str, inner: str, join_key: str, stats: Stats) -> int:
    """
    Simple fan-out heuristics derived from the assignment statistics.
    """
    if outer == "Stock" and inner == "Product" and join_key == "IDP":
        return 1  # one product for each stock line
    if outer == "Product" and inner == "Stock" and join_key == "IDP":
        return stats.N_WAREHOUSES  # every product tracked in all warehouses
    if outer == "OrderLine" and inner == "Product" and join_key == "IDP":
        return 1  # one product for each order line
    if outer == "Product" and inner == "OrderLine" and join_key == "IDP":
        return stats.N_ORDERLINES // stats.N_PRODUCTS
    return 1


def nested_loop_join(
    layout: DBLayout,
    stats: Stats,
    infra: Infra,
    outer_collection: str,
    inner_collection: str,
    join_key: str,
    outer_filter_key: str,
    outer_selectivity: float,
    outer_projected: Optional[List[str]],
    inner_projected: Optional[List[str]],
    sharded: bool,
    shard_aligned: bool,
) -> OperatorCost:
    """
    Basic nested loop: outer filtered first, then each outer row probes inner on the join key.
    """
    outer_cost = filter_operator(
        layout,
        stats,
        infra,
        outer_collection,
        outer_filter_key,
        outer_selectivity,
        outer_projected,
        sharded=sharded,
        shard_aware=shard_aligned,
        indexed=True,
    )

    outer_docs = outer_cost.output_docs
    matches_per_outer = _join_multiplicity(outer_collection, inner_collection, join_key, stats)
    inner_hits = outer_docs * matches_per_outer

    inner_schema = layout.collections[inner_collection]
    inner_doc_sz = doc_size_bytes(inner_schema)
    inner_proj_sz = projection_size(inner_schema, inner_projected or [])

    shards_touched = 1 if not sharded else (1 if shard_aligned else infra.SERVERS)
    scanned_bytes = outer_cost.scanned_bytes + inner_hits * inner_doc_sz
    output_size = inner_hits * (projection_size(layout.collections[outer_collection], outer_projected or []) + inner_proj_sz)

    parallelism = shards_touched if sharded else 1
    time_s, carbon, price = _costs_from_scan(scanned_bytes, shards_touched, parallelism)

    return OperatorCost(
        name="nested_loop_sharded" if sharded else "nested_loop_no_shard",
        output_docs=inner_hits,
        output_size_bytes=output_size,
        scanned_bytes=scanned_bytes,
        shards_touched=shards_touched,
        time_s=time_s,
        carbon_kg=carbon,
        price_usd=price,
    )


def _distinct_for_group(collection: str, group_keys: List[str], stats: Stats, filtered_docs: float, filter_key: Optional[str]) -> float:
    # Tailored to the aggregate queries (Q6, Q7).
    if collection == "OrderLine" and group_keys == ["IDP"]:
        if filter_key == "IDC":
            return stats.AVG_PRODUCTS_PER_CLIENT
        return stats.N_PRODUCTS
    if collection == "OrderLine" and group_keys == ["IDC"]:
        return stats.N_CLIENTS
    # fallback: pessimistic "all docs land in unique groups"
    return filtered_docs


def aggregate_operator(
    layout: DBLayout,
    stats: Stats,
    infra: Infra,
    collection: str,
    group_keys: List[str],
    projected_fields: Optional[List[str]],
    sharded: bool,
    shard_aligned: bool,
    filter_key: Optional[str] = None,
    filter_selectivity: float = 1.0,
) -> OperatorCost:
    total_docs = collection_cardinality(collection, stats)
    schema = layout.collections[collection]
    doc_sz = doc_size_bytes(schema)
    proj_sz = projection_size(schema, projected_fields or group_keys)

    input_docs = total_docs * filter_selectivity
    shards_touched = infra.SERVERS if sharded else 1

    scanned_bytes = input_docs * doc_sz
    parallelism = shards_touched if sharded else 1
    latency_scope = 1 if (sharded and shard_aligned) else shards_touched
    time_s, carbon, price = _costs_from_scan(scanned_bytes, latency_scope if sharded else 1, parallelism)

    distinct_groups = _distinct_for_group(collection, group_keys, stats, input_docs, filter_key)
    # Add an aggregate metric (sum) per row
    agg_payload = KV_OVERHEAD_B + INT_B
    output_size = distinct_groups * (proj_sz + agg_payload)

    return OperatorCost(
        name="aggregate_sharded" if sharded else "aggregate_no_shard",
        output_docs=distinct_groups,
        output_size_bytes=output_size,
        scanned_bytes=scanned_bytes,
        shards_touched=shards_touched if sharded else 1,
        time_s=time_s,
        carbon_kg=carbon,
        price_usd=price,
    )
