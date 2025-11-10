from dataclasses import dataclass
from typing import Tuple
from app.constants import Stats, Infra


@dataclass(frozen=True)
class ShardReport:
    collection: str
    strategy: str
    docs_per_server: float
    distinct_values_per_server: float


def _avg_docs_per_server(total_docs: int, servers: int) -> float:
    return total_docs / servers


def _avg_distinct_per_server(distinct_values: int, servers: int) -> float:
    return distinct_values / servers


def st_by_idp(stats: Stats, infra: Infra) -> ShardReport:
    # Stock sharded by product ID
    total_docs = stats.N_STOCK
    distinct_idp = stats.N_PRODUCTS
    return ShardReport(
        collection="Stock",
        strategy="St - #IDP",
        docs_per_server=_avg_docs_per_server(total_docs, infra.SERVERS),
        distinct_values_per_server=_avg_distinct_per_server(distinct_idp, infra.SERVERS),
    )


def st_by_idw(stats: Stats, infra: Infra) -> ShardReport:
    # Stock sharded by warehouse ID
    total_docs = stats.N_STOCK
    distinct_idw = stats.N_WAREHOUSES
    return ShardReport(
        collection="Stock",
        strategy="St - #IDW",
        docs_per_server=_avg_docs_per_server(total_docs, infra.SERVERS),
        distinct_values_per_server=_avg_distinct_per_server(distinct_idw, infra.SERVERS),
    )


def ol_by_idc(stats: Stats, infra: Infra) -> ShardReport:
    total_docs = stats.N_ORDERLINES
    distinct_idc = stats.N_CLIENTS
    return ShardReport(
        collection="OrderLine",
        strategy="OL - #IDC",
        docs_per_server=_avg_docs_per_server(total_docs, infra.SERVERS),
        distinct_values_per_server=_avg_distinct_per_server(distinct_idc, infra.SERVERS),
    )


def ol_by_idp(stats: Stats, infra: Infra) -> ShardReport:
    total_docs = stats.N_ORDERLINES
    distinct_idp = stats.N_PRODUCTS
    return ShardReport(
        collection="OrderLine",
        strategy="OL - #IDP",
        docs_per_server=_avg_docs_per_server(total_docs, infra.SERVERS),
        distinct_values_per_server=_avg_distinct_per_server(distinct_idp, infra.SERVERS),
    )


def prod_by_idp(stats: Stats, infra: Infra) -> ShardReport:
    total_docs = stats.N_PRODUCTS
    distinct_idp = stats.N_PRODUCTS
    return ShardReport(
        collection="Product",
        strategy="Prod - #IDP",
        docs_per_server=_avg_docs_per_server(total_docs, infra.SERVERS),
        distinct_values_per_server=_avg_distinct_per_server(distinct_idp, infra.SERVERS),
    )


def prod_by_brand(stats: Stats, infra: Infra) -> ShardReport:
    total_docs = stats.N_PRODUCTS
    distinct_brands = stats.N_BRANDS
    return ShardReport(
        collection="Product",
        strategy="Prod - #brand",
        docs_per_server=_avg_docs_per_server(total_docs, infra.SERVERS),
        distinct_values_per_server=_avg_distinct_per_server(distinct_brands, infra.SERVERS),
    )


def all_sharding_reports(stats: Stats, infra: Infra):
    return [
        st_by_idp(stats, infra),
        st_by_idw(stats, infra),
        ol_by_idc(stats, infra),
        ol_by_idp(stats, infra),
        prod_by_idp(stats, infra),
        prod_by_brand(stats, infra),
    ]
