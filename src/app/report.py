from typing import Dict, List
from pathlib import Path
from app.constants import Stats, Averages, Infra
from app.schemas import all_layouts
from app.sizer import db_sizes, pretty_gb
from app.sharding import all_sharding_reports
from app.operators import (
    selectivity_for,
    filter_operator,
    nested_loop_join,
    aggregate_operator,
)

def print_header(title: str):
    print("=" * 80)
    print(title)
    print("=" * 80)

def format_bytes(b: float) -> str:
    """
    Pretty-print bytes with adaptive units so tiny values do not round to 0.000 GB.
    """
    if b >= 1024 ** 3:
        return f"{b / (1024**3):,.3f} GB"
    if b >= 1024 ** 2:
        return f"{b / (1024**2):,.3f} MB"
    if b >= 1024:
        return f"{b / 1024:,.3f} KB"
    return f"{b:,.0f} B"

def format_price(value: float) -> str:
    # Always show small prices with higher precision so values under $0.01 don't print as zero.
    if value >= 1:
        return f"${value:,.2f}"
    return f"${value:,.6f}"

def run_all_reports():
    stats = Stats()
    avgs = Averages()
    infra = Infra()

    # Prepare run output capture
    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)
    existing: List[Path] = sorted(p for p in output_dir.glob("run_*.txt") if p.is_file())
    run_id = 0
    if existing:
        last = existing[-1].stem.split("_")[-1]
        try:
            run_id = int(last) + 1
        except ValueError:
            run_id = len(existing)
    run_path = output_dir / f"run_{run_id}.txt"

    lines: List[str] = []

    def emit(msg: str = ""):
        lines.append(msg)
        print(msg)

    def header(msg: str):
        emit("=" * 80)
        emit(msg)
        emit("=" * 80)

    def subtitle(title: str):
        emit("-" * 80)
        emit(title)
        emit("-" * 80)

    # Denormalizations & Sizes
    header("STEP 2.3–2.5: Denormalizations, Document Sizes, Collection Sizes, DB Sizes")
    layouts = all_layouts(avgs)

    for dbname, layout in layouts.items():
        total_b, doc_sizes, coll_sizes = db_sizes(layout.collections, stats)

        emit(f"\n{dbname}")
        emit("-" * 80)
        emit("Average Document Size (bytes):")
        for coll, dsz in doc_sizes.items():
            emit(f"\t{coll:12s} : {dsz:,} B")

        emit("\nCollection Sizes (GB):")
        for coll, csz in coll_sizes.items():
            emit(f"\t{coll:12s} : {pretty_gb(csz):,.3f} GB")

        emit(f"\nTotal Database Size (GB): {pretty_gb(total_b):,.3f} GB")

        # 2.5.4 short qualitative note
        emit("\nDenormalization tradeoffs (quick notes):")
        if dbname == "DB1":
            emit("\t- Joins still needed for Stock/OrderLine searches; Product hot, but Stock separate.")
        elif dbname == "DB2":
            emit("\t- Big Product docs (embedded 200 Stock entries/product). Faster per-product stock reads; heavier writes.")
        elif dbname == "DB3":
            emit("\t- Stock docs heavy (embedded Product). Great for stock-centric access; duplicates product across 20M docs.")
        elif dbname == "DB4":
            emit("\t- OrderLine embeds Product snapshot; speeds brand/date + OL joins, but duplicates Product across 4B OL.")
        elif dbname == "DB5":
            emit("\t- Product embeds ~40k OrderLines each (!); huge Product docs, painful updates; great for product-centric OL scans.")

    # Sharding stats
    header("STEP 2.6: Sharding Strategies - Averages per Server")
    for rep in all_sharding_reports(stats, infra):
        emit(f"{rep.strategy:12s} | Coll={rep.collection:10s} | "
             f"docs/server={rep.docs_per_server:,.1f} | "
             f"distinct-values/server={rep.distinct_values_per_server:,.3f}")

    # Homework 3.3: Filters & Nested Loop operators (demonstrated on DB1)
    header("STEP 3.3 & 4.2: Operator costs per DB (Q1–Q7)")

    def print_cost(dbname: str, cost):
        emit(f"[{dbname}] out={cost.output_docs:,.1f} docs ({format_bytes(cost.output_size_bytes)}) | "
             f"scan={format_bytes(cost.scanned_bytes)} | shards={cost.shards_touched} | "
             f"time={cost.time_s:.3f}s | carbon={cost.carbon_kg:.3f}kg | price={format_price(cost.price_usd)}")

    # Query builders returning OperatorCost or a string for N/A
    def q1(layout):
        if "Stock" not in layout.collections:
            return "N/A (Stock not in layout)"
        sel_q1 = selectivity_for("Stock", "IDP", stats) * selectivity_for("Stock", "IDW", stats)
        return filter_operator(
            layout, stats, infra,
            collection="Stock",
            filter_key="IDP+IDW",
            selectivity=sel_q1,
            projected_fields=["quantity", "location"],
            sharded=True,
            shard_aware=True,
            indexed=True,
        )

    def q2(layout):
        if "Product" not in layout.collections:
            return "N/A (Product not in layout)"
        sel_q2 = selectivity_for("Product", "brand", stats, value="Apple")
        return filter_operator(
            layout, stats, infra,
            collection="Product",
            filter_key="brand",
            selectivity=sel_q2,
            projected_fields=["name", "price"],
            sharded=False,
            shard_aware=False,
            indexed=True,
        )

    def q3(layout):
        if "OrderLine" not in layout.collections:
            return "N/A (OrderLine not in layout)"
        sel_q3 = selectivity_for("OrderLine", "date", stats)
        return filter_operator(
            layout, stats, infra,
            collection="OrderLine",
            filter_key="date",
            selectivity=sel_q3,
            projected_fields=["IDP", "quantity"],
            sharded=True,
            shard_aware=False,  # date not a shard key -> all shards
            indexed=True,
        )

    def q4(layout):
        if "Stock" not in layout.collections or "Product" not in layout.collections:
            return "N/A (missing Stock/Product)"
        sel_q4 = selectivity_for("Stock", "IDW", stats)
        return nested_loop_join(
            layout, stats, infra,
            outer_collection="Stock",
            inner_collection="Product",
            join_key="IDP",
            outer_filter_key="IDW",
            outer_selectivity=sel_q4,
            outer_projected=["IDP", "quantity"],
            inner_projected=["name"],
            sharded=True,
            shard_aligned=False,
        )

    def q5(layout):
        if "Product" not in layout.collections or "Stock" not in layout.collections:
            return "N/A (missing Product/Stock)"
        sel_q5 = selectivity_for("Product", "brand", stats, value="Apple")
        return nested_loop_join(
            layout, stats, infra,
            outer_collection="Product",
            inner_collection="Stock",
            join_key="IDP",
            outer_filter_key="brand",
            outer_selectivity=sel_q5,
            outer_projected=["name", "price"],
            inner_projected=["IDW", "quantity"],
            sharded=False,
            shard_aligned=False,
        )

    def q6(layout):
        if "OrderLine" not in layout.collections:
            return "N/A (OrderLine not in layout)"
        return aggregate_operator(
            layout, stats, infra,
            collection="OrderLine",
            group_keys=["IDP"],
            projected_fields=["IDP"],
            sharded=True,
            shard_aligned=True,
        )

    def q7(layout):
        if "OrderLine" not in layout.collections:
            return "N/A (OrderLine not in layout)"
        sel_q7 = selectivity_for("OrderLine", "IDC", stats)
        return aggregate_operator(
            layout, stats, infra,
            collection="OrderLine",
            group_keys=["IDP"],
            projected_fields=["IDP"],
            sharded=False,
            shard_aligned=False,
            filter_key="IDC",
            filter_selectivity=sel_q7,
        )

    query_plan = [
        ("Q1 filter (Stock by IDP & IDW, sharded)", q1, "All stock-containing layouts behave similarly; embedding product/OL doesn't change this point lookup."),
        ("Q2 filter (Apple products, no sharding)", q2, "DB1/DB2 are light; DB5 is slow/expensive because Product embeds huge OL arrays. Avoid DB5 for brand filters."),
        ("Q3 filter (OrderLine by date, sharded)", q3, "DB1–DB3 scan ~2.4GB; DB4 explodes to 14GB because OL embeds Product. Avoid DB4 for OL-heavy filters."),
        ("Q4 nested loop (Stock->Product, sharded)", q4, "DB1 cheap (separate small docs); DB5 is ~7000x heavier because Product carries embedded OL. Prefer DB1/DB3 for this join."),
        ("Q5 nested loop (Apple products distribution)", q5, "DB1 stays tiny; DB5 balloons scan due to embedded OL in Product. DB1 clearly wins for brand→stock lookups."),
        ("Q6 aggregate (SUM qty by product, sharded)", q6, "DB1–DB3 similar; DB4 costs 5x due to bulky OL docs. Avoid DB4 for aggregates over OL."),
        ("Q7 aggregate (client 125 order mix)", q7, "DB1–DB3 tiny; DB4 higher scan for the same reason (embedded product in OL). Use non-embedded OL for per-client aggregates."),
    ]

    for title, builder, note in query_plan:
        subtitle(title)
        for dbname, layout in layouts.items():
            res = builder(layout)
            if isinstance(res, str):
                emit(f"[{dbname}] {res}")
            else:
                print_cost(dbname, res)
        emit(note)

    # Persist the captured output
    run_path.write_text("\n".join(lines), encoding="utf-8")
