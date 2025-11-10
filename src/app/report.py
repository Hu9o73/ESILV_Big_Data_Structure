from typing import Dict
from app.constants import Stats, Averages, Infra
from app.schemas import all_layouts
from app.sizer import db_sizes, pretty_gb
from app.sharding import all_sharding_reports

def print_header(title: str):
    print("=" * 80)
    print(title)
    print("=" * 80)

def run_all_reports():
    stats = Stats()
    avgs = Averages()
    infra = Infra()

    # Denormalizations & Sizes
    print_header("STEP 2.3–2.5: Denormalizations, Document Sizes, Collection Sizes, DB Sizes")
    layouts = all_layouts(avgs)

    for dbname, layout in layouts.items():
        total_b, doc_sizes, coll_sizes = db_sizes(layout.collections, stats)

        print(f"\n{dbname}")
        print("-" * 80)
        print("Average Document Size (bytes):")
        for coll, dsz in doc_sizes.items():
            print(f"\t{coll:12s} : {dsz:,} B")

        print("\nCollection Sizes (GB):")
        for coll, csz in coll_sizes.items():
            print(f"\t{coll:12s} : {pretty_gb(csz):,.3f} GB")

        print(f"\nTotal Database Size (GB): {pretty_gb(total_b):,.3f} GB")

        # 2.5.4 short qualitative note
        print("\nDenormalization tradeoffs (quick notes):")
        if dbname == "DB1":
            print("\t- Joins still needed for Stock/OrderLine searches; Product hot, but Stock separate.")
        elif dbname == "DB2":
            print("\t- Big Product docs (embedded 200 Stock entries/product). Faster per-product stock reads; heavier writes.")
        elif dbname == "DB3":
            print("\t- Stock docs heavy (embedded Product). Great for stock-centric access; duplicates product across 20M docs.")
        elif dbname == "DB4":
            print("\t- OrderLine embeds Product snapshot; speeds brand/date + OL joins, but duplicates Product across 4B OL.")
        elif dbname == "DB5":
            print("\t- Product embeds ~40k OrderLines each (!); huge Product docs, painful updates; great for product-centric OL scans.")

    # Sharding stats
    print_header("STEP 2.6: Sharding Strategies - Averages per Server")
    for rep in all_sharding_reports(stats, infra):
        print(f"{rep.strategy:12s} | Coll={rep.collection:10s} | "
              f"docs/server={rep.docs_per_server:,.1f} | "
              f"distinct-values/server={rep.distinct_values_per_server:,.3f}")
