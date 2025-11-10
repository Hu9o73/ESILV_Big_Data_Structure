from dataclasses import dataclass

# === Canonical sizes from the assignment ===
INT_B = 8          # Integer/Number
NUMBER_B = 8
STRING_B = 80      # "String"
DATE_B = 20        # "Date" -> specific string
LONGSTRING_B = 200 # "LongString"
KV_OVERHEAD_B = 12 # Key+Value pairs / Arrays : 12B + values

@dataclass(frozen=True)
class Infra:
    SERVERS: int = 1000  # 1,000 servers in the cluster

@dataclass(frozen=True)
class Stats:
    # From Section 2.2
    N_CLIENTS: int = 10_000_000         # 10^7 customers
    N_PRODUCTS: int = 100_000           # 10^5 products
    N_ORDERLINES: int = 4_000_000_000   # 4 * 10^9 order lines
    N_WAREHOUSES: int = 200             # 200 warehouses
    N_BRANDS: int = 5_000               # 5,000 distinct brands
    N_APPLE_PRODUCTS: int = 50          # 50 Apple products
    # Derived:
    @property
    def N_STOCK(self) -> int:
        # For a stock, even if quantity is 0, an instance exists per (product, warehouse)
        return self.N_PRODUCTS * self.N_WAREHOUSES

# Average cardinalities used for array fields during sizing
@dataclass(frozen=True)
class Averages:
    PRODUCT_CATEGORIES_PER_PRODUCT: int = 2   # 1..5, average 2
    STOCKS_PER_PRODUCT: int = Stats().N_WAREHOUSES  # 200 (one per warehouse)
    # Embedding order lines under product (DB5)
    ORDERLINES_PER_PRODUCT: int = Stats().N_ORDERLINES // Stats().N_PRODUCTS  # ~40,000
