from typing import Dict, Any
from dataclasses import dataclass
from app.constants import Averages

# --- Base entity snippets (normalized) ---

def price_schema() -> Dict[str, Any]:
    # price -> detailed with currency + VAT
    return {
        "type": "object",
        "fields": {
            "value": {"type": "number"},
            "currency": {"type": "string"},
            "vat": {"type": "number"},
        },
    }

def category_schema() -> Dict[str, Any]:
    # From ERD: Categories(title)
    return {"type": "object", "fields": {"title": {"type": "string"}}}

def supplier_schema() -> Dict[str, Any]:
    # Supplier(IDS, name, SIRET, headOffice, Revenue)
    return {
        "type": "object",
        "fields": {
            "IDS": {"type": "int"},
            "name": {"type": "string"},
            "SIRET": {"type": "string"},
            "headOffice": {"type": "string"},
            "revenue": {"type": "number"},
        },
    }

def product_core_schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "fields": {
            "IDP": {"type": "int"},
            "name": {"type": "string"},
            "price": price_schema(),
            "brand": {"type": "string"},
            "description": {"type": "longstring"},
            "image_url": {"type": "string"},
        },
    }

def stock_schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "fields": {
            "IDP": {"type": "int"},
            "IDW": {"type": "int"},  # warehouse ID (we model as int)
            "quantity": {"type": "int"},
            "location": {"type": "string"},
        },
    }

def warehouse_schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "fields": {
            "IDW": {"type": "int"},
            "address": {"type": "string"},
            "capacity": {"type": "int"},
        },
    }

def orderline_schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "fields": {
            "IDP": {"type": "int"},
            "IDC": {"type": "int"},
            "date": {"type": "date"},
            "quantity": {"type": "int"},
            "deliveryDate": {"type": "date"},
            "comment": {"type": "string"},
            "grade": {"type": "int"},
        },
    }

def client_schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "fields": {
            "IDC": {"type": "int"},
            "ln": {"type": "string"},
            "fn": {"type": "string"},
            "address": {"type": "string"},
            "nationality": {"type": "string"},
            "birthDate": {"type": "date"},
            "email": {"type": "string"},
        },
    }

# --- Denormalized "collection sets" for DB1..DB5 ---

@dataclass(frozen=True) # Frozen dataclass to ensure "read only"
class DBLayout:
    """
    A DB layout is a dict of collection_name -> schema (as above).
    """
    collections: Dict[str, Dict[str, Any]]

def db1(a: Averages) -> DBLayout:
    """
    DB1: Prod{[Cat],Supp}, St, Wa, OL, Cl
    """
    prod = product_core_schema()
    prod["fields"]["categories"] = {"type": "array", "items": category_schema(), "avg_len": a.PRODUCT_CATEGORIES_PER_PRODUCT}
    prod["fields"]["supplier"] = supplier_schema()
    return DBLayout({
        "Product": prod,
        "Stock": stock_schema(),
        "Warehouse": warehouse_schema(),
        "OrderLine": orderline_schema(),
        "Client": client_schema(),
    })

def db2(a: Averages) -> DBLayout:
    """
    DB2: Prod{[Cat],Supp,[St]}, Wa, OL, Cl
    """
    prod = product_core_schema()
    prod["fields"]["categories"] = {"type": "array", "items": category_schema(), "avg_len": a.PRODUCT_CATEGORIES_PER_PRODUCT}
    prod["fields"]["supplier"] = supplier_schema()
    prod["fields"]["stocks"] = {"type": "array", "items": stock_schema(), "avg_len": a.STOCKS_PER_PRODUCT}
    return DBLayout({
        "Product": prod,
        "Warehouse": warehouse_schema(),
        "OrderLine": orderline_schema(),
        "Client": client_schema(),
    })

def db3(a: Averages) -> DBLayout:
    """
    DB3: St{Prod{[Cat],Supp}}, Wa, OL, Cl
    """
    st = stock_schema()
    # embed product under stock
    prod = product_core_schema()
    prod["fields"]["categories"] = {"type": "array", "items": category_schema(), "avg_len": a.PRODUCT_CATEGORIES_PER_PRODUCT}
    prod["fields"]["supplier"] = supplier_schema()
    st["fields"]["product"] = prod
    return DBLayout({
        "Stock": st,
        "Warehouse": warehouse_schema(),
        "OrderLine": orderline_schema(),
        "Client": client_schema(),
    })

def db4(a: Averages) -> DBLayout:
    """
    DB4: St, Wa, OL{Prod{[Cat],Supp}}, Cl
    """
    ol = orderline_schema()
    prod = product_core_schema()
    prod["fields"]["categories"] = {"type": "array", "items": category_schema(), "avg_len": a.PRODUCT_CATEGORIES_PER_PRODUCT}
    prod["fields"]["supplier"] = supplier_schema()
    ol["fields"]["product"] = prod
    return DBLayout({
        "Stock": stock_schema(),
        "Warehouse": warehouse_schema(),
        "OrderLine": ol,
        "Client": client_schema(),
    })

def db5(a: Averages) -> DBLayout:
    """
    DB5: Prod{[Cat],Supp,[OL]}, St, Wa, Cl
    """
    prod = product_core_schema()
    prod["fields"]["categories"] = {"type": "array", "items": category_schema(), "avg_len": a.PRODUCT_CATEGORIES_PER_PRODUCT}
    prod["fields"]["supplier"] = supplier_schema()
    prod["fields"]["orderLines"] = {"type": "array", "items": orderline_schema(), "avg_len": a.ORDERLINES_PER_PRODUCT}
    return DBLayout({
        "Product": prod,
        "Stock": stock_schema(),
        "Warehouse": warehouse_schema(),
        "Client": client_schema(),
    })

def all_layouts(a: Averages) -> Dict[str, DBLayout]:
    return {
        "DB1": db1(a),
        "DB2": db2(a),
        "DB3": db3(a),
        "DB4": db4(a),
        "DB5": db5(a),
    }
