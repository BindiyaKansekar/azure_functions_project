"""Snowflake connector helpers — connection pooling and batch loading into RAW layer."""
from __future__ import annotations
import json
import logging
import os
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator

import snowflake.connector
from snowflake.connector import DictCursor

logger = logging.getLogger(__name__)


def _get_connection() -> snowflake.connector.SnowflakeConnection:
    """Open a new Snowflake connection using environment variables."""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "RAW_LOADER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "RETAIL_DW"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
        session_parameters={"QUERY_TAG": "intellidoc-azure-functions"},
        network_timeout=30,
        login_timeout=15,
    )


@contextmanager
def get_cursor() -> Generator[DictCursor, None, None]:
    """Context manager that yields a cursor and closes the connection on exit."""
    conn = _get_connection()
    try:
        with conn.cursor(DictCursor) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────
# Generic raw-layer loader
# ──────────────────────────────────────────────────────────

def insert_raw_batch(
    table: str,
    records: list[dict],
    src_key_col: str,
    src_key_values: list[str],
    batch_id: str | None = None,
) -> int:
    """
    Insert a batch of records into a RAW schema table.

    Each record is stored as a JSON VARIANT in the PAYLOAD column alongside
    audit columns (FILE_NAME, FILE_ROW_NUMBER, LOAD_TIMESTAMP, BATCH_ID).

    Returns the number of rows inserted.
    """
    if not records:
        return 0

    batch_id = batch_id or str(uuid.uuid4())
    now_ts = datetime.now(timezone.utc).isoformat()

    rows = [
        (src_key_values[i], json.dumps(rec), f"azure-function/{batch_id}", i + 1, batch_id)
        for i, rec in enumerate(records)
    ]

    sql = f"""
        INSERT INTO RETAIL_DW.RAW.{table}
            ({src_key_col}, PAYLOAD, FILE_NAME, FILE_ROW_NUMBER, BATCH_ID)
        SELECT
            column1,
            PARSE_JSON(column2),
            column3,
            column4::NUMBER,
            column5
        FROM VALUES {", ".join("(%s, %s, %s, %s, %s)" for _ in rows)}
    """

    flat_params = [v for row in rows for v in row]

    with get_cursor() as cur:
        cur.execute(sql, flat_params)
        inserted = cur.rowcount

    logger.info("Inserted %d rows into RAW.%s (batch=%s)", inserted, table, batch_id)
    return inserted


def execute_query(sql: str, params: tuple = ()) -> list[dict]:
    """Run a SELECT query and return rows as list of dicts."""
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def call_procedure(procedure: str, args: tuple = ()) -> None:
    """Call a Snowflake stored procedure."""
    placeholders = ", ".join("%s" for _ in args)
    with get_cursor() as cur:
        cur.execute(f"CALL {procedure}({placeholders})", args)
    logger.info("Called procedure %s", procedure)


# ──────────────────────────────────────────────────────────
# Domain-specific loaders
# ──────────────────────────────────────────────────────────

def load_orders(orders: list[dict], batch_id: str) -> int:
    return insert_raw_batch(
        table="ORDERS",
        records=orders,
        src_key_col="SRC_ORDER_ID",
        src_key_values=[o["order_id"] for o in orders],
        batch_id=batch_id,
    )


def load_order_items(items: list[dict], batch_id: str) -> int:
    return insert_raw_batch(
        table="ORDER_ITEMS",
        records=items,
        src_key_col="SRC_ORDER_ID",
        src_key_values=[i["order_id"] for i in items],
        batch_id=batch_id,
    )


def load_payments(payments: list[dict], batch_id: str) -> int:
    return insert_raw_batch(
        table="PAYMENTS",
        records=payments,
        src_key_col="SRC_PAYMENT_ID",
        src_key_values=[p["payment_id"] for p in payments],
        batch_id=batch_id,
    )


def load_customers(customers: list[dict], batch_id: str) -> int:
    return insert_raw_batch(
        table="CUSTOMERS",
        records=customers,
        src_key_col="SRC_CUSTOMER_ID",
        src_key_values=[c["customer_id"] for c in customers],
        batch_id=batch_id,
    )


def load_inventory(records: list[dict], batch_id: str) -> int:
    return insert_raw_batch(
        table="INVENTORY",
        records=records,
        src_key_col="SRC_PRODUCT_ID",
        src_key_values=[r["product_id"] for r in records],
        batch_id=batch_id,
    )


def load_campaign_events(events: list[dict], batch_id: str) -> int:
    return insert_raw_batch(
        table="CAMPAIGNS",
        records=events,
        src_key_col="SRC_CAMPAIGN_ID",
        src_key_values=[e["campaign_id"] for e in events],
        batch_id=batch_id,
    )


# ──────────────────────────────────────────────────────────
# Data quality helpers
# ──────────────────────────────────────────────────────────

_DQ_CHECKS = [
    # (check_name, table, layer, sql, threshold_pct)
    ("null_customer_id",    "CUSTOMERS",   "raw",    "SELECT COUNT(*) FROM RETAIL_DW.RAW.CUSTOMERS WHERE PAYLOAD:customer_id IS NULL",  0),
    ("null_order_id",       "ORDERS",      "raw",    "SELECT COUNT(*) FROM RETAIL_DW.RAW.ORDERS WHERE PAYLOAD:order_id IS NULL",         0),
    ("negative_total",      "ORDERS",      "silver", "SELECT COUNT(*) FROM RETAIL_DW.SILVER.ORDERS WHERE TOTAL_AMOUNT < 0",              0),
    ("duplicate_orders",    "ORDERS",      "silver", "SELECT COUNT(*) - COUNT(DISTINCT ORDER_ID) FROM RETAIL_DW.SILVER.ORDERS",           0),
    ("stale_inventory",     "INVENTORY",   "silver",
     "SELECT COUNT(*) FROM RETAIL_DW.SILVER.INVENTORY WHERE SNAPSHOT_DATE < DATEADD('day', -2, CURRENT_DATE())", 0),
    ("orphan_order_items",  "ORDER_ITEMS", "silver",
     "SELECT COUNT(*) FROM RETAIL_DW.SILVER.ORDER_ITEMS oi WHERE NOT EXISTS (SELECT 1 FROM RETAIL_DW.SILVER.ORDERS o WHERE o.ORDER_ID = oi.ORDER_ID)", 0),
]


def run_dq_checks() -> list[dict]:
    from datetime import datetime, timezone
    results = []
    for check_name, table, layer, sql, threshold in _DQ_CHECKS:
        try:
            rows = execute_query(sql)
            failed_count = list(rows[0].values())[0] if rows else 0
            passed = int(failed_count) <= threshold
            results.append({
                "check_name": check_name,
                "table_name": table,
                "layer": layer,
                "passed": passed,
                "failed_rows": int(failed_count),
                "threshold": threshold,
                "message": "OK" if passed else f"{failed_count} rows failed check",
                "checked_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as exc:
            results.append({
                "check_name": check_name,
                "table_name": table,
                "layer": layer,
                "passed": False,
                "message": f"Check failed with error: {exc}",
                "checked_at": datetime.now(timezone.utc).isoformat(),
            })
    return results
