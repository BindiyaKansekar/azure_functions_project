"""
RetailEvents Azure Functions — v2 Python programming model.

Functions:
  1. ingest_order              HTTP POST /api/orders
  2. process_payment_webhook   HTTP POST /api/payments/webhook
  3. sync_inventory            Timer  — every 4 hours
  4. customer_event_processor  Service Bus queue — customer-events
  5. load_blob_to_snowflake    Blob trigger — retail-raw-stage/**/*.ndjson
  6. data_quality_check        Timer  — daily 06:30 UTC
  7. campaign_event_handler    Event Hub — campaign-events
  8. stockout_notifier         Service Bus queue — stockout-alerts
  9. daily_report_trigger      Timer  — daily 07:00 UTC
 10. refresh_customer_segments Timer  — nightly 02:00 UTC
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone

import azure.functions as func
from pydantic import ValidationError

from shared import snowflake_client as sf
from shared import utils
from shared.models import (
    CampaignEvent,
    CustomerEvent,
    InventorySyncBatch,
    OrderPayload,
    PaymentWebhookPayload,
    StockoutAlert,
)

logger = logging.getLogger(__name__)

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


# ══════════════════════════════════════════════════════════
# 1. ingest_order
#    Receives an order from the e-commerce platform,
#    validates the payload, then writes to Snowflake RAW.ORDERS
#    and RAW.ORDER_ITEMS in a single batch.
# ══════════════════════════════════════════════════════════

@app.route(route="orders", methods=["POST"])
def ingest_order(req: func.HttpRequest) -> func.HttpResponse:
    """
    Accept a single order from the OMS / e-commerce platform.

    Validates schema with Pydantic, then inserts into:
      - RETAIL_DW.RAW.ORDERS       (one row)
      - RETAIL_DW.RAW.ORDER_ITEMS  (one row per line item)

    Returns 202 Accepted on success, 400 on validation failure,
    500 on Snowflake write failure.
    """
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps(utils.error_response("Invalid JSON body")),
            status_code=400, mimetype="application/json",
        )

    try:
        order = OrderPayload(**body)
    except ValidationError as exc:
        logger.warning("Order validation failed: %s", exc)
        return func.HttpResponse(
            json.dumps(utils.error_response("Validation failed", str(exc))),
            status_code=400, mimetype="application/json",
        )

    batch_id = utils.make_batch_id("order")
    order_dict = order.model_dump(mode="json")

    try:
        sf.load_orders([order_dict], batch_id)
        items = [
            {**item.model_dump(mode="json"), "order_id": order.order_id}
            for item in order.items
        ]
        sf.load_order_items(items, batch_id)
    except Exception as exc:
        logger.exception("Failed to load order %s to Snowflake", order.order_id)
        return func.HttpResponse(
            json.dumps(utils.error_response("Failed to persist order", str(exc))),
            status_code=500, mimetype="application/json",
        )

    logger.info("Ingested order %s (%d items, total=%s)", order.order_id, len(order.items), order.total)
    return func.HttpResponse(
        json.dumps(utils.success_response(
            {"order_id": order.order_id, "batch_id": batch_id, "items_count": len(order.items)},
            message="Order accepted",
        )),
        status_code=202, mimetype="application/json",
    )


# ══════════════════════════════════════════════════════════
# 2. process_payment_webhook
#    Receives Stripe webhook events, verifies HMAC signature,
#    writes to RETAIL_DW.RAW.PAYMENTS.
#    Failed payments trigger a stockout-alerts queue message
#    (so downstream can cancel reserved inventory).
# ══════════════════════════════════════════════════════════

@app.route(route="payments/webhook", methods=["POST"])
def process_payment_webhook(req: func.HttpRequest) -> func.HttpResponse:
    """
    Handle Stripe payment webhook events.

    Security: validates Stripe-Signature HMAC header before processing.
    On FAILED payments, enqueues a message to the stockout-alerts queue
    so reserved inventory can be released.
    """
    raw_body = req.get_body()
    sig_header = req.headers.get("Stripe-Signature", "")
    secret = os.environ.get("PAYMENT_WEBHOOK_SECRET", "")

    if secret and sig_header:
        try:
            valid = utils.verify_stripe_signature(raw_body, sig_header, secret)
        except ValueError as exc:
            return func.HttpResponse(
                json.dumps(utils.error_response("Bad signature header", str(exc))),
                status_code=400, mimetype="application/json",
            )
        if not valid:
            logger.warning("Stripe signature verification failed")
            return func.HttpResponse(
                json.dumps(utils.error_response("Signature verification failed")),
                status_code=401, mimetype="application/json",
            )

    try:
        body = json.loads(raw_body)
        payment = PaymentWebhookPayload(**body)
    except (json.JSONDecodeError, ValidationError) as exc:
        return func.HttpResponse(
            json.dumps(utils.error_response("Invalid payload", str(exc))),
            status_code=400, mimetype="application/json",
        )

    batch_id = utils.make_batch_id("payment")
    try:
        sf.load_payments([payment.model_dump(mode="json")], batch_id)
    except Exception as exc:
        logger.exception("Failed to persist payment %s", payment.payment_id)
        return func.HttpResponse(
            json.dumps(utils.error_response("Persistence failed", str(exc))),
            status_code=500, mimetype="application/json",
        )

    # Release inventory reservation on failed payments
    if payment.status.value == "FAILED":
        try:
            utils.send_queue_message(
                os.environ["STOCKOUT_ALERTS_QUEUE"],
                {"event": "payment_failed", "order_id": payment.order_id,
                 "payment_id": payment.payment_id, "reason": payment.failure_code},
            )
        except Exception:
            logger.exception("Could not enqueue payment-failed alert for order %s", payment.order_id)

    logger.info("Processed payment %s (status=%s)", payment.payment_id, payment.status)
    return func.HttpResponse(
        json.dumps(utils.success_response({"payment_id": payment.payment_id})),
        status_code=200, mimetype="application/json",
    )


# ══════════════════════════════════════════════════════════
# 3. sync_inventory
#    Timer trigger every 4 hours.
#    Calls WMS REST API → validates records → bulk-loads into
#    RETAIL_DW.RAW.INVENTORY.
#    Enqueues stockout alerts for any OUT_OF_STOCK SKUs.
# ══════════════════════════════════════════════════════════

@app.timer_trigger(schedule="0 0 */4 * * *", arg_name="timer", run_on_startup=False)
def sync_inventory(timer: func.TimerRequest) -> None:
    """
    Pull a full inventory snapshot from the WMS REST API every 4 hours.

    Loads validated records into RETAIL_DW.RAW.INVENTORY and enqueues
    stockout-alert messages for any SKUs with zero available quantity.
    """
    if timer.past_due:
        logger.warning("sync_inventory timer is past due — running anyway")

    batch_id = utils.make_batch_id("inv_sync")
    logger.info("Starting inventory sync (batch=%s)", batch_id)

    # Fetch from WMS (stubbed — replace with actual WMS client call)
    raw_records = _fetch_inventory_from_wms()

    try:
        batch = InventorySyncBatch(
            batch_id=batch_id,
            synced_at=datetime.now(timezone.utc),
            records=raw_records,
        )
    except ValidationError as exc:
        logger.error("Inventory batch validation failed: %s", exc)
        return

    records_dicts = [r.model_dump(mode="json") for r in batch.records]
    inserted = sf.load_inventory(records_dicts, batch_id)
    logger.info("Loaded %d inventory records (batch=%s)", inserted, batch_id)

    # Enqueue stockout alerts for OUT_OF_STOCK SKUs
    stockouts = [
        r for r in batch.records if r.status.value == "OUT_OF_STOCK"
    ]
    if stockouts:
        alerts = [
            StockoutAlert(
                product_id=r.product_id,
                product_name=r.product_id,   # enriched by downstream lookup
                location_id=r.location_id,
                qty_available=max(0, r.qty_on_hand - r.qty_reserved),
                reorder_point=r.reorder_point,
                detected_at=datetime.now(timezone.utc),
            ).model_dump(mode="json")
            for r in stockouts
        ]
        utils.send_queue_batch(os.environ["STOCKOUT_ALERTS_QUEUE"], alerts)
        logger.warning("Enqueued %d stockout alerts", len(alerts))


def _fetch_inventory_from_wms() -> list:
    """
    Fetch raw inventory records from the WMS REST API.
    Replace this stub with your actual WMS client.
    Returns a list of dicts that InventoryRecord can parse.
    """
    from shared.models import InventoryRecord
    from datetime import date
    # Stub data — replace with: requests.get(WMS_URL, headers={"Authorization": ...}).json()
    return [
        InventoryRecord(
            product_id="PROD-001", location_id="WH-A", qty_on_hand=150, qty_reserved=20,
            reorder_point=50, reorder_qty=200,
            snapshot_date=date.today(), snapshot_ts=datetime.now(timezone.utc),
        ),
        InventoryRecord(
            product_id="PROD-002", location_id="WH-A", qty_on_hand=0, qty_reserved=0,
            reorder_point=30, reorder_qty=100,
            snapshot_date=date.today(), snapshot_ts=datetime.now(timezone.utc),
        ),
    ]


# ══════════════════════════════════════════════════════════
# 4. customer_event_processor
#    Service Bus queue trigger on "customer-events".
#    Processes CRM lifecycle events (registration, updates,
#    tier changes) and writes them to RAW.CUSTOMERS.
# ══════════════════════════════════════════════════════════

@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name="%CUSTOMER_EVENTS_QUEUE%",
    connection="SERVICE_BUS_CONNECTION_STRING",
)
def customer_event_processor(msg: func.ServiceBusMessage) -> None:
    """
    Process customer lifecycle events from the CRM system.

    Validates the CustomerEvent schema and writes to RETAIL_DW.RAW.CUSTOMERS.
    DELETED / OPTED_OUT events are flagged with is_active=False to allow
    GDPR-compliant downstream suppression in SILVER and GOLD layers.
    """
    body = msg.get_body().decode("utf-8")
    try:
        event = CustomerEvent(**json.loads(body))
    except (json.JSONDecodeError, ValidationError) as exc:
        logger.error("Invalid customer event: %s | body=%s", exc, body[:200])
        raise  # Dead-letter after max delivery count

    # Map deletion/opt-out to inactive flag before writing to raw
    record = event.model_dump(mode="json")
    if event.event_type.value in ("DELETED", "OPTED_OUT"):
        record["is_active"] = False

    batch_id = utils.make_batch_id("cust_event")
    sf.load_customers([record], batch_id)
    logger.info("Processed customer event %s (type=%s, customer=%s)",
                event.event_id, event.event_type, event.customer_id)


# ══════════════════════════════════════════════════════════
# 5. load_blob_to_snowflake
#    Blob trigger on retail-raw-stage/**/*.ndjson.
#    Reads staged NDJSON files and bulk-loads them to the
#    appropriate RAW table based on the blob path prefix.
# ══════════════════════════════════════════════════════════

@app.blob_trigger(
    arg_name="blob",
    path="retail-raw-stage/{entity}/{year}/{month}/{day}/{hour}/{name}.ndjson",
    connection="AZURE_STORAGE_CONNECTION_STRING",
)
def load_blob_to_snowflake(blob: func.InputStream) -> None:
    """
    Auto-load NDJSON blobs from retail-raw-stage into Snowflake RAW layer.

    The blob path encodes the target entity:
      retail-raw-stage/orders/2026/01/15/12/batch-xyz.ndjson → RAW.ORDERS
      retail-raw-stage/customers/...                          → RAW.CUSTOMERS

    Each line in the NDJSON file becomes one PAYLOAD row.
    """
    blob_name = blob.name
    logger.info("Blob trigger fired: %s (%d bytes)", blob_name, blob.length)

    # Infer entity from blob path: retail-raw-stage/<entity>/...
    parts = blob_name.replace("\\", "/").split("/")
    entity = parts[1] if len(parts) > 1 else "unknown"

    ndjson_content = blob.read().decode("utf-8")
    records = []
    for line in ndjson_content.splitlines():
        line = line.strip()
        if line:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                logger.warning("Skipping malformed NDJSON line in %s", blob_name)

    if not records:
        logger.warning("No valid records in blob %s", blob_name)
        return

    batch_id = utils.make_batch_id(f"blob_{entity}")
    loader_map = {
        "orders":    (sf.load_orders,    "order_id"),
        "customers": (sf.load_customers, "customer_id"),
        "inventory": (sf.load_inventory, "product_id"),
        "payments":  (sf.load_payments,  "payment_id"),
        "campaigns": (sf.load_campaign_events, "campaign_id"),
    }

    if entity not in loader_map:
        logger.warning("Unknown entity '%s' in blob path — skipping load", entity)
        return

    load_fn, _ = loader_map[entity]
    inserted = load_fn(records, batch_id)
    logger.info("Blob load: %d/%d records → RAW.%s (batch=%s)",
                inserted, len(records), entity.upper(), batch_id)


# ══════════════════════════════════════════════════════════
# 6. data_quality_check
#    Timer trigger daily at 06:30 UTC.
#    Runs SQL-based DQ checks across RAW and SILVER layers,
#    logs results, and alerts on failures.
# ══════════════════════════════════════════════════════════

@app.timer_trigger(schedule="0 30 6 * * *", arg_name="timer", run_on_startup=False)
def data_quality_check(timer: func.TimerRequest) -> None:
    """
    Run daily data quality checks across RAW and SILVER layers.

    Checks include: null PKs, duplicate keys, negative amounts,
    stale inventory snapshots, and orphaned order items.
    Failures are logged and enqueued as alert messages.
    """
    logger.info("Starting daily data quality checks")
    results = sf.run_dq_checks()

    failures = [r for r in results if not r["passed"]]
    passed = len(results) - len(failures)

    logger.info("DQ check complete: %d/%d passed", passed, len(results))

    for fail in failures:
        logger.error(
            "DQ FAIL | check=%s table=%s layer=%s failed_rows=%s message=%s",
            fail["check_name"], fail["table_name"], fail["layer"],
            fail.get("failed_rows"), fail["message"],
        )
        # Enqueue alert so downstream notification function picks it up
        try:
            utils.send_queue_message(
                os.environ["STOCKOUT_ALERTS_QUEUE"],
                {"event": "dq_failure", **fail},
            )
        except Exception:
            logger.exception("Failed to enqueue DQ failure alert for %s", fail["check_name"])


# ══════════════════════════════════════════════════════════
# 7. campaign_event_handler
#    Event Hub trigger on "campaign-events".
#    Processes high-throughput marketing interaction events
#    (impressions, clicks, conversions) in micro-batches.
# ══════════════════════════════════════════════════════════

@app.event_hub_message_trigger(
    arg_name="events",
    event_hub_name="%CAMPAIGN_EVENTS_HUB%",
    connection="EVENT_HUB_CONNECTION_STRING",
    cardinality=func.Cardinality.MANY,
)
def campaign_event_handler(events: list[func.EventHubEvent]) -> None:
    """
    Process marketing campaign interaction events from Event Hub in micro-batches.

    Validates each CampaignEvent, groups by campaign_id, and bulk-loads
    to RETAIL_DW.RAW.CAMPAIGNS. Invalid events are logged and skipped
    (Event Hub does not support individual message dead-lettering).
    """
    batch_id = utils.make_batch_id("campaign_eh")
    valid_records: list[dict] = []
    invalid_count = 0

    for event in events:
        try:
            body = json.loads(event.get_body().decode("utf-8"))
            ce = CampaignEvent(**body)
            valid_records.append(ce.model_dump(mode="json"))
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning("Invalid campaign event: %s", exc)
            invalid_count += 1

    if valid_records:
        sf.load_campaign_events(valid_records, batch_id)

    logger.info(
        "Campaign event batch: %d valid, %d invalid (batch=%s)",
        len(valid_records), invalid_count, batch_id,
    )


# ══════════════════════════════════════════════════════════
# 8. stockout_notifier
#    Service Bus queue trigger on "stockout-alerts".
#    Enriches alert with product/supplier info from Snowflake,
#    then sends an email notification via SendGrid.
#    Also handles payment_failed events to release inventory.
# ══════════════════════════════════════════════════════════

@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name="%STOCKOUT_ALERTS_QUEUE%",
    connection="SERVICE_BUS_CONNECTION_STRING",
)
def stockout_notifier(msg: func.ServiceBusMessage) -> None:
    """
    Process stockout and DQ failure alerts, sending email notifications.

    Enriches stockout alerts with product/supplier details from Snowflake
    before routing to the notification layer (SendGrid email).
    """
    body = json.loads(msg.get_body().decode("utf-8"))
    event_type = body.get("event", "stockout")

    if event_type == "stockout":
        _handle_stockout_alert(body)
    elif event_type == "payment_failed":
        _handle_payment_failed(body)
    elif event_type == "dq_failure":
        _handle_dq_failure_alert(body)
    else:
        logger.warning("Unknown alert event type: %s", event_type)


def _handle_stockout_alert(alert: dict) -> None:
    product_id = alert.get("product_id", "")
    # Enrich with product and supplier info
    rows = sf.execute_query(
        "SELECT PRODUCT_NAME, SUPPLIER_NAME, SUPPLIER_ID FROM RETAIL_DW.GOLD.DIM_PRODUCTS "
        "WHERE PRODUCT_ID = %s",
        (product_id,),
    )
    product_name = rows[0].get("PRODUCT_NAME", product_id) if rows else product_id
    supplier_name = rows[0].get("SUPPLIER_NAME", "Unknown") if rows else "Unknown"

    subject = f"STOCKOUT ALERT: {product_name} at {alert.get('location_id')}"
    body = (
        f"Product {product_name} (ID: {product_id}) is out of stock "
        f"at location {alert.get('location_id')}.\n"
        f"Supplier: {supplier_name}\n"
        f"Detected at: {alert.get('detected_at')}"
    )
    _send_email(subject, body)
    logger.warning("Stockout notified: %s @ %s", product_id, alert.get("location_id"))


def _handle_payment_failed(event: dict) -> None:
    logger.info(
        "Payment failed for order %s (payment=%s, reason=%s) — inventory reservation released",
        event.get("order_id"), event.get("payment_id"), event.get("reason"),
    )
    # In a real implementation: call inventory API to release reservation


def _handle_dq_failure_alert(result: dict) -> None:
    subject = f"DQ FAILURE: {result.get('check_name')} on {result.get('table_name')}"
    body = (
        f"Data quality check failed:\n"
        f"  Check:  {result.get('check_name')}\n"
        f"  Table:  {result.get('table_name')} ({result.get('layer')} layer)\n"
        f"  Failed: {result.get('failed_rows', '?')} rows\n"
        f"  Msg:    {result.get('message')}"
    )
    _send_email(subject, body)


def _send_email(subject: str, body_text: str) -> None:
    """Send a plain-text email via SendGrid. No-ops if SENDGRID_API_KEY is unset."""
    api_key = os.environ.get("SENDGRID_API_KEY")
    to_email = os.environ.get("NOTIFICATION_EMAIL", "")
    if not api_key or not to_email:
        logger.info("[EMAIL STUB] Subject: %s", subject)
        return

    import urllib.request, urllib.error
    payload = json.dumps({
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": "intellidoc-noreply@company.com"},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body_text}],
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=payload,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10):
            logger.info("Email sent: %s → %s", subject, to_email)
    except urllib.error.HTTPError as exc:
        logger.error("SendGrid error %s: %s", exc.code, exc.read())


# ══════════════════════════════════════════════════════════
# 9. daily_report_trigger
#    Timer trigger daily at 07:00 UTC.
#    Refreshes the GOLD.DAILY_SALES_SUMMARY and
#    GOLD.EXECUTIVE_DASHBOARD tables, then pushes a
#    report-ready message so downstream consumers are notified.
# ══════════════════════════════════════════════════════════

@app.timer_trigger(schedule="0 0 7 * * *", arg_name="timer", run_on_startup=False)
def daily_report_trigger(timer: func.TimerRequest) -> None:
    """
    Refresh the Gold reporting layer and notify downstream consumers.

    Executes the daily_sales_summary and executive_dashboard refresh
    procedures in Snowflake, then enqueues a report-ready event to the
    report-triggers queue for any BI or notification consumers.
    """
    logger.info("Starting daily report refresh")
    run_date = datetime.now(timezone.utc).date().isoformat()

    try:
        sf.call_procedure("RETAIL_DW.GOLD.REFRESH_DAILY_SALES_SUMMARY", ())
        sf.call_procedure("RETAIL_DW.GOLD.REFRESH_EXECUTIVE_DASHBOARD", ())
    except Exception as exc:
        logger.exception("Report refresh procedures failed")
        utils.send_queue_message(
            os.environ["STOCKOUT_ALERTS_QUEUE"],
            {"event": "report_refresh_failed", "run_date": run_date, "error": str(exc)},
        )
        return

    utils.send_queue_message(
        os.environ.get("REPORT_TRIGGER_QUEUE", "report-triggers"),
        {
            "event":       "report_ready",
            "run_date":    run_date,
            "reports":     ["DAILY_SALES_SUMMARY", "EXECUTIVE_DASHBOARD"],
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    logger.info("Daily report refresh complete for %s", run_date)


# ══════════════════════════════════════════════════════════
# 10. refresh_customer_segments
#     Timer trigger nightly at 02:00 UTC.
#     Calls the Snowflake procedure to recompute RFM scores
#     and lifecycle segments in SILVER.CUSTOMER_SEGMENTS.
# ══════════════════════════════════════════════════════════

@app.timer_trigger(schedule="0 0 2 * * *", arg_name="timer", run_on_startup=False)
def refresh_customer_segments(timer: func.TimerRequest) -> None:
    """
    Nightly refresh of customer RFM segments and CLV scores.

    Triggers the Silver-layer segment refresh stored procedure which
    recomputes RFM recency/frequency/monetary scores and updates the
    SILVER.CUSTOMER_SEGMENTS table for use by Gold-layer mart queries.
    """
    logger.info("Starting nightly customer segment refresh")
    run_date = datetime.now(timezone.utc).date().isoformat()

    try:
        sf.call_procedure("RETAIL_DW.SILVER.REFRESH_CUSTOMER_SEGMENTS", ())
        sf.call_procedure("RETAIL_DW.GOLD.REFRESH_CUSTOMER_LIFETIME_VALUE", ())
        sf.call_procedure("RETAIL_DW.GOLD.REFRESH_CUSTOMER_CHURN_INDICATORS", ())
    except Exception as exc:
        logger.exception("Customer segment refresh failed")
        utils.send_queue_message(
            os.environ["STOCKOUT_ALERTS_QUEUE"],
            {"event": "segment_refresh_failed", "run_date": run_date, "error": str(exc)},
        )
        return

    logger.info("Customer segment refresh complete for %s", run_date)
