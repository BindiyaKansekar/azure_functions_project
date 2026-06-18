"""Unit tests for Azure Functions — mock all external calls."""
from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import azure.functions as func


# ──────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────

def _make_http_request(body: dict, method: str = "POST") -> func.HttpRequest:
    return func.HttpRequest(
        method=method,
        url="https://localhost/api/test",
        headers={"Content-Type": "application/json"},
        params={},
        route_params={},
        body=json.dumps(body).encode("utf-8"),
    )


def _valid_order() -> dict:
    return {
        "order_id":        "ORD-001",
        "customer_id":     "CUST-001",
        "channel":         "WEB",
        "currency":        "USD",
        "order_timestamp": "2026-01-15T12:00:00Z",
        "items": [
            {
                "item_id":    "ITEM-001",
                "product_id": "PROD-001",
                "quantity":   2,
                "unit_price": "29.99",
            }
        ],
    }


# ──────────────────────────────────────────────────────────
# ingest_order tests
# ──────────────────────────────────────────────────────────

class TestIngestOrder(unittest.TestCase):

    @patch("function_app.sf.load_orders", return_value=1)
    @patch("function_app.sf.load_order_items", return_value=1)
    def test_valid_order_returns_202(self, mock_items, mock_orders):
        from function_app import ingest_order
        req = _make_http_request(_valid_order())
        resp = ingest_order(req)

        self.assertEqual(resp.status_code, 202)
        body = json.loads(resp.get_body())
        self.assertEqual(body["status"], "success")
        self.assertEqual(body["order_id"], "ORD-001")
        self.assertEqual(body["items_count"], 1)
        mock_orders.assert_called_once()
        mock_items.assert_called_once()

    def test_invalid_json_returns_400(self):
        from function_app import ingest_order
        req = func.HttpRequest(
            method="POST",
            url="https://localhost/api/test",
            headers={},
            params={},
            route_params={},
            body=b"not valid json",
        )
        resp = ingest_order(req)
        self.assertEqual(resp.status_code, 400)

    def test_missing_required_field_returns_400(self):
        from function_app import ingest_order
        bad_order = _valid_order()
        del bad_order["customer_id"]
        req = _make_http_request(bad_order)
        resp = ingest_order(req)
        self.assertEqual(resp.status_code, 400)
        body = json.loads(resp.get_body())
        self.assertEqual(body["status"], "error")

    def test_empty_items_returns_400(self):
        from function_app import ingest_order
        order = _valid_order()
        order["items"] = []
        req = _make_http_request(order)
        resp = ingest_order(req)
        self.assertEqual(resp.status_code, 400)

    @patch("function_app.sf.load_orders", side_effect=Exception("Snowflake down"))
    @patch("function_app.sf.load_order_items", return_value=0)
    def test_snowflake_failure_returns_500(self, mock_items, mock_orders):
        from function_app import ingest_order
        req = _make_http_request(_valid_order())
        resp = ingest_order(req)
        self.assertEqual(resp.status_code, 500)


# ──────────────────────────────────────────────────────────
# process_payment_webhook tests
# ──────────────────────────────────────────────────────────

class TestPaymentWebhook(unittest.TestCase):

    def _valid_payment_body(self) -> dict:
        return {
            "payment_id":  "PAY-001",
            "order_id":    "ORD-001",
            "customer_id": "CUST-001",
            "method":      "CREDIT_CARD",
            "status":      "CAPTURED",
            "amount":      "59.98",
            "currency":    "USD",
            "timestamp":   "2026-01-15T12:05:00Z",
        }

    @patch("function_app.sf.load_payments", return_value=1)
    def test_valid_payment_no_signature_returns_200(self, mock_load):
        from function_app import process_payment_webhook
        req = _make_http_request(self._valid_payment_body())
        resp = process_payment_webhook(req)
        self.assertEqual(resp.status_code, 200)
        mock_load.assert_called_once()

    @patch("function_app.sf.load_payments", return_value=1)
    @patch("function_app.utils.send_queue_message")
    def test_failed_payment_enqueues_alert(self, mock_queue, mock_load):
        from function_app import process_payment_webhook
        body = self._valid_payment_body()
        body["status"] = "FAILED"
        body["failure_code"] = "card_declined"
        req = _make_http_request(body)
        resp = process_payment_webhook(req)
        self.assertEqual(resp.status_code, 200)
        mock_queue.assert_called_once()
        alert = mock_queue.call_args[0][1]
        self.assertEqual(alert["event"], "payment_failed")
        self.assertEqual(alert["order_id"], "ORD-001")

    def test_refund_without_original_payment_returns_400(self):
        from function_app import process_payment_webhook
        body = self._valid_payment_body()
        body["is_refund"] = True  # no original_payment_id
        req = _make_http_request(body)
        resp = process_payment_webhook(req)
        self.assertEqual(resp.status_code, 400)


# ──────────────────────────────────────────────────────────
# Models tests
# ──────────────────────────────────────────────────────────

class TestModels(unittest.TestCase):

    def test_order_item_line_total(self):
        from shared.models import OrderItem
        item = OrderItem(
            item_id="I1", product_id="P1",
            quantity=3, unit_price=Decimal("10.00"),
            discount_amount=Decimal("5.00"),
        )
        self.assertEqual(item.line_total, Decimal("25.00"))

    def test_order_total(self):
        from shared.models import OrderPayload, OrderItem
        order = OrderPayload(
            order_id="O1", customer_id="C1",
            order_timestamp=datetime.now(timezone.utc),
            currency="USD",
            items=[OrderItem(item_id="I1", product_id="P1",
                             quantity=2, unit_price=Decimal("20.00"))],
            shipping_amount=Decimal("5.00"),
        )
        self.assertEqual(order.total, Decimal("45.00"))

    def test_inventory_auto_status_out_of_stock(self):
        from datetime import date
        from shared.models import InventoryRecord, InventoryStatus
        rec = InventoryRecord(
            product_id="P1", location_id="WH1",
            qty_on_hand=0, qty_reserved=0,
            reorder_point=10,
            snapshot_date=date.today(),
            snapshot_ts=datetime.now(timezone.utc),
        )
        self.assertEqual(rec.status, InventoryStatus.OUT_OF_STOCK)

    def test_inventory_auto_status_low_stock(self):
        from datetime import date
        from shared.models import InventoryRecord, InventoryStatus
        rec = InventoryRecord(
            product_id="P1", location_id="WH1",
            qty_on_hand=5, qty_reserved=0,
            reorder_point=10,
            snapshot_date=date.today(),
            snapshot_ts=datetime.now(timezone.utc),
        )
        self.assertEqual(rec.status, InventoryStatus.LOW_STOCK)

    def test_customer_event_type_enum(self):
        from shared.models import CustomerEvent, CustomerEventType
        event = CustomerEvent(
            event_id="E1", event_type=CustomerEventType.REGISTERED,
            customer_id="C1", timestamp=datetime.now(timezone.utc),
        )
        self.assertEqual(event.event_type, CustomerEventType.REGISTERED)


# ──────────────────────────────────────────────────────────
# Utils tests
# ──────────────────────────────────────────────────────────

class TestUtils(unittest.TestCase):

    def test_make_batch_id_with_prefix(self):
        from shared.utils import make_batch_id
        bid = make_batch_id("orders")
        self.assertTrue(bid.startswith("orders_"))

    def test_make_batch_id_without_prefix(self):
        from shared.utils import make_batch_id
        bid = make_batch_id()
        self.assertIsInstance(bid, str)
        self.assertGreater(len(bid), 5)

    def test_success_response_shape(self):
        from shared.utils import success_response
        resp = success_response({"key": "val"}, message="Created")
        self.assertEqual(resp["status"], "success")
        self.assertEqual(resp["message"], "Created")
        self.assertEqual(resp["key"], "val")

    def test_error_response_shape(self):
        from shared.utils import error_response
        resp = error_response("Bad input", details="field X missing")
        self.assertEqual(resp["status"], "error")
        self.assertIn("details", resp)

    def test_verify_stripe_signature_bad_header(self):
        from shared.utils import verify_stripe_signature
        with self.assertRaises(ValueError):
            verify_stripe_signature(b"body", "malformed", "secret")


if __name__ == "__main__":
    unittest.main()
