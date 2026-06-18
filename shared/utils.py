"""Shared utilities: blob staging, HMAC validation, notifications, retry."""
from __future__ import annotations
import hashlib
import hmac
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.servicebus import ServiceBusClient, ServiceBusMessage

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────
# Blob staging
# ──────────────────────────────────────────────────────────

def stage_to_blob(
    container: str,
    blob_path: str,
    data: Any,
    content_type: str = "application/json",
) -> str:
    """
    Serialize `data` to JSON and upload to Azure Blob Storage.
    Returns the full blob URL.
    """
    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = client.get_blob_client(container=container, blob=blob_path)

    payload = json.dumps(data, default=str).encode("utf-8")
    blob_client.upload_blob(
        payload,
        overwrite=True,
        content_settings=ContentSettings(content_type=content_type),
    )
    logger.info("Staged %d bytes → %s/%s", len(payload), container, blob_path)
    return blob_client.url


def stage_batch(container: str, prefix: str, records: list[dict]) -> str:
    """Stage a list of records as a newline-delimited JSON blob."""
    batch_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H")
    blob_path = f"{prefix}/{ts}/{batch_id}.ndjson"

    ndjson = "\n".join(json.dumps(r, default=str) for r in records).encode("utf-8")
    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = client.get_blob_client(container=container, blob=blob_path)
    blob_client.upload_blob(
        ndjson,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/x-ndjson"),
    )
    logger.info("Staged batch %s (%d records) → %s/%s", batch_id, len(records), container, blob_path)
    return batch_id


# ──────────────────────────────────────────────────────────
# Webhook signature verification
# ──────────────────────────────────────────────────────────

def verify_stripe_signature(
    payload: bytes,
    sig_header: str,
    secret: str,
    tolerance_seconds: int = 300,
) -> bool:
    """
    Validate a Stripe webhook signature (Stripe-Signature header).
    Raises ValueError on malformed header; returns False on signature mismatch.
    """
    try:
        parts = {k: v for k, v in (p.split("=", 1) for p in sig_header.split(","))}
        timestamp = int(parts["t"])
        signatures = [v for k, v in parts.items() if k == "v1"]
    except (KeyError, ValueError) as exc:
        raise ValueError(f"Malformed Stripe-Signature header: {exc}") from exc

    now = int(datetime.now(timezone.utc).timestamp())
    if abs(now - timestamp) > tolerance_seconds:
        logger.warning("Stripe webhook timestamp outside tolerance (%ds)", abs(now - timestamp))
        return False

    signed_payload = f"{timestamp}.{payload.decode('utf-8')}".encode()
    expected = hmac.new(secret.encode(), signed_payload, hashlib.sha256).hexdigest()
    return any(hmac.compare_digest(expected, sig) for sig in signatures)


# ──────────────────────────────────────────────────────────
# Service Bus helpers
# ──────────────────────────────────────────────────────────

def send_queue_message(queue_name: str, body: dict, session_id: str | None = None) -> None:
    """Send a single JSON message to an Azure Service Bus queue."""
    conn_str = os.environ["SERVICE_BUS_CONNECTION_STRING"]
    with ServiceBusClient.from_connection_string(conn_str) as sb_client:
        sender = sb_client.get_queue_sender(queue_name=queue_name)
        with sender:
            msg = ServiceBusMessage(
                json.dumps(body, default=str),
                content_type="application/json",
                session_id=session_id,
            )
            sender.send_messages(msg)
    logger.info("Sent message to queue '%s'", queue_name)


def send_queue_batch(queue_name: str, items: list[dict]) -> None:
    """Send multiple JSON messages to a queue in a single batch."""
    conn_str = os.environ["SERVICE_BUS_CONNECTION_STRING"]
    with ServiceBusClient.from_connection_string(conn_str) as sb_client:
        sender = sb_client.get_queue_sender(queue_name=queue_name)
        with sender:
            batch = sender.create_message_batch()
            for item in items:
                msg = ServiceBusMessage(
                    json.dumps(item, default=str),
                    content_type="application/json",
                )
                try:
                    batch.add_message(msg)
                except ValueError:
                    # Batch full — send current batch and start a new one
                    sender.send_messages(batch)
                    batch = sender.create_message_batch()
                    batch.add_message(msg)
            if batch:
                sender.send_messages(batch)
    logger.info("Sent %d messages to queue '%s'", len(items), queue_name)


# ──────────────────────────────────────────────────────────
# Response helpers
# ──────────────────────────────────────────────────────────

def success_response(data: dict | None = None, message: str = "OK") -> dict:
    return {"status": "success", "message": message, **(data or {})}


def error_response(message: str, details: str | None = None) -> dict:
    resp = {"status": "error", "message": message}
    if details:
        resp["details"] = details
    return resp


def make_batch_id(prefix: str = "") -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    uid = str(uuid.uuid4())[:8]
    return f"{prefix}_{ts}_{uid}" if prefix else f"{ts}_{uid}"
