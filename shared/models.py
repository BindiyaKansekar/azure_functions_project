"""Pydantic models for all inbound event payloads."""
from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field, field_validator, model_validator


# ──────────────────────────────────────────────────────────
# Enums
# ──────────────────────────────────────────────────────────

class OrderStatus(str, Enum):
    PENDING   = "PENDING"
    CONFIRMED = "CONFIRMED"
    SHIPPED   = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"
    RETURNED  = "RETURNED"


class OrderChannel(str, Enum):
    WEB    = "WEB"
    MOBILE = "MOBILE"
    STORE  = "STORE"
    PHONE  = "PHONE"


class PaymentStatus(str, Enum):
    PENDING    = "PENDING"
    AUTHORIZED = "AUTHORIZED"
    CAPTURED   = "CAPTURED"
    FAILED     = "FAILED"
    REFUNDED   = "REFUNDED"


class PaymentMethod(str, Enum):
    CREDIT_CARD   = "CREDIT_CARD"
    DEBIT_CARD    = "DEBIT_CARD"
    PAYPAL        = "PAYPAL"
    APPLE_PAY     = "APPLE_PAY"
    BANK_TRANSFER = "BANK_TRANSFER"


class CustomerEventType(str, Enum):
    REGISTERED    = "REGISTERED"
    PROFILE_UPDATED = "PROFILE_UPDATED"
    TIER_CHANGED  = "TIER_CHANGED"
    DELETED       = "DELETED"
    OPTED_OUT     = "OPTED_OUT"


class InventoryStatus(str, Enum):
    IN_STOCK     = "IN_STOCK"
    LOW_STOCK    = "LOW_STOCK"
    OUT_OF_STOCK = "OUT_OF_STOCK"
    DISCONTINUED = "DISCONTINUED"


# ──────────────────────────────────────────────────────────
# Order models
# ──────────────────────────────────────────────────────────

class OrderItem(BaseModel):
    item_id:         str
    product_id:      str
    quantity:        int = Field(gt=0)
    unit_price:      Decimal = Field(gt=0, decimal_places=4)
    discount_amount: Decimal = Field(default=Decimal("0"), ge=0, decimal_places=4)
    tax_amount:      Decimal = Field(default=Decimal("0"), ge=0, decimal_places=4)

    @property
    def line_total(self) -> Decimal:
        return (self.unit_price * self.quantity) - self.discount_amount + self.tax_amount


class OrderPayload(BaseModel):
    """Inbound order event from the e-commerce platform."""
    order_id:        str
    customer_id:     str
    store_id:        Optional[str] = None
    channel:         OrderChannel = OrderChannel.WEB
    currency:        str = Field(default="USD", min_length=3, max_length=3)
    order_timestamp: datetime
    items:           list[OrderItem] = Field(min_length=1)
    promotion_id:    Optional[str] = None
    shipping_amount: Decimal = Field(default=Decimal("0"), ge=0, decimal_places=2)
    tax_amount:      Decimal = Field(default=Decimal("0"), ge=0, decimal_places=2)
    status:          OrderStatus = OrderStatus.PENDING
    metadata:        dict = Field(default_factory=dict)

    @field_validator("currency")
    @classmethod
    def currency_uppercase(cls, v: str) -> str:
        return v.upper()

    @property
    def subtotal(self) -> Decimal:
        return sum(item.line_total for item in self.items)

    @property
    def total(self) -> Decimal:
        return self.subtotal + self.shipping_amount + self.tax_amount


# ──────────────────────────────────────────────────────────
# Payment models
# ──────────────────────────────────────────────────────────

class PaymentWebhookPayload(BaseModel):
    """Stripe-compatible payment webhook payload."""
    payment_id:          str
    order_id:            str
    customer_id:         str
    method:              PaymentMethod
    status:              PaymentStatus
    amount:              Decimal = Field(gt=0, decimal_places=2)
    currency:            str = Field(default="USD", min_length=3, max_length=3)
    timestamp:           datetime
    gateway_ref:         Optional[str] = None
    is_refund:           bool = False
    original_payment_id: Optional[str] = None
    failure_code:        Optional[str] = None
    failure_message:     Optional[str] = None

    @model_validator(mode="after")
    def refund_must_have_original(self) -> "PaymentWebhookPayload":
        if self.is_refund and not self.original_payment_id:
            raise ValueError("Refunds must reference an original_payment_id")
        return self


# ──────────────────────────────────────────────────────────
# Customer models
# ──────────────────────────────────────────────────────────

class CustomerEvent(BaseModel):
    """Customer lifecycle event from CRM / identity platform."""
    event_id:     str
    event_type:   CustomerEventType
    customer_id:  str
    timestamp:    datetime
    first_name:   Optional[str] = None
    last_name:    Optional[str] = None
    email:        Optional[str] = None
    phone:        Optional[str] = None
    date_of_birth: Optional[date] = None
    gender:       Optional[str] = None
    tier:         Optional[str] = None
    is_active:    bool = True
    source_system: str = "CRM"
    changes:      dict = Field(default_factory=dict)   # field: {old, new} for updates


# ──────────────────────────────────────────────────────────
# Inventory models
# ──────────────────────────────────────────────────────────

class InventoryRecord(BaseModel):
    """Inventory snapshot record from WMS."""
    product_id:        str
    location_id:       str
    location_type:     str = "WAREHOUSE"
    qty_on_hand:       int = Field(ge=0)
    qty_reserved:      int = Field(ge=0, default=0)
    reorder_point:     Optional[int] = None
    reorder_qty:       Optional[int] = None
    snapshot_date:     date
    snapshot_ts:       datetime
    status:            InventoryStatus = InventoryStatus.IN_STOCK

    @model_validator(mode="after")
    def set_status_from_qty(self) -> "InventoryRecord":
        available = self.qty_on_hand - self.qty_reserved
        if available <= 0:
            self.status = InventoryStatus.OUT_OF_STOCK
        elif self.reorder_point and available <= self.reorder_point:
            self.status = InventoryStatus.LOW_STOCK
        else:
            self.status = InventoryStatus.IN_STOCK
        return self


class InventorySyncBatch(BaseModel):
    batch_id:    str
    source:      str = "WMS"
    synced_at:   datetime
    records:     list[InventoryRecord]
    total_skus:  int = 0

    @model_validator(mode="after")
    def set_total_skus(self) -> "InventorySyncBatch":
        self.total_skus = len(self.records)
        return self


# ──────────────────────────────────────────────────────────
# Campaign / marketing models
# ──────────────────────────────────────────────────────────

class CampaignEvent(BaseModel):
    """Marketing campaign interaction event from Marketo / Segment."""
    event_id:     str
    campaign_id:  str
    customer_id:  Optional[str] = None
    session_id:   Optional[str] = None
    event_type:   str   # impression | click | conversion | unsubscribe
    channel:      str
    timestamp:    datetime
    metadata:     dict = Field(default_factory=dict)


# ──────────────────────────────────────────────────────────
# Stockout alert model
# ──────────────────────────────────────────────────────────

class StockoutAlert(BaseModel):
    product_id:    str
    product_name:  str
    location_id:   str
    qty_available: int
    reorder_point: Optional[int] = None
    detected_at:   datetime
    supplier_id:   Optional[str] = None


# ──────────────────────────────────────────────────────────
# Data quality result model
# ──────────────────────────────────────────────────────────

class DQCheckResult(BaseModel):
    check_name:  str
    table_name:  str
    layer:       str
    passed:      bool
    row_count:   Optional[int] = None
    failed_rows: Optional[int] = None
    threshold:   Optional[float] = None
    actual_value: Optional[float] = None
    message:     str = ""
    checked_at:  datetime
