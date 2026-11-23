from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from finingest.contracts import isoformat_z, load_contracts, utc_now
from finingest.models import Envelope, ProducedEvent


def _keys(root: Path | None, source: str) -> list[str]:
    bundle = load_contracts(root)
    keys = [entry.source_account_key for (src, _), entry in bundle.crosswalk.items() if src == source]
    if not keys:
        raise RuntimeError(f"no crosswalk keys for {source}")
    return keys


def _stamp(moment: datetime) -> str:
    return isoformat_z(moment)


def card_events(n: int, moment: datetime, keys: list[str], id_suffix: str = "") -> list[ProducedEvent]:
    events: list[ProducedEvent] = []
    for i in range(n):
        key = keys[i % len(keys)]
        ts = _stamp(moment + timedelta(minutes=i))
        if i % 2 == 0:
            event_id = f"AUTH2-{i:04d}{id_suffix}"
            payload = {
                "authorization_id": event_id,
                "card_token": key,
                "merchant_id": f"m-{100 + i}",
                "amount_minor": 1000 + i * 25,
                "currency": "USD",
                "authorized_at": ts,
                "response_code": "05" if i and i % 4 == 0 else "00",
                "network_token": f"net-{i}",
            }
            version = "2.0"
        else:
            event_id = f"AUTH1-{i:04d}{id_suffix}"
            payload = {
                "authId": event_id,
                "panToken": key,
                "merchantId": f"m-{100 + i}",
                "amt": f"{Decimal(10 + i) + Decimal('0.25'):.2f}",
                "ccy": "USD",
                "authTs": ts,
                "decision": "DECLINED" if i % 5 == 0 else "APPROVED",
            }
            version = "1.0"
        events.append(
            ProducedEvent(Envelope("card_processor", version, event_id, ts, payload), key)
        )
    return events


def ach_events(n: int, moment: datetime, keys: list[str], id_suffix: str = "") -> list[ProducedEvent]:
    statuses = ["POSTED", "POSTED", "PENDING", "RETURNED"]
    events: list[ProducedEvent] = []
    for i in range(n):
        key = keys[i % len(keys)]
        when = moment + timedelta(minutes=i)
        ts = _stamp(when)
        event_id = f"{100000000000000 + i}{id_suffix}"
        payload = {
            "trace_number": event_id,
            "odfi_account": key,
            "rdfi_account": f"rdfi-{2000 + i}",
            "amount": f"{Decimal('25.00') + Decimal(i):.2f}",
            "sec_code": "PPD" if i % 2 == 0 else "CCD",
            "effective_entry_date": when.date().isoformat(),
            "event_time": ts,
            "status": statuses[i % len(statuses)],
            "direction": "CREDIT" if i % 2 == 0 else "DEBIT",
        }
        events.append(ProducedEvent(Envelope("ach_network", "1.0", event_id, ts, payload), key))
    return events


def brokerage_events(n: int, moment: datetime, keys: list[str], id_suffix: str = "") -> list[ProducedEvent]:
    symbols = ["AAPL", "MSFT", "TLT"]
    events: list[ProducedEvent] = []
    for i in range(n):
        key = keys[i % len(keys)]
        when = moment + timedelta(minutes=i)
        ts = _stamp(when)
        event_id = f"TRD-{i:05d}{id_suffix}"
        payload = {
            "trade_id": event_id,
            "account_number": key,
            "symbol": symbols[i % len(symbols)],
            "side": "BUY" if i % 2 == 0 else "SELL",
            "quantity": str(i + 1),
            "price": "10.50",
            "trade_time": ts,
            "settlement_date": (when + timedelta(days=1)).date().isoformat(),
        }
        events.append(ProducedEvent(Envelope("brokerage", "1.0", event_id, ts, payload), key))
    return events


def loan_events(n: int, moment: datetime, keys: list[str], id_suffix: str = "") -> list[ProducedEvent]:
    events: list[ProducedEvent] = []
    for i in range(n):
        key = keys[i % len(keys)]
        ts = _stamp(moment + timedelta(minutes=i))
        status = "REVERSED" if i % 7 == 6 else "POSTED"
        amount = f"{Decimal('400.00') + Decimal(i):.2f}"
        if i % 3 == 0:
            event_id = f"PAY2-{i:04d}{id_suffix}"
            payload = {
                "payment_id": event_id,
                "loan_id": key,
                "borrower_id": f"b-{i}",
                "payment_amount": amount,
                "principal": "300.00",
                "interest": "80.00",
                "fee_amount": "15.00",
                "remaining_balance": "12000.00",
                "paid_at": ts,
                "payment_status": status,
            }
            version = "2.0"
        else:
            event_id = f"PAY1-{i:04d}{id_suffix}"
            payload = {
                "payment_ref": event_id,
                "loan_number": key,
                "borrower_id": f"b-{i}",
                "payment_amount": amount,
                "principal": "300.00",
                "interest": "80.00",
                "paid_at": ts,
                "payment_status": status,
            }
            version = "1.0"
        events.append(ProducedEvent(Envelope("loan_servicer", version, event_id, ts, payload), key))
    return events


def wire_events(n: int, moment: datetime, keys: list[str], id_suffix: str = "") -> list[ProducedEvent]:
    events: list[ProducedEvent] = []
    for i in range(n):
        key = keys[i % len(keys)]
        when = moment + timedelta(minutes=i)
        ts = _stamp(when)
        event_id = f"IMAD{when.strftime('%Y%m%d')}{i:06d}{id_suffix}"
        payload = {
            "wire_imad": event_id,
            "debtor_account": key,
            "creditor_account": f"bene-{300 + i}",
            "instructed_amount": f"{Decimal('1000.00') + Decimal(i) * Decimal('10'):.2f}",
            "currency": "EUR" if i % 5 == 0 else "USD",
            "value_date": when.date().isoformat(),
            "uetr": f"uetr-{i:04d}",
            "event_time": ts,
            "status": "PENDING" if i % 4 == 0 else "SETTLED",
        }
        events.append(ProducedEvent(Envelope("treasury_wires", "1.0", event_id, ts, payload), key))
    return events


def build_catalog(
    *,
    per_source: int = 10,
    seed_time: datetime | None = None,
    include_faults: bool = False,
    root: Path | None = None,
    id_suffix: str = "",
) -> list[ProducedEvent]:
    moment = seed_time or (utc_now() - timedelta(minutes=90))
    events = [
        *card_events(per_source, moment, _keys(root, "card_processor"), id_suffix),
        *ach_events(per_source, moment, _keys(root, "ach_network"), id_suffix),
        *brokerage_events(per_source, moment, _keys(root, "brokerage"), id_suffix),
        *loan_events(per_source, moment, _keys(root, "loan_servicer"), id_suffix),
        *wire_events(per_source, moment, _keys(root, "treasury_wires"), id_suffix),
    ]
    if include_faults:
        events.extend(_faults(events, moment))
    return events


def _faults(events: list[ProducedEvent], moment: datetime) -> list[ProducedEvent]:
    card = next(event for event in events if event.envelope.source == "card_processor")
    trade = next(event for event in events if event.envelope.source == "brokerage")
    duplicate_card = deepcopy(card)
    duplicate_trade = deepcopy(trade)

    broken = deepcopy(card)
    broken_payload = {
        key: value
        for key, value in broken.envelope.payload.items()
        if key not in {"amt", "amount_minor"}
    }
    broken_id = "AUTH-MISSING-AMT"
    if "authorization_id" in broken_payload:
        broken_payload["authorization_id"] = broken_id
    if "authId" in broken_payload:
        broken_payload["authId"] = broken_id
    broken = ProducedEvent(
        Envelope(broken.envelope.source, broken.envelope.schema_version, broken_id, broken.envelope.event_ts, broken_payload),
        broken.key,
    )

    unknown = ProducedEvent(
        Envelope("ach_network", "9.9", "TRACE-UNKNOWN-VERSION", _stamp(moment), {"trace_number": "TRACE-UNKNOWN-VERSION"}),
        "026009593:1001",
    )

    ts = _stamp(moment + timedelta(minutes=5))
    unresolved_id = f"IMAD{moment.strftime('%Y%m%d')}404404"
    unresolved = ProducedEvent(
        Envelope(
            "treasury_wires",
            "1.0",
            unresolved_id,
            ts,
            {
                "wire_imad": unresolved_id,
                "debtor_account": "WIRE-404",
                "creditor_account": "bene-unknown",
                "instructed_amount": "50.00",
                "currency": "USD",
                "value_date": moment.date().isoformat(),
                "uetr": "uetr-404",
                "event_time": ts,
                "status": "SETTLED",
            },
        ),
        "WIRE-404",
    )
    return [duplicate_card, duplicate_trade, broken, unknown, unresolved]
