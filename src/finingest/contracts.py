from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from pathlib import Path

import yaml

from finingest.models import ContractError, Envelope
from finingest.paths import contracts_path, crosswalk_path, sla_path

TWOPLACES = Decimal("0.01")


@dataclass(frozen=True)
class CrosswalkEntry:
    source: str
    source_account_key: str
    canonical_account_id: str
    legal_name: str
    product: str


@dataclass(frozen=True)
class ContractBundle:
    raw: dict
    crosswalk: dict[tuple[str, str], CrosswalkEntry]
    slas: dict

    @property
    def partitions(self) -> int:
        return int(self.raw["partitions"])

    @property
    def currencies(self) -> set[str]:
        return set(self.raw["accepted_currencies"])

    def source(self, name: str) -> dict:
        for source in self.raw["sources"]:
            if source["name"] == name:
                return source
        raise ContractError(f"unknown_source:{name}")

    def version(self, source: str, schema_version: str) -> dict:
        spec = self.source(source)
        for version in spec["versions"]:
            if str(version["version"]) == str(schema_version):
                return version
        raise ContractError("unknown_schema_version")

    def topic_for(self, source: str) -> str:
        return self.source(source)["topic"]

    def topics(self) -> list[str]:
        return [source["topic"] for source in self.raw["sources"]]


def load_contracts(root: Path | None = None) -> ContractBundle:
    with contracts_path(root).open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    with sla_path(root).open(encoding="utf-8") as handle:
        slas = yaml.safe_load(handle)
    crosswalk: dict[tuple[str, str], CrosswalkEntry] = {}
    with crosswalk_path(root).open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            entry = CrosswalkEntry(
                source=row["source"],
                source_account_key=row["source_account_key"],
                canonical_account_id=row["canonical_account_id"],
                legal_name=row["legal_name"],
                product=row["product"],
            )
            key = (entry.source, entry.source_account_key)
            if key in crosswalk:
                raise ContractError(f"duplicate_crosswalk:{key}")
            crosswalk[key] = entry
    _validate_shape(raw, crosswalk)
    return ContractBundle(raw=raw, crosswalk=crosswalk, slas=slas)


def _validate_shape(raw: dict, crosswalk: dict) -> None:
    names = [source["name"] for source in raw["sources"]]
    if len(names) != 5 or len(set(names)) != 5:
        raise ContractError("expected_five_sources")
    topics = [source["topic"] for source in raw["sources"]]
    if len(set(topics)) != len(topics):
        raise ContractError("duplicate_topic")
    for source in raw["sources"]:
        if not source["versions"]:
            raise ContractError(f"missing_versions:{source['name']}")
        for version in source["versions"]:
            for key in ("event_id", "account_key", "required", "fields"):
                if key not in version:
                    raise ContractError(f"incomplete_version:{source['name']}:{version.get('version')}")
            id_field = version["event_id"]
            account_field = version["account_key"]
            if id_field not in version["required"] or account_field not in version["required"]:
                raise ContractError(f"id_or_account_not_required:{source['name']}")
    if not crosswalk:
        raise ContractError("empty_crosswalk")


def _as_decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ContractError("invalid_amount") from exc


def _apply_spec(spec: dict, payload: dict) -> object:
    if "const" in spec:
        value: object = spec["const"]
    elif "multiply" in spec:
        value = Decimal("1")
        for name in spec["multiply"]:
            if name not in payload:
                raise ContractError(f"missing_field:{name}")
            value *= _as_decimal(payload[name])
    elif "field" in spec:
        name = spec["field"]
        if name not in payload:
            raise ContractError(f"missing_field:{name}")
        value = payload[name]
    else:
        raise ContractError("invalid_mapping")
    if "scale" in spec or "multiply" in spec:
        value = (_as_decimal(value) * _as_decimal(spec.get("scale", "1"))).quantize(
            TWOPLACES, rounding=ROUND_HALF_EVEN
        )
    if "map" in spec:
        mapped = spec["map"].get(str(value))
        if mapped is None:
            raise ContractError("unmapped_value")
        value = mapped
    if spec.get("date"):
        text = str(value)
        if "T" in text:
            text = text.split("T", 1)[0]
        value = text[:10]
    return value


def normalize(envelope: Envelope, bundle: ContractBundle, *, ingested_at: str) -> dict:
    if not isinstance(envelope.payload, dict):
        raise ContractError("payload_not_object")
    try:
        version = bundle.version(envelope.source, envelope.schema_version)
    except ContractError:
        raise
    payload = envelope.payload
    missing = [field for field in version["required"] if field not in payload or payload[field] in (None, "")]
    if missing:
        raise ContractError("missing_field:" + ",".join(missing))
    source_event_id = str(payload[version["event_id"]])
    if source_event_id != envelope.event_id:
        raise ContractError("event_id_mismatch")
    fields = {name: _apply_spec(spec, payload) for name, spec in version["fields"].items()}
    amount = fields["amount"]
    if not isinstance(amount, Decimal) or amount <= 0:
        raise ContractError("non_positive_amount")
    currency = str(fields["currency"])
    if currency not in bundle.currencies:
        raise ContractError("invalid_currency")
    direction = str(fields["direction"])
    if direction not in {"inflow", "outflow"}:
        raise ContractError("invalid_direction")
    account_key = str(payload[version["account_key"]])
    identity = bundle.crosswalk.get((envelope.source, account_key))
    canonical_account_id = identity.canonical_account_id if identity else None
    return {
        "canonical_event_id": f"{envelope.source}:{source_event_id}",
        "source_system": envelope.source,
        "source_event_id": source_event_id,
        "schema_version": str(envelope.schema_version),
        "event_type": str(fields["event_type"]),
        "canonical_account_id": canonical_account_id,
        "source_account_key": account_key,
        "identity_status": "resolved" if identity else "unresolved",
        "counterparty": str(fields["counterparty"]),
        "instrument": str(fields["instrument"]),
        "amount": f"{amount:.2f}",
        "currency": currency,
        "direction": direction,
        "event_ts": str(fields["event_ts"]),
        "effective_date": str(fields["effective_date"]),
        "status": str(fields["status"]),
        "attributes_json": json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
        "ingested_at": ingested_at,
    }


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
