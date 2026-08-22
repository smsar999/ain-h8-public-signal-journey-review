# -*- coding: utf-8 -*-
"""R1.2 canonical episode identity contract.

The legal identity is the structured :class:`EpisodeIdentityV1`.  ``episode_id``
is a deterministic storage/display encoding derived from it, while
``episode_key_sha256`` is the authoritative fingerprint.  Runtime code must never
reuse an older symbol-scoped identifier when a new signal bar is born.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import logging
import re
from typing import Any, Dict, Mapping, Optional, Tuple
from zoneinfo import ZoneInfo

IDENTITY_CONTRACT_VERSION = "EPISODE_IDENTITY_V1"
try:
    import gann20_episode_contract as _gann20_contract
    DEFAULT_DETECTOR_CONTRACT_VERSION = str(_gann20_contract.VERSION)
    try:
        from pathlib import Path as _IdentityPath
        DEFAULT_DETECTOR_CONTRACT_HASH = hashlib.sha256(
            _IdentityPath(str(_gann20_contract.__file__)).read_bytes()
        ).hexdigest()
    except Exception:
        DEFAULT_DETECTOR_CONTRACT_HASH = hashlib.sha256(
            (DEFAULT_DETECTOR_CONTRACT_VERSION + "\0GANN20_RUNTIME_CONTRACT").encode("utf-8")
        ).hexdigest()
except Exception:
    DEFAULT_DETECTOR_CONTRACT_VERSION = "V86CL_R163_GANN20_EPISODE_CONTRACT"
    DEFAULT_DETECTOR_CONTRACT_HASH = hashlib.sha256(
        (DEFAULT_DETECTOR_CONTRACT_VERSION + "\0GANN20_RUNTIME_CONTRACT").encode("utf-8")
    ).hexdigest()
DEFAULT_EPISODE_SIDE = "LONG"
_LOG = logging.getLogger("EpisodeIdentityV1")


class EpisodeIdentityError(ValueError):
    """Base fail-closed identity error."""


class EpisodeIdentityConflict(EpisodeIdentityError):
    """The textual id and structured identity describe different episodes."""


def _market_family(market_key: str) -> str:
    # Episode identity needs the actual market family, not the GANN display
    # threshold family.  The latter intentionally maps FX to ``generic`` and
    # previously caused FX Episode timestamps to fall back to UTC.
    try:
        from market_key_contract import market_family, transport_family
        family = str(market_family(market_key) or "").strip().lower()
        transport = str(transport_family(market_key) or "local").strip().lower()
        if family == "sa":
            return "sa"
        if family == "us":
            return "us_api" if transport == "api" else "us_local"
        if family == "fx":
            return "fx"
    except Exception as exc:
        _LOG.debug("MARKET_FAMILY_CONTRACT_FALLBACK market=%r error=%s", market_key, exc)
    text = str(market_key or "").strip().lower()
    if "السعود" in text or "saudi" in text or text in {"sa", "tadawul"}:
        return "sa"
    if "us" in text or "usa" in text or "أمريك" in text:
        return "us_local"
    if "forex" in text or "الفوركس" in text or "السلع" in text or text == "fx":
        return "fx"
    return "generic"


def _market_timezone(market_key: str) -> ZoneInfo:
    family = _market_family(market_key)
    if family == "sa":
        return ZoneInfo("Asia/Riyadh")
    if family in {"us", "us_local", "us_api", "fx"}:
        return ZoneInfo("America/New_York")
    return ZoneInfo("UTC")


def normalize_market_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise EpisodeIdentityError("EPISODE_MARKET_KEY_REQUIRED")
    # Legal identity must not depend on UI/display aliases.  Canonicalize only
    # recognized market families; preserve truly generic/custom keys instead of
    # collapsing unrelated markets into one ``generic_local`` identity.
    try:
        from market_key_contract import canonical_runtime_key, market_family
        if str(market_family(text) or "").strip().lower() != "generic":
            canonical = str(canonical_runtime_key(text) or "").strip()
            if canonical:
                return canonical
    except Exception as exc:
        _LOG.debug("MARKET_KEY_CANONICALIZATION_FALLBACK market=%r error=%s", text, exc)
    return text


def normalize_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        raise EpisodeIdentityError("EPISODE_SYMBOL_REQUIRED")
    return text


def normalize_timeframe(value: Any) -> str:
    text = str(value or "30M").strip().upper().replace("MIN", "M")
    aliases = {"M30": "30M", "30": "30M", "H1": "1H", "60M": "1H"}
    return aliases.get(text, text or "30M")


def normalize_episode_side(value: Any) -> str:
    text = str(value or DEFAULT_EPISODE_SIDE).strip().upper()
    aliases = {"BUY": "LONG", "BULL": "LONG", "SELL": "SHORT", "BEAR": "SHORT"}
    text = aliases.get(text, text)
    if text not in {"LONG", "SHORT"}:
        raise EpisodeIdentityError(f"INVALID_EPISODE_SIDE:{text}")
    return text


def _parse_datetime(value: Any) -> dt.datetime:
    if isinstance(value, dt.datetime):
        return value
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime()
        except Exception as exc:
            _LOG.debug("PANDAS_DATETIME_CONVERSION_FALLBACK value=%r error=%s", value, exc)
    text = str(value or "").strip()
    if not text:
        raise EpisodeIdentityError("SIGNAL_BAR_TIME_REQUIRED")
    text = text.replace("Z", "+00:00")
    try:
        return dt.datetime.fromisoformat(text.replace("T", " "))
    except Exception as exc:
        raise EpisodeIdentityError(f"INVALID_SIGNAL_BAR_TIME:{value}") from exc


def _timeframe_minutes(timeframe: str) -> Optional[int]:
    tf = normalize_timeframe(timeframe)
    m = re.fullmatch(r"(\d+)M", tf)
    if m:
        return int(m.group(1))
    h = re.fullmatch(r"(\d+)H", tf)
    if h:
        return int(h.group(1)) * 60
    return None


def canonical_bar_times(
    market_key: str,
    signal_bar_time: Any,
    timeframe: Any = "30M",
) -> Tuple[str, str]:
    """Return canonical exchange-local and UTC bar-end timestamps.

    Naive input is interpreted in the exchange timezone.  Aware input is converted
    into the exchange timezone.  Seconds/microseconds are normalized to the bar
    boundary.  Non-boundary timestamps fail closed instead of silently creating a
    second identity for the same bar.
    """
    zone = _market_timezone(market_key)
    raw = _parse_datetime(signal_bar_time)
    local = raw.replace(tzinfo=zone) if raw.tzinfo is None else raw.astimezone(zone)
    minutes = _timeframe_minutes(str(timeframe or "30M"))
    if minutes:
        minute_of_day = local.hour * 60 + local.minute
        if minute_of_day % minutes != 0 or local.second != 0 or local.microsecond != 0:
            raise EpisodeIdentityError(
                f"SIGNAL_BAR_NOT_ON_TIMEFRAME_BOUNDARY:{local.isoformat()}:{normalize_timeframe(timeframe)}"
            )
    local = local.replace(second=0, microsecond=0)
    utc = local.astimezone(dt.timezone.utc)
    return (
        local.isoformat(timespec="seconds"),
        utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
    )


@dataclasses.dataclass(frozen=True)
class EpisodeIdentityV1:
    market_key: str
    symbol: str
    signal_bar_end_utc: str
    detector_family: str
    detector_contract_version: str
    detector_contract_hash: str
    timeframe: str
    episode_side: str
    identity_contract_version: str = IDENTITY_CONTRACT_VERSION
    signal_bar_end_exchange_local: str = ""

    def canonical_mapping(self) -> Dict[str, str]:
        return {
            "identity_contract_version": self.identity_contract_version,
            "market_key": self.market_key,
            "symbol": self.symbol,
            "signal_bar_end_utc": self.signal_bar_end_utc,
            "detector_family": self.detector_family,
            "detector_contract_version": self.detector_contract_version,
            "timeframe": self.timeframe,
            "episode_side": self.episode_side,
        }

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.canonical_mapping(), ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False,
        ).encode("utf-8")

    @property
    def episode_key_sha256(self) -> str:
        return hashlib.sha256(b"EPISODE_KEY_V1\0" + self.canonical_bytes()).hexdigest()

    @property
    def episode_id(self) -> str:
        family = _market_family(self.market_key)
        market = family if family in {"sa", "us_local", "us_api", "fx"} else _generic_market_slug(self.market_key)
        symbol = re.sub(r"[^A-Za-z0-9_\-]+", "_", self.symbol).strip("_") or "NA"
        instant = dt.datetime.fromisoformat(self.signal_bar_end_utc.replace("Z", "+00:00"))
        local = instant.astimezone(_market_timezone(self.market_key)).replace(tzinfo=None)
        bar = local.strftime("%Y%m%dT%H%M%S.000")
        detector = re.sub(r"[^A-Za-z0-9_\-]+", "_", self.detector_family.upper()).strip("_") or "EP"
        # Preserve the established GANN20 id format for the current LONG/V1
        # contract.  Any future side/contract change receives a fingerprint suffix,
        # so the one-to-one identity relation remains fail-closed.
        base = f"{detector}-{market}-{symbol}-{bar}"
        if (
            detector == "GANN20"
            and self.detector_contract_version == DEFAULT_DETECTOR_CONTRACT_VERSION
            and self.episode_side == DEFAULT_EPISODE_SIDE
            and self.timeframe == "30M"
        ):
            return base
        return f"{base}-{self.episode_key_sha256[:12]}"


def _generic_market_slug(market_key: str) -> str:
    ascii_part = re.sub(r"[^A-Za-z0-9_\-]+", "_", str(market_key)).strip("_") or "market"
    digest = hashlib.sha1(str(market_key).encode("utf-8")).hexdigest()[:8]
    return f"{ascii_part}_{digest}"


def build_episode_identity(
    *,
    market_key: Any,
    symbol: Any,
    signal_bar_time: Any,
    detector_family: Any = "GANN20",
    detector_contract_version: Any = DEFAULT_DETECTOR_CONTRACT_VERSION,
    detector_contract_hash: Any = DEFAULT_DETECTOR_CONTRACT_HASH,
    timeframe: Any = "30M",
    episode_side: Any = DEFAULT_EPISODE_SIDE,
) -> EpisodeIdentityV1:
    market = normalize_market_key(market_key)
    tf = normalize_timeframe(timeframe)
    local, utc = canonical_bar_times(market, signal_bar_time, tf)
    detector = str(detector_family or "GANN20").strip().upper()
    if not detector:
        raise EpisodeIdentityError("DETECTOR_FAMILY_REQUIRED")
    contract = str(detector_contract_version or "").strip().upper()
    if not contract:
        raise EpisodeIdentityError("DETECTOR_CONTRACT_VERSION_REQUIRED")
    contract_hash = str(detector_contract_hash or "").strip().lower()
    if not contract_hash:
        raise EpisodeIdentityError("DETECTOR_CONTRACT_HASH_REQUIRED")
    if contract == DEFAULT_DETECTOR_CONTRACT_VERSION and contract_hash != DEFAULT_DETECTOR_CONTRACT_HASH:
        raise EpisodeIdentityConflict("DETECTOR_CONTRACT_VERSION_HASH_MISMATCH")
    return EpisodeIdentityV1(
        market_key=market,
        symbol=normalize_symbol(symbol),
        signal_bar_end_utc=utc,
        detector_family=detector,
        detector_contract_version=contract,
        detector_contract_hash=contract_hash,
        timeframe=tf,
        episode_side=normalize_episode_side(episode_side),
        signal_bar_end_exchange_local=local,
    )


def identity_from_mapping(
    row: Mapping[str, Any],
    *,
    market_key: Any = "",
    symbol: Any = "",
    signal_bar_time: Any = "",
    detector_family: Any = "GANN20",
    detector_contract_version: Any = DEFAULT_DETECTOR_CONTRACT_VERSION,
    detector_contract_hash: Any = DEFAULT_DETECTOR_CONTRACT_HASH,
    timeframe: Any = "30M",
    episode_side: Any = DEFAULT_EPISODE_SIDE,
) -> EpisodeIdentityV1:
    data = dict(row or {})
    return build_episode_identity(
        market_key=data.get("episode_market_key") or data.get("market_key") or data.get("market") or market_key,
        symbol=data.get("episode_symbol") or data.get("symbol") or symbol,
        signal_bar_time=(
            data.get("episode_signal_bar_time") or data.get("signal_bar_time")
            or data.get("pulse_bar_time") or data.get("bar_datetime") or signal_bar_time
        ),
        detector_family=data.get("episode_detector_family") or data.get("detector_family") or detector_family,
        detector_contract_version=(
            data.get("detector_contract_version") or data.get("gann20_contract_version")
            or detector_contract_version
        ),
        detector_contract_hash=(
            data.get("detector_contract_hash") or detector_contract_hash
        ),
        timeframe=data.get("episode_timeframe") or data.get("timeframe") or timeframe,
        episode_side=data.get("episode_side") or data.get("side") or episode_side,
    )


def _legacy_id_matches_identity(value: str, identity: EpisodeIdentityV1) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    detector = re.escape(identity.detector_family.upper())
    market = re.escape(_market_family(identity.market_key))
    symbol = re.escape(re.sub(r"[^A-Za-z0-9_\-]+", "_", identity.symbol).strip("_") or "NA")
    instant = dt.datetime.fromisoformat(identity.signal_bar_end_utc.replace("Z", "+00:00"))
    local = instant.astimezone(_market_timezone(identity.market_key)).replace(tzinfo=None)
    bar = local.strftime("%Y%m%dT%H%M%S")
    match = re.fullmatch(
        rf"{detector}-{market}-{symbol}-{bar}(?:\.000)?(?:-([0-9a-fA-F]{{12}}))?",
        text, flags=re.IGNORECASE,
    )
    if not match:
        return False
    suffix = str(match.group(1) or "").lower()
    return not suffix or suffix == identity.episode_key_sha256[:12]


def stamp_episode_identity(row: Mapping[str, Any], identity: EpisodeIdentityV1) -> Dict[str, Any]:
    out = dict(row or {})
    expected_id = identity.episode_id
    existing_ids = {
        str(out.get(key) or "").strip()
        for key in ("episode_id", "pulse_episode_id")
        if str(out.get(key) or "").strip()
    }
    # ``id`` is often a lifecycle signal id (SIG-...), not an Episode id.  It is
    # checked only when it actually parses as an Episode boundary representation.
    raw_id = str(out.get("id") or "").strip()
    if raw_id and (parse_episode_id(raw_id) or raw_id.upper().startswith(identity.detector_family.upper() + "-")):
        existing_ids.add(raw_id)
    incompatible = [
        value for value in existing_ids
        if value != expected_id and not _legacy_id_matches_identity(value, identity)
    ]
    if incompatible:
        raise EpisodeIdentityConflict(
            f"EPISODE_IDENTITY_CONFLICT:expected={expected_id}:existing={sorted(existing_ids)}"
        )
    out.update({
        "episode_id": expected_id,
        "pulse_episode_id": expected_id,
        "episode_key": "|".join([
            identity.market_key, identity.symbol, identity.signal_bar_end_utc,
            identity.detector_family, identity.detector_contract_version,
            identity.timeframe, identity.episode_side,
        ]),
        "episode_key_sha256": identity.episode_key_sha256,
        "episode_market_key": identity.market_key,
        "episode_symbol": identity.symbol,
        "episode_signal_bar_time": identity.signal_bar_end_exchange_local,
        "episode_signal_bar_end_utc": identity.signal_bar_end_utc,
        "episode_detector_family": identity.detector_family,
        "detector_contract_version": identity.detector_contract_version,
        "detector_contract_hash": identity.detector_contract_hash,
        "episode_timeframe": identity.timeframe,
        "episode_side": identity.episode_side,
        "identity_contract_version": identity.identity_contract_version,
    })
    return out


def ensure_episode_identity(
    row: Mapping[str, Any],
    **defaults: Any,
) -> Dict[str, Any]:
    data = dict(row or {})
    identity = identity_from_mapping(data, **defaults)
    return stamp_episode_identity(data, identity)



def parse_episode_id(value: Any) -> Dict[str, str]:
    """Parse the deterministic storage id at trust boundaries.

    New production code creates EpisodeIdentity first and never reconstructs
    identity from text.  This parser exists for legacy/UI boundary validation.
    """
    text = str(value or "").strip()
    if not text:
        return {}
    match = re.match(
        r"^(?P<detector>[A-Za-z0-9_]+)-(?P<market>sa|us_local|us_api|fx)-"
        r"(?P<symbol>.+)-(?P<bar>\d{8}T\d{6}\.000)(?:-(?P<suffix>[0-9a-fA-F]{12}))?$",
        text,
    )
    if match:
        return {
            "episode_id": text,
            "detector_family": str(match.group("detector") or "").upper(),
            "market_family": str(match.group("market") or ""),
            "symbol": str(match.group("symbol") or "").upper(),
            "bar_token": str(match.group("bar") or ""),
            "fingerprint_suffix": str(match.group("suffix") or "").lower(),
        }
    try:
        from r152_signal_contract import parse_identity_from_id as legacy_parser
        parsed = dict(legacy_parser(text) or {})
    except Exception:
        parsed = {}
    if parsed:
        parsed.setdefault("symbol", str(parsed.get("id_symbol") or parsed.get("symbol") or "").upper())
    return parsed

def identities_match(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    try:
        a = identity_from_mapping(left)
        b = identity_from_mapping(right)
    except EpisodeIdentityError:
        return False
    return a.episode_key_sha256 == b.episode_key_sha256 and a.episode_id == b.episode_id


__all__ = [
    "IDENTITY_CONTRACT_VERSION", "DEFAULT_DETECTOR_CONTRACT_VERSION",
    "DEFAULT_DETECTOR_CONTRACT_HASH",
    "DEFAULT_EPISODE_SIDE", "EpisodeIdentityError", "EpisodeIdentityConflict",
    "EpisodeIdentityV1", "build_episode_identity", "identity_from_mapping",
    "stamp_episode_identity", "ensure_episode_identity", "identities_match", "parse_episode_id",
    "canonical_bar_times", "normalize_timeframe", "normalize_episode_side",
]
