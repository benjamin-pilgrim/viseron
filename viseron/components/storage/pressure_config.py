"""Normalize the simple per-tier retention and full-storage policy."""

import copy
import math
import re
from decimal import Decimal, InvalidOperation
from typing import Any

import voluptuous as vol

from viseron.components.storage.util import calculate_age, calculate_bytes

PERCENT_SCALE = 100

POLICIES = ("oldest_first", "ordinary_first", "preserve_events")


def _limit(value, units):
    if isinstance(value, bool):
        raise vol.Invalid("Retention limits must be positive")
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([A-Za-z]+)\s*", value)
        if not match or match[2].lower() not in units:
            raise vol.Invalid("Unsupported retention limit or unit")
        number = float(match[1]) * units[match[2].lower()]
    else:
        raise vol.Invalid("Retention limits need a number and unit")
    if not math.isfinite(number) or number <= 0 or number != int(number):
        raise vol.Invalid("Retention limits must be positive whole base units")
    return int(number)


def _size_limit(value):
    if isinstance(value, str) and value.strip().endswith("%"):
        match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)%\s*", value)
        if not match:
            raise vol.Invalid("Percentage size limits need a number followed by %")
        number = Decimal(match[1])
        if not 0 < number <= PERCENT_SCALE:
            raise vol.Invalid(
                "Percentage size limits must be greater than 0 and at most 100"
            )
        return f"{number.normalize():f}%"
    return _limit(
        value,
        {
            "kb": 1000,
            "mb": 1000**2,
            "gb": 1000**3,
            "tb": 1000**4,
            "kib": 1024,
            "mib": 1024**2,
            "gib": 1024**3,
            "tib": 1024**4,
        },
    )


def resolve_size_limit(value: int | str, capacity: int) -> int:
    """Resolve percentages from total filesystem capacity in its storage worker."""
    if isinstance(value, str):
        if capacity <= 0:
            raise ValueError("Cannot resolve a percentage without filesystem capacity")
        return max(1, int(Decimal(value.removesuffix("%")) * capacity / PERCENT_SCALE))
    return value


def validate_retention_size(value: Any) -> Any:
    """Validate one retention size before schema coercion can obscure its units."""
    if not isinstance(value, dict) or "percent" not in value:
        return value
    if any(value.get(unit) is not None for unit in ("gb", "mb")):
        raise vol.Invalid("Use percent or gb/mb for a size limit, not both")
    try:
        percent = format(Decimal(str(value["percent"])), "f")
    except InvalidOperation as error:
        raise vol.Invalid("Percentage size limits need a finite number") from error
    return {**value, "percent": float(_size_limit(f"{percent}%")[:-1])}


def normalize_tier(value):
    """Keep old configs unchanged; simple limits enable both recording classes."""
    tier = copy.deepcopy(value)
    if "on_full" in tier:
        tier["on_full"] = vol.Schema({vol.Required("policy"): vol.In(POLICIES)})(
            tier["on_full"]
        )
    for kind in ("continuous", "events"):
        rules = tier.get(kind)
        if not isinstance(rules, dict):
            continue
        for limit in ("max_size", "min_size"):
            if limit in rules:
                rules[limit] = validate_retention_size(rules[limit])
    if "retention" not in tier:
        return tier
    retention = vol.Schema(
        {vol.Optional("max_age"): object, vol.Optional("max_size"): object}
    )(tier["retention"])
    normalized = {}
    if "max_age" in retention:
        normalized["max_age"] = _limit(
            retention["max_age"], {"m": 60, "h": 3600, "d": 86400, "w": 604800}
        )
        if normalized["max_age"] % 60:
            raise vol.Invalid("max_age must be a whole number of minutes")
    if "max_size" in retention:
        normalized["max_size"] = _size_limit(retention["max_size"])
    # A normalized camera override may pass through the schema twice.
    for kind in ("continuous", "events"):
        expected = {}
        if "max_age" in normalized:
            expected["max_age"] = {"minutes": normalized["max_age"] // 60}
        # Size is owned by the root pressure controller, not one allowance
        # per camera or recording class. Age continues through normal tiers.
        if kind in tier:
            old = tier[kind]
            if not isinstance(old, dict) or not {
                "max_age",
                "max_size",
                "min_age",
                "min_size",
            }.issubset(old):
                raise vol.Invalid(
                    "Use retention or continuous/events settings, not both"
                )
            if (
                calculate_age(old["max_age"]).total_seconds()
                != normalized.get("max_age", 0)
                or calculate_bytes(old["max_size"])
                or calculate_age(old["min_age"]).total_seconds()
                or calculate_bytes(old["min_size"])
                or old["max_size"].get("percent")
                or old["min_size"].get("percent")
            ):
                raise vol.Invalid(
                    "Use retention or continuous/events settings, not both"
                )
        tier[kind] = expected
    tier["retention"] = normalized
    tier.setdefault("on_full", {"policy": "preserve_events"})
    return tier
