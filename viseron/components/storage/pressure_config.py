"""Normalize the simple per-tier retention and full-storage policy."""

import copy
import math
import re

import voluptuous as vol

from viseron.components.storage.util import calculate_age, calculate_bytes

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


def normalize_tier(value):
    """Keep old configs unchanged; simple limits enable both recording classes."""
    tier = copy.deepcopy(value)
    if "on_full" in tier:
        tier["on_full"] = vol.Schema({vol.Required("policy"): vol.In(POLICIES)})(
            tier["on_full"]
        )
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
        normalized["max_size"] = _limit(
            retention["max_size"],
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
            ):
                raise vol.Invalid(
                    "Use retention or continuous/events settings, not both"
                )
        tier[kind] = expected
    tier["retention"] = normalized
    tier.setdefault("on_full", {"policy": "preserve_events"})
    return tier
