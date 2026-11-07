"""One-shot heating boost restore.

The receiver hold duration is not the deadline. Zigbee2MQTT may replace a
finite duration with null or 65535, both of which mean the attribute is
unused, and the boost then runs until something else changes the setpoint.
A session stores the first pre-boost mode and setpoint, and a Home Assistant
timer publishes at most one restore.

This does not guarantee the house returns to the previous state:

- A boost started on the thermostat or another MQTT client has no snapshot
  and is left alone.
- A change made only on the thermostat while no state message arrives can be
  missed. The restore then writes the saved pre-boost setpoint, never the
  boost temperature.
- Unknown mode or setpoint never starts a boost, turns heating off, or
  rewrites a setpoint.
- After a restart, nothing is published until a state message for this device
  arrives, and nothing is published unless that message is still the commanded
  boost. A stale or already-changed report is left untouched.
- The session is marked finished and saved before the restore publish. A crash
  in that gap skips the restore; the next start does not send a second one.
- Explicit cancel after a manual setpoint change publishes nothing, so it
  cannot write the boost temperature back or turn heating off. The receiver
  can stay in emergency heating until another control changes the mode.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from homeassistant.components.climate.const import HVACMode

from .const import DOMAIN

HEATING_BOOST_MODE = "emergency_heating"
_SETPOINT_TOLERANCE = 0.05
_MODES = {HVACMode.HEAT, HVACMode.AUTO, HVACMode.OFF}


_SAFE_ENTRY_ID = re.compile(r"^[A-Za-z0-9_-]{1,80}$")


def heating_boost_storage_key(entry_id: str) -> str:
    """Return one storage file name for one config entry.

    The suffix is a single path segment. Unsafe ids are hashed so two devices
    cannot share a file or write outside the storage directory.
    """
    if _SAFE_ENTRY_ID.fullmatch(entry_id):
        suffix = entry_id
    else:
        digest = hashlib.sha256(entry_id.encode()).hexdigest()[:16]
        suffix = f"id-{digest}"
    return f"{DOMAIN}.heating_boost.{suffix}"


def can_restore_snapshot(mode: HVACMode | None, setpoint: float | None) -> bool:
    """Return whether a later restore can undo a boost without guessing."""
    if mode == HVACMode.AUTO:
        return True
    if mode in {HVACMode.HEAT, HVACMode.OFF}:
        return setpoint is not None
    return False


def resolved_boost_minutes(requested: float | None, default: float) -> int:
    """Return a duration in minutes.

    None uses the configured default. Zero stays zero so a stop is not turned
    into a new full-length boost.
    """
    if requested is None:
        return int(default)
    return int(requested)


def temperatures_close(left: float | None, right: float | None) -> bool:
    """Return whether two setpoints are the same Hive step."""
    if left is None or right is None:
        return False
    return abs(float(left) - float(right)) < _SETPOINT_TOLERANCE


@dataclass
class HeatingBoostSession:
    """Saved heating boost that this integration started."""

    entry_id: str
    pre_boost_hvac_mode: HVACMode | None
    pre_boost_setpoint: float | None
    commanded_temperature: float
    duration_minutes: int
    started_at: datetime
    deadline: datetime
    token: str
    saw_commanded_setpoint: bool = False
    superseded: bool = False
    finished: bool = False
    loaded_from_storage: bool = False

    def to_storage(self) -> dict[str, Any]:
        """Return JSON storage for this session."""
        mode = (
            None if self.pre_boost_hvac_mode is None else self.pre_boost_hvac_mode.value
        )
        return {
            "entry_id": self.entry_id,
            "pre_boost_hvac_mode": mode,
            "pre_boost_setpoint": self.pre_boost_setpoint,
            "commanded_temperature": self.commanded_temperature,
            "duration_minutes": self.duration_minutes,
            "started_at": self.started_at.isoformat(),
            "deadline": self.deadline.isoformat(),
            "token": self.token,
            "saw_commanded_setpoint": self.saw_commanded_setpoint,
            "superseded": self.superseded,
            "finished": self.finished,
        }


def _corrupt(detail: str) -> ValueError:
    """Build the error used for a storage document this integration cannot trust."""
    message = f"Corrupt heating boost session: {detail}"
    return ValueError(message)


def session_from_storage(raw: Any, *, entry_id: str) -> HeatingBoostSession:
    """Parse a stored session or raise ValueError."""
    if not isinstance(raw, dict):
        raise _corrupt("not an object")  # noqa: EM101
    if raw.get("entry_id") != entry_id:
        raise _corrupt("belongs to another device")  # noqa: EM101
    mode_raw = raw.get("pre_boost_hvac_mode")
    if mode_raw is None:
        mode = None
    elif mode_raw in {item.value for item in _MODES}:
        mode = HVACMode(mode_raw)
    else:
        raise _corrupt("mode is invalid")  # noqa: EM101
    return HeatingBoostSession(
        entry_id=entry_id,
        pre_boost_hvac_mode=mode,
        pre_boost_setpoint=_optional_temperature(raw.get("pre_boost_setpoint")),
        commanded_temperature=_required_temperature(raw.get("commanded_temperature")),
        duration_minutes=_required_duration(raw.get("duration_minutes")),
        started_at=_required_datetime(raw.get("started_at")),
        deadline=_required_datetime(raw.get("deadline")),
        token=_required_token(raw.get("token")),
        saw_commanded_setpoint=_required_bool(raw.get("saw_commanded_setpoint")),
        superseded=_required_bool(raw.get("superseded")),
        finished=_required_bool(raw.get("finished")),
    )


def note_device_report(
    session: HeatingBoostSession,
    setpoint: float | None,
) -> None:
    """Record a device setpoint against an open session.

    The report that still shows the pre-boost setpoint is not a manual change.
    A later report that leaves the commanded boost temperature is.
    """
    if session.finished or setpoint is None:
        return
    if temperatures_close(setpoint, session.commanded_temperature):
        session.saw_commanded_setpoint = True
        return
    if session.saw_commanded_setpoint:
        session.superseded = True
        session.finished = True
        return
    if session.pre_boost_setpoint is not None and not temperatures_close(
        setpoint, session.pre_boost_setpoint
    ):
        session.superseded = True
        session.finished = True


def decide_heating_restore(  # noqa: PLR0911
    session: HeatingBoostSession,
    *,
    fresh_state_seen: bool,
    system_mode: str | None,
    setpoint: float | None,
) -> str:
    """Return wait, skip, heat, auto, or off.

    wait means a restarted session has not seen a device report yet.
    skip publishes nothing.
    """
    if session.finished or session.superseded:
        return "skip"
    if session.loaded_from_storage and not fresh_state_seen:
        return "wait"
    if session.pre_boost_hvac_mode not in _MODES:
        return "skip"
    if (
        session.pre_boost_hvac_mode in {HVACMode.HEAT, HVACMode.OFF}
        and session.pre_boost_setpoint is None
    ):
        return "skip"
    if session.pre_boost_hvac_mode == HVACMode.HEAT and temperatures_close(
        session.pre_boost_setpoint, session.commanded_temperature
    ):
        # Writing this setpoint would reaffirm the boost temperature.
        return "skip"

    if session.loaded_from_storage:
        return _decide_restart_restore(session, system_mode, setpoint)
    return _decide_live_restore(session, system_mode, setpoint)


def _decide_restart_restore(
    session: HeatingBoostSession,
    system_mode: str | None,
    setpoint: float | None,
) -> str:
    """Restore a restarted session only while the device is still on that boost."""
    if system_mode != HEATING_BOOST_MODE:
        return "skip"
    if not temperatures_close(setpoint, session.commanded_temperature):
        return "skip"
    if (
        session.pre_boost_hvac_mode == HVACMode.OFF
        and not session.saw_commanded_setpoint
    ):
        return "skip"
    return _action_for_mode(session)


def _decide_live_restore(
    session: HeatingBoostSession,
    system_mode: str | None,
    setpoint: float | None,
) -> str:
    """Restore a session that was started in this process."""
    if setpoint is not None and not temperatures_close(
        setpoint, session.commanded_temperature
    ):
        stale_pre_boost = not session.saw_commanded_setpoint and temperatures_close(
            setpoint, session.pre_boost_setpoint
        )
        if not stale_pre_boost:
            return "skip"
    if (
        session.saw_commanded_setpoint
        and system_mode not in (None, HEATING_BOOST_MODE)
        and temperatures_close(setpoint, session.pre_boost_setpoint)
    ):
        # The receiver already left boost and restored the old setpoint.
        return "skip"
    return _action_for_mode(session)


def _action_for_mode(session: HeatingBoostSession) -> str:
    """Map a known pre-boost mode to a restore publish."""
    if session.pre_boost_hvac_mode == HVACMode.AUTO:
        return "auto"
    if session.pre_boost_hvac_mode == HVACMode.HEAT:
        return "heat"
    if session.pre_boost_hvac_mode == HVACMode.OFF:
        return "off"
    return "skip"


def _optional_temperature(value: Any) -> float | None:
    if value is None:
        return None
    return _required_temperature(value)


def _required_temperature(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _corrupt("temperature is invalid")  # noqa: EM101
    return float(value)


def _required_duration(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _corrupt("duration is invalid")  # noqa: EM101
    duration = int(value)
    if duration <= 0:
        raise _corrupt("duration is invalid")  # noqa: EM101
    return duration


def _required_datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise _corrupt("time is invalid")  # noqa: EM101
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as err:
        raise _corrupt("time is invalid") from err  # noqa: EM101
    if parsed.tzinfo is None:
        raise _corrupt("time is not timezone-aware")  # noqa: EM101
    return parsed


def _required_token(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise _corrupt("token is invalid")  # noqa: EM101
    return value


def _required_bool(value: Any) -> bool:
    if not isinstance(value, bool):
        raise _corrupt("flag is invalid")  # noqa: EM101
    return value
