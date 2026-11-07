"""Tests for Zigbee2MQTT payload handling in the Hive coordinator."""

from __future__ import annotations

import json
import logging
from typing import Any

from custom_components.hive_local_thermostat.const import MODEL_SLR1, MODEL_SLR2
from custom_components.hive_local_thermostat.coordinator import HiveCoordinator

from homeassistant.components.climate.const import HVACMode
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.core import HomeAssistant
from homeassistant.util.dt import utcnow

_LOGGER_NAME = "custom_components.hive_local_thermostat"


def _message(payload: dict[str, Any], *, retain: bool = False) -> ReceiveMessage:
    """Build an MQTT message the coordinator can handle."""
    return ReceiveMessage(
        topic="zigbee2mqtt/HiveReceiver",
        payload=json.dumps(payload),
        qos=0,
        retain=retain,
        subscribed_topic="zigbee2mqtt/HiveReceiver",
        timestamp=0.0,
    )


def _coordinator(
    hass: HomeAssistant,
    model: str = MODEL_SLR2,
    entry_id: str = "test-entry",
) -> HiveCoordinator:
    """Return a coordinator that does not publish MQTT."""
    return HiveCoordinator(
        hass,
        entry_id,
        model,
        "zigbee2mqtt/HiveReceiver",
        show_heat_schedule_mode=True,
        show_water_schedule_mode=True,
    )


def _slr2_payload(**overrides: Any) -> dict[str, Any]:
    """Return a complete SLR2 state payload."""
    payload: dict[str, Any] = {
        "local_temperature_heat": 19.5,
        "occupied_heating_setpoint_heat": 21,
        "running_state_heat": "idle",
        "running_state_water": "off",
        "system_mode_heat": "heat",
        "system_mode_water": "off",
        "temperature_setpoint_hold_duration_heat": 0,
        "temperature_setpoint_hold_duration_water": 0,
        "temperature_setpoint_hold_heat": False,
        "temperature_setpoint_hold_water": False,
    }
    payload.update(overrides)
    return payload


def _handle(
    coordinator: HiveCoordinator,
    payload: dict[str, Any],
    caplog: Any,
) -> None:
    """Handle a payload and fail if the coordinator logs its catch-all error."""
    with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
        coordinator.handle_mqtt_message(_message(payload))
    assert "Error handling MQTT message" not in caplog.text


async def test_scheduled_heat_payload_updates_state(
    hass: HomeAssistant, caplog: Any
) -> None:
    """A complete heating payload still sets mode and temperatures."""
    coordinator = _coordinator(hass)

    _handle(coordinator, _slr2_payload(), caplog)

    assert coordinator.hvac_mode == HVACMode.AUTO
    assert coordinator.current_temperature == 19.5
    assert coordinator.target_temperature == 21
    assert coordinator.heat_boost is False
    assert coordinator.heat_boost_remaining == 0


async def test_null_heat_boost_duration_does_not_raise(
    hass: HomeAssistant, caplog: Any
) -> None:
    """A boost state with a null duration must not compare None to an int."""
    coordinator = _coordinator(hass)
    payload = _slr2_payload(
        system_mode_heat="emergency_heating",
        temperature_setpoint_hold_heat=True,
        temperature_setpoint_hold_duration_heat=None,
        occupied_heating_setpoint_heat=25,
        running_state_heat="heat",
    )

    _handle(coordinator, payload, caplog)

    assert coordinator.heat_boost is True
    assert coordinator.hvac_mode == HVACMode.HEAT
    assert coordinator.target_temperature == 25
    assert coordinator.heat_boost_remaining == 0


async def test_missing_heat_boost_duration_uses_tracked_countdown(
    hass: HomeAssistant, caplog: Any
) -> None:
    """When the duration key is absent, keep the countdown started by the boost button."""
    coordinator = _coordinator(hass)
    coordinator.heat_boost_started = utcnow()
    coordinator.heat_boost_started_duration = 90
    payload = _slr2_payload(
        system_mode_heat="emergency_heating",
        temperature_setpoint_hold_heat=True,
    )
    del payload["temperature_setpoint_hold_duration_heat"]

    _handle(coordinator, payload, caplog)

    assert coordinator.heat_boost is True
    # int() truncates a fraction of a minute that elapsed while handling the message.
    assert coordinator.heat_boost_remaining in {89, 90}


async def test_null_duration_keeps_tracked_heat_countdown(
    hass: HomeAssistant, caplog: Any
) -> None:
    """JSON null during an active boost keeps the locally tracked remaining time."""
    coordinator = _coordinator(hass)
    coordinator.heat_boost_started = utcnow()
    coordinator.heat_boost_started_duration = 60
    payload = _slr2_payload(
        system_mode_heat="emergency_heating",
        temperature_setpoint_hold_heat=True,
        temperature_setpoint_hold_duration_heat=None,
    )

    _handle(coordinator, payload, caplog)

    assert coordinator.heat_boost_remaining in {59, 60}


async def test_null_water_boost_duration_does_not_raise(
    hass: HomeAssistant, caplog: Any
) -> None:
    """A water boost with a null duration must not raise."""
    coordinator = _coordinator(hass)
    coordinator.water_boost_started = utcnow()
    coordinator.water_boost_started_duration = 30
    payload = _slr2_payload(
        system_mode_water="emergency_heating",
        temperature_setpoint_hold_water=True,
        temperature_setpoint_hold_duration_water=None,
    )

    _handle(coordinator, payload, caplog)

    assert coordinator.water_boost is True
    assert coordinator.water_mode == "boost"
    assert coordinator.water_boost_remaining in {29, 30}


async def test_non_numeric_boost_duration_does_not_raise(
    hass: HomeAssistant, caplog: Any
) -> None:
    """A non-numeric duration is treated as missing."""
    coordinator = _coordinator(hass)
    payload = _slr2_payload(
        system_mode_heat="emergency_heating",
        temperature_setpoint_hold_heat=True,
        temperature_setpoint_hold_duration_heat="not-a-number",
    )

    _handle(coordinator, payload, caplog)

    assert coordinator.heat_boost is True
    assert coordinator.heat_boost_remaining == 0


async def test_string_boost_duration_is_accepted(
    hass: HomeAssistant, caplog: Any
) -> None:
    """Numeric durations encoded as strings are still applied."""
    coordinator = _coordinator(hass)
    payload = _slr2_payload(
        system_mode_heat="emergency_heating",
        temperature_setpoint_hold_heat=True,
        temperature_setpoint_hold_duration_heat="45",
    )

    _handle(coordinator, payload, caplog)

    assert coordinator.heat_boost_remaining == 45


async def test_reported_boost_minutes_are_stored(
    hass: HomeAssistant, caplog: Any
) -> None:
    """A normal boost duration is stored without correction."""
    coordinator = _coordinator(hass)
    payload = _slr2_payload(
        system_mode_heat="emergency_heating",
        temperature_setpoint_hold_heat=True,
        temperature_setpoint_hold_duration_heat=45,
    )

    _handle(coordinator, payload, caplog)

    assert coordinator.heat_boost_remaining == 45
    assert coordinator.heat_boost is True


async def test_out_of_range_boost_duration_is_corrected_without_raising(
    hass: HomeAssistant, caplog: Any
) -> None:
    """The known 65535 duration bug is still corrected when no config entry exists."""
    coordinator = _coordinator(hass)
    payload = _slr2_payload(
        system_mode_heat="emergency_heating",
        temperature_setpoint_hold_heat=True,
        temperature_setpoint_hold_duration_heat=65535,
    )

    _handle(coordinator, payload, caplog)

    assert coordinator.heat_boost_remaining == 0


async def test_slr1_null_boost_duration_does_not_raise(
    hass: HomeAssistant, caplog: Any
) -> None:
    """SLR1/OTR1 payloads with a null hold duration must not raise."""
    coordinator = _coordinator(hass, MODEL_SLR1)
    payload = {
        "local_temperature": 18,
        "occupied_heating_setpoint": 22,
        "running_state": "heat",
        "system_mode": "emergency_heating",
        "temperature_setpoint_hold": True,
        "temperature_setpoint_hold_duration": None,
    }

    _handle(coordinator, payload, caplog)

    assert coordinator.heat_boost is True
    assert coordinator.current_temperature == 18
    assert coordinator.target_temperature == 22
    assert coordinator.heat_boost_remaining == 0


async def test_null_temperatures_do_not_raise(hass: HomeAssistant, caplog: Any) -> None:
    """Null temperature fields are stored as unknown instead of raising."""
    coordinator = _coordinator(hass)
    payload = _slr2_payload(
        local_temperature_heat=None,
        occupied_heating_setpoint_heat=None,
    )

    _handle(coordinator, payload, caplog)

    assert coordinator.current_temperature is None
    assert coordinator.target_temperature is None
    assert coordinator.hvac_mode == HVACMode.AUTO
