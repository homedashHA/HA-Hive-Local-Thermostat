"""Real local MQTT broker test for heating boost deadlines.

The broker listens only on 127.0.0.1. Topics are lab/* and there are no
credentials, home brokers, or thermostat devices. The deadline uses the same
minute timer as a 15 minute boost, shortened to 1 minute so the run is bounded.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from pathlib import Path
from typing import Any

import pytest
import pytest_socket
from custom_components.hive_local_thermostat.const import (
    CONF_MODEL,
    CONF_MQTT_TOPIC,
    CONF_SHOW_HEAT_SCHEDULE_MODE,
    CONF_SHOW_WATER_SCHEDULE_MODE,
    MODEL_SLR2,
)
from paho.mqtt.client import Client
from paho.mqtt.enums import CallbackAPIVersion
from pytest_homeassistant_custom_component.common import MockConfigEntry

from homeassistant.components.mqtt.const import CONF_BROKER, CONF_DISCOVERY
from homeassistant.const import CONF_PORT
from homeassistant.core import HomeAssistant
from homeassistant.loader import DATA_CUSTOM_COMPONENTS

_PORT = 18883
_DEADLINE_MINUTES = 1
_BOOST_TEMPERATURE = 25


def _state(
    *,
    mode: str,
    setpoint: float,
    hold: bool,
    duration: Any = 0,
    include_duration: bool = True,
) -> str:
    """Return one SLR2 state document."""
    payload: dict[str, Any] = {
        "local_temperature_heat": 19.0,
        "occupied_heating_setpoint_heat": setpoint,
        "running_state_heat": "idle",
        "running_state_water": "off",
        "system_mode_heat": mode,
        "system_mode_water": "off",
        "temperature_setpoint_hold_heat": hold,
        "temperature_setpoint_hold_water": False,
        "temperature_setpoint_hold_duration_water": 0,
    }
    if include_duration:
        payload["temperature_setpoint_hold_duration_heat"] = duration
    return json.dumps(payload)


class _BrokerCapture:
    """Background subscriber for lab set topics."""

    def __init__(self) -> None:
        """Connect to the local broker."""
        self.messages: list[tuple[float, str, str]] = []
        self._client = Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id="hive-lab-observer",
        )
        self._client.on_message = self._on_message
        self._client.connect("127.0.0.1", _PORT, keepalive=30)
        self._client.subscribe("lab/#", qos=0)
        self._thread = threading.Thread(target=self._client.loop_forever, daemon=True)
        self._thread.start()

    def _on_message(self, _client: Any, _userdata: Any, message: Any) -> None:
        """Record one received publish."""
        self.messages.append(
            (time.monotonic(), message.topic, message.payload.decode())
        )

    def close(self) -> None:
        """Disconnect the observer."""
        self._client.disconnect()
        self._thread.join(timeout=5)

    def sets(self, topic: str) -> list[dict[str, Any]]:
        """Return parsed documents published to one set topic."""
        suffix = f"{topic}/set"
        return [
            json.loads(payload)
            for _stamp, item_topic, payload in self.messages
            if item_topic == suffix
        ]


def _ensure_configuration_yaml(config_dir: str) -> None:
    """MQTT setup reads configuration.yaml even when broker settings are in the entry."""
    path = Path(config_dir) / "configuration.yaml"
    if not path.exists():
        path.write_text("\n", encoding="utf-8")


def _install_component(hass: HomeAssistant) -> None:
    """Expose this repo's integration to the temporary Home Assistant config."""
    source = (
        Path(__file__).resolve().parents[1]
        / "custom_components"
        / "hive_local_thermostat"
    )
    destination = (
        Path(hass.config.config_dir) / "custom_components" / "hive_local_thermostat"
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not destination.exists():
        destination.symlink_to(source)
    hass.data.pop(DATA_CUSTOM_COMPONENTS, None)


def _hive_entry(entry_id: str, topic: str) -> MockConfigEntry:
    """Return one lab thermostat entry."""
    return MockConfigEntry(
        domain="hive_local_thermostat",
        entry_id=entry_id,
        title=entry_id,
        unique_id=entry_id,
        options={
            CONF_MQTT_TOPIC: topic,
            CONF_MODEL: MODEL_SLR2,
            CONF_SHOW_HEAT_SCHEDULE_MODE: True,
            CONF_SHOW_WATER_SCHEDULE_MODE: False,
        },
    )


async def _setup_entry(hass: HomeAssistant, entry: MockConfigEntry) -> None:
    """Add and start a config entry."""
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()


async def _wait_for(
    hass: HomeAssistant,
    predicate: Any,
    limit_seconds: float,
) -> bool:
    """Poll until predicate is true or the limit expires."""
    deadline = time.monotonic() + limit_seconds
    while time.monotonic() < deadline:
        await hass.async_block_till_done()
        if predicate():
            return True
        await asyncio.sleep(0.2)
    await hass.async_block_till_done()
    return bool(predicate())


def _emergency_count(payloads: list[dict[str, Any]]) -> int:
    """Count boost commands."""
    return sum(item.get("system_mode_heat") == "emergency_heating" for item in payloads)


def _writes_boost_temperature(payload: dict[str, Any]) -> bool:
    """Return whether a document sets the boost temperature."""
    value = payload.get("occupied_heating_setpoint_heat")
    return (
        isinstance(value, (int, float))
        and abs(float(value) - _BOOST_TEMPERATURE) < 0.05
    )


def _turns_off(payload: dict[str, Any]) -> bool:
    """Return whether a document turns heating off."""
    return payload.get("system_mode_heat") == "off"


@pytest.mark.timeout(180)
@pytest.mark.enable_socket
async def test_real_broker_deadline_restore_and_reload(  # noqa: PLR0915
    hass: HomeAssistant,
    enable_custom_integrations: None,
    hass_storage: dict[str, Any],
) -> None:
    """Exercise timer, publish, reload, and storage through the MQTT integration."""
    del enable_custom_integrations
    pytest_socket.enable_socket()
    _ensure_configuration_yaml(hass.config.config_dir)
    _install_component(hass)
    capture = _BrokerCapture()
    try:
        mqtt_entry = MockConfigEntry(
            domain="mqtt",
            title="lab-broker",
            data={
                CONF_BROKER: "127.0.0.1",
                CONF_PORT: _PORT,
                CONF_DISCOVERY: False,
            },
            version=1,
            minor_version=2,
        )
        await _setup_entry(hass, mqtt_entry)

        entries = {
            "hold": _hive_entry("labhold0001", "lab/hold"),
            "schedule": _hive_entry("labsched0001", "lab/schedule"),
            "manual": _hive_entry("labmanual0001", "lab/manual"),
            "duration": _hive_entry("labduration01", "lab/duration"),
            "unload": _hive_entry("labunload001", "lab/unload"),
            "restart": _hive_entry("labrestart01", "lab/restart"),
            "repeat": _hive_entry("labrepeat001", "lab/repeat"),
        }
        for entry in entries.values():
            await _setup_entry(hass, entry)

        client = Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id="hive-lab-publisher",
        )
        client.connect("127.0.0.1", _PORT, keepalive=30)
        client.loop_start()

        def publish_state(
            topic: str,
            *,
            mode: str,
            setpoint: float,
            hold: bool,
            duration: Any = 0,
            include_duration: bool = True,
            retain: bool = False,
        ) -> None:
            """Publish a device state with no home topic."""
            client.publish(
                topic,
                _state(
                    mode=mode,
                    setpoint=setpoint,
                    hold=hold,
                    duration=duration,
                    include_duration=include_duration,
                ),
                retain=retain,
            )

        publish_state("lab/hold", mode="heat", setpoint=20, hold=True)
        publish_state("lab/schedule", mode="heat", setpoint=18, hold=False)
        publish_state("lab/manual", mode="heat", setpoint=21, hold=True)
        publish_state("lab/duration", mode="heat", setpoint=20, hold=True)
        publish_state("lab/unload", mode="heat", setpoint=20, hold=True)
        publish_state("lab/restart", mode="heat", setpoint=20, hold=True)
        publish_state("lab/repeat", mode="heat", setpoint=20, hold=True)

        for entry in entries.values():
            assert await _wait_for(
                hass,
                lambda entry_id=entry.entry_id: _primed_now(hass, entry_id),
                10,
            )

        started = time.monotonic()
        for name in (
            "hold",
            "schedule",
            "manual",
            "duration",
            "unload",
            "restart",
            "repeat",
        ):
            coordinator = entries[name].runtime_data.coordinator
            coordinator.heating_boost_duration = _DEADLINE_MINUTES
            coordinator.heating_boost_temperature = _BOOST_TEMPERATURE
            await coordinator.async_heating_boost()
            if name == "hold":
                await coordinator.async_heating_boost()
                await coordinator.async_heating_boost()

        await hass.async_block_till_done()
        assert await _wait_for(
            hass,
            lambda: (
                _emergency_count(capture.sets("lab/hold")) == 1
                and _emergency_count(capture.sets("lab/repeat")) == 1
            ),
            5,
        ), capture.messages

        await coordinator_set_temperature(entries["manual"])
        assert await _wait_for(
            hass,
            lambda: any(
                item.get("occupied_heating_setpoint_heat") == 20
                and "system_mode_heat" not in item
                for item in capture.sets("lab/manual")
            ),
            5,
        ), capture.sets("lab/manual")

        publish_state(
            "lab/duration",
            mode="emergency_heating",
            setpoint=25,
            hold=True,
            include_duration=False,
        )
        publish_state(
            "lab/duration",
            mode="emergency_heating",
            setpoint=25,
            hold=True,
            duration=None,
        )
        publish_state(
            "lab/duration",
            mode="emergency_heating",
            setpoint=25,
            hold=True,
            duration=65535,
        )
        await hass.async_block_till_done()
        assert _emergency_count(capture.sets("lab/duration")) == 1

        await hass.config_entries.async_unload(entries["unload"].entry_id)
        await hass.async_block_till_done()

        await hass.config_entries.async_unload(entries["restart"].entry_id)
        await hass.async_block_till_done()
        publish_state(
            "lab/restart",
            mode="emergency_heating",
            setpoint=25,
            hold=True,
            duration=65535,
            retain=True,
        )
        await _setup_entry(hass, entries["restart"])
        await asyncio.sleep(2)
        await hass.async_block_till_done()
        assert _emergency_count(capture.sets("lab/restart")) == 1
        assert not any(
            item.get("temperature_setpoint_hold_duration_heat") == "0"
            for item in capture.sets("lab/restart")
        )
        publish_state(
            "lab/restart",
            mode="emergency_heating",
            setpoint=25,
            hold=True,
            duration=None,
            retain=False,
        )

        await asyncio.sleep(6)
        repeat = entries["repeat"].runtime_data.coordinator
        await repeat.async_heating_boost()
        await hass.async_block_till_done()
        assert await _wait_for(
            hass, lambda: _emergency_count(capture.sets("lab/repeat")) == 2, 5
        ), capture.sets("lab/repeat")

        def restores_done() -> bool:
            hold_done = any(
                item.get("occupied_heating_setpoint_heat") == 20
                and item.get("temperature_setpoint_hold_heat") == "1"
                for item in capture.sets("lab/hold")
            )
            schedule_done = any(
                item.get("temperature_setpoint_hold_heat") == "0"
                and "occupied_heating_setpoint_heat" not in item
                for item in capture.sets("lab/schedule")
            )
            duration_done = any(
                item.get("occupied_heating_setpoint_heat") == 20
                and item.get("temperature_setpoint_hold_heat") == "1"
                for item in capture.sets("lab/duration")
            )
            restart_done = any(
                item.get("occupied_heating_setpoint_heat") == 20
                and item.get("temperature_setpoint_hold_heat") == "1"
                for item in capture.sets("lab/restart")
            )
            repeat_done = any(
                item.get("occupied_heating_setpoint_heat") == 20
                and item.get("temperature_setpoint_hold_heat") == "1"
                for item in capture.sets("lab/repeat")
            )
            return all(
                (hold_done, schedule_done, duration_done, restart_done, repeat_done)
            )

        assert await _wait_for(hass, restores_done, 80)

        elapsed = time.monotonic() - started
        assert elapsed >= 60

        hold_sets = capture.sets("lab/hold")
        assert _emergency_count(hold_sets) == 1
        assert hold_sets[-2]["occupied_heating_setpoint_heat"] == 20
        assert hold_sets[-2]["temperature_setpoint_hold_heat"] == "1"
        assert all(not _turns_off(item) for item in hold_sets)

        schedule_sets = capture.sets("lab/schedule")
        assert _emergency_count(schedule_sets) == 1
        assert schedule_sets[-1] == {
            "system_mode_heat": "heat",
            "temperature_setpoint_hold_heat": "0",
            "temperature_setpoint_hold_duration_heat": "0",
        }
        assert all(
            not _writes_boost_temperature(item)
            or item.get("system_mode_heat") == "emergency_heating"
            for item in schedule_sets
        )

        manual_sets = capture.sets("lab/manual")
        assert _emergency_count(manual_sets) == 1
        assert not any(
            item.get("occupied_heating_setpoint_heat") == 21
            for item in manual_sets
            if item.get("system_mode_heat") == "heat"
        )
        assert all(not _turns_off(item) for item in manual_sets)
        assert all(
            not _writes_boost_temperature(item)
            or item.get("system_mode_heat") == "emergency_heating"
            for item in manual_sets
        )

        duration_sets = capture.sets("lab/duration")
        assert _emergency_count(duration_sets) == 1
        assert any(
            item.get("occupied_heating_setpoint_heat") == 20
            and item.get("temperature_setpoint_hold_heat") == "1"
            for item in duration_sets
        )

        unload_sets = capture.sets("lab/unload")
        assert _emergency_count(unload_sets) == 1
        assert all(
            item.get("system_mode_heat") == "emergency_heating" for item in unload_sets
        )

        restart_sets = capture.sets("lab/restart")
        assert _emergency_count(restart_sets) == 1
        assert (
            sum(
                item.get("occupied_heating_setpoint_heat") == 20
                and item.get("temperature_setpoint_hold_heat") == "1"
                for item in restart_sets
            )
            == 1
        )

        repeat_sets = capture.sets("lab/repeat")
        assert _emergency_count(repeat_sets) == 2
        assert (
            sum(
                item.get("temperature_setpoint_hold_heat") == "1"
                and item.get("occupied_heating_setpoint_heat") == 20
                for item in repeat_sets
            )
            == 1
        )

        session_keys = [
            key
            for key in hass_storage
            if key.startswith("hive_local_thermostat.heating_boost.")
        ]
        assert len(session_keys) == len(entries)
        assert len(set(session_keys)) == len(session_keys)
        stored_ids = {hass_storage[key]["data"]["entry_id"] for key in session_keys}
        assert stored_ids == {entry.entry_id for entry in entries.values()}
        assert all("/" not in key and ".." not in key for key in session_keys)

    finally:
        if "client" in locals():
            client.loop_stop()
            client.disconnect()
        capture.close()


def _primed_now(hass: HomeAssistant, entry_id: str) -> bool:
    """Return whether the coordinator has seen a heating mode."""
    entry = hass.config_entries.async_get_entry(entry_id)
    if entry is None or entry.runtime_data is None:
        return False
    return entry.runtime_data.coordinator.hvac_mode is not None


async def coordinator_set_temperature(entry: MockConfigEntry) -> None:
    """Publish the manual setpoint that must block a later restore."""
    await entry.runtime_data.coordinator.async_set_temperature(20)
