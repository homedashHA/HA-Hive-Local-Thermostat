"""Fake-MQTT tests for the heating boost deadline and restore."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from custom_components.hive_local_thermostat.const import (
    MODEL_SLR1,
    MODEL_SLR2,
)
from custom_components.hive_local_thermostat.coordinator import HiveCoordinator
from custom_components.hive_local_thermostat.heating_boost import (
    heating_boost_storage_key,
)
from pytest_homeassistant_custom_component.common import async_fire_time_changed

from homeassistant.components.climate.const import HVACMode
from homeassistant.helpers.storage import Store
from tests.test_coordinator import _coordinator, _handle, _message, _slr2_payload

_BOOST_MINUTES = 15
_BOOST_TEMPERATURE = 25


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> dict[str, datetime]:
    """Control the coordinator clock without moving the asyncio monotonic clock."""
    state = {"now": datetime.now(UTC).replace(microsecond=0)}
    monkeypatch.setattr(
        "custom_components.hive_local_thermostat.coordinator.utcnow",
        lambda: state["now"],
    )
    return state


@pytest.fixture
def published(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    """Capture MQTT publishes and skip the heat-mode follow-up delay."""
    sent: list[tuple[str, str]] = []

    async def _publish(
        _hass: Any, topic: str, payload: str, *_args: Any, **_kwargs: Any
    ) -> None:
        sent.append((topic, payload))

    async def _sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(
        "custom_components.hive_local_thermostat.coordinator.mqtt_client.async_publish",
        _publish,
    )
    monkeypatch.setattr(
        "custom_components.hive_local_thermostat.coordinator.sleep",
        _sleep,
    )
    return sent


def _payloads(sent: list[tuple[str, str]]) -> list[dict[str, Any]]:
    """Return parsed publish documents."""
    return [json.loads(payload) for _topic, payload in sent]


def _heating_messages(sent: list[tuple[str, str]], topic: str) -> list[dict[str, Any]]:
    """Return parsed heating documents published to one set topic."""
    return [json.loads(payload) for item_topic, payload in sent if item_topic == topic]


async def _advance(hass: Any, clock: dict[str, datetime], seconds: float) -> None:
    """Move the coordinator clock and fire due timers."""
    clock["now"] = clock["now"] + timedelta(seconds=seconds)
    async_fire_time_changed(hass, clock["now"])
    await hass.async_block_till_done()
    await hass.async_block_till_done()


def _prime(
    coordinator: HiveCoordinator,
    caplog: Any,
    *,
    mode: str = "heat",
    setpoint: float = 20,
    hold: bool = True,
) -> None:
    """Put the coordinator in a known heating state."""
    _handle(
        coordinator,
        _slr2_payload(
            system_mode_heat=mode,
            occupied_heating_setpoint_heat=setpoint,
            temperature_setpoint_hold_heat=hold,
            running_state_heat="idle",
        ),
        caplog,
    )


async def _boost(
    coordinator: HiveCoordinator,
    *,
    minutes: int = _BOOST_MINUTES,
    temperature: float = _BOOST_TEMPERATURE,
) -> None:
    """Start a heating boost with an explicit duration and temperature."""
    coordinator.heating_boost_duration = minutes
    coordinator.heating_boost_temperature = temperature
    await coordinator.async_heating_boost(minutes, temperature)


def _contains_boost_temperature(payload: dict[str, Any]) -> bool:
    """Return whether a document writes the commanded boost temperature."""
    for key in ("occupied_heating_setpoint_heat", "occupied_heating_setpoint"):
        value = payload.get(key)
        if (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and abs(float(value) - _BOOST_TEMPERATURE) < 0.05
        ):
            return True
    return False


def _turns_heating_off(payload: dict[str, Any]) -> bool:
    """Return whether a document sets heating off."""
    return (
        payload.get("system_mode_heat") == "off" or payload.get("system_mode") == "off"
    )


def _write_corrupt_storage(path: str) -> None:
    """Write an unreadable storage file outside the async test body."""
    storage_path = Path(path)
    storage_path.parent.mkdir(parents=True, exist_ok=True)
    storage_path.write_text("{", encoding="utf-8")


async def test_deadline_restores_manual_hold_without_mqtt(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """A 15 minute boost ends on the timer when no further MQTT arrives."""
    coordinator = _coordinator(hass)
    _prime(coordinator, caplog, setpoint=20, hold=True)

    await _boost(coordinator)
    assert len(published) == 1
    assert _payloads(published)[0]["system_mode_heat"] == "emergency_heating"
    assert _payloads(published)[0]["temperature_setpoint_hold_duration_heat"] == 15

    await _advance(hass, clock, _BOOST_MINUTES * 60)

    restored = _payloads(published)[1:]
    assert restored[0]["system_mode_heat"] == "heat"
    assert restored[0]["occupied_heating_setpoint_heat"] == 20
    assert restored[0]["temperature_setpoint_hold_heat"] == "1"
    assert restored[0]["temperature_setpoint_hold_duration_heat"] == "0"
    assert all(not _turns_heating_off(item) for item in restored)
    await _advance(hass, clock, 60)
    assert len(published) == 1 + len(restored)


async def test_null_missing_and_65535_do_not_publish_another_boost(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """Missing, null, and 65535 durations leave the local deadline in charge."""
    coordinator = _coordinator(hass)
    _prime(coordinator, caplog)
    await _boost(coordinator)

    missing = _slr2_payload(
        system_mode_heat="emergency_heating",
        occupied_heating_setpoint_heat=25,
        temperature_setpoint_hold_heat=True,
        running_state_heat="heat",
    )
    del missing["temperature_setpoint_hold_duration_heat"]
    _handle(coordinator, missing, caplog)
    await hass.async_block_till_done()
    assert len(published) == 1
    assert coordinator.heat_boost_remaining == _BOOST_MINUTES

    _handle(
        coordinator,
        _slr2_payload(
            system_mode_heat="emergency_heating",
            occupied_heating_setpoint_heat=25,
            temperature_setpoint_hold_heat=True,
            temperature_setpoint_hold_duration_heat=None,
            running_state_heat="heat",
        ),
        caplog,
    )
    await hass.async_block_till_done()
    assert len(published) == 1

    _handle(
        coordinator,
        _slr2_payload(
            system_mode_heat="emergency_heating",
            occupied_heating_setpoint_heat=25,
            temperature_setpoint_hold_heat=True,
            temperature_setpoint_hold_duration_heat=65535,
            running_state_heat="heat",
        ),
        caplog,
    )
    await hass.async_block_till_done()
    assert len(published) == 1
    assert coordinator.heat_boost_remaining > 0
    assert all(
        item.get("system_mode_heat") != "emergency_heating"
        for item in _payloads(published)[1:]
    )

    await _advance(hass, clock, _BOOST_MINUTES * 60)
    assert _payloads(published)[1]["occupied_heating_setpoint_heat"] == 20
    assert (
        sum(
            item.get("system_mode_heat") == "emergency_heating"
            for item in _payloads(published)
        )
        == 1
    )


async def test_native_expiry_and_delayed_mqtt_publish_once(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """A receiver that already restored is not written again, including after a late report."""
    native = _coordinator(hass, entry_id="native")
    _prime(native, caplog)
    await _boost(native)
    _handle(
        native,
        _slr2_payload(
            system_mode_heat="emergency_heating",
            occupied_heating_setpoint_heat=25,
            temperature_setpoint_hold_duration_heat=15,
            running_state_heat="heat",
        ),
        caplog,
    )
    await hass.async_block_till_done()
    before = len(published)
    _handle(
        native,
        _slr2_payload(
            system_mode_heat="heat",
            occupied_heating_setpoint_heat=20,
            temperature_setpoint_hold_heat=True,
            running_state_heat="idle",
        ),
        caplog,
    )
    await hass.async_block_till_done()
    await _advance(hass, clock, _BOOST_MINUTES * 60)
    assert len(published) == before

    delayed = _coordinator(hass, entry_id="delayed")
    delayed.hvac_mode = HVACMode.HEAT
    delayed.target_temperature = 20
    await _boost(delayed)
    boosts_before_deadline = len(published)
    await _advance(hass, clock, _BOOST_MINUTES * 60)
    restored = len(published) - boosts_before_deadline
    assert restored >= 1
    _handle(
        delayed,
        _slr2_payload(
            system_mode_heat="emergency_heating",
            occupied_heating_setpoint_heat=25,
            temperature_setpoint_hold_duration_heat=None,
            running_state_heat="heat",
        ),
        caplog,
    )
    await hass.async_block_till_done()
    await _advance(hass, clock, 120)
    assert len(published) == boosts_before_deadline + restored


async def test_repeated_presses_keep_the_first_snapshot(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """Presses at +0s, +3s and +4s send one command and keep the original setpoint."""
    coordinator = _coordinator(hass)
    _prime(coordinator, caplog, setpoint=20, hold=True)
    await _boost(coordinator)
    coordinator.target_temperature = 25
    await coordinator.async_heating_boost(
        _BOOST_MINUTES,
        _BOOST_TEMPERATURE,
        snapshot_setpoint=25,
        snapshot_mode=HVACMode.HEAT,
    )
    await _advance(hass, clock, 3)
    await coordinator.async_heating_boost(
        _BOOST_MINUTES, _BOOST_TEMPERATURE, snapshot_setpoint=25
    )
    await _advance(hass, clock, 1)
    await coordinator.async_heating_boost(
        _BOOST_MINUTES, _BOOST_TEMPERATURE, snapshot_setpoint=25
    )

    assert (
        sum(
            item.get("system_mode_heat") == "emergency_heating"
            for item in _payloads(published)
        )
        == 1
    )
    assert coordinator.pre_boost_occupied_heating_setpoint_heat == 20

    await _advance(hass, clock, 5)
    await coordinator.async_heating_boost(
        _BOOST_MINUTES, _BOOST_TEMPERATURE, snapshot_setpoint=25
    )
    assert coordinator.pre_boost_occupied_heating_setpoint_heat == 20
    assert (
        sum(
            item.get("system_mode_heat") == "emergency_heating"
            for item in _payloads(published)
        )
        == 2
    )

    await _advance(hass, clock, 14 * 60)
    assert all(
        item.get("occupied_heating_setpoint_heat") != 20
        for item in _payloads(published)
    )
    await _advance(hass, clock, 60)
    assert _payloads(published)[-2]["occupied_heating_setpoint_heat"] == 20
    assert all(not _turns_heating_off(item) for item in _payloads(published))


async def test_manual_lower_target_blocks_restore(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """A later setpoint of 20 is kept: no write of 25 and no off command."""
    coordinator = _coordinator(hass)
    _prime(coordinator, caplog, setpoint=21, hold=True)
    await _boost(coordinator)
    _handle(
        coordinator,
        _slr2_payload(
            system_mode_heat="emergency_heating",
            occupied_heating_setpoint_heat=25,
            temperature_setpoint_hold_duration_heat=None,
            running_state_heat="heat",
        ),
        caplog,
    )
    await hass.async_block_till_done()
    before = len(published)

    await coordinator.async_set_temperature(20)
    manual = _payloads(published)[before:]
    assert manual == [{"occupied_heating_setpoint_heat": 20}]
    assert all(not _contains_boost_temperature(item) for item in manual)
    assert all(not _turns_heating_off(item) for item in manual)

    await coordinator.async_heating_boost_cancel()
    await _advance(hass, clock, _BOOST_MINUTES * 60)
    assert _payloads(published)[before:] == manual


async def test_mqtt_setpoint_change_blocks_restore(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """A device setpoint that leaves 25 after the boost was seen is not overwritten."""
    coordinator = _coordinator(hass)
    _prime(coordinator, caplog, setpoint=21, hold=True)
    await _boost(coordinator)
    _handle(
        coordinator,
        _slr2_payload(
            system_mode_heat="emergency_heating",
            occupied_heating_setpoint_heat=25,
            temperature_setpoint_hold_duration_heat=10,
            running_state_heat="heat",
        ),
        caplog,
    )
    await hass.async_block_till_done()
    before = len(published)
    _handle(
        coordinator,
        _slr2_payload(
            system_mode_heat="emergency_heating",
            occupied_heating_setpoint_heat=19,
            temperature_setpoint_hold_duration_heat=None,
            running_state_heat="heat",
        ),
        caplog,
    )
    await hass.async_block_till_done()
    await _advance(hass, clock, _BOOST_MINUTES * 60)
    assert published[before:] == []


async def test_schedule_restore_does_not_write_a_setpoint(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """Schedule mode is restored with hold off and without a boost setpoint."""
    coordinator = _coordinator(hass)
    _prime(coordinator, caplog, setpoint=20, hold=False)
    assert coordinator.hvac_mode == HVACMode.AUTO
    await _boost(coordinator)
    await _advance(hass, clock, _BOOST_MINUTES * 60)
    restored = _payloads(published)[-1]
    assert restored == {
        "system_mode_heat": "heat",
        "temperature_setpoint_hold_heat": "0",
        "temperature_setpoint_hold_duration_heat": "0",
    }


async def test_explicit_cancel_is_single_and_unknown_snapshot_publishes_nothing(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """Cancel restores once. Unknown mode or setpoint does not turn heating off."""
    known = _coordinator(hass, entry_id="known-cancel")
    _prime(known, caplog)
    await _boost(known)
    await known.async_heating_boost_cancel()
    restored_count = len(published)
    await known.async_heating_boost_cancel()
    await _advance(hass, clock, _BOOST_MINUTES * 60)
    assert len(published) == restored_count
    assert _payloads(published)[-1]["occupied_heating_setpoint_heat"] == 20

    unknown = _coordinator(hass, entry_id="unknown-cancel")
    unknown.heating_boost_temperature = 25
    before = len(published)
    await unknown.async_heating_boost(15, 25)
    await unknown.async_heating_boost_cancel()
    await _advance(hass, clock, 20 * 60)
    assert published[before:] == []

    heat_without_setpoint = _coordinator(hass, entry_id="heat-without-setpoint")
    heat_without_setpoint.hvac_mode = HVACMode.HEAT
    before = len(published)
    await heat_without_setpoint.async_heating_boost(15, 25)
    await _advance(hass, clock, 20 * 60)
    assert published[before:] == []


async def test_explicit_duration_zero_does_not_start_default_boost(
    hass: Any, published: list[tuple[str, str]], caplog: Any
) -> None:
    """Zero stays zero: it does not become the 120 minute default or a new boost."""
    idle = _coordinator(hass, entry_id="idle-zero")
    idle.heating_boost_duration = 120
    await idle.async_heating_boost(0, 25)
    assert published == []

    active = _coordinator(hass, entry_id="active-zero")
    _prime(active, caplog, setpoint=20, hold=True)
    await _boost(active)
    before = len(published)
    await active.async_heating_boost(0)
    assert _payloads(published)[before]["occupied_heating_setpoint_heat"] == 20
    assert all(
        item.get("system_mode_heat") != "emergency_heating"
        for item in _payloads(published)[before:]
    )
    assert all(
        item.get("temperature_setpoint_hold_duration_heat") != 120
        for item in _payloads(published)
    )


async def test_off_snapshot_restores_off_without_writing_boost_temperature(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """A known previous off mode is restored without repeating setpoint 25."""
    coordinator = _coordinator(hass)
    _prime(coordinator, caplog, mode="off", setpoint=12, hold=False)
    assert coordinator.hvac_mode == HVACMode.OFF
    await _boost(coordinator)
    before = len(published)
    await _advance(hass, clock, _BOOST_MINUTES * 60)
    restored = _payloads(published)[before:]
    assert restored == [
        {"system_mode_heat": "off", "temperature_setpoint_hold_heat": "0"}
    ]
    assert all(not _contains_boost_temperature(item) for item in restored)


async def test_restart_expired_requires_fresh_stuck_boost(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """After a restart, restore only a device that is still at the commanded boost."""
    first = _coordinator(hass, entry_id="restart-me")
    _prime(first, caplog, setpoint=20, hold=True)
    await _boost(first)
    first.async_unload_heating_boost()
    await _advance(hass, clock, _BOOST_MINUTES * 60 + 5)
    published.clear()

    restarted = _coordinator(hass, entry_id="restart-me")
    await restarted.async_load_heating_boost_session()
    await _advance(hass, clock, 0)
    assert published == []

    _handle(
        restarted,
        _slr2_payload(
            system_mode_heat="heat",
            occupied_heating_setpoint_heat=20,
            temperature_setpoint_hold_heat=True,
            running_state_heat="idle",
        ),
        caplog,
    )
    await hass.async_block_till_done()
    assert published == []

    # A different process load of the same saved session sees the stuck boost.
    # The previous report finished that loaded copy, so start from storage again
    # by boosting was not repeated: reload is covered by a second entry below.
    stuck = _coordinator(hass, entry_id="restart-stuck")
    stuck.hvac_mode = HVACMode.HEAT
    stuck.target_temperature = 20
    await _boost(stuck)
    stuck.async_unload_heating_boost()
    await _advance(hass, clock, _BOOST_MINUTES * 60 + 5)
    published.clear()
    loaded = _coordinator(hass, entry_id="restart-stuck")
    await loaded.async_load_heating_boost_session()
    await hass.async_block_till_done()
    assert published == []
    loaded.handle_mqtt_message(
        _message(
            _slr2_payload(
                system_mode_heat="emergency_heating",
                occupied_heating_setpoint_heat=25,
                temperature_setpoint_hold_duration_heat=65535,
                running_state_heat="heat",
            ),
            retain=True,
        )
    )
    await hass.async_block_till_done()
    await _advance(hass, clock, 0)
    assert published == []
    _handle(
        loaded,
        _slr2_payload(
            system_mode_heat="emergency_heating",
            occupied_heating_setpoint_heat=25,
            temperature_setpoint_hold_duration_heat=65535,
            running_state_heat="heat",
        ),
        caplog,
    )
    await hass.async_block_till_done()
    assert _payloads(published)[0]["occupied_heating_setpoint_heat"] == 20
    assert all(
        item.get("system_mode_heat") != "emergency_heating"
        for item in _payloads(published)
    )
    assert all(not _turns_heating_off(item) for item in _payloads(published))


async def test_restart_unexpired_does_not_recreate_boost_and_manual_20_is_kept(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """An in-progress session is not boosted again, and a reported 20 is not replaced."""
    running = _coordinator(hass, entry_id="still-running")
    _prime(running, caplog, setpoint=20, hold=True)
    await _boost(running)
    running.async_unload_heating_boost()
    published.clear()
    resumed = _coordinator(hass, entry_id="still-running")
    await resumed.async_load_heating_boost_session()
    await hass.async_block_till_done()
    assert published == []
    _handle(
        resumed,
        _slr2_payload(
            system_mode_heat="emergency_heating",
            occupied_heating_setpoint_heat=25,
            temperature_setpoint_hold_duration_heat=None,
            running_state_heat="heat",
        ),
        caplog,
    )
    await hass.async_block_till_done()
    assert published == []
    await _advance(hass, clock, _BOOST_MINUTES * 60)
    assert _payloads(published)[0]["occupied_heating_setpoint_heat"] == 20

    changed = _coordinator(hass, entry_id="changed-after-restart")
    _prime(changed, caplog, setpoint=21, hold=True)
    await _boost(changed)
    changed.async_unload_heating_boost()
    changed_loaded = _coordinator(hass, entry_id="changed-after-restart")
    await changed_loaded.async_load_heating_boost_session()
    published.clear()
    _handle(
        changed_loaded,
        _slr2_payload(
            system_mode_heat="emergency_heating",
            occupied_heating_setpoint_heat=20,
            temperature_setpoint_hold_duration_heat=65535,
            running_state_heat="heat",
        ),
        caplog,
    )
    await hass.async_block_till_done()
    await _advance(hass, clock, _BOOST_MINUTES * 60)
    assert published == []


async def test_missing_and_corrupt_persistence_publish_nothing(
    hass: Any, published: list[tuple[str, str]]
) -> None:
    """Missing storage and a damaged session do not write to the device."""
    missing = _coordinator(hass, entry_id="no-such-session")
    await missing.async_load_heating_boost_session()
    await hass.async_block_till_done()
    assert missing.heating_boost_diagnostics() is None
    assert published == []

    store = Store(hass, 1, heating_boost_storage_key("corrupt-session"))
    await store.async_save(
        {"entry_id": "corrupt-session", "pre_boost_hvac_mode": "banana"}
    )
    corrupt = _coordinator(hass, entry_id="corrupt-session")
    await corrupt.async_load_heating_boost_session()
    await hass.async_block_till_done()
    assert corrupt.heating_boost_diagnostics() is None
    assert published == []

    path_store = Store(hass, 1, heating_boost_storage_key("broken-file"))
    _write_corrupt_storage(path_store.path)
    broken = _coordinator(hass, entry_id="broken-file")
    await broken.async_load_heating_boost_session()
    await hass.async_block_till_done()
    assert broken.heating_boost_diagnostics() is None
    assert published == []


async def test_unload_cancels_deadline_and_second_press_has_one_restore(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """Unload drops the callback, and refreshing the deadline restores only once."""
    unloaded = _coordinator(hass, entry_id="unload-me")
    _prime(unloaded, caplog)
    await _boost(unloaded)
    unloaded.async_unload_heating_boost()
    before = len(published)
    await _advance(hass, clock, 30 * 60)
    assert len(published) == before

    refreshed = _coordinator(hass, entry_id="refresh-me")
    _prime(refreshed, caplog, setpoint=20, hold=True)
    await _boost(refreshed)
    await _advance(hass, clock, 6)
    await refreshed.async_heating_boost(_BOOST_MINUTES, _BOOST_TEMPERATURE)
    await _advance(hass, clock, 14 * 60)
    assert all(
        "occupied_heating_setpoint_heat" not in item
        or item["system_mode_heat"] == "emergency_heating"
        for item in _payloads(published)[before:]
    )
    await _advance(hass, clock, 60)
    heat_restores = [
        item
        for item in _payloads(published)
        if item.get("temperature_setpoint_hold_duration_heat") == "0"
        and item.get("occupied_heating_setpoint_heat") == 20
    ]
    assert len(heat_restores) == 1


async def test_devices_and_homes_are_isolated(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """One home's deadline must not restore another home's setpoint."""
    home_a = _coordinator(hass, entry_id="home-a")
    home_a.topic = "zigbee2mqtt/home-a"
    home_b = _coordinator(hass, entry_id="home-b")
    home_b.topic = "zigbee2mqtt/home-b"
    _prime(home_a, caplog, setpoint=20, hold=True)
    _prime(home_b, caplog, setpoint=18, hold=True)
    await _boost(home_a)
    await _boost(home_b)
    await _advance(hass, clock, _BOOST_MINUTES * 60)

    topic_a = "zigbee2mqtt/home-a/set"
    topic_b = "zigbee2mqtt/home-b/set"
    assert (
        _heating_messages(published, topic_a)[-2]["occupied_heating_setpoint_heat"]
        == 20
    )
    assert (
        _heating_messages(published, topic_b)[-2]["occupied_heating_setpoint_heat"]
        == 18
    )
    assert all(
        item.get("occupied_heating_setpoint_heat") != 18
        for item in _heating_messages(published, topic_a)
    )

    home_c = _coordinator(hass, entry_id="home-c")
    await home_c.async_load_heating_boost_session()
    await hass.async_block_till_done()
    assert home_c.heating_boost_diagnostics() is None


async def test_slr1_restore_uses_single_channel_fields(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """SLR1/OTR1 restore uses the unsuffixed heating fields."""
    coordinator = _coordinator(hass, MODEL_SLR1, entry_id="slr1")
    _handle(
        coordinator,
        {
            "local_temperature": 18,
            "occupied_heating_setpoint": 20,
            "running_state": "idle",
            "system_mode": "heat",
            "temperature_setpoint_hold": True,
            "temperature_setpoint_hold_duration": 0,
        },
        caplog,
    )
    await _boost(coordinator)
    await _advance(hass, clock, _BOOST_MINUTES * 60)
    restored = _payloads(published)[-2]
    assert restored["system_mode"] == "heat"
    assert restored["occupied_heating_setpoint"] == 20
    assert "system_mode_heat" not in restored
    assert not _turns_heating_off(restored)


async def test_water_path_is_not_given_a_heating_deadline(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """Water boost, null duration, and 65535 do not publish a heating restore."""
    coordinator = _coordinator(hass, MODEL_SLR2, entry_id="water")
    coordinator.water_boost_duration = 30
    await coordinator.async_water_boost(30)
    assert _payloads(published) == [
        {
            "system_mode_water": "emergency_heating",
            "temperature_setpoint_hold_duration_water": 30,
            "temperature_setpoint_hold_water": 1,
        }
    ]
    _handle(
        coordinator,
        _slr2_payload(
            system_mode_water="emergency_heating",
            temperature_setpoint_hold_water=True,
            temperature_setpoint_hold_duration_water=None,
        ),
        caplog,
    )
    _handle(
        coordinator,
        _slr2_payload(
            system_mode_water="emergency_heating",
            temperature_setpoint_hold_water=True,
            temperature_setpoint_hold_duration_water=65535,
        ),
        caplog,
    )
    await hass.async_block_till_done()
    await _advance(hass, clock, 180 * 60)
    assert all("system_mode_heat" not in item for item in _payloads(published))
    assert len(published) == 1


async def test_finished_session_is_not_restored_again_after_restart(
    hass: Any, clock: dict[str, datetime], published: list[tuple[str, str]], caplog: Any
) -> None:
    """A restore that was saved as finished does not run a second time."""
    coordinator = _coordinator(hass, entry_id="finished-session")
    _prime(coordinator, caplog)
    await _boost(coordinator)
    await _advance(hass, clock, _BOOST_MINUTES * 60)
    coordinator.async_unload_heating_boost()
    published.clear()

    restarted = _coordinator(hass, entry_id="finished-session")
    await restarted.async_load_heating_boost_session()
    _handle(
        restarted,
        _slr2_payload(
            system_mode_heat="emergency_heating",
            occupied_heating_setpoint_heat=25,
            temperature_setpoint_hold_duration_heat=65535,
            running_state_heat="heat",
        ),
        caplog,
    )
    await hass.async_block_till_done()
    await _advance(hass, clock, 60)
    assert published == []


def test_storage_keys_cannot_escape_or_collide() -> None:
    """Device ids cannot share a storage file or leave the storage directory."""
    home_a = heating_boost_storage_key("home-a")
    home_b = heating_boost_storage_key("home-b")
    slash = heating_boost_storage_key("home/a")
    parent = heating_boost_storage_key("../home-a")
    assert home_a != home_b
    assert home_a == "hive_local_thermostat.heating_boost.home-a"
    assert slash != home_a
    assert slash != parent
    for key in (home_a, home_b, slash, parent):
        assert "/" not in key
        assert ".." not in key
        assert key.count(".") == 2
