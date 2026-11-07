"""Coordinator for Hive Local Thermostat integration."""

from __future__ import annotations

import json
from asyncio import Lock, sleep
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any, cast
from uuid import uuid4

from homeassistant.components.climate.const import (
    PRESET_BOOST,
    PRESET_NONE,
    HVACAction,
    HVACMode,
)
from homeassistant.components.mqtt import client as mqtt_client
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.event import async_track_point_in_utc_time
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util.dt import utcnow

from .const import (
    DEFAULT_FROST_TEMPERATURE,
    DEFAULT_HEATING_BOOST_MINUTES,
    DEFAULT_HEATING_BOOST_TEMPERATURE,
    DEFAULT_WATER_BOOST_MINUTES,
    DOMAIN,
    HEATING_BOOST_DEBOUNCE_SECONDS,
    HEATING_BOOST_STORAGE_VERSION,
    HIVE_BOOST,
    LOGGER,
    MODEL_SLR2,
)
from .heating_boost import (
    HEATING_BOOST_MODE,
    HeatingBoostSession,
    decide_heating_restore,
    heating_boost_storage_key,
    note_device_report,
    resolved_boost_minutes,
    session_from_storage,
)

PRESET_MAP = {
    PRESET_NONE: "",
    PRESET_BOOST: HIVE_BOOST,
}

BOOST_ERROR = 65000
_HIVE_BOOST_MODE = HEATING_BOOST_MODE


def _optional_int(value: Any) -> int | None:
    """Return an int from a Zigbee2MQTT field.

    JSON null, a missing value, booleans, and non-numeric strings return None
    so callers never compare those values with an int.
    """
    parsed: int | None = None
    if isinstance(value, int) and not isinstance(value, bool):
        parsed = value
    elif isinstance(value, float):
        parsed = int(value)
    elif isinstance(value, str) and value.strip():
        try:
            parsed = int(float(value.strip()))
        except ValueError:
            parsed = None
    return parsed


def _reported_boost_minutes(
    data: dict[str, Any], mode: Any, duration_key: str
) -> int | None:
    """Return boost minutes reported by Zigbee2MQTT.

    Inactive boosts are zero. An active boost with a missing, null, or
    non-numeric duration is None rather than a value that cannot be compared.
    """
    if mode != _HIVE_BOOST_MODE:
        return 0
    if duration_key not in data:
        return None
    return _optional_int(data[duration_key])


def _tracked_boost_remaining(
    started: datetime | None, started_duration: int
) -> int | None:
    """Return minutes left on a locally tracked boost, if one was started."""
    if started is None or started_duration <= 0:
        return None
    elapsed_minutes = (utcnow() - started).total_seconds() / 60
    return max(0, int(started_duration - elapsed_minutes))


def _mode_text(mode: Any) -> str:
    """Return a system mode string, or empty when the payload omitted it."""
    if isinstance(mode, str):
        return mode
    return ""


class HiveCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Class to manage fetching Hive data from MQTT."""

    current_temperature: float | None = None
    target_temperature: float | None = None
    preset_mode: str | None = None
    hvac_mode: HVACMode | None = None
    water_mode: str | None = None

    running_state_heat: str = ""
    running_state_water: str = ""

    heat_boost: bool = False
    water_boost: bool = False
    heat_boost_started: datetime | None = None
    water_boost_started: datetime | None = None
    heat_boost_started_duration: int = 0
    water_boost_started_duration: int = 0
    heat_boost_remaining: int = 0
    water_boost_remaining: int = 0

    pre_boost_hvac_mode: HVACMode | None = None
    pre_boost_occupied_heating_setpoint_heat: float | None = None
    pre_boost_water_mode: str | None = None

    # Number entity values
    heating_boost_duration: float = DEFAULT_HEATING_BOOST_MINUTES
    heating_boost_temperature: float = DEFAULT_HEATING_BOOST_TEMPERATURE
    heating_frost_prevention: float = DEFAULT_FROST_TEMPERATURE
    water_boost_duration: float = DEFAULT_WATER_BOOST_MINUTES

    # Diagnostics
    last_mqtt_payload: dict[str, Any] | None = None

    def __init__(
        self,
        hass: HomeAssistant,
        entry_id: str,
        model: str,
        topic: str,
        show_heat_schedule_mode: bool,  # noqa: FBT001
        show_water_schedule_mode: bool,  # noqa: FBT001
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            LOGGER,
            name=f"{DOMAIN}_{entry_id}",
        )
        self.entry_id = entry_id
        self.model = model
        self.topic = topic
        self.show_heating_schedule_mode = show_heat_schedule_mode
        self.show_water_schedule_mode = show_water_schedule_mode
        self.data: dict[str, Any] = {}
        self._heating_session: HeatingBoostSession | None = None
        self._heating_state_seen = False
        self._reported_heat_system_mode: str | None = None
        self._deadline_unsub: Callable[[], None] | None = None
        self._boost_store: Store[dict[str, Any]] | None = None
        self._boost_lock = Lock()
        self._boost_unloaded = False
        self._applying_restore = False
        self._last_heating_boost_at: datetime | None = None

    @property
    def topic_get(self) -> str:
        """Return the topic getter."""
        return self.topic + "/get"

    @property
    def topic_set(self) -> str:
        """Return the topic setter."""
        return self.topic + "/set"

    @property
    def boost_remaining_heat(self) -> int:
        """Return the remaining boost time for heating."""
        return self.heat_boost_remaining

    @property
    def boost_remaining_water(self) -> int:
        """Return the remaining boost time for water."""
        return self.water_boost_remaining

    @property
    def hvac_action(self) -> HVACAction | None:
        """Return the current HVAC action."""
        if self.running_state_heat == "preheating":
            return HVACAction.PREHEATING
        if self.running_state_heat == "heat":
            return HVACAction.HEATING
        if self.running_state_heat == "idle":
            return HVACAction.IDLE
        if self.running_state_heat == "off":
            return HVACAction.OFF
        return None

    @property
    def local_temperature_heat(self) -> float | None:
        """Return the local temperature for heating."""
        return self.current_temperature

    def climate_preset(self, mode: str) -> str:
        """Get the current preset."""
        return next(
            (k for k, v in PRESET_MAP.items() if v == mode), PRESET_MAP[PRESET_NONE]
        )

    @callback
    def handle_mqtt_message(self, message: ReceiveMessage) -> None:  # noqa: C901, PLR0912, PLR0915
        """Handle received MQTT message."""
        topic = message.topic
        payload = message.payload
        LOGGER.debug("Received from %s payload: %s", topic, payload)

        if not payload:
            LOGGER.error(
                "Received empty payload on topic %s, check that you have the correct topic name",
                topic,
            )
            return

        self.current_temperature = None
        self.target_temperature = None
        self.preset_mode = None
        self.hvac_mode = None
        self.heat_boost = False
        self.water_boost = False
        self.water_mode = None

        try:
            parsed_data: dict[str, Any] = json.loads(payload)

            # Store last payload for diagnostics
            self.last_mqtt_payload = parsed_data

            if not self.valid_data_for_model(parsed_data):
                return

            if self.model == MODEL_SLR2:
                system_mode_heat = parsed_data.get("system_mode_heat")
                system_mode_water = parsed_data.get("system_mode_water")
                reported_boost_remaining_heat = _reported_boost_minutes(
                    parsed_data,
                    system_mode_heat,
                    "temperature_setpoint_hold_duration_heat",
                )
                reported_boost_remaining_water = _reported_boost_minutes(
                    parsed_data,
                    system_mode_water,
                    "temperature_setpoint_hold_duration_water",
                )
                reported_boost_temperature = parsed_data.get(
                    "occupied_heating_setpoint_heat"
                )
                self.running_state_heat = cast(
                    str,
                    parsed_data.get("running_state_heat")
                    if parsed_data.get("running_state_heat")
                    else "preheating",
                )
                self.running_state_water = cast(
                    str,
                    parsed_data.get("running_state_water")
                    if parsed_data.get("running_state_water")
                    else "preheating",
                )
                self.current_temperature = parsed_data.get("local_temperature_heat")
                heating_setpoint = parsed_data.get("occupied_heating_setpoint_heat")
                if heating_setpoint == 1:
                    self.target_temperature = self.heating_frost_prevention
                else:
                    self.target_temperature = heating_setpoint
                self.preset_mode = self.climate_preset(_mode_text(system_mode_heat))
                if system_mode_heat == "heat":
                    if (
                        parsed_data.get("temperature_setpoint_hold_heat") is False
                        and self.show_heating_schedule_mode
                    ):
                        self.hvac_mode = HVACMode.AUTO
                    else:
                        self.hvac_mode = HVACMode.HEAT
                if system_mode_heat == _HIVE_BOOST_MODE:
                    self.hvac_mode = HVACMode.HEAT
                    self.heat_boost = True
                if system_mode_heat == "off":
                    self.hvac_mode = HVACMode.OFF

                if (
                    system_mode_heat != _HIVE_BOOST_MODE
                    and not self._heating_session_is_open()
                ):
                    self.pre_boost_occupied_heating_setpoint_heat = (
                        self.target_temperature
                    )
                    self.pre_boost_hvac_mode = self.hvac_mode
                self._remember_heat_mode(
                    system_mode_heat, parsed_data, retained=message.retain
                )
                if system_mode_water == "heat":
                    if parsed_data.get("temperature_setpoint_hold_water") is False:
                        if self.show_water_schedule_mode:
                            self.water_mode = "auto"
                        else:
                            self.water_mode = "heat"
                    else:
                        self.water_mode = "heat"
                if system_mode_water == _HIVE_BOOST_MODE:
                    self.water_mode = "boost"
                    self.water_boost = True
                if system_mode_water == "off":
                    self.water_mode = "off"

                if system_mode_water != _HIVE_BOOST_MODE:
                    self.pre_boost_water_mode = self.water_mode
            else:
                system_mode = parsed_data.get("system_mode")
                reported_boost_remaining_heat = _reported_boost_minutes(
                    parsed_data,
                    system_mode,
                    "temperature_setpoint_hold_duration",
                )
                reported_boost_temperature = parsed_data.get(
                    "occupied_heating_setpoint"
                )
                self.running_state_heat = cast(
                    str,
                    parsed_data.get("running_state")
                    if parsed_data.get("running_state")
                    else "preheating",
                )
                self.current_temperature = parsed_data.get("local_temperature")
                heating_setpoint = parsed_data.get("occupied_heating_setpoint")
                if heating_setpoint == 1:
                    self.target_temperature = self.heating_frost_prevention
                else:
                    self.target_temperature = heating_setpoint
                self.preset_mode = self.climate_preset(_mode_text(system_mode))

                if system_mode == "heat":
                    if (
                        parsed_data.get("temperature_setpoint_hold") is False
                        and self.show_heating_schedule_mode
                    ):
                        self.hvac_mode = HVACMode.AUTO
                    else:
                        self.hvac_mode = HVACMode.HEAT
                if system_mode == _HIVE_BOOST_MODE:
                    self.hvac_mode = HVACMode.HEAT
                    self.heat_boost = True
                if system_mode == "off":
                    self.hvac_mode = HVACMode.OFF

                if (
                    system_mode != _HIVE_BOOST_MODE
                    and not self._heating_session_is_open()
                ):
                    self.pre_boost_occupied_heating_setpoint_heat = (
                        self.target_temperature
                    )
                    self.pre_boost_hvac_mode = self.hvac_mode
                self._remember_heat_mode(
                    system_mode, parsed_data, retained=message.retain
                )

            self.correct_heat_boost(
                reported_boost_remaining_heat, reported_boost_temperature
            )
            self.record_heat_boost_state()
            follow_up = self._sync_heating_session_from_report()
            self._apply_open_heating_session_display()

            if self.model == MODEL_SLR2:
                if self.correct_water_boost(reported_boost_remaining_water):
                    self._schedule_heating_follow_up(follow_up)
                    return  # Correction made, exit to avoid state update loop
                self.record_water_boost_state()

            self.async_set_updated_data(parsed_data)
            self._schedule_heating_follow_up(follow_up)
        except json.JSONDecodeError:
            LOGGER.error("Failed to parse JSON from MQTT payload: %s", payload)
        except Exception as err:  # noqa: BLE001
            LOGGER.error("Error handling MQTT message: %s", err)

    def valid_data_for_model(self, data: dict[str, Any]) -> bool:
        """Check if data is valid for the current model."""
        if self.model == MODEL_SLR2:
            if "system_mode_water" not in data:
                LOGGER.error(
                    "Received data does not contain 'system_mode_water' for SLR2, check you have the correct model set"
                )
                return False
        elif "system_mode_water" in data:
            LOGGER.error(
                "Received data contains 'system_mode_water' for SLR1/OTR1, check you have the correct model set"
            )
            return False
        return True

    def correct_heat_boost(
        self,
        reported_boost_remaining_heat: int | None,
        _reported_boost_temperature: float | None,
    ) -> bool:
        """Check and correct boost remaining heat if necessary.

        A missing duration and 65535 both mean the receiver is not counting.
        Neither value starts another boost. The session timer owns the deadline.
        """
        if (
            reported_boost_remaining_heat is None
            or reported_boost_remaining_heat > BOOST_ERROR
        ):
            tracked = _tracked_boost_remaining(
                self.heat_boost_started, self.heat_boost_started_duration
            )
            if tracked is not None:
                self.heat_boost_remaining = tracked
            elif reported_boost_remaining_heat is not None:
                self.heat_boost_remaining = 0
            LOGGER.debug(
                "Heating boost duration was %s; keeping the local deadline",
                "missing" if reported_boost_remaining_heat is None else "indefinite",
            )
            return False
        self.heat_boost_remaining = reported_boost_remaining_heat
        return False

    def correct_water_boost(self, reported_boost_remaining_water: int | None) -> bool:
        """Check and correct boost remaining water if necessary."""
        if reported_boost_remaining_water is None:
            tracked = _tracked_boost_remaining(
                self.water_boost_started, self.water_boost_started_duration
            )
            if tracked is not None:
                self.water_boost_remaining = tracked
            LOGGER.debug(
                "Water boost duration was missing from the MQTT payload; using %s minutes remaining",
                self.water_boost_remaining,
            )
            return False

        if reported_boost_remaining_water > BOOST_ERROR:
            tracked = _tracked_boost_remaining(
                self.water_boost_started, self.water_boost_started_duration
            )
            self.water_boost_remaining = 0 if tracked is None else tracked

            LOGGER.warning(
                "Correcting reported boost remaining water from %d to %d",
                reported_boost_remaining_water,
                self.water_boost_remaining,
            )
            if self.config_entry is not None:
                self.config_entry.async_create_task(
                    self.hass, self.async_water_boost(self.water_boost_remaining)
                )
            return True
        self.water_boost_remaining = reported_boost_remaining_water
        return False

    def record_heat_boost_state(self) -> None:
        """Record and track boost state for heating."""
        if not self._heating_session_is_open() and (
            self.heat_boost and self.heat_boost_remaining > 0
        ):
            # Boost is active, record the start time if not already recorded
            if not self.heat_boost_started:
                self.heat_boost_started = utcnow()
                self.heat_boost_started_duration = self.heat_boost_remaining
        elif not self.heat_boost:
            # Boost is not active, clear tracking state
            self.heat_boost_started = None
            self.heat_boost_started_duration = 0

        if self.water_boost and self.water_boost_remaining > 0:
            # Water boost is active, record the start time if not already recorded
            if not self.water_boost_started:
                self.water_boost_started = utcnow()
                self.water_boost_started_duration = self.water_boost_remaining
        elif not self.water_boost:
            # Water boost is not active, clear tracking state
            self.water_boost_started = None
            self.water_boost_started_duration = 0

    def record_water_boost_state(self) -> None:
        """Record and track boost state for water."""
        if self.water_boost and self.water_boost_remaining > 0:
            # Water boost is active, record the start time if not already recorded
            if not self.water_boost_started:
                self.water_boost_started = utcnow()
                self.water_boost_started_duration = self.water_boost_remaining
        elif not self.water_boost:
            # Water boost is not active, clear tracking state
            self.water_boost_started = None
            self.water_boost_started_duration = 0

    async def _async_publish_set(self, payload: str) -> None:
        """Publish MQTT set message."""
        LOGGER.debug("Sending to %s message %s", self.topic_set, payload)
        await mqtt_client.async_publish(self.hass, self.topic_set, payload)

    async def async_water_boost(
        self, boost_duration_minutes: int | None = None
    ) -> None:
        """Send water boost command."""

        self.pre_boost_water_mode = self.water_mode

        duration = str(int(boost_duration_minutes or self.water_boost_duration))
        payload = (
            r'{"system_mode_water":"emergency_heating","temperature_setpoint_hold_duration_water":'
            + duration
            + r',"temperature_setpoint_hold_water":1}'
        )

        self.water_boost = True
        self.water_boost_started = utcnow()
        self.water_boost_started_duration = int(duration)

        await self._async_publish_set(payload)

    async def async_water_boost_cancel(self) -> None:
        """Cancel water boost command."""

        if self.pre_boost_water_mode == "auto":
            await self.async_water_scheduled()
        elif self.pre_boost_water_mode == "heat":
            await self.async_water_always_on()
        else:
            await self.async_water_always_off()

    async def async_water_scheduled(self) -> None:
        """Send water scheduled command."""

        payload = r'{"system_mode_water":"heat","temperature_setpoint_hold_water":"0","temperature_setpoint_hold_duration_water":"0"}'
        await self._async_publish_set(payload)

    async def async_water_always_on(self) -> None:
        """Send water always on command."""

        payload = r'{"system_mode_water":"heat","temperature_setpoint_hold_water":1}'
        await self._async_publish_set(payload)

    async def async_water_always_off(self) -> None:
        """Send water always off command."""

        payload = r'{"system_mode_water":"off","temperature_setpoint_hold_water":0}'
        await self._async_publish_set(payload)

    async def async_heating_boost(
        self,
        boost_duration_minutes: float | None = None,
        boost_temperature: float | None = None,
        *,
        snapshot_mode: HVACMode | None = None,
        snapshot_setpoint: float | None = None,
    ) -> None:
        """Send one heating boost command and arm its restore deadline."""
        if self._boost_unloaded:
            return
        minutes = resolved_boost_minutes(
            boost_duration_minutes, self.heating_boost_duration
        )
        if minutes == 0:
            await self._async_finish_heating_boost("duration_0")
            return
        if minutes < 0:
            return
        temperature = float(
            self.heating_boost_temperature
            if boost_temperature is None
            else boost_temperature
        )
        async with self._boost_lock:
            if not self._prepare_heating_boost(
                minutes,
                temperature,
                snapshot_mode=snapshot_mode,
                snapshot_setpoint=snapshot_setpoint,
            ):
                return
            await self._async_save_heating_session()
        await self._async_publish_set(self._heating_boost_payload(minutes, temperature))
        self.async_update_listeners()

    async def async_heating_boost_cancel(self) -> None:
        """End a heating boost this integration started, once, without guessing."""
        await self._async_finish_heating_boost("cancel")

    async def async_set_temperature(self, temperature: float) -> None:
        """Set temperature."""
        if self._mark_manual_heating_change():
            await self._async_save_heating_session()

        if self.model == MODEL_SLR2:
            payload = r'{"occupied_heating_setpoint_heat":' + str(temperature) + r"}"
        else:
            payload = r'{"occupied_heating_setpoint":' + str(temperature) + r"}"

        await self._async_publish_set(payload)

    async def async_set_hvac_mode_off(self) -> None:
        """Set HVAC mode to off."""
        if self._mark_manual_heating_change():
            await self._async_save_heating_session()

        if self.model == MODEL_SLR2:
            payload = r'{"system_mode_heat":"off","temperature_setpoint_hold_heat":"0"}'
        else:
            payload = r'{"system_mode":"off","temperature_setpoint_hold":"0"}'

        self.hvac_mode = HVACMode.OFF
        await self._async_publish_set(payload)

        await sleep(0.5)

        if self.model == MODEL_SLR2:
            payload = (
                r'{"occupied_heating_setpoint_heat":'
                + str(self.heating_frost_prevention)
                + r',"temperature_setpoint_hold_heat":"1","temperature_setpoint_hold_duration_heat:"65535"}'
            )
        else:
            payload = (
                r'{"occupied_heating_setpoint":'
                + str(self.heating_frost_prevention)
                + r',"temperature_setpoint_hold":"1","temperature_setpoint_hold_duration:"65535"}'
            )

        await self._async_publish_set(payload)

    async def async_set_hvac_mode_auto(self) -> None:
        """Set HVAC mode to auto."""
        if self._mark_manual_heating_change():
            await self._async_save_heating_session()

        if self.model == MODEL_SLR2:
            payload = r'{"system_mode_heat":"heat","temperature_setpoint_hold_heat":"0","temperature_setpoint_hold_duration_heat":"0"}'
        else:
            payload = r'{"system_mode":"heat","temperature_setpoint_hold":"0","temperature_setpoint_hold_duration":"0"}'

        self.hvac_mode = HVACMode.AUTO
        await self._async_publish_set(payload)

    async def async_set_hvac_mode_heat(
        self,
        temperature: float,
        set_from_temperature: bool = False,  # noqa: FBT001, FBT002
    ) -> None:
        """Set HVAC mode to heat."""
        if self._mark_manual_heating_change():
            await self._async_save_heating_session()

        if self.model == MODEL_SLR2:
            payload = (
                r'{"system_mode_heat":"heat","occupied_heating_setpoint_heat":'
                + str(temperature)
                + r',"temperature_setpoint_hold_heat":"1","temperature_setpoint_hold_duration_heat":"0"}'
            )

            payload_heating_setpoint = (
                r'{"system_mode_heat":"heat","occupied_heating_setpoint_heat":'
                + str(temperature)
                + r"}"
            )
        else:
            payload = (
                r'{"system_mode":"heat","occupied_heating_setpoint":'
                + str(temperature)
                + r',"temperature_setpoint_hold":"1","temperature_setpoint_hold_duration":"0"}'
            )

            payload_heating_setpoint = (
                r'{"system_mode":"heat","occupied_heating_setpoint":'
                + str(temperature)
                + r"}"
            )

        self.hvac_mode = HVACMode.HEAT
        await self._async_publish_set(payload)

        if not set_from_temperature:
            await sleep(0.5)
            await self._async_publish_set(payload_heating_setpoint)

    def heating_boost_diagnostics(self) -> dict[str, Any] | None:
        """Return the open or finished heating boost session, without its token."""
        session = self._heating_session
        if session is None:
            return None
        return {
            "pre_boost_hvac_mode": (
                None
                if session.pre_boost_hvac_mode is None
                else session.pre_boost_hvac_mode.value
            ),
            "pre_boost_setpoint": session.pre_boost_setpoint,
            "commanded_temperature": session.commanded_temperature,
            "deadline": session.deadline.isoformat(),
            "finished": session.finished,
            "superseded": session.superseded,
            "loaded_from_storage": session.loaded_from_storage,
            "saw_commanded_setpoint": session.saw_commanded_setpoint,
        }

    async def async_load_heating_boost_session(self) -> None:
        """Load a boost session saved before the last restart."""
        try:
            raw = await self._heating_store().async_load()
        except Exception:  # noqa: BLE001
            LOGGER.warning(
                "Heating boost session could not be read for %s",
                self.entry_id,
                exc_info=True,
            )
            return
        if raw is None:
            return
        try:
            session = session_from_storage(raw, entry_id=self.entry_id)
        except ValueError:
            LOGGER.warning(
                "Ignoring corrupt heating boost session for %s", self.entry_id
            )
            return
        session.loaded_from_storage = True
        self._heating_session = session
        self._mirror_heating_session()
        if not session.finished:
            self._arm_heating_deadline()

    @callback
    def async_unload_heating_boost(self) -> None:
        """Cancel the restore timer so unload cannot publish later."""
        self._boost_unloaded = True
        self._cancel_heating_deadline()

    def _heating_session_is_open(self) -> bool:
        """Return whether a boost we started is still waiting to finish."""
        session = self._heating_session
        return session is not None and not session.finished

    def _heating_store(self) -> Store[dict[str, Any]]:
        """Return the per-entry session store."""
        if self._boost_store is None:
            self._boost_store = Store(
                self.hass,
                HEATING_BOOST_STORAGE_VERSION,
                heating_boost_storage_key(self.entry_id),
            )
        return self._boost_store

    def _remember_heat_mode(
        self, mode: Any, payload: dict[str, Any], *, retained: bool
    ) -> None:
        """Remember a heating mode from a payload that contained one.

        After a restart, a retained message can be older than the thermostat.
        It must not authorize a restore; the next live report does.
        """
        key = "system_mode_heat" if self.model == MODEL_SLR2 else "system_mode"
        if key not in payload:
            return
        if (
            retained
            and self._heating_session is not None
            and self._heating_session.loaded_from_storage
        ):
            return
        self._heating_state_seen = True
        self._reported_heat_system_mode = mode if isinstance(mode, str) else None

    def _mark_manual_heating_change(self) -> bool:
        """Close an open boost because the user sent a different heating command."""
        if self._applying_restore:
            return False
        session = self._heating_session
        if session is None or session.finished:
            return False
        session.superseded = True
        session.finished = True
        self._cancel_heating_deadline()
        self.heat_boost = False
        self.heat_boost_remaining = 0
        return True

    def _prepare_heating_boost(
        self,
        minutes: int,
        temperature: float,
        *,
        snapshot_mode: HVACMode | None,
        snapshot_setpoint: float | None,
    ) -> bool:
        """Open or refresh a session. Return whether a command should be published."""
        now = utcnow()
        if (
            self._heating_session_is_open()
            and self._last_heating_boost_at is not None
            and (now - self._last_heating_boost_at).total_seconds()
            < HEATING_BOOST_DEBOUNCE_SECONDS
        ):
            LOGGER.debug("Ignoring rapid duplicate heating boost for %s", self.entry_id)
            return False

        if not self._heating_session_is_open():
            mode = snapshot_mode if snapshot_mode is not None else self.hvac_mode
            if mode not in {HVACMode.HEAT, HVACMode.AUTO, HVACMode.OFF}:
                mode = None
            setpoint = (
                _setpoint_or_none(snapshot_setpoint)
                if snapshot_setpoint is not None
                else _setpoint_or_none(self.target_temperature)
            )
            self._heating_session = HeatingBoostSession(
                entry_id=self.entry_id,
                pre_boost_hvac_mode=mode,
                pre_boost_setpoint=setpoint,
                commanded_temperature=temperature,
                duration_minutes=minutes,
                started_at=now,
                deadline=now + timedelta(minutes=minutes),
                token=uuid4().hex,
            )
        else:
            session = self._heating_session
            assert session is not None
            session.commanded_temperature = temperature
            session.duration_minutes = minutes
            session.started_at = now
            session.deadline = now + timedelta(minutes=minutes)
            session.token = uuid4().hex
            session.saw_commanded_setpoint = False
            session.loaded_from_storage = False

        self._last_heating_boost_at = now
        self._mirror_heating_session()
        self.heat_boost = True
        self.heat_boost_started = now
        self.heat_boost_started_duration = minutes
        self._arm_heating_deadline()
        self._apply_open_heating_session_display()
        return True

    def _heating_boost_payload(self, minutes: int, temperature: float) -> str:
        """Return the single Zigbee2MQTT heating boost document."""
        duration = str(minutes)
        target = str(temperature)
        if self.model == MODEL_SLR2:
            return (
                '{"system_mode_heat":"emergency_heating",'
                '"temperature_setpoint_hold_duration_heat":'
                + duration
                + ',"temperature_setpoint_hold_heat":1,'
                + '"occupied_heating_setpoint_heat":'
                + target
                + "}"
            )
        return (
            '{"system_mode":"emergency_heating",'
            '"temperature_setpoint_hold_duration":'
            + duration
            + ',"temperature_setpoint_hold":1,"occupied_heating_setpoint":'
            + target
            + "}"
        )

    def _heating_off_payload(self) -> str:
        """Return a mode-off document that does not write a setpoint."""
        if self.model == MODEL_SLR2:
            return '{"system_mode_heat":"off","temperature_setpoint_hold_heat":"0"}'
        return '{"system_mode":"off","temperature_setpoint_hold":"0"}'

    def _mirror_heating_session(self) -> None:
        """Expose the first snapshot on the legacy coordinator attributes."""
        session = self._heating_session
        if session is None:
            return
        self.pre_boost_hvac_mode = session.pre_boost_hvac_mode
        self.pre_boost_occupied_heating_setpoint_heat = session.pre_boost_setpoint

    def _sync_heating_session_from_report(self) -> str | None:
        """Apply the latest device report. Return save, restore, or nothing."""
        session = self._heating_session
        if session is None or session.finished:
            return None
        if session.loaded_from_storage and not self._heating_state_seen:
            return None
        saw_before = session.saw_commanded_setpoint
        note_device_report(session, _setpoint_or_none(self.target_temperature))
        if (
            not session.finished
            and session.loaded_from_storage
            and self._heating_state_seen
            and self._reported_heat_system_mode != _HIVE_BOOST_MODE
        ):
            session.finished = True
        if session.finished:
            self._cancel_heating_deadline()
            self.heat_boost = False
            self.heat_boost_remaining = 0
            return "save"
        if (
            session.loaded_from_storage
            and self._heating_state_seen
            and session.deadline <= utcnow()
            and self._reported_heat_system_mode == _HIVE_BOOST_MODE
        ):
            return "restore"
        if session.saw_commanded_setpoint and not saw_before:
            return "save"
        return None

    def _schedule_heating_follow_up(self, action: str | None) -> None:
        """Persist or restore after the MQTT handler returns."""
        if action is None or self._boost_unloaded:
            return
        self.hass.async_create_task(
            self._async_heating_follow_up(action),
            eager_start=False,
        )

    async def _async_heating_follow_up(self, action: str) -> None:
        """Run the session follow-up chosen by the device report."""
        if self._boost_unloaded:
            return
        if action == "save":
            async with self._boost_lock:
                await self._async_save_heating_session()
            return
        if action == "restore":
            await self._async_finish_heating_boost("restart")

    def _apply_open_heating_session_display(self) -> None:
        """Show the session deadline while it is still open."""
        session = self._heating_session
        if session is None or session.finished:
            return
        self.heat_boost = True
        remaining_minutes = int((session.deadline - utcnow()).total_seconds() // 60)
        self.heat_boost_remaining = max(remaining_minutes, 0)

    def _arm_heating_deadline(self) -> None:
        """Schedule the one restore for the current session token."""
        self._cancel_heating_deadline()
        session = self._heating_session
        if session is None or session.finished or self._boost_unloaded:
            return
        token = session.token
        scheduled_for = session.deadline

        @callback
        def _fired(scheduled: datetime) -> None:
            current = self._heating_session
            if (
                self._boost_unloaded
                or current is None
                or current.finished
                or current.token != token
                or current.deadline > scheduled + timedelta(seconds=1)
            ):
                return
            self.hass.async_create_task(
                self._async_finish_heating_boost("deadline"),
                name=f"{DOMAIN} heating boost deadline {self.entry_id}",
                eager_start=False,
            )

        when = max(scheduled_for, utcnow())
        self._deadline_unsub = async_track_point_in_utc_time(self.hass, _fired, when)

    def _cancel_heating_deadline(self) -> None:
        """Cancel the armed restore callback."""
        if self._deadline_unsub is not None:
            self._deadline_unsub()
            self._deadline_unsub = None

    async def _async_save_heating_session(self) -> None:
        """Persist the session. A failure leaves the in-memory decision intact."""
        session = self._heating_session
        if session is None:
            return
        try:
            await self._heating_store().async_save(session.to_storage())
        except Exception:  # noqa: BLE001
            LOGGER.warning(
                "Heating boost session could not be saved for %s",
                self.entry_id,
                exc_info=True,
            )

    async def _async_finish_heating_boost(self, reason: str) -> None:
        """Publish at most one restore, or deliberately publish nothing."""
        if self._boost_unloaded:
            return
        publish: str | None = None
        restore_setpoint: float | None = None
        async with self._boost_lock:
            session = self._heating_session
            if session is None or session.finished:
                return
            action = decide_heating_restore(
                session,
                fresh_state_seen=self._heating_state_seen,
                system_mode=self._reported_heat_system_mode,
                setpoint=_setpoint_or_none(self.target_temperature),
            )
            if action == "wait":
                LOGGER.debug(
                    "Heating boost restore for %s is waiting for a device report",
                    self.entry_id,
                )
                return
            session.finished = True
            self._cancel_heating_deadline()
            await self._async_save_heating_session()
            self.heat_boost = False
            self.heat_boost_remaining = 0
            if action == "skip":
                LOGGER.info(
                    "Heating boost for %s ended without a device write (%s)",
                    self.entry_id,
                    reason,
                )
                self.async_update_listeners()
                return
            publish = action
            restore_setpoint = session.pre_boost_setpoint
        if self._boost_unloaded or publish is None:
            return
        self._applying_restore = True
        try:
            await self._async_publish_heating_restore(publish, restore_setpoint)
        finally:
            self._applying_restore = False
        self.async_update_listeners()

    async def _async_publish_heating_restore(
        self, action: str, setpoint: float | None
    ) -> None:
        """Publish the pre-boost mode without recreating the boost."""
        if action == "heat" and setpoint is not None:
            await self.async_set_hvac_mode_heat(setpoint)
            return
        if action == "auto":
            await self.async_set_hvac_mode_auto()
            return
        if action == "off":
            self.hvac_mode = HVACMode.OFF
            await self._async_publish_set(self._heating_off_payload())


def _setpoint_or_none(value: Any) -> float | None:
    """Return a setpoint float, or None for missing and boolean values."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)
