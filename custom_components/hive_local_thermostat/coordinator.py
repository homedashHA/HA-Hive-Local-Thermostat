"""Coordinator for Hive Local Thermostat integration."""

from __future__ import annotations

import json
from asyncio import sleep
from datetime import datetime
from typing import Any, cast

from homeassistant.components.climate.const import (
    PRESET_BOOST,
    PRESET_NONE,
    HVACAction,
    HVACMode,
)
from homeassistant.components.mqtt import client as mqtt_client
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util.dt import utcnow

from .const import (
    DEFAULT_FROST_TEMPERATURE,
    DEFAULT_HEATING_BOOST_MINUTES,
    DEFAULT_HEATING_BOOST_TEMPERATURE,
    DEFAULT_WATER_BOOST_MINUTES,
    DOMAIN,
    HIVE_BOOST,
    LOGGER,
    MODEL_SLR2,
)

PRESET_MAP = {
    PRESET_NONE: "",
    PRESET_BOOST: HIVE_BOOST,
}

BOOST_ERROR = 65000
_HIVE_BOOST_MODE = "emergency_heating"


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

                if system_mode_heat != _HIVE_BOOST_MODE:
                    self.pre_boost_occupied_heating_setpoint_heat = (
                        self.target_temperature
                    )
                    self.pre_boost_hvac_mode = self.hvac_mode
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

                if system_mode != _HIVE_BOOST_MODE:
                    self.pre_boost_occupied_heating_setpoint_heat = (
                        self.target_temperature
                    )
                    self.pre_boost_hvac_mode = self.hvac_mode

            if self.correct_heat_boost(
                reported_boost_remaining_heat, reported_boost_temperature
            ):
                return  # Correction made, exit to avoid state update loop
            self.record_heat_boost_state()

            if self.model == MODEL_SLR2:
                if self.correct_water_boost(reported_boost_remaining_water):
                    return  # Correction made, exit to avoid state update loop
                self.record_water_boost_state()

            self.async_set_updated_data(parsed_data)
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
        reported_boost_temperature: float | None,
    ) -> bool:
        """Check and correct boost remaining heat if necessary."""
        if reported_boost_remaining_heat is None:
            # Zigbee2MQTT often publishes emergency_heating before the hold
            # duration, or omits it entirely. Keep the locally tracked countdown
            # and do not republish a boost from a value the device did not send.
            tracked = _tracked_boost_remaining(
                self.heat_boost_started, self.heat_boost_started_duration
            )
            if tracked is not None:
                self.heat_boost_remaining = tracked
            LOGGER.debug(
                "Heating boost duration was missing from the MQTT payload; using %s minutes remaining",
                self.heat_boost_remaining,
            )
            return False

        if reported_boost_remaining_heat > BOOST_ERROR:
            tracked = _tracked_boost_remaining(
                self.heat_boost_started, self.heat_boost_started_duration
            )
            self.heat_boost_remaining = 0 if tracked is None else tracked

            LOGGER.warning(
                "Correcting reported boost remaining heat from %d to %d",
                reported_boost_remaining_heat,
                self.heat_boost_remaining,
            )
            if self.config_entry is not None:
                self.config_entry.async_create_task(
                    self.hass,
                    self.async_heating_boost(
                        self.heat_boost_remaining, reported_boost_temperature
                    ),
                )
            return True
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
        if self.heat_boost and self.heat_boost_remaining > 0:
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
        boost_duration_minutes: int | None = None,
        boost_temperature: float | None = None,
    ) -> None:
        """Send heating boost command."""

        self.pre_boost_occupied_heating_setpoint_heat = self.target_temperature
        self.pre_boost_hvac_mode = self.hvac_mode

        duration = str(int(boost_duration_minutes or self.heating_boost_duration))
        temperature = str(boost_temperature or self.heating_boost_temperature)

        if self.model == MODEL_SLR2:
            payload = (
                r'{"system_mode_heat":"emergency_heating","temperature_setpoint_hold_duration_heat":'
                + duration
                + r',"temperature_setpoint_hold_heat":1,"occupied_heating_setpoint_heat":'
                + temperature
                + r"}"
            )
        else:
            payload = (
                r'{"system_mode":"emergency_heating","temperature_setpoint_hold_duration":'
                + duration
                + r',"temperature_setpoint_hold":1,"occupied_heating_setpoint":'
                + temperature
                + r"}"
            )

        self.heat_boost = True
        self.heat_boost_started = utcnow()
        self.heat_boost_started_duration = int(duration)

        await self._async_publish_set(payload)

    async def async_heating_boost_cancel(self) -> None:
        """Cancel heating boost command."""

        if self.pre_boost_hvac_mode == HVACMode.AUTO:
            await self.async_set_hvac_mode_auto()
        elif self.pre_boost_hvac_mode == HVACMode.HEAT:
            if self.pre_boost_occupied_heating_setpoint_heat is not None:
                await self.async_set_hvac_mode_heat(
                    self.pre_boost_occupied_heating_setpoint_heat
                )
            else:
                await self.async_set_hvac_mode_heat(self.heating_frost_prevention)
        else:
            await self.async_set_hvac_mode_off()

    async def async_set_temperature(self, temperature: float) -> None:
        """Set temperature."""

        if self.model == MODEL_SLR2:
            payload = r'{"occupied_heating_setpoint_heat":' + str(temperature) + r"}"
        else:
            payload = r'{"occupied_heating_setpoint":' + str(temperature) + r"}"

        await self._async_publish_set(payload)

    async def async_set_hvac_mode_off(self) -> None:
        """Set HVAC mode to off."""

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
