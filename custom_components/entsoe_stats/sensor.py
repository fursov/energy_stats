from __future__ import annotations

import logging
from datetime import datetime, timedelta
from math import ceil

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.sensor import PLATFORM_SCHEMA, SensorEntity
from homeassistant.const import CURRENCY_EURO, STATE_UNKNOWN
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from homeassistant.util import dt

log = logging.getLogger(__name__)

DEFAULT_WINDOWS_LENGTHS = tuple(
    1 + 0.5 * i for i in range(11)
)  # Default from 1h to 6h with step 0.5
ENTSOE_CONFIG_ID = "entsoe_prices_entity"
HIGH_PRICE_MARGIN_RATIO = "high_price_margin_ratio"

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {
        vol.Optional(
            ENTSOE_CONFIG_ID, default="sensor.average_electricity_price_today"
        ): cv.string,
        vol.Optional(
            HIGH_PRICE_MARGIN_RATIO, default=0.0
        ): vol.Coerce(float),
    }
)


def setup_platform(
    _hass: HomeAssistant,
    config: ConfigType,
    add_entities: AddEntitiesCallback,
    _discovery_info: DiscoveryInfoType | None = None,
) -> None:
    entsoe_prices_entity = config[ENTSOE_CONFIG_ID]
    high_price_margin_ratio = config[HIGH_PRICE_MARGIN_RATIO]
    add_entities([EntsoeStatsCalculator(entsoe_prices_entity, high_price_margin_ratio)])


def convert_entsoe_data(entsoe_prices):
    prices = entsoe_prices.attributes.get("prices")
    return list(
        [
            {
                "time": datetime.fromisoformat(value["time"]),
                "price": float(value["price"]),
            }
            for value in prices
        ]
    )


def get_double_precision_for_last_24_hours(hourprices, time_now) -> tuple:
    # We are monitoring prices within 24 hours. If we have prices for the future
    # then start from now. If we do not have enough future data, limit window
    # to the 24 hour in the past from last known time
    last_known_time = sorted([value["time"] for value in hourprices])[-1]
    time_for_24h_from_last = last_known_time - timedelta(days=1)
    time_for_24h_ahead_from_now = time_now + timedelta(hours=23)
    min_time = min(time_now, time_for_24h_from_last)
    max_time = min(time_for_24h_ahead_from_now, last_known_time)
    # Double the data to get the 30-minutes resolution
    hour_prices_24h = []
    half_hour_prices_24h = []
    for item in hourprices:
        hour, price = item["time"], item["price"]
        if min_time <= hour <= max_time:
            hour_prices_24h.append(
                {
                    "time": hour,
                    "price": price,
                }
            )
            half_hour_prices_24h.append(
                {
                    "time": hour,
                    "price": price / 2,
                }
            )
            half_hour_prices_24h.append(
                {
                    "time": hour + timedelta(minutes=30),
                    "price": price / 2,
                }
            )
    return hour_prices_24h, half_hour_prices_24h


def get_stats_for_consumption_duration(
    hour_prices_24h, half_hour_prices_24h, duration: float, high_price_margin_ratio: float, time_now: datetime
) -> dict:
    total_price = sum([value["price"] for value in half_hour_prices_24h])
    window = ceil(duration * 2)
    if window > len(half_hour_prices_24h):
        log.warning("Not enough data for window: %f", duration)
        return {
            "window": duration,
            "time": STATE_UNKNOWN,
            "total_best": STATE_UNKNOWN,
            "average_best": STATE_UNKNOWN,
            "average_other_time": STATE_UNKNOWN,
            "high_prices": STATE_UNKNOWN,
            "high_price_now": STATE_UNKNOWN,
        }
    half_hour_windows = [
        half_hour_prices_24h[i : i + window]
        for i in range(len(half_hour_prices_24h) - window + 1)
    ]
    window_totals = tuple(
        [
            sum(map(lambda x: x["price"], window_prices))
            for window_prices in half_hour_windows
        ]
    )
    min_price = min(window_totals)
    min_price_index = window_totals.index(min_price)
    total_best_price = sum(
        [
            value["price"]
            for value in half_hour_prices_24h[
                min_price_index : min_price_index + window
            ]
        ]
    )
    avrg_best_price = total_best_price / duration
    total_other_price = total_price - total_best_price
    avrg_other_price = total_other_price / ((len(half_hour_prices_24h) - window) / 2)
    high_price_limit = avrg_other_price * (1 + high_price_margin_ratio)
    high_prices = tuple(
        [value for value in hour_prices_24h if value["price"] > high_price_limit]
    )
    now_high_price = any(value["time"] == time_now for value in high_prices)
    return {
        "window": duration,
        "time": half_hour_prices_24h[min_price_index]["time"],
        "total": round(total_best_price, 5),
        "average": round(avrg_best_price, 5),
        "average_other_time": round(avrg_other_price, 5),
        "high_prices": high_prices,
        "high_price_now": now_high_price,
    }


class EntsoeStatsCalculator(SensorEntity):
    _attr_icon = "mdi:flash"
    _attr_name = "entsoe_stats_prices"
    _attr_native_unit_of_measurement = CURRENCY_EURO
    _attr_unique_id = "prices"

    def __init__(self, entsoe_prices_entity: str, high_price_margin_ratio: float):
        self._entsoe_prices_entity = entsoe_prices_entity
        self._high_price_margin_ratio = high_price_margin_ratio
        self._prices = STATE_UNKNOWN
        self._known_prices = STATE_UNKNOWN
        self._best_prices = STATE_UNKNOWN

    @property
    def state(self):
        return self._prices

    @property
    def extra_state_attributes(self):
        return {
            "prices": self._known_prices,
            "best_prices": self._best_prices,
        }

    def update(self):
        log.debug("Sensor update called")
        entsoe_prices = self.hass.states.get(self._entsoe_prices_entity)
        try:
            self._known_prices = convert_entsoe_data(entsoe_prices)
        except Exception as exc:
            log.exception(
                "cannot convert ENTSOe prices to usable format. Original message %s",
                repr(exc),
            )
        self._prices = min(value["price"] for value in self._known_prices)
        time_now = dt.now()
        hour_prices_24h, half_hour_prices_24h = get_double_precision_for_last_24_hours(
            self._known_prices, time_now
        )
        self._best_prices = {}
        for duration in DEFAULT_WINDOWS_LENGTHS:
            self._best_prices[duration] = get_stats_for_consumption_duration(
                hour_prices_24h, half_hour_prices_24h, duration, self._high_price_margin_ratio, time_now
            )
