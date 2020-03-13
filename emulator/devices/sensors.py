""" Sensor classes """

from emulator.devices.base import WebhookDevice, CustomEventProtoDevice, TValue


class Mass(WebhookDevice):
    def _value(self, value: str) -> TValue:
        return float(value)


class End(WebhookDevice):
    def _value(self, value: str) -> TValue:
        if value not in ('press', 'release'):
            raise ValueError(f'need to be "press" or "release", got "{value}"')
        return value


class Temperature(CustomEventProtoDevice):
    def _value(self, value: str) -> TValue:
        value = int(value)
        if value < -500 or value > 500:
            raise ValueError('must be between -500 and 500')
        return value/10


class Keyboard(CustomEventProtoDevice):
    def _value(self, value: str) -> TValue:
        return value
