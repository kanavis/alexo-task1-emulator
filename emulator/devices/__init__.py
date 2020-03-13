
from typing import Dict, Type

from emulator.devices.base import Device
from emulator.devices.sensors import Mass, End, Temperature, Keyboard

device_map: Dict[str, Type[Device]] = {
    'mass': Mass,
    'end': End,
    'temperature': Temperature,
    'keyboard': Keyboard,
}
