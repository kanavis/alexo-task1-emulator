""" Config """
from typing import NamedTuple, Dict

import yaml


class Config:
    class ConfigError(Exception):
        pass

    class DeviceConfig(NamedTuple):
        type: str
        host: str
        port: str

    devices: Dict[str, DeviceConfig]

    def __init__(self, file_path: str):
        with open(file_path) as f:
            self.data = yaml.safe_load(f)

        if 'devices' not in self.data:
            raise self.ConfigError('No devices section')

        self.devices = {}
        for k, v in self.data['devices'].items():
            self.devices[k] = self.DeviceConfig(**v)
