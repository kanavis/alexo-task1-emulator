#!/usr/bin/env python
import emulator.patch

import logging
import os
import sys
from typing import List, Dict

from gevent import Greenlet, joinall

from emulator.common import init
from emulator.devices import Device, device_map
from emulator.event_socket import EventSocketServer

log = logging.getLogger('emulator')
emulator.patch.noop()
base_path = os.path.realpath(os.path.dirname(__file__))

config = init(base_path, False)


# Create devices
devices: Dict[str, Device] = {}
for name, dc in config.devices.items():
    try:
        device_cls = device_map[dc.type]
    except KeyError:
        raise ValueError(f'Unknown device type {dc.type}')

    # noinspection PyBroadException
    try:
        device = device_cls(base_path, name, dc.host, dc.port)
    except Exception:
        log.exception(f'Exception during device {name} init')
        sys.exit(255)

    devices[name] = device


def runner(f_run, d_name):
    while True:
        # noinspection PyBroadException
        try:
            log.info(f'Starting {d_name}')
            f_run()
            log.info(f'{d_name} stopped. Exiting')
            sys.exit(255)
        except Exception:
            log.exception(f'Exception in {d_name} runtime:')
            sys.exit(255)


# Run device greenlets
greenlets: List[Greenlet] = [
    Greenlet.spawn(runner, device.run, f'[device {name}]')
    for name, device in devices.items()
]

# Listen for events
event_listener = EventSocketServer(base_path, devices)
greenlets.append(Greenlet.spawn(runner, event_listener.run, 'event listener'))

# Join greenlets
try:
    joinall(greenlets)
except KeyboardInterrupt:
    print('Killed')
