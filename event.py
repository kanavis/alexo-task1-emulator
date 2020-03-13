#!/usr/bin/env python
import time

import emulator.patch

import os
import argparse
import logging

from emulator.common import init
from emulator.event_socket import EventSocketClient

log = logging.getLogger('emulator')
emulator.patch.noop()
base_path = os.path.realpath(os.path.dirname(__file__))


def pos_int(val):
    val = int(val)
    if val < 0:
        raise argparse.ArgumentTypeError('Positive int required')
    return val


parser = argparse.ArgumentParser(description='Send event')
parser.add_argument('--ts', type=int, help='timestamp, default=now')
parser.add_argument('--retries', type=pos_int, help='retry webhook N times')
parser.add_argument('device', type=str, help='device name')
parser.add_argument('value', type=str, help='event value')

args = parser.parse_args()

config = init(base_path, True)
event_socket = EventSocketClient(base_path)
event_id = event_socket.event(
    dev_name=args.device,
    ts=args.ts or int(time.time()),
    value=args.value,
    retries=args.retries,
)
if event_id:
    log.info(f'Sent event id={event_id}')
else:
    log.info('Failed to send event')
