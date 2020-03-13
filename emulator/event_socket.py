""" Event socket to send and receive data """
import json
import logging
import os
from gevent import socket
from typing import Dict, Any, Optional

from emulator.devices import Device

log = logging.getLogger('emulator.event_socket')


class EventSocket:
    def __init__(self, path: str):
        self.socket_path = os.path.join(path, 'data', 'event.sock')
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    @staticmethod
    def send_dgram(sock: socket.socket, data):
        b_data = json.dumps(data).encode('utf8')
        sock.send(len(b_data).to_bytes(4, 'big'))
        sock.send(b_data)

    @staticmethod
    def read_dgram(sock: socket.socket) -> Any:
        b_len = sock.recv(4)
        data_len = int.from_bytes(b_len, 'big')
        data_raw = sock.recv(data_len)
        return json.loads(data_raw.decode('utf8'))


class EventSocketClient(EventSocket):
    def event(self, dev_name: str, ts: int,
              value: str, retries: Optional[int] = None) -> Optional[int]:
        try:
            self.socket.connect(self.socket_path)
        except OSError as err:
            log.error(f'Connect to event socket {self.socket_path}: {err}')
            return
        data = {
            'dev_name': dev_name,
            'ts': ts,
            'value': value,
        }
        if retries is not None:
            data['retries'] = retries
        self.send_dgram(self.socket, data)
        try:
            reply = self.read_dgram(self.socket)
        except json.JSONDecodeError as err:
            log.error(f'Wrong reply from server: {err}')
            return

        if 'error' in reply:
            log.error(f'Server said: {reply["error"]}')
            return

        if 'id' not in reply or not isinstance(reply['id'], int):
            log.error(f'Wrong reply from server: {reply}')
            return

        return reply['id']


class EventSocketServer(EventSocket):
    def __init__(self, path: str, devices: Dict[str, Device]):
        super().__init__(path)
        self.devices = devices

    def reply_error(self, sock: socket.socket, message: str):
        try:
            self.send_dgram(sock, {'error': message})
        except socket.error as err:
            log.error(f'Error sending error reply: {err}')

    def serve(self, sock: socket.socket):
        # noinspection PyBroadException
        try:
            data = self.read_dgram(sock)
            assert isinstance(data, dict), f'Data is not a dict: {data}'
            assert 'dev_name' in data, f'dev_name not set: {data}'
            assert 'ts' in data, f'ts not set: {data}'
            assert 'value' in data, f'value not set: {data}'
            assert isinstance(data['dev_name'], str), f'dev_name fmt: {data}'
            assert isinstance(data['ts'], int), f'ts fmt: {data}'
            assert isinstance(data['value'], str), f'value fmt: {data}'
            kwargs = {}
            if 'retries' in data:
                assert isinstance(data['retries'], int), f'retries fmt: {data}'
                kwargs['retries'] = data['retries']

            if not data['dev_name'] in self.devices:
                raise ValueError(f'Unknown device {data["dev_name"]}')
            device = self.devices[data['dev_name']]

            try:
                event_id = device.event(data['ts'], data['value'], **kwargs)
                self.send_dgram(sock, {'id': event_id})
            except ValueError as err:
                raise ValueError(f'Wrong event data format: {err}')
            except Device.EventError as err:
                self.reply_error(sock, str(err))
        except AssertionError as err:
            msg = f'wrong data received: {err}'
            log.error(f'Event socket: {msg}')
            self.reply_error(sock, msg)
        except ValueError as err:
            log.warning(f'Event socket: {err}')
            self.reply_error(sock, str(err))
        except Exception as err:
            log.exception('Event socket: exception in serve')
            self.reply_error(sock, f'Internal error: {err}')
        finally:
            try:
                sock.close()
            except OSError:
                pass

    def run(self):
        try:
            os.unlink(self.socket_path)
        except OSError:
            if os.path.exists(self.socket_path):
                raise
        try:
            self.socket.bind(self.socket_path)
        except OSError as err:
            log.error(f'Listen {self.socket_path}: {err}')
            return

        self.socket.listen()
        log.info('Event socket: listening')
        while True:
            connection, _ = self.socket.accept()
            self.serve(connection)
