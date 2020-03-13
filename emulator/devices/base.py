""" Basic device classes """

import json
import logging
import os
import struct
from contextlib import contextmanager
from datetime import datetime
import sqlite3
from typing import Union, Optional, Tuple
from urllib.parse import urlparse

import requests
from flask import Flask, abort, Response, request
from gevent import sleep, socket
from gevent.lock import BoundedSemaphore
from gevent.pool import Pool
from gevent.pywsgi import WSGIServer

log = logging.getLogger('emulator.devices.device')
TValue = Union[str, int, float]


class Device:
    class EventError(Exception):
        pass

    def __init__(self, path: str, name: str, host: str, port: int):
        """ Constructor """
        self._name = name
        self.host = host
        self.port = port
        self.datadir = os.path.join(path, 'data')
        if not os.path.exists(self.datadir):
            os.mkdir(self.datadir)
        db_path = os.path.join(self.datadir, f'device_{self._name}.sqlite3')
        self.lock_db = BoundedSemaphore(1)
        self.db = sqlite3.connect(db_path)

    @property
    def name(self):
        return f'[DEV {self._name}]'

    @contextmanager
    def cursor(self):
        """ Cursor decorator with database lock """
        cursor = self.db.cursor()
        try:
            self.lock_db.acquire()
            yield cursor
            self.db.commit()
        finally:
            # noinspection PyBroadException
            try:
                if self.lock_db.locked():
                    self.lock_db.release()
            except Exception:
                pass
            # noinspection PyBroadException
            try:
                cursor.close()
            except Exception:
                pass

    def run(self):
        """ Run """
        raise NotImplementedError()

    def event(self, ts: datetime, value: TValue, retries: int = 0) -> int:
        """ Notify event """
        raise NotImplementedError()

    def _value(self, value: str) -> TValue:
        """ Convert value """
        raise NotImplementedError()


class WebhookDevice(Device):
    server: WSGIServer

    def __init__(self, path: str, name: str, host: str, port: int):
        super().__init__(path, name, host, port)

        with self.cursor() as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS subscription 
                (callback_url VARCHAR(255))
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS event_id 
                (id int integer)
            """)
            res = c.execute("""SELECT callback_url FROM subscription""")
            row = res.fetchone()
            if row:
                self.callback_url = row[0]
                log.info(
                    f'{self.name} Loaded callback URL "{self.callback_url}"')
            else:
                self.callback_url = None

    def run(self):
        app = Flask('device_{}'.format(self.name))

        @app.route('/subscribe', methods=['GET'])
        def subscribe():
            callback = request.args.get('callback')
            if callback is None:
                abort(Response('Missing callback param', 400))

            # noinspection PyBroadException
            try:
                res = urlparse(callback)
                if not res.scheme:
                    raise Exception('No scheme')
                if not res.netloc:
                    raise Exception('No host')
            except Exception as err:
                message = f'Wrong callback format: {err}'
                log.warning('{} Subscribe call from {}: {}'.format(
                    log.name,
                    request.remote_addr,
                    message,
                ))
                abort(Response(message, 400))

            with self.cursor() as c:
                c.execute("""DELETE FROM subscription WHERE 1""")
                c.execute(
                    """INSERT INTO subscription VALUES (?)""",
                    (callback,),
                )
            self.callback_url = callback
            log.info('{} Subscribe call from {}: subscribed to {}'.format(
                self.name,
                request.remote_addr,
                self.callback_url,
            ))

            return 'OK'

        self.server = WSGIServer((self.host, self.port), app)
        log.info(f'{self.name} Listening for HTTP on {self.host}:{self.port}')
        self.server.serve_forever()

    def _value(self, value: str) -> Union[int, str, float]:
        raise NotImplementedError()

    def event(self, ts: int, value: TValue, retries: int = 0) -> int:
        if self.callback_url is None:
            log.warning(f'{self.name}: no subscriptions')
            raise self.EventError('No subscriptions')

        # Get event id
        with self.cursor() as c:
            res = c.execute("""SELECT id FROM event_id""")
            row = res.fetchone()
            if row:
                event_id = int(row[0]) + 1
            else:
                event_id = 1
            event = {
                'id': event_id,
                'time': ts,
                'value': value,
            }
            c.execute("""UPDATE event_id SET id = ?""", (event_id,))

        # Send event
        while True:
            request_error = None
            try:
                res = requests.post(self.callback_url, json=event)
                if not res.ok:
                    request_error = (
                        f'callback returned {res.status_code}:'
                        f' {res.content}'
                    )
            except requests.exceptions.RequestException as err:
                request_error = str(err)

            if request_error is None:
                log.info('{} Sent event to url'.format(
                    self.name,
                    self.callback_url
                ))
                return event_id
            else:
                log.error(f'{self.name} Error sending event: {request_error}')

                if retries == 0:
                    raise self.EventError(request_error)

                log.info(
                    f'{self.name} Retrying in 5 seconds... '
                    f'Attempts left: {retries}'
                )
                sleep(5)
                retries -= 1


class CustomEventProtoDevice(Device):

    class ProtocolError(Exception):
        pass

    def __init__(self, path: str, name: str, host: str, port: int):
        super().__init__(path, name, host, port)

        with self.cursor() as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id integer primary key AUTOINCREMENT,
                    ts integer,
                    value text
                )
            """)
            c.execute("""
                CREATE INDEX IF NOT EXISTS time on events(ts)
            """)

        self.executor_pool = Pool(10)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock.bind((self.host, self.port))

    def serve(self, conn: socket.socket, address: Tuple[str, int]):
        """ Serve incoming connection """
        try:
            conn.settimeout(3)
            # Read
            try:
                magic_number = conn.recv(4)
                if magic_number != b'\xde\xad\xbe\xef':
                    raise self.ProtocolError(
                        f'Wrong magic number from {address}')

                timestamp_b = conn.recv(8)
                timestamp = int.from_bytes(timestamp_b, 'big')
                term = conn.recv(1)
                if term != b'\0':
                    raise self.ProtocolError(f'Wrong term from {address}')

            except socket.timeout:
                log.warning(f'{self.name} Read timeout from {address}')
                conn.close()
                return

            except self.ProtocolError as err:
                log.warning(f'{self.name} {err}')
                conn.close()
                return

            # Reply
            with self.cursor() as c:
                c.execute(
                    """
                        SELECT id, ts, value FROM events 
                        WHERE time >= ? ORDER BY ts
                    """,
                    (timestamp,),
                )
                rows = c.fetchall()

                conn.send(b'\xde\xad\xbe\xef')
                conn.send(len(rows).to_bytes(4, 'big'))
                for event_id, event_ts, raw_value in rows:
                    assert isinstance(event_id, int)
                    assert isinstance(event_ts, int)
                    value = self._value(raw_value)
                    conn.send(event_id.to_bytes(8, 'big'))
                    conn.send(event_ts.to_bytes(8, 'big'))
                    if isinstance(value, int):
                        conn.send((8).to_bytes(4, 'big'))
                        conn.send(value.to_bytes(8, 'big'))
                    elif isinstance(value, float):
                        conn.send((8).to_bytes(4, 'big'))
                        conn.send(bytes(struct.pack('f', value)))
                    elif isinstance(value, str):
                        b_value = value.encode('utf8')
                        conn.send(len(b_value).to_bytes(4, 'big'))
                        conn.send(b_value)
                    else:
                        raise AssertionError('Wrong value type returned')
                conn.send(b'\0')
                conn.close()

        except Exception:
            log.exception(f'Exception in {self.name} handler for {address}')
            conn.close()
            raise

    def run(self):
        self.sock.listen()
        log.info(f'{self.name} listening on {self.host}:{self.port}')

        while True:
            connection, client_address = self.sock.accept()
            log.info(f'{self.name} New connection from {client_address}')
            self.executor_pool.apply_async(
                self.serve, (connection, client_address))

    def _value(self, value: str) -> TValue:
        raise NotImplementedError()

    def event(self, ts: int, value: TValue,
              retries: int = 0) -> Optional[int]:
        with self.cursor() as c:
            c.execute(
                """INSERT INTO events (ts, value) VALUES (?, ?)""",
                (ts, str(value)),
            )
            return c.lastrowid
