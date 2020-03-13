import logging
import os
import sys

from emulator.config import Config

log = logging.getLogger('emulator')


def init(path: str, logger_simple: bool) -> Config:
    # Load config
    config_path = os.path.join(path, 'config.yaml')
    config = Config(config_path)

    # Setup logger
    log.setLevel(logging.DEBUG)
    usual_formatter = logging.Formatter(
        '%(message)s' if logger_simple
        else '%(asctime)s %(levelname)s - %(message)s',
    )
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(usual_formatter)
    log.addHandler(console_handler)

    return config
