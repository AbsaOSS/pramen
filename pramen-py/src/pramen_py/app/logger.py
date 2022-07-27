import sys

from typing import Optional

from environs import Env
from loguru import logger


def setup_logger(level: str = "INFO", env: Optional[Env] = None) -> int:
    force_debug = env.bool("PRAMENPY_DEBUG", False) if env else False
    level = "DEBUG" if force_debug else level
    logger.remove()
    if level == "INFO":
        logger.disable("py4j")
    logger_id = logger.add(
        sink=sys.stderr,
        level=level,
        format="{time} | {level} | {name} | {extra} | {message}",
    )
    logger.debug(f"Logger setup complete. Logger level is {level}.")
    return logger_id
