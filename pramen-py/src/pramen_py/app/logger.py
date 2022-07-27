#  Copyright 2022 ABSA Group Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

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
