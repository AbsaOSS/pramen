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

import os
import pathlib

from environs import Env

from pramen_py.runner.runner_transformation import (
    DEFAULT_TRANSFORMATIONS_DIR,
    get_transformations_dir,
)


def test_get_transformations_dir():
    os.environ["test_dir_os"] = "os"
    with open(".env.for_test", "w") as fobj:
        fobj.write("test_dir_env=env\n")
    env = Env()
    env.read_env(".env.for_test", recurse=False)

    assert get_transformations_dir("test_dir_os", env) == pathlib.Path("os")
    assert get_transformations_dir("test_dir_env", env) == pathlib.Path("env")
    assert get_transformations_dir("test_dir_def", env) == pathlib.Path(
        DEFAULT_TRANSFORMATIONS_DIR
    )
