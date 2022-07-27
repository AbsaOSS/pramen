from environs import Env

from pramen_py.app.logger import setup_logger


env = Env(expand_vars=True)
env.read_env()

setup_logger(env=env)
