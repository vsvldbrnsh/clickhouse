from enum import StrEnum

from airflow.models import Variable

class Env(StrEnum):
    PROD = "PROD"
    TEST = "TEST"

env = Variable.get("AIRFLOW_ENV", default_var=Env.TEST)

if env == Env.PROD:
    from . import prod as config
else:
    from . import test as config