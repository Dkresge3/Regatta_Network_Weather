import logging
import os
import psycopg2
import pydantic
import yaml
from io import StringIO
from typing import Dict, Any, Optional
from pydantic import BaseModel
from jinja2 import Environment

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DBTarget(BaseModel):
    type: str
    keepalives_idle: int
    connect_timeout: int
    retries: int
    user: str
    schema: str
    account: Optional[str] = None
    role: Optional[str] = None
    database: str
    password: Optional[str] = None
    port: int


DBTarget.update_forward_refs()


class DBTProfile(BaseModel):
    target: str
    outputs: Dict[str, DBTarget]


DBTProfile.update_forward_refs()


def env_var(variable_name: str, default: Optional[str] = None) -> Optional[str]:
    logging.debug(f"Getting environment variable: {variable_name}")
    return os.getenv(variable_name, default)


jinja_env = Environment()
jinja_env.filters["env_var"] = env_var


def get_profiles() -> Dict[str, DBTProfile]:
    with open("profiles.yml") as f:
        template = jinja_env.from_string(f.read())

    profiles_string = template.render()
    obj: Dict[str, Any] = yaml.load(StringIO(profiles_string), Loader=yaml.SafeLoader)
    if "config" in obj.keys():
        obj.pop("config")
    for k, v in obj.items():
        if "defaults" in v["outputs"]:
            v["outputs"].pop("defaults")

    profiles = pydantic.parse_obj_as(Dict[str, DBTProfile], obj)
    return profiles


def connect_to_postgres(db_target: DBTarget):
    try:
        conn = psycopg2.connect(
            dbname=db_target.database,
            user=db_target.user,
            password=db_target.password,
            host=db_target.account,
            port=db_target.port,
        )
        logging.info("Successfully connected to PostgreSQL")
        return conn
    except Exception as e:
        logging.exception("Error connecting to PostgreSQL")
        return None


def get_postgres_connection(connect_profile: str, connect_target: str):
    profiles = get_profiles()
    target = profiles[connect_profile].outputs[connect_target]
    return connect_to_postgres(target)
