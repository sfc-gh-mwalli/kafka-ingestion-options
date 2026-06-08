import os
import tomllib

# Suppress Snowpipe Streaming SDK INFO/WARN logs — only show errors
os.environ.setdefault("SS_LOG_LEVEL", "error")

def _load_pat_from_connections_toml():
    toml_path = os.path.expanduser("~/.snowflake/connections.toml")
    with open(toml_path, "rb") as f:
        conns = tomllib.load(f)
    for conn in conns.values():
        if (
            conn.get("account", "").upper() == "SFSENORTHAMERICA-MWALLI_AWSUSEAST2"
            and conn.get("user", "").upper() == "MWALLI"
            and conn.get("password")
        ):
            return conn["password"]
    raise RuntimeError("PAT not found in ~/.snowflake/connections.toml for SFSENORTHAMERICA-MWALLI_AWSUSEAST2 / MWALLI")

SNOWFLAKE_ACCOUNT = "SFSENORTHAMERICA-MWALLI_AWSUSEAST2"
SNOWFLAKE_ROLE = "SYSADMIN"
SNOWFLAKE_DATABASE = "KAFKA_DEMO"
SNOWFLAKE_SCHEMA = "STREAMING"
SNOWFLAKE_TABLE = "CLICKSTREAM"
SNOWFLAKE_PAT = os.environ.get("SNOWFLAKE_PAT") or _load_pat_from_connections_toml()

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "events"
KAFKA_GROUP_ID = "snowpipe-demo"
