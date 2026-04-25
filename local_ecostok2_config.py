import os


SUPABASE_URL = "https://ecostok2.lidertex.cloud"
SUPABASE_SERVICE_KEY_ENV = "ECOSTOK2_SUPABASE_SERVICE_KEY"

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
    "user": os.getenv("POSTGRES_USER", "ecostock"),
    "password": os.getenv("POSTGRES_PASSWORD", "Kd*2m5Th"),
    "dbname": os.getenv("POSTGRES_DB", "onec_ecostock_retail"),
    "port": os.getenv("POSTGRES_PORT", "5444"),
}

TARGET_TABLES = {
    "sales": "ecostok2_sales_analytics",
    "inventory": "ecostok2_inventory_analytics",
    "visitors": "ecostok2_visitors_analytics",
    "weights": "ecostok2_product_weights",
}

DATE_FIELDS = {
    "sales": "sale_date",
    "inventory": "snapshot_date",
    "visitors": "visit_date",
}

DRY_RUN_LIMIT = 1000
BATCH_SIZE = 500
DEFAULT_DAYS_BACK = 3


def get_db_config() -> dict:
    return dict(DB_CONFIG)


def get_supabase_url() -> str:
    return SUPABASE_URL


def get_supabase_key() -> str:
    key = os.getenv(SUPABASE_SERVICE_KEY_ENV)
    if not key:
        raise RuntimeError(
            f"Missing required env var: {SUPABASE_SERVICE_KEY_ENV}. "
            "Refusing to proceed without local Supabase service key."
        )
    return key


def get_target_table(target: str) -> str:
    return TARGET_TABLES[target]


def get_weights_table() -> str:
    return TARGET_TABLES["weights"]
