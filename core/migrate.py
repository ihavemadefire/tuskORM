import os
import sys
import asyncpg
from typing import Dict, List
from .db_ops import get_db_config


# Directory where models will be generated
MODELS_DIR = "models"

# PostgreSQL → Python type mapping
PG_TO_PY_TYPE = {
    "integer": "int",
    "smallint": "int",
    "bigint": "int",
    "serial": "int",
    "bigserial": "int",
    "uuid": "uuid.UUID",
    "text": "str",
    "varchar": "str",
    "char": "str",
    "boolean": "bool",
    "real": "float",
    "double precision": "float",
    "numeric": "float",
    "json": "dict",
    "jsonb": "dict",
    "timestamp": "datetime.datetime",
    "timestamptz": "datetime.datetime",
    "date": "datetime.date",
    "time": "datetime.time",
}

ASYNC_SETUP = """
import uuid
from tuskorm.models.base_model import BaseModel
from pydantic import Field
import datetime
"""


async def fetch_table_metadata(
    conn, table_names: List[str] = None
) -> Dict[str, Dict[str, Dict[str, str]]]:
    """Fetch table metadata, grouped by schema."""
    query = """
        SELECT table_schema, table_name, column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    """

    if table_names:
        placeholders = ", ".join(f"'{t}'" for t in table_names)
        query += f" AND table_name IN ({placeholders})"

    rows = await conn.fetch(query)
    schemas = {}

    for row in rows:
        schema = row["table_schema"]
        table = row["table_name"]
        column = row["column_name"]
        data_type = row["data_type"]
        nullable = row["is_nullable"] == "YES"
        default = row["column_default"]

        if default:
            if default.startswith("'") and default.endswith("'"):
                default = default.strip("'")
            elif "::" in default:
                default = default.split("::")[0]
            elif default.lower() in ["true", "false"]:
                default = default.capitalize()

        py_type = PG_TO_PY_TYPE.get(data_type, "Any")

        if default and "nextval" in str(default):
            field_def = f"{column}: {py_type} = Field(default_factory=lambda: None)"
        else:
            field_def = f"{column}: {py_type}"
            if default:
                field_def += f" = {default}"
            elif not nullable:
                field_def += " = Field(...)"

        if schema not in schemas:
            schemas[schema] = {}
        if table not in schemas[schema]:
            schemas[schema][table] = []
        schemas[schema][table].append(field_def)

    return schemas


def singularize_table_name(table_name: str) -> str:
    """Convert table name to singular form (basic heuristic)."""
    if table_name.endswith("ies"):
        return table_name[:-3] + "y"
    elif table_name.endswith("s") and not table_name.endswith("ss"):
        return table_name[:-1]
    return table_name  # Assume already singular


async def generate_models(table_names: List[str] = None):
    """Generate TuskORM models from the database schema."""
    os.makedirs(MODELS_DIR, exist_ok=True)

    try:
        db_config = await get_db_config()
        conn = await asyncpg.connect(**db_config)
    except Exception as e:
        print(f"❌ Could not connect to database: {e}", file=sys.stderr)
        sys.exit(1)

    schemas = await fetch_table_metadata(conn, table_names)
    await conn.close()

    if not schemas:
        print("❌ No matching tables found.", file=sys.stderr)
        return

    for schema, tables in schemas.items():
        schema_dir = os.path.join(MODELS_DIR, schema)
        os.makedirs(schema_dir, exist_ok=True)  # Create schema subdirectory

        for table, fields in tables.items():
            model_name = "".join(
                word.capitalize() for word in singularize_table_name(table).split("_")
            )
            model_filename = os.path.join(schema_dir, f"{table}.py")

            formatted_fields = "\n    ".join(fields)

            model_content = f"""{ASYNC_SETUP}

class {model_name}(BaseModel):
    {formatted_fields}

    class Meta:
        table_name = "{table}"
"""

            with open(model_filename, "w") as model_file:
                model_file.write(model_content)

            print(f"✅ Generated model: {model_filename}")
