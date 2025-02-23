import os
import sys
import asyncio
import asyncpg
import argparse
from typing import Dict, List
import json

# PostgreSQL connection details (Adjust as needed)
DB_CONFIG = {
    "database": os.getenv("TUSK_DB_NAME", "tuskorm_test"),
    "user": os.getenv("TUSK_DB_USER", "tuskorm"),
    "password": os.getenv("TUSK_DB_PASSWORD", "tuskorm"),
    "host": os.getenv("TUSK_DB_HOST", "localhost"),
    "port": int(os.getenv("TUSK_DB_PORT", "5432")),  # Convert port to int
}

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


async def generate_models(db_config: dict, table_names: List[str] = None):
    """Generate TuskORM models from the database schema."""
    os.makedirs(MODELS_DIR, exist_ok=True)

    try:
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


def configure_db():
    """
    Prompts the user for database connection details and stores them in a .DBConfig file.
    """
    print("=== Database Configuration ===")

    # Prompt for required connection fields
    host = input("Enter database host (e.g., localhost): ").strip()
    port = input("Enter database port (e.g., 5432): ").strip()
    username = input("Enter username: ").strip()
    password = input("Enter password: ").strip()
    database = input("Enter database name: ").strip()

    # Construct configuration dictionary
    config = {
        "host": host,
        "port": port,
        "user": username,
        "password": password,
        "database": database,
    }

    # Optional: confirm before saving
    print("\nConfiguration:")
    for key, value in config.items():
        print(f"{key}: {value}")
    confirm = input("\nSave this configuration? (y/n): ").lower()
    if confirm != "y":
        print("Configuration not saved.")
        return

    # Write config to .DBConfig file in JSON format
    try:
        with open(".DBConfig", "w") as config_file:
            json.dump(config, config_file, indent=4)
        print("Configuration saved successfully to .DBConfig")
    except Exception as e:
        print(f"Failed to write configuration file: {e}")


async def test_db():
    """
    Tests the database connection using the configuration stored in .DBConfig.
    """
    try:
        with open(".DBConfig", "r") as config_file:
            config = json.load(config_file)
    except FileNotFoundError:
        print("❌ Configuration file not found.")
        return
    except Exception as e:
        print(f"❌ Error reading configuration file: {e}")
        return

    try:
        conn = await asyncpg.connect(**config)
        print("✅ Database connection successful.")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")


async def run_command(command, table_names: List[str] = None):
    """Handle command execution."""
    if command == "generate_models":
        await generate_models(DB_CONFIG, table_names)
    elif command == "configure_db":
        configure_db()
    elif command == "test_db":
        await test_db()
    else:
        print(f"❌ Unknown command: {command}")


def main():
    """Main function to handle command-line arguments."""
    parser = argparse.ArgumentParser(description="TuskORM CLI")
    parser.add_argument("command", help="The command to run (e.g., generate_models)")
    parser.add_argument(
        "tables", nargs="*", help="Optional list of table names to generate models for"
    )
    args = parser.parse_args()

    try:
        asyncio.run(run_command(args.command, args.tables))
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
