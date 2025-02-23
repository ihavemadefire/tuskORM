import asyncio
import asyncpg
import importlib
import os
from pathlib import Path
from tuskorm.models.base_model import BaseModel

# 🔹 PostgreSQL connection details
DB_CONFIG = {
    "database": "tuskorm_test",
    "user": "tuskorm",
    "password": "tuskorm",
    "host": "localhost",
    "port": 5432,
}

# 🔹 Directory where models are stored
MODELS_DIR = Path("models")


async def fetch_all_data(conn):
    """Dynamically import models and fetch all data from each."""
    for schema_dir in MODELS_DIR.iterdir():
        if not schema_dir.is_dir():
            continue  # Skip non-directories

        schema_name = schema_dir.name
        print(f"\n🔹 Schema: {schema_name.upper()}")

        for model_file in schema_dir.glob("*.py"):
            model_name = model_file.stem  # Get filename without .py extension
            module_path = f"models.{schema_name}.{model_name}"

            try:
                # 🔹 Dynamically import the model
                model_module = importlib.import_module(module_path)
                model_class = next(
                    obj
                    for obj in model_module.__dict__.values()
                    if isinstance(obj, type)
                    and issubclass(obj, BaseModel)
                    and obj is not BaseModel
                )

                print(f"\n📌 Table: {schema_name}.{model_name}")

                # 🔹 Fetch all records using TuskORM
                records = await model_class.fetch_all(conn)

                if records:
                    # 🔹 Print column headers
                    column_names = model_class.model_fields.keys()
                    print(f"{' | '.join(column_names)}")
                    print("-" * (len(column_names) * 15))

                    # 🔹 Print row data
                    for record in records:
                        print(
                            " | ".join(
                                str(getattr(record, col)) for col in column_names
                            )
                        )
                else:
                    print("⚠️ No data found.")

            except Exception as e:
                print(f"❌ Failed to load model {model_name}: {e}")


async def main():
    """Main function to connect and fetch data."""
    conn = await asyncpg.connect(**DB_CONFIG)
    await fetch_all_data(conn)
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
