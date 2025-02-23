import asyncio
import asyncpg
import random
import uuid
import datetime
from faker import Faker

# 🔹 Initialize Faker for generating realistic test data
fake = Faker()

# 🔹 PostgreSQL connection details
DB_CONFIG = {
    "database": "tuskorm_test",
    "user": "tuskorm",
    "password": "tuskorm",
    "host": "localhost",
    "port": 5432,
}

# 🔹 Define schemas
SCHEMAS = ["public", "analytics", "inventory"]

# 🔹 Define tables and their structure (NO FOREIGN KEYS)
TABLES = {
    "public": [
        (
            "users",
            [
                ("id", "UUID PRIMARY KEY DEFAULT gen_random_uuid()"),
                ("name", "TEXT NOT NULL"),
                ("email", "TEXT UNIQUE NOT NULL"),
                ("age", "INTEGER"),
                ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
            ],
        ),
        (
            "orders",
            [
                ("id", "UUID PRIMARY KEY DEFAULT gen_random_uuid()"),
                ("order_status", "TEXT DEFAULT 'pending'"),
                ("total_price", "REAL NOT NULL"),
                ("placed_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
            ],
        ),
        (
            "products",
            [
                ("id", "UUID PRIMARY KEY DEFAULT gen_random_uuid()"),
                ("name", "TEXT NOT NULL"),
                ("price", "REAL NOT NULL"),
                ("stock", "INTEGER NOT NULL"),
                ("category", "TEXT NOT NULL"),
            ],
        ),
        (
            "categories",
            [
                ("id", "UUID PRIMARY KEY DEFAULT gen_random_uuid()"),
                ("name", "TEXT UNIQUE NOT NULL"),
                ("description", "TEXT"),
            ],
        ),
        (
            "reviews",
            [
                ("id", "UUID PRIMARY KEY DEFAULT gen_random_uuid()"),
                ("rating", "INTEGER CHECK (rating BETWEEN 1 AND 5)"),
                ("review_text", "TEXT"),
                ("review_date", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
            ],
        ),
    ],
    "analytics": [
        (
            "traffic_logs",
            [
                ("id", "UUID PRIMARY KEY DEFAULT gen_random_uuid()"),
                ("page_url", "TEXT NOT NULL"),
                ("session_id", "UUID NOT NULL"),
                ("timestamp", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
            ],
        ),
        (
            "sales_data",
            [
                ("id", "UUID PRIMARY KEY DEFAULT gen_random_uuid()"),
                ("quantity_sold", "INTEGER NOT NULL"),
                ("sale_amount", "REAL NOT NULL"),
                ("sale_date", "DATE DEFAULT CURRENT_DATE"),
            ],
        ),
        (
            "user_engagement",
            [
                ("id", "UUID PRIMARY KEY DEFAULT gen_random_uuid()"),
                ("session_length", "INTEGER"),
                ("actions_performed", "INTEGER"),
                ("last_active", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
            ],
        ),
    ],
    "inventory": [
        (
            "warehouses",
            [
                ("id", "UUID PRIMARY KEY DEFAULT gen_random_uuid()"),
                ("location", "TEXT NOT NULL"),
                ("capacity", "INTEGER NOT NULL"),
                ("manager", "TEXT"),
            ],
        ),
        (
            "inventory_stock",
            [
                ("id", "UUID PRIMARY KEY DEFAULT gen_random_uuid()"),
                ("stock_level", "INTEGER NOT NULL"),
            ],
        ),
        (
            "suppliers",
            [
                ("id", "UUID PRIMARY KEY DEFAULT gen_random_uuid()"),
                ("name", "TEXT NOT NULL"),
                ("contact_email", "TEXT"),
                ("phone", "TEXT"),
                ("address", "TEXT NOT NULL"),
            ],
        ),
    ],
}


async def reset_database(conn):
    """Drops existing schemas and recreates them."""
    print("🛑 Dropping existing schemas...")
    for schema in SCHEMAS:
        await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await conn.execute(f"CREATE SCHEMA {schema};")
    print("✅ Schemas reset successfully!")


async def create_tables(conn):
    """Creates tables in the database."""
    print("📌 Creating tables...")
    for schema, tables in TABLES.items():
        for table_name, columns in tables:
            col_defs = ", ".join(f"{col[0]} {col[1]}" for col in columns)
            await conn.execute(f"CREATE TABLE {schema}.{table_name} ({col_defs});")
    print("✅ Tables created successfully!")


async def create_tables(conn):
    """Creates tables based on predefined schema and table structure."""
    for schema, tables in TABLES.items():
        for table_name, columns in tables:
            col_defs = ", ".join(
                f"{col_name} {col_type}" for col_name, col_type in columns
            )
            await conn.execute(
                f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({col_defs});"
            )
    print("✅ Tables created successfully!")


async def insert_fake_data(conn):
    """Inserts 200 rows of fake data into each table."""
    for schema, tables in TABLES.items():
        for table_name, columns in tables:
            col_names = [
                col[0] for col in columns if col[0] != "id"
            ]  # Exclude ID (UUID autogen)
            placeholders = ", ".join(f"${i+1}" for i in range(len(col_names)))

            for _ in range(200):
                values = []
                for col_name, col_type in columns:
                    if col_name == "id":
                        continue
                    elif "TEXT" in col_type:
                        values.append(
                            fake.word() if "category" in col_name else fake.name()
                        )
                    elif "INTEGER" in col_type:
                        if "rating" in col_name:
                            values.append(
                                random.randint(1, 5)
                            )  # ✅ Ensure rating is 1-5
                        else:
                            values.append(random.randint(1, 100))
                    elif "REAL" in col_type:
                        values.append(round(random.uniform(10.0, 500.0), 2))
                    elif "BOOLEAN" in col_type:
                        values.append(random.choice([True, False]))
                    elif "TIMESTAMP" in col_type:
                        values.append(fake.date_time_this_year())
                    elif "UUID" in col_type:
                        values.append(str(uuid.uuid4()))
                    else:
                        values.append(None)

                query = f"INSERT INTO {schema}.{table_name} ({', '.join(col_names)}) VALUES ({placeholders})"
                await conn.execute(query, *values)

    print("✅ Fake data inserted successfully!")


async def main():
    """Main function to create schemas, tables, and populate with fake data."""
    conn = await asyncpg.connect(**DB_CONFIG)
    await reset_database(conn)
    await create_tables(conn)
    await insert_fake_data(conn)
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
