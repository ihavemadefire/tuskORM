import pytest
from tuskorm.db.async_db import AsyncDatabase

TEST_DB_PARAMS = {
    "database": "tuskorm_test",
    "user": "tuskorm",
    "password": "tuskorm",
    "host": "localhost",
    "port": 5432
}
db = AsyncDatabase(**TEST_DB_PARAMS)


@pytest.mark.asyncio
async def test_database_connection():
    """Tests connecting and disconnecting from the database."""

    # Ensure initial state
    assert db.pool is None

    # Test connect()
    await db.connect()
    assert db.pool is not None

    # Test disconnect()
    await db.disconnect()
    assert db.pool is None


@pytest.mark.asyncio
async def test_fetch_one():
    """Tests fetching a single record from the database."""

    # Ensure database is connected
    await db.connect()

    query = "SELECT 1 AS test_col"
    result = await db.fetch_one(query)

    # Assertions
    assert result is not None
    assert result["test_col"] == 1

    # Cleanup
    await db.disconnect()


@pytest.mark.asyncio
async def test_fetch_all():
    """Tests fetching multiple records from the database."""

    # Ensure database is connected
    await db.connect()

    query = "SELECT * FROM (VALUES (1), (2), (3)) AS t(id)"
    result = await db.fetch_all(query)

    # Assertions
    assert result is not None
    assert len(result) == 3  # ✅ Ensure correct number of records
    assert [row["id"] for row in result] == [1, 2, 3]  # ✅ Ensure correct values

    # Cleanup
    await db.disconnect()


@pytest.mark.asyncio
async def test_execute():
    """Tests executing a simple SQL statement."""

    # Ensure database is connected
    await db.connect()

    # Create a temporary table
    create_query = "CREATE TEMP TABLE test_table (id SERIAL PRIMARY KEY, name TEXT)"
    await db.execute(create_query)

    # Insert a record
    insert_query = "INSERT INTO test_table (name) VALUES ($1) RETURNING id"
    inserted_id = await db.fetch_one(insert_query, "test_entry")

    # Assertions
    assert inserted_id is not None  # ✅ Ensure record is inserted

    # Cleanup
    await db.disconnect()
