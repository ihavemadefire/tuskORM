import os
import pytest
import subprocess
import asyncpg
import asyncio
import logging
from pathlib import Path
from tuskorm.models.base_model import BaseModel
from unittest.mock import AsyncMock, patch

# Test database connection details
TEST_DB_PARAMS = {
    "database": "tuskorm_test",
    "user": "tuskorm",
    "password": "tuskorm",
    "host": "localhost",
    "port": 5432,
}

# Path to the tusk.py script
TUSK_SCRIPT = Path(__file__).parent.parent / "tusk.py"


@pytest.fixture
async def db_pool():
    """Fixture to provide a database connection pool for testing."""
    loop = asyncio.get_running_loop()  # Ensure correct event loop is used
    pool = await asyncpg.create_pool(
        database="tuskorm_test",
        user="tuskorm",
        password="tuskorm",
        host="localhost",
        port=5432,
        loop=loop  # Explicitly passing the loop
    )
    yield pool
    await pool.close()


@pytest.mark.asyncio
async def test_create_valid_record(db_pool):
    """Test successful creation of a record."""
    class TestUser(BaseModel):
        class Meta:
            table_name = "test_users"

    async with db_pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS test_users;")
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL,
                age INTEGER DEFAULT 30
            );
            """
        )

    user = await TestUser.create(db_pool, name="Alice", age=25)
    
    assert user is not None, "Expected a user object to be returned"
    assert user.name == "Alice"
    assert user.age == 25


@pytest.mark.asyncio
async def test_create_record_unique_constraint(db_pool):
    """Test handling of unique constraint violation."""
    class TestUser(BaseModel):
        class Meta:
            table_name = "test_users"

    async with db_pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS test_users;")
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                email TEXT UNIQUE NOT NULL
            );
            """
        )

    user1 = await TestUser.create(db_pool, email="test@example.com")
    assert user1 is not None

    user2 = await TestUser.create(db_pool, email="test@example.com")  # Should fail
    assert user2 is None, "Expected None due to unique constraint violation"


@pytest.mark.asyncio
async def test_fetch_one_record(db_pool):
    """Test fetching a single record."""
    class TestUser(BaseModel):
        class Meta:
            table_name = "test_users"

    async with db_pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS test_users;")
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL,
                age INTEGER DEFAULT 30
            );
            """
        )
        await conn.execute("INSERT INTO test_users (name, age) VALUES ('Alice', 25)")

    user = await TestUser.fetch_one(db_pool, name="Alice")
    
    assert user is not None, "Expected a user to be fetched"
    assert user.name == "Alice"
    assert user.age == 25


@pytest.mark.asyncio
async def test_fetch_all_records(db_pool):
    """Test fetching multiple records."""
    class TestUser(BaseModel):
        class Meta:
            table_name = "test_users"

    async with db_pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS test_users;")
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL,
                age INTEGER DEFAULT 30
            );
            """
        )
        await conn.execute("INSERT INTO test_users (name, age) VALUES ('Alice', 25), ('Bob', 30)")

    users = await TestUser.fetch_all(db_pool)
    
    assert len(users) == 2, "Expected two users to be fetched"
    assert any(user.name == "Alice" for user in users)
    assert any(user.name == "Bob" for user in users)


@pytest.mark.asyncio
async def test_insert_query_generation():
    """Test insert query generation logic."""
    class TestUser(BaseModel):
        class Meta:
            table_name = "test_users"

    values = {"name": "Alice", "age": 25}
    query = TestUser._build_insert_query(values)

    assert query == "INSERT INTO test_users (name, age) VALUES ($1, $2) RETURNING *"


@pytest.mark.asyncio
async def test_select_query_generation():
    """Test select query generation logic."""
    class TestUser(BaseModel):
        class Meta:
            table_name = "test_users"

    query, values = TestUser._build_select_query(["name", "age"], {"id": "1234"})
    
    assert query == "SELECT name, age FROM test_users WHERE id = $1"
    assert values == ("1234",)


@pytest.mark.asyncio
async def test_handle_db_exception(caplog):
    """Test database error handling."""
    class TestUser(BaseModel):
        class Meta:
            table_name = "test_users"

    with caplog.at_level(logging.ERROR):
        try:
            raise asyncpg.UniqueViolationError("duplicate key")
        except Exception as e:
            TestUser._handle_db_exception("create", e)

    assert any(
        "Unique constraint violation" in record.message for record in caplog.records
    ), f"Expected log message not found. Captured logs: {[record.message for record in caplog.records]}"


@pytest.mark.asyncio
async def test_generate_models_no_db_connection(monkeypatch):
    """Test handling of database connection failure by modifying the env variable."""

    # Patch the environment variable to simulate incorrect DB port
    monkeypatch.setenv("TUSK_DB_PORT", "9999")

    result = subprocess.run(
        ["python", str(TUSK_SCRIPT), "generate_models"],
        capture_output=True,
        text=True
    )

    assert result.returncode == 1, "Expected exit code 1 for failed DB connection"
    assert "Could not connect to database" in result.stderr, "Expected database connection error message not found."