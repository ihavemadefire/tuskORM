import pytest
import asyncpg
from tuskorm.models.base_model import BaseModel
import uuid


@pytest.fixture
async def db_pool():
    """Fixture to provide a database connection pool for testing."""
    pool = await asyncpg.create_pool(
        database="tuskorm_test",
        user="tuskorm",
        password="tuskorm",
        host="localhost",
        port=5432,
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
        await conn.execute(
            "INSERT INTO test_users (name, age) VALUES ('Alice', 25), ('Bob', 30)"
        )

    users = await TestUser.fetch_all(db_pool)

    assert len(users) == 2, "Expected two users to be fetched"
    assert any(user.name == "Alice" for user in users)
    assert any(user.name == "Bob" for user in users)
