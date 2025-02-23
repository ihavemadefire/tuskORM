import pytest
import asyncpg
from tuskorm.models.migration import Migration
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
async def test_sync_schema_add_column(db_pool):
    """Test that `sync_schema` correctly adds a new column to an existing table."""

    class TestUser(BaseModel, Migration):
        class Meta:
            table_name = "test_users"

    async with db_pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS test_users;")
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL
            );
            """
        )

    class UpdatedTestUser(BaseModel, Migration):
        id: uuid.UUID
        name: str
        age: int  # New column

        class Meta:
            table_name = "test_users"

    await UpdatedTestUser.sync_schema(db_pool)

    async with db_pool.acquire() as conn:
        result = await conn.fetch(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'test_users' AND column_name = 'age';
            """
        )

    assert len(result) == 1, "Expected new column 'age' to be added."
