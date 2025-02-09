import pytest
import asyncpg
import uuid
from tuskorm.models.base_model import BaseModel


# 🏗️ **Step 1: Define a Test Model**
class ModelUser(BaseModel):
    id: uuid.UUID
    name: str
    age: int

    class Meta:
        table_name = "test_users"


@pytest.fixture
async def db_pool():
    """Fixture for creating a PostgreSQL connection pool."""
    pool = await asyncpg.create_pool(
        database="tuskorm_test",
        user="tuskorm",
        password="tuskorm",
        host="localhost",
        port=5432,
    )

    async with pool.acquire() as conn:
        # Ensure clean test environment
        await conn.execute("DROP TABLE IF EXISTS test_users;")

    yield pool
    await pool.close()


@pytest.mark.asyncio
async def test_sync_schema_add_column(db_pool):
    """
    Test that `sync_schema` correctly adds a new column to an existing table.
    """
    # 🔹 1. Create initial table
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL
            );
            """
        )

    # 🔹 2. Modify Model: Add a new column `age`
    await ModelUser.sync_schema(db_pool)

    # 🔹 3. Verify new column exists
    async with db_pool.acquire() as conn:
        result = await conn.fetch(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'test_users' AND column_name = 'age';
            """
        )

    assert len(result) == 1, "Column 'age' should be added."


@pytest.mark.asyncio
async def test_sync_schema_remove_column(db_pool):
    """
    Test that `sync_schema` correctly removes a column that was removed from the model.
    """
    # 🔹 1. Create table with an extra column `old_column`
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL,
                old_column TEXT
            );
            """
        )
