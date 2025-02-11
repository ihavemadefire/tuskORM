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
    # 🔹 1. Drop table in a separate transaction
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP TABLE IF EXISTS test_users;")
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
    # 🔹 1. Drop table in a separate transaction
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP TABLE IF EXISTS test_users;")
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


@pytest.mark.asyncio
async def test_sync_schema_rename_column(db_pool):
    """
    Test that `sync_schema` correctly renames an existing column instead of dropping it.
    """
    # 🔹 1. Drop table in a separate transaction
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP TABLE IF EXISTS test_users;")
    # 🔹 1. Create initial table with old column name
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL
            );
            """
        )

    # 🔹 2. Modify model to rename `name` → `full_name`
    class TestUser(BaseModel):
        id: uuid.UUID
        full_name: str  # Renamed from `name`

        class Meta:
            table_name = "test_users"
            renamed_columns = {"name": "full_name"}  # Tell ORM about renaming

    # 🔹 3. Run `sync_schema()`
    await TestUser.sync_schema(db_pool)

    # 🔹 4. Verify that `full_name` exists, and `name` does not
    async with db_pool.acquire() as conn:
        result = await conn.fetchval(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'test_users' AND column_name = 'full_name';
            """
        )
    assert result == "full_name"  # Column was renamed successfully


@pytest.mark.asyncio
async def test_sync_schema_type_change_int_to_text(db_pool):
    """
    Test that `sync_schema` correctly changes a column type from INTEGER to TEXT.
    """
    # 🔹 1. Drop table in a separate transaction
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP TABLE IF EXISTS test_users;")

    # 🔹 1. Create initial table with `age` as INTEGER
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                age INTEGER NOT NULL
            );
            """
        )

    # 🔹 2. Modify Model: Change `age` to `TEXT`
    class UpdatedTestUser(BaseModel):
        id: uuid.UUID
        age: str  # Changed to TEXT

        class Meta:
            table_name = "test_users"

    # 🔹 3. Apply Migration
    await UpdatedTestUser.sync_schema(db_pool)

    # 🔹 4. Verify Change
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'test_users' AND column_name = 'age';
            """
        )
    assert row["data_type"] == "text", f"Expected TEXT, but got {row['data_type']}"


@pytest.mark.asyncio
async def test_sync_schema_type_change_text_to_int(db_pool):
    """
    Test that `sync_schema` correctly changes a column type from TEXT to INTEGER.
    """
    # 🔹 1. Drop table in a separate transaction
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP TABLE IF EXISTS test_users;")

    # 🔹 2. Recreate the table with `age` as TEXT
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                age TEXT NOT NULL
            );
            """
        )

    # 🔹 3. Modify Model: Change `age` to `INTEGER`
    class UpdatedTestUser(BaseModel):
        id: uuid.UUID
        age: int  # Changed to INTEGER

        class Meta:
            table_name = "test_users"

    # 🔹 4. Apply Migration
    await UpdatedTestUser.sync_schema(db_pool)

    # 🔹 5. Verify Change
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'test_users' AND column_name = 'age';
            """
        )
    assert (
        row["data_type"] == "integer"
    ), f"Expected INTEGER, but got {row['data_type']}"


class TestPgTypeMapping:
    """Tests for `_pg_type()` method in BaseModel."""

    @pytest.mark.parametrize(
        "python_type, expected_pg_type",
        [
            (int, "INTEGER"),  # ✅ int → INTEGER
            (str, "TEXT"),  # ✅ str → TEXT
            (bool, "BOOLEAN"),  # ✅ bool → BOOLEAN
            (float, "REAL"),  # ✅ float → REAL
            (uuid.UUID, "UUID"),  # ✅ uuid.UUID → UUID
            (list, "TEXT"),  # ❌ Unsupported types default to TEXT
            (dict, "TEXT"),  # ❌ Unsupported types default to TEXT
            (None, "TEXT"),  # ❌ None should default to TEXT
        ],
    )
    def test_pg_type_mapping(self, python_type, expected_pg_type):
        """Test that `_pg_type` correctly maps Python types to PostgreSQL types."""
        assert (
            BaseModel._pg_type(python_type) == expected_pg_type
        ), f"Failed for type: {python_type}"
