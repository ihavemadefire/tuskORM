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

@pytest.mark.asyncio
async def test_sync_schema_add_multiple_columns(db_pool):
    """
    Test that `sync_schema` correctly adds multiple new columns.
    """
    # 🔹 1. Create initial table
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid()
            );
            """
        )

    # 🔹 2. Modify Model: Add new columns `name` and `age`
    class UpdatedTestUser(BaseModel):
        id: uuid.UUID
        name: str
        age: int

        class Meta:
            table_name = "test_users"

    # 🔹 3. Apply Migration
    await UpdatedTestUser.sync_schema(db_pool)

    # 🔹 4. Verify new columns exist
    async with db_pool.acquire() as conn:
        result = await conn.fetch(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'test_users' AND column_name IN ('name', 'age');
            """
        )

    assert len(result) == 2, "Columns 'name' and 'age' should be added."

@pytest.mark.asyncio
async def test_sync_schema_drop_column(db_pool):
    """
    Test that `sync_schema` correctly removes a column that was removed from the model.
    """
        # 🔹 1. Drop table in a separate transaction
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP TABLE IF EXISTS test_users;")

    # 🔹 1. Create table with an extra column `obsolete_column`
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL,
                obsolete_column TEXT
            );
            """
        )

    # 🔹 2. Modify Model: Remove `obsolete_column`
    class UpdatedTestUser(BaseModel):
        id: uuid.UUID
        name: str

        class Meta:
            table_name = "test_users"

    # 🔹 3. Apply Migration
    await UpdatedTestUser.sync_schema(db_pool)

    # 🔹 4. Verify `obsolete_column` no longer exists
    async with db_pool.acquire() as conn:
        result = await conn.fetch(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'test_users' AND column_name = 'obsolete_column';
            """
        )

    assert len(result) == 0, "Column 'obsolete_column' should be dropped."

@pytest.mark.asyncio
async def test_sync_schema_rename_multiple_columns(db_pool):
    """
    Test that `sync_schema` correctly renames multiple existing columns.
    """
        # 🔹 1. Drop table in a separate transaction
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP TABLE IF EXISTS test_users;")

    # 🔹 1. Create initial table with old column names
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL
            );
            """
        )

    # 🔹 2. Modify Model: Rename `first_name` → `fname`, `last_name` → `lname`
    class UpdatedTestUser(BaseModel):
        id: uuid.UUID
        fname: str  # Renamed from `first_name`
        lname: str  # Renamed from `last_name`

        class Meta:
            table_name = "test_users"
            renamed_columns = {"first_name": "fname", "last_name": "lname"}

    # 🔹 3. Apply Migration
    await UpdatedTestUser.sync_schema(db_pool)

    # 🔹 4. Verify renamed columns exist and old ones do not
    async with db_pool.acquire() as conn:
        new_result = await conn.fetch(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'test_users' AND column_name IN ('fname', 'lname');
            """
        )

        old_result = await conn.fetch(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'test_users' AND column_name IN ('first_name', 'last_name');
            """
        )

    assert len(new_result) == 2, "Columns 'fname' and 'lname' should exist."
    assert len(old_result) == 0, "Columns 'first_name' and 'last_name' should not exist."

@pytest.mark.asyncio
async def test_sync_schema_change_multiple_column_types(db_pool):
    """
    Test that `sync_schema` correctly changes types of multiple columns.
    """
        # 🔹 1. Drop table in a separate transaction
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP TABLE IF EXISTS test_users;")

    # 🔹 1. Create initial table with old types
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                age TEXT NOT NULL,
                active TEXT NOT NULL
            );
            """
        )

    # 🔹 2. Modify Model: Change `age` to INTEGER, `active` to BOOLEAN
    class UpdatedTestUser(BaseModel):
        id: uuid.UUID
        age: int
        active: bool

        class Meta:
            table_name = "test_users"

    # 🔹 3. Apply Migration
    await UpdatedTestUser.sync_schema(db_pool)

    # 🔹 4. Verify type changes
    async with db_pool.acquire() as conn:
        result = await conn.fetch(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'test_users' AND column_name IN ('age', 'active');
            """
        )

    type_map = {row["column_name"]: row["data_type"] for row in result}

    assert type_map.get("age") == "integer", f"Expected 'age' to be INTEGER, but got {type_map.get('age')}"
    assert type_map.get("active") == "boolean", f"Expected 'active' to be BOOLEAN, but got {type_map.get('active')}"


@pytest.mark.asyncio
async def test_sync_schema_rename_and_change_type(db_pool):
    """
    Test renaming a column and changing its type in a single migration.
    """
    # 🔹 1. Drop table and create initial schema
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP TABLE IF EXISTS test_users;")
    
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                old_column TEXT NOT NULL
            );
            """
        )

    # 🔹 2. Modify Model: Rename `old_column` → `new_column` and change to INTEGER
    class UpdatedTestUser(BaseModel):
        id: uuid.UUID
        new_column: int  # Renamed and changed from TEXT → INTEGER

        class Meta:
            table_name = "test_users"
            renamed_columns = {"old_column": "new_column"}

    # 🔹 3. Apply Migration
    await UpdatedTestUser.sync_schema(db_pool)

    # 🔹 4. Verify Changes
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'test_users' AND column_name = 'new_column';
            """
        )
    assert row["data_type"] == "integer", f"Expected INTEGER, but got {row['data_type']}"


@pytest.mark.asyncio
async def test_sync_schema_add_rename_delete_columns(db_pool):
    """
    Test adding a column, renaming another, and deleting a column in one migration.
    """
    # 🔹 1. Drop table and create initial schema
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP TABLE IF EXISTS test_users;")
    
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                old_name TEXT NOT NULL,
                obsolete_column TEXT
            );
            """
        )

    # 🔹 2. Modify Model: Rename `old_name` → `full_name`, add `age`, remove `obsolete_column`
    class UpdatedTestUser(BaseModel):
        id: uuid.UUID
        full_name: str  # Renamed from `old_name`
        age: int  # Newly added column

        class Meta:
            table_name = "test_users"
            renamed_columns = {"old_name": "full_name"}

    # 🔹 3. Apply Migration
    await UpdatedTestUser.sync_schema(db_pool)

    # 🔹 4. Verify Changes
    async with db_pool.acquire() as conn:
        columns = await conn.fetch(
            """
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'test_users';
            """
        )
        column_names = {row["column_name"] for row in columns}

    assert "full_name" in column_names, "Renamed column 'full_name' is missing"
    assert "age" in column_names, "New column 'age' is missing"
    assert "obsolete_column" not in column_names, "Obsolete column was not removed"


@pytest.mark.asyncio
async def test_sync_schema_add_not_null_with_default(db_pool):
    """
    Test adding a new NOT NULL column with a default value.
    """
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP TABLE IF EXISTS test_users;")

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL
            );
            """
        )

    # 🔹 2. Modify Model: Add a new NOT NULL column with a default value
    class UpdatedTestUser(BaseModel):
        id: uuid.UUID
        name: str
        status: str = "active"  # New column with default value

        class Meta:
            table_name = "test_users"

    # 🔹 3. Apply Migration
    await UpdatedTestUser.sync_schema(db_pool)

    # 🔹 4. Verify Changes
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT column_name, column_default, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'test_users' AND column_name = 'status';
            """
        )
    
    # ✅ Assert default value and NOT NULL constraint
    assert row["column_default"] == "'active'::text", f"Expected 'active' as default, but got {row['column_default']}"
    assert row["is_nullable"] == "NO", "Column 'status' should be NOT NULL"

@pytest.mark.asyncio
async def test_sync_schema_add_not_null_with_default(db_pool):
    """
    Test adding a new NOT NULL column with a default value.
    """
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP TABLE IF EXISTS test_users;")

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL
            );
            """
        )

    # 🔹 2. Modify Model: Add a new NOT NULL column with a default value
    class UpdatedTestUser(BaseModel):
        id: uuid.UUID
        name: str
        status: str = "active"  # New column with default value

        class Meta:
            table_name = "test_users"

    # 🔹 3. Apply Migration
    await UpdatedTestUser.sync_schema(db_pool)

    # 🔹 4. Verify Changes
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT column_name, column_default, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'test_users' AND column_name = 'status';
            """
        )
    
    # ✅ Assert default value and NOT NULL constraint
    assert row["column_default"] == "'active'::text", f"Expected 'active' as default, but got {row['column_default']}"
    assert row["is_nullable"] == "NO", "Column 'status' should be NOT NULL"