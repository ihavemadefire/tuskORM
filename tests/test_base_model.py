import pytest
import asyncpg
import uuid
import asyncio
from typing import List, Optional
from tuskorm.models.base_model import BaseModel  # Import the BaseModel from tuskORM


# Define a test model
class UserModel(BaseModel):
    id: uuid.UUID
    name: str
    age: Optional[int] = None
    status: Optional[str] = None
    email: Optional[str] = None

    class Meta:
        table_name = "test_users"


@pytest.fixture
async def db_pool():
    """
    Creates a PostgreSQL connection pool for tests within a function-scoped event loop.
    Ensures the test table exists.
    """
    pool = await asyncpg.create_pool(
        database="tuskorm_test",
        user="tuskorm",
        password="tuskorm",
        host="localhost",
        port=5432,
    )

    async with pool.acquire() as conn:
        # Drop and create the table to ensure a clean schema
        await conn.execute("DROP TABLE IF EXISTS test_users;")
        await conn.execute(
            """
            CREATE TABLE test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL,
                age INT,
                status TEXT,
                email TEXT
            );
            """
        )

    yield pool  # Provide the connection pool to the test function

    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS test_users;")  # Cleanup after tests

    await pool.close()


@pytest.mark.asyncio
async def test_create(db_pool):
    """Test that a user can be created and stored in the database."""
    user = await UserModel.create(db_pool, name="Alice", age=25)
    assert user.name == "Alice"
    assert user.age == 25


@pytest.mark.asyncio
async def test_fetch_one(db_pool):
    """Test that a single user can be fetched by criteria."""
    user = await UserModel.create(db_pool, name="Bob", age=30)
    fetched_user = await UserModel.fetch_one(db_pool, ["name", "age"], name="Bob")
    assert fetched_user is not None
    assert fetched_user.name == "Bob"
    assert fetched_user.age == 30


@pytest.mark.asyncio
async def test_fetch_all(db_pool):
    """Test fetching all users."""
    await UserModel.create(db_pool, name="Charlie", age=35)
    await UserModel.create(db_pool, name="Diana", age=40)

    users = await UserModel.fetch_all(db_pool, ["name", "age"])
    assert len(users) >= 2  # Ensuring at least two users exist


@pytest.mark.asyncio
async def test_fetch_filter(db_pool):
    """Test fetching users with filtering."""
    await UserModel.create(db_pool, name="Eve", age=45)
    users = await UserModel.fetch_filter(db_pool, {"age": 45}, ["name"])
    assert len(users) == 1
    assert users[0].name == "Eve"


@pytest.mark.asyncio
async def test_update(db_pool):
    """Test updating a user's details."""
    user = await UserModel.create(db_pool, name="Frank", age=50)
    await user.update(db_pool, age=55)
    updated_user = await UserModel.fetch_one(db_pool, ["name", "age"], name="Frank")
    assert updated_user.age == 55


@pytest.mark.asyncio
async def test_delete(db_pool):
    """Test deleting a user from the database."""
    user = await UserModel.create(db_pool, name="Grace", age=60)
    await user.delete(db_pool)
    deleted_user = await UserModel.fetch_one(db_pool, ["name", "age"], name="Grace")
    assert deleted_user is None


# ---- Extended Filter Tests ----


@pytest.mark.asyncio
async def test_fetch_filter_greater_eq(db_pool):
    """Test fetching users where age is greater than or equal to 30."""
    await UserModel.create(db_pool, name="Alice", age=25)
    await UserModel.create(db_pool, name="Bob", age=30)
    await UserModel.create(db_pool, name="Charlie", age=35)

    users = await UserModel.fetch_filter(db_pool, {"age__greaterEq": 30}, ["name"])
    names = {user.name for user in users}

    assert "Bob" in names
    assert "Charlie" in names
    assert "Alice" not in names


@pytest.mark.asyncio
async def test_fetch_filter_less_eq(db_pool):
    """Test fetching users where age is less than or equal to 30."""
    await UserModel.create(db_pool, name="David", age=28)
    await UserModel.create(db_pool, name="Eve", age=35)

    users = await UserModel.fetch_filter(db_pool, {"age__lessEq": 30}, ["name"])
    names = {user.name for user in users}

    assert "David" in names
    assert "Eve" not in names


@pytest.mark.asyncio
async def test_fetch_filter_like(db_pool):
    """Test fetching users with a LIKE condition."""
    await UserModel.create(db_pool, name="Henry", age=40)
    await UserModel.create(db_pool, name="Helen", age=29)
    await UserModel.create(db_pool, name="Hank", age=22)

    users = await UserModel.fetch_filter(db_pool, {"name__like": "He%"}, ["name"])
    names = {user.name for user in users}

    assert "Henry" in names
    assert "Helen" in names
    assert "Hank" not in names


@pytest.mark.asyncio
async def test_fetch_filter_in(db_pool):
    """Test fetching users with an IN condition."""
    await UserModel.create(db_pool, name="Ivy", age=50, status="active")
    await UserModel.create(db_pool, name="Jack", age=45, status="inactive")
    await UserModel.create(db_pool, name="Jill", age=32, status="pending")

    users = await UserModel.fetch_filter(
        db_pool, {"status__in": ["active", "pending"]}, ["name"]
    )
    names = {user.name for user in users}

    assert "Ivy" in names
    assert "Jill" in names
    assert "Jack" not in names


@pytest.mark.asyncio
async def test_fetch_filter_not_in(db_pool):
    """Test fetching users with a NOT IN condition."""
    await UserModel.create(db_pool, name="Kyle", status="banned")
    await UserModel.create(db_pool, name="Laura", status="active")
    await UserModel.create(db_pool, name="Leo", status="inactive")

    users = await UserModel.fetch_filter(
        db_pool, {"status__notIn": ["banned", "inactive"]}, ["name"]
    )
    names = {user.name for user in users}

    assert "Laura" in names
    assert "Kyle" not in names
    assert "Leo" not in names


@pytest.mark.asyncio
async def test_fetch_filter_is_null(db_pool):
    """Test fetching users where a field IS NULL."""
    await UserModel.create(db_pool, name="Mike", email=None)
    await UserModel.create(db_pool, name="Nancy", email="nancy@example.com")

    users = await UserModel.fetch_filter(db_pool, {"email__isNull": None}, ["name"])
    names = {user.name for user in users}

    assert "Mike" in names
    assert "Nancy" not in names


@pytest.mark.asyncio
async def test_fetch_filter_is_not_null(db_pool):
    """Test fetching users where a field IS NOT NULL."""
    await UserModel.create(db_pool, name="Oscar", email="oscar@example.com")
    await UserModel.create(db_pool, name="Pam", email=None)

    users = await UserModel.fetch_filter(db_pool, {"email__isNotNull": None}, ["name"])
    names = {user.name for user in users}

    assert "Oscar" in names
    assert "Pam" not in names
