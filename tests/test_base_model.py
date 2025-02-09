import pytest
import asyncpg
import uuid
import asyncio
from typing import List
from tuskorm.models.base_model import BaseModel  # Import the BaseModel from tuskORM

# Define a test model
class UserModel(BaseModel):
    id: uuid.UUID
    name: str
    age: int

    class Meta:
        table_name = "test_users"


@pytest.fixture
async def db_pool():
    """
    Creates a PostgreSQL connection pool for tests within a function-scoped event loop.
    """
    pool = await asyncpg.create_pool(
        database="tuskorm_test",
        user="tuskorm",
        password="tuskorm",
        host="localhost",
        port=5432
    )
    
    async with pool.acquire() as conn:
        # Ensure the test table exists
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS test_users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL,
                age INT NOT NULL
            );
            """
        )
    yield pool

    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS test_users;")

    await pool.close()


@pytest.mark.asyncio
async def test_create(db_pool):
    """
    Test that a user can be created and stored in the database.
    """
    user = await UserModel.create(db_pool, name="Alice", age=25)
    assert user.name == "Alice"
    assert user.age == 25


@pytest.mark.asyncio
async def test_fetch_one(db_pool):
    """
    Test that a single user can be fetched by criteria.
    """
    user = await UserModel.create(db_pool, name="Bob", age=30)
    fetched_user = await UserModel.fetch_one(db_pool, ["name", "age"], name="Bob")
    assert fetched_user is not None
    assert fetched_user.name == "Bob"
    assert fetched_user.age == 30


@pytest.mark.asyncio
async def test_fetch_all(db_pool):
    """
    Test fetching all users.
    """
    await UserModel.create(db_pool, name="Charlie", age=35)
    await UserModel.create(db_pool, name="Diana", age=40)

    users = await UserModel.fetch_all(db_pool, ["name", "age"])
    assert len(users) >= 2  # Ensuring at least two users exist


@pytest.mark.asyncio
async def test_fetch_filter(db_pool):
    """
    Test fetching users with filtering.
    """
    await UserModel.create(db_pool, name="Eve", age=45)
    users = await UserModel.fetch_filter(db_pool, {"age": 45}, ["name"])
    assert len(users) == 1
    assert users[0].name == "Eve"


@pytest.mark.asyncio
async def test_update(db_pool):
    """
    Test updating a user's details.
    """
    user = await UserModel.create(db_pool, name="Frank", age=50)
    await user.update(db_pool, age=55)
    updated_user = await UserModel.fetch_one(db_pool, ["name", "age"], name="Frank")
    assert updated_user.age == 55


@pytest.mark.asyncio
async def test_delete(db_pool):
    """
    Test deleting a user from the database.
    """
    user = await UserModel.create(db_pool, name="Grace", age=60)
    await user.delete(db_pool)
    deleted_user = await UserModel.fetch_one(db_pool, ["name", "age"], name="Grace")
    assert deleted_user is None
