import os
import pytest
import subprocess
import asyncpg
from pathlib import Path
import shutil

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
    pool = await asyncpg.create_pool(**TEST_DB_PARAMS)
    yield pool
    await pool.close()


@pytest.mark.asyncio
async def test_generate_models_all_tables(db_pool):
    """Test generating models for all tables in the database."""
    async with db_pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS test_users;")
        await conn.execute(
            """
            CREATE TABLE test_users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER DEFAULT 30,
                is_active BOOLEAN DEFAULT TRUE
            );
            """
        )

    # Run the command
    subprocess.run(["python", str(TUSK_SCRIPT), "generate_models"], check=True)

    # Check if the model file exists
    model_path = Path("models/public/test_users.py")
    assert model_path.exists(), "Model file for test_users was not created."

    # Read the generated model
    with model_path.open("r") as f:
        model_content = f.read()

    # Verify model contents
    assert "class TestUser(BaseModel):" in model_content
    assert "id: int" in model_content
    assert "name: str" in model_content
    assert "age: int = 30" in model_content
    assert "is_active: bool = True" in model_content
    assert 'table_name = "test_users"' in model_content

    # Cleanup
    os.remove(model_path)


@pytest.mark.asyncio
async def test_generate_models_specific_table(db_pool):
    """Test generating models for a specific table."""
    async with db_pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS test_orders;")
        await conn.execute(
            """
            CREATE TABLE test_orders (
                id SERIAL PRIMARY KEY,
                amount FLOAT NOT NULL,
                status TEXT DEFAULT 'pending'
            );
            """
        )

    # Run the command for only the `test_orders` table
    subprocess.run(
        ["python", str(TUSK_SCRIPT), "generate_models", "test_orders"], check=True
    )

    # Check if only the `test_orders.py` model is created
    model_path = Path("models/public/test_orders.py")
    assert model_path.exists(), "Model file for test_orders was not created."

    # Read the generated model
    with model_path.open("r") as f:
        model_content = f.read()

    # Verify model contents
    assert "class TestOrder(BaseModel):" in model_content
    assert "id: int" in model_content
    assert "amount: float" in model_content
    assert "status: str = 'pending'" in model_content
    assert 'table_name = "test_orders"' in model_content

    # Ensure no unwanted models were created
    assert not Path(
        "models/test_users.py"
    ).exists(), "test_users model should not be created."

    # Cleanup
    os.remove(model_path)


@pytest.mark.asyncio
async def test_generate_models_nonexistent_table():
    """Test attempting to generate a model for a non-existent table."""
    result = subprocess.run(
        ["python", str(TUSK_SCRIPT), "generate_models", "fake_table"],
        capture_output=True,
        text=True,
    )

    # Ensure the command fails gracefully
    assert "No matching tables found" in result.stderr

@pytest.mark.asyncio
async def test_generate_models_with_schema(db_pool):
    """Test generating models with schema-aware imports and correct directory structure."""

    async with db_pool.acquire() as conn:
        # Ensure schema exists
        await conn.execute("DROP SCHEMA IF EXISTS analytics CASCADE;")
        await conn.execute("DROP SCHEMA IF EXISTS public CASCADE;")
        await conn.execute("CREATE SCHEMA analytics;")
        await conn.execute("CREATE SCHEMA public;")

        # Create tables in different schemas
        await conn.execute(
            """
            CREATE TABLE analytics.reports (
                id SERIAL PRIMARY KEY,
                report_name TEXT NOT NULL
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE public.users (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL
            );
            """
        )

    # Run the command to generate models
    subprocess.run(["python", str(TUSK_SCRIPT), "generate_models"], check=True)

    # Check if the schema subdirectories exist
    analytics_model_path = Path("models/analytics/reports.py")
    public_model_path = Path("models/public/users.py")

    assert (
        analytics_model_path.exists()
    ), "Model file for analytics.reports was not created."
    assert public_model_path.exists(), "Model file for public.users was not created."

    # Read the generated analytics model
    with analytics_model_path.open("r") as f:
        analytics_model_content = f.read()

    # Cleanup
    os.remove(analytics_model_path)
    os.remove(public_model_path)
    shutil.rmtree("models/analytics")
    shutil.rmtree("models/public")
