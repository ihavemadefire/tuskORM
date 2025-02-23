import pytest
import logging
import asyncpg
from tuskorm.models.base_model import BaseModel
from unittest.mock import patch
import subprocess


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
    """Test handling of database connection failure."""
    monkeypatch.setenv("TUSK_DB_PORT", "9999")

    result = subprocess.run(
        ["python", "tusk.py", "generate_models"], capture_output=True, text=True
    )

    assert result.returncode == 1, "Expected exit code 1 for failed DB connection"
    assert (
        "Could not connect to database" in result.stderr
    ), "Expected database connection error message not found."
