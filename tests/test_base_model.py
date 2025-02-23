import pytest
import logging
import asyncpg
from tuskorm.models.base_model import BaseModel
from unittest.mock import patch
import subprocess
import os
import json


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
async def test_generate_models_no_db_connection(monkeypatch, tmp_path):
    # Create a temporary .DBConfig with a bad port
# Create a temporary .DBConfig with a bad port
    bad_config = {
        "host": "localhost",
        "port": 9999,  # Invalid port for connection
        "username": "tuskorm",
        "password": "tuskorm",
        "database": "tuskorm_test"
    }
    config_file = tmp_path / ".DBConfig"
    config_file.write_text(json.dumps(bad_config))
    
    # Build the full path to tusk.py relative to the tests directory
    script_path = os.path.join(os.path.dirname(__file__), "..", "tusk.py")
    
    # Change working directory so that tusk.py can pick up the temporary .DBConfig file
    monkeypatch.chdir(tmp_path)
    
    result = subprocess.run(
        ["python", script_path, "generate_models"],
        capture_output=True, text=True
    )
    
    assert result.returncode == 1, "Expected exit code 1 for failed DB connection"
