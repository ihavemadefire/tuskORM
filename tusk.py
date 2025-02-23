import os
import sys
import asyncio
import asyncpg
import argparse
from typing import Dict, List
import json
from core.db_ops import configure_db, test_db
from core.migrate import generate_models



async def run_command(command, table_names: List[str] = None):
    """Handle command execution."""
    if command == "generate_models":
        await generate_models(table_names)
    elif command == "configure_db":
        configure_db()
    elif command == "test_db":
        await test_db()
    else:
        print(f"❌ Unknown command: {command}")


def main():
    """Main function to handle command-line arguments."""
    parser = argparse.ArgumentParser(description="TuskORM CLI")
    parser.add_argument("command", help="The command to run (e.g., generate_models)")
    parser.add_argument(
        "tables", nargs="*", help="Optional list of table names to generate models for"
    )
    args = parser.parse_args()

    try:
        asyncio.run(run_command(args.command, args.tables))
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
