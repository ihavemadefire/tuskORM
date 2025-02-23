import asyncpg
import logging
import uuid
from pydantic_core import PydanticUndefinedType


logger = logging.getLogger(__name__)


class Migration:
    """Mixin class for handling database schema migrations."""

    @classmethod
    async def _get_existing_columns(cls, pool: asyncpg.Pool) -> dict:
        query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{cls.Meta.table_name}';
        """
        async with pool.acquire() as conn:
            rows = await conn.fetch(query)
            result = {row["column_name"]: row["data_type"] for row in rows}
            print(f"🔍 DB Schema for {cls.Meta.table_name}: {result}")  # Debugging log
            return result

    @classmethod
    async def sync_schema(cls, pool: asyncpg.Pool) -> None:
        """Ensure the table schema matches the model definition, applying necessary migrations."""

        existing_columns = await cls._get_existing_columns(pool)
        model_fields = cls.model_fields
        renamed_columns = getattr(cls.Meta, "renamed_columns", {})

        alter_statements = []

        # 🔄 Step 1: Handle column renaming before adding new columns
        for old_name, new_name in renamed_columns.items():
            if old_name in existing_columns and new_name not in existing_columns:
                print(f"🔄 Renaming column: {old_name} → {new_name}")
                alter_statements.append(
                    f"ALTER TABLE {cls.Meta.table_name} RENAME COLUMN {old_name} TO {new_name}"
                )

        # ✅ Execute renaming before adding new columns
        async with pool.acquire() as conn:
            async with conn.transaction():
                for statement in alter_statements:
                    await conn.execute(statement)

        # Refresh schema after renaming
        existing_columns = await cls._get_existing_columns(pool)
        alter_statements = []

        # ➕ Step 2: Handle added columns
        for field_name, field_type in model_fields.items():
            if field_name not in existing_columns:
                print(f"➕ Adding column: {field_name}")
                column_type = cls._pg_type(field_type.annotation)

                # ✅ Extract correct default value from Pydantic field definition
                field_info = cls.model_fields[field_name]
                default_value = field_info.default

                # ✅ Ensure only valid defaults are applied
                if type(default_value) != PydanticUndefinedType:
                    if isinstance(default_value, str):
                        default_value = (
                            f"'{default_value}'"  # Ensure proper SQL string formatting
                        )
                    alter_statements.append(
                        f"ALTER TABLE {cls.Meta.table_name} ADD COLUMN {field_name} {column_type} DEFAULT {default_value} NOT NULL"
                    )
                else:
                    alter_statements.append(
                        f"ALTER TABLE {cls.Meta.table_name} ADD COLUMN {field_name} {column_type}"
                    )
        # ⚠️ Step 3: Handle type changes
        for field_name, field_type in model_fields.items():
            if field_name in existing_columns:
                current_type = existing_columns[field_name]
                new_type = cls._pg_type(field_type.annotation)

                if current_type != new_type:
                    print(
                        f"⚠️ Changing type of {field_name} from {current_type} → {new_type}"
                    )
                    if current_type.lower() == "text" and new_type.lower() == "boolean":
                        alter_statements.append(
                            f"ALTER TABLE {cls.Meta.table_name} ALTER COLUMN {field_name} SET DATA TYPE {new_type} USING {field_name}::BOOLEAN"
                        )
                    elif (
                        current_type.lower() == "text" and new_type.lower() == "integer"
                    ):
                        alter_statements.append(
                            f"ALTER TABLE {cls.Meta.table_name} ALTER COLUMN {field_name} SET DATA TYPE {new_type} USING {field_name}::INTEGER"
                        )
                    else:
                        alter_statements.append(
                            f"ALTER TABLE {cls.Meta.table_name} ALTER COLUMN {field_name} SET DATA TYPE {new_type}"
                        )

        # 🚨 Step 4: Handle removed columns **(must be executed last)**
        for column_name in existing_columns.keys():
            if (
                column_name not in model_fields
                and column_name not in renamed_columns.values()
            ):
                print(f"⚠️ Dropping column {column_name}")
                alter_statements.append(
                    f"ALTER TABLE {cls.Meta.table_name} DROP COLUMN {column_name}"
                )

        # ✅ Execute remaining ALTER statements
        if alter_statements:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    for statement in alter_statements:
                        print(f"🚀 Executing schema update: {statement}")
                        await conn.execute(statement)

    @classmethod
    def _pg_type(cls, python_type):
        """Maps Python types to PostgreSQL column types."""
        type_mapping = {
            int: "INTEGER",
            str: "TEXT",
            bool: "BOOLEAN",
            float: "REAL",
            uuid.UUID: "UUID",
        }
        return type_mapping.get(python_type, "TEXT")  # Default to TEXT
