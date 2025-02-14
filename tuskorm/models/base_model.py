import asyncpg
import uuid
import logging
from typing import Type, Dict, Any, List, Optional, Union
from pydantic import BaseModel as PydanticModel, Field, ConfigDict
from pydantic_core import PydanticUndefinedType
from asyncpg.exceptions import (
    UniqueViolationError,
    ForeignKeyViolationError,
    PostgresError,
    SyntaxOrAccessError,
)

# 🔹 Setup logging for debugging & error tracking
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseModel(PydanticModel):
    """
    Base class for ORM models in TuskORM.
    Provides automatic table name inference, field registration, and CRUD operations.
    """

    id: uuid.UUID = Field(default_factory=uuid.uuid4)

    model_config = ConfigDict(
        extra="allow", from_attributes=True
    )  # Allows missing fields

    class Meta:
        """Meta class for defining additional table properties."""

        table_name: str = ""

    def __init_subclass__(cls, **kwargs):
        """
        Auto-infer table name based on the class name if not explicitly defined.
        """
        super().__init_subclass__(**kwargs)
        if not cls.Meta.table_name:
            cls.Meta.table_name = cls.__name__.lower() + "s"  # Simple pluralization

    @classmethod
    async def create(cls, pool: asyncpg.Pool, **kwargs) -> Optional["BaseModel"]:
        """
        Insert a new record into the database with support for constraints.
        """
        model_fields = cls.model_fields  # Get all fields from the model
        default_values = {
            k: getattr(cls, k, None)
            for k in model_fields
            if getattr(cls, k, None) is not None
        }

        # Merge provided kwargs with default values for fields that were not explicitly set
        final_values = {**default_values, **kwargs}

        columns = ", ".join(final_values.keys())
        values = ", ".join(f"${i+1}" for i in range(len(final_values)))
        query = f"INSERT INTO {cls.Meta.table_name} ({columns}) VALUES ({values}) RETURNING id, {columns}"

        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(query, *final_values.values())
                return cls(**dict(row)) if row else None
        except UniqueViolationError:
            logger.error(
                f"⚠️ Unique constraint violation on table `{cls.Meta.table_name}` for data: {final_values}"
            )
        except ForeignKeyViolationError:
            logger.error(
                f"⚠️ Foreign key constraint violated on table `{cls.Meta.table_name}` for data: {final_values}"
            )
        except PostgresError as e:
            logger.error(
                f"❌ Database error in `create()` for `{cls.Meta.table_name}`: {e}"
            )
        return None

    @classmethod
    async def fetch_one(
        cls, pool: asyncpg.Pool, columns: Optional[List[str]] = None, **conditions
    ) -> Optional["BaseModel"]:
        """
        Retrieve a single record with error handling.
        """
        selected_columns = ", ".join(set(columns or []) | {"id"})
        where_clause = (
            " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(conditions.keys()))
            if conditions
            else ""
        )
        query = f"SELECT {selected_columns} FROM {cls.Meta.table_name}"
        if where_clause:
            query += f" WHERE {where_clause} LIMIT 1"

        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(query, *conditions.values())
                return cls(**dict(row)) if row else None
        except PostgresError as e:
            logger.error(
                f"❌ Database error in `fetch_one()` for `{cls.Meta.table_name}`: {e}"
            )
        return None

    @classmethod
    async def fetch_all(
        cls, pool: asyncpg.Pool, columns: Optional[List[str]] = None, **conditions
    ) -> List["BaseModel"]:
        """
        Retrieve multiple records with error handling.
        """
        selected_columns = ", ".join(set(columns or []) | {"id"})
        where_clause = (
            " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(conditions.keys()))
            if conditions
            else ""
        )
        query = f"SELECT {selected_columns} FROM {cls.Meta.table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(query, *conditions.values())
                return [cls(**dict(row)) for row in rows]
        except PostgresError as e:
            logger.error(
                f"❌ Database error in `fetch_all()` for `{cls.Meta.table_name}`: {e}"
            )
        return []

    @classmethod
    def _parse_filter_key(cls, key: str):
        """
        Parse filter key to extract the column name and SQL operator.

        Example:
            "age__greaterEq" -> ("age", ">=")
            "name__like" -> ("name", "LIKE")
        """
        operators = {
            "greaterEq": ">=",
            "greater": ">",
            "less": "<=",
            "lessEq": "<",
            "notEq": "!=",
            "like": "LIKE",
            "in": "IN",
            "notIn": "NOT IN",
            "isNull": "IS NULL",
            "isNotNull": "IS NOT NULL",
        }
        if "__" in key:
            column, op = key.split("__", 1)
            return operators.get(op, "="), column  # Default to '=' if unknown
        return "=", key  # Default to '=' operator

    @classmethod
    async def fetch_filter(
        cls,
        pool: asyncpg.Pool,
        conditions: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        columns: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        distinct: bool = False,
    ) -> List["BaseModel"]:
        """
        Retrieve multiple records from the database based on filtering criteria.

        Supports:
        - Custom comparison operators (`=`, `>`, `<`, `!=`, `LIKE`, `IN`, `IS NULL`)
        - Ordering results (`ORDER BY`)
        - Pagination (`LIMIT`, `OFFSET`)
        - Logical `OR` conditions
        - Selecting distinct rows (`DISTINCT`)
        """

        query_values = []
        where_clauses = []

        # 🔹 Ensure valid column selection
        selected_columns = (
            ", ".join(set(columns or []) | {"id"})
            if not distinct
            else f"DISTINCT {', '.join(set(columns or []) | {'id'})}"
        )
        query = f"SELECT {selected_columns} FROM {cls.Meta.table_name}"

        # 🔹 Handle WHERE conditions
        if conditions:
            try:
                if isinstance(conditions, list):  # OR conditions
                    or_clauses = []
                    for condition in conditions:
                        sub_clauses = []
                        for key, value in condition.items():
                            operator, column = cls._parse_filter_key(key)

                            if operator in ["IS NULL", "IS NOT NULL"]:
                                sub_clauses.append(f"{column} {operator}")
                            elif operator in ["IN", "NOT IN"]:
                                if not isinstance(value, list):
                                    raise ValueError(
                                        f"Expected list for '{key}', got {type(value)}"
                                    )
                                placeholders = ", ".join(
                                    f"${len(query_values) + j + 1}"
                                    for j in range(len(value))
                                )
                                sub_clauses.append(
                                    f"{column} {operator} ({placeholders})"
                                )
                                query_values.extend(value)
                            else:
                                sub_clauses.append(
                                    f"{column} {operator} ${len(query_values) + 1}"
                                )
                                query_values.append(value)

                        or_clauses.append(f"({' AND '.join(sub_clauses)})")
                    where_clauses.append(f"({' OR '.join(or_clauses)})")

                else:  # AND conditions
                    for key, value in conditions.items():
                        operator, column = cls._parse_filter_key(key)

                        if operator in ["IS NULL", "IS NOT NULL"]:
                            where_clauses.append(f"{column} {operator}")
                        elif operator in ["IN", "NOT IN"]:
                            if not isinstance(value, list):
                                raise ValueError(
                                    f"Expected list for '{key}', got {type(value)}"
                                )
                            placeholders = ", ".join(
                                f"${len(query_values) + j + 1}"
                                for j in range(len(value))
                            )
                            where_clauses.append(
                                f"{column} {operator} ({placeholders})"
                            )
                            query_values.extend(value)
                        else:
                            where_clauses.append(
                                f"{column} {operator} ${len(query_values) + 1}"
                            )
                            query_values.append(value)

            except ValueError as e:
                logger.error(
                    f"❌ Invalid filter condition in `fetch_filter()` for `{cls.Meta.table_name}`: {e}"
                )
                return []

        # 🔹 Apply WHERE clause if necessary
        if where_clauses:
            query += f" WHERE {' AND '.join(where_clauses)}"

        # 🔹 Handle ORDER BY
        if order_by:
            order_clause = ", ".join(
                f"{col[1:]} DESC" if col.startswith("-") else f"{col} ASC"
                for col in order_by
            )
            query += f" ORDER BY {order_clause}"

        # 🔹 Handle LIMIT and OFFSET
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"

        logger.debug(
            f"📋 Executing `fetch_filter` Query: {query} with values: {query_values}"
        )

        # 🔹 Execute Query Safely
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(query, *query_values)
                return [cls(**dict(row)) for row in rows]
        except PostgresError as e:
            logger.error(
                f"❌ Database error in `fetch_filter()` for `{cls.Meta.table_name}`: {e}"
            )
        return []

    async def update(self, pool: asyncpg.Pool, **updates) -> bool:
        """
        Update the current record with error handling.
        """
        if not updates:
            return False

        set_clause = ", ".join(
            f"{key} = ${i+2}" for i, key in enumerate(updates.keys())
        )
        query = f"UPDATE {self.Meta.table_name} SET {set_clause} WHERE id = $1"

        try:
            async with pool.acquire() as conn:
                await conn.execute(query, self.id, *updates.values())
                return True
        except PostgresError as e:
            logger.error(f"❌ Error updating record in `{self.Meta.table_name}`: {e}")
        return False

    async def delete(self, pool: asyncpg.Pool) -> bool:
        """
        Delete the current record with error handling.
        """
        query = f"DELETE FROM {self.Meta.table_name} WHERE id = $1"

        try:
            async with pool.acquire() as conn:
                await conn.execute(query, self.id)
                return True
        except PostgresError as e:
            logger.error(f"❌ Error deleting record from `{self.Meta.table_name}`: {e}")
        return False

    ############### Migrations Functions ####################
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
