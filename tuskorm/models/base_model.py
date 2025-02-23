import asyncpg
import uuid
import logging
import datetime
from typing import Type, Dict, Any, List, Optional, Union
from pydantic import BaseModel as PydanticModel, Field, ConfigDict
from pydantic_core import PydanticUndefinedType
from asyncpg.exceptions import (
    UniqueViolationError,
    ForeignKeyViolationError,
    PostgresError,
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
        """Auto-infer table name based on the class name if not explicitly defined."""
        super().__init_subclass__(**kwargs)
        if not cls.Meta.table_name:
            cls.Meta.table_name = cls.__name__.lower() + "s"  # Simple pluralization

    ########### CRUD Operations ###########
    @classmethod
    async def create(cls, pool: asyncpg.Pool, **kwargs) -> Optional["BaseModel"]:
        """Insert a new record into the database with support for constraints."""
        final_values = {k: v for k, v in kwargs.items() if v is not None}

        query = cls._build_insert_query(final_values)

        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(query, *final_values.values())
                return cls(**dict(row)) if row else None
        except Exception as e:
            cls._handle_db_exception("create", e, final_values)
        return None

    @classmethod
    async def fetch_one(
        cls, pool: asyncpg.Pool, columns: Optional[List[str]] = None, **conditions
    ) -> Optional["BaseModel"]:
        """Retrieve a single record with error handling."""
        query, values = cls._build_select_query(columns, conditions, limit=1)
        return await cls._execute_fetch(pool, query, values)

    @classmethod
    async def fetch_all(
        cls, pool: asyncpg.Pool, columns: Optional[List[str]] = None, **conditions
    ) -> List["BaseModel"]:
        """Retrieve multiple records with error handling."""
        query, values = cls._build_select_query(columns, conditions)
        return await cls._execute_fetch(pool, query, values, multiple=True)

    @classmethod
    async def _execute_fetch(
        cls, pool: asyncpg.Pool, query: str, values: tuple, multiple: bool = False
    ):
        """Execute fetch query and return results."""
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(query, *values)
                return (
                    [cls(**dict(row)) for row in rows]
                    if multiple
                    else (cls(**dict(rows[0])) if rows else None)
                )
        except Exception as e:
            cls._handle_db_exception("fetch", e)
        return [] if multiple else None

    @classmethod
    def _build_insert_query(cls, values: Dict[str, Any]) -> str:
        """Generate SQL INSERT query dynamically."""
        columns = ", ".join(values.keys())
        placeholders = ", ".join(f"${i+1}" for i in range(len(values)))
        return f"INSERT INTO {cls.Meta.table_name} ({columns}) VALUES ({placeholders}) RETURNING *"

    @classmethod
    def _build_select_query(
        cls,
        columns: Optional[List[str]],
        conditions: Dict[str, Any],
        limit: Optional[int] = None,
    ) -> tuple:
        """Generate SQL SELECT query dynamically."""
        selected_columns = ", ".join(columns) if columns else "*"
        where_clause = " AND ".join(
            f"{key} = ${i+1}" for i, key in enumerate(conditions.keys())
        )
        query = f"SELECT {selected_columns} FROM {cls.Meta.table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        if limit:
            query += f" LIMIT {limit}"
        return query, tuple(conditions.values())

    @classmethod
    def _handle_db_exception(
        cls, operation: str, exception: Exception, data: Optional[Dict[str, Any]] = None
    ):
        """Centralized error handling for database operations."""
        if isinstance(exception, UniqueViolationError):
            logger.error(
                f"⚠️ Unique constraint violation in `{operation}` on `{cls.Meta.table_name}` for data: {data}"
            )
        elif isinstance(exception, ForeignKeyViolationError):
            logger.error(
                f"⚠️ Foreign key constraint violated in `{operation}` on `{cls.Meta.table_name}` for data: {data}"
            )
        elif isinstance(exception, PostgresError):
            logger.error(
                f"❌ Database error in `{operation}` for `{cls.Meta.table_name}`: {exception}"
            )
        else:
            logger.error(
                f"❌ Unexpected error in `{operation}` for `{cls.Meta.table_name}`: {exception}"
            )

    ############### CRUD Operations ####################
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
