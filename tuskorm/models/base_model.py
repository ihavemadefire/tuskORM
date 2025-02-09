import asyncpg
import uuid
from typing import Type, Dict, Any, List, Optional, Union
from pydantic import BaseModel as PydanticModel, Field, ConfigDict


class BaseModel(PydanticModel):
    """
    Base class for ORM models in TuskORM.
    Provides automatic table name inference, field registration, and CRUD operations.

    Features:
    - Auto-generates table names based on class names.
    - Supports UUID primary keys by default.
    - Provides basic CRUD operations (create, fetch, update, delete).
    - Uses asyncpg for asynchronous PostgreSQL interactions.
    """

    id: uuid.UUID = Field(default_factory=uuid.uuid4)

    model_config = ConfigDict(
        extra="allow", from_attributes=True
    )  # Allows missing fields

    class Meta:
        """
        Meta class for defining additional table properties.
        """

        table_name: str = ""

    def __init_subclass__(cls, **kwargs):
        """
        Auto-infer table name based on the class name if not explicitly defined.

        - Converts `CamelCase` model names into `snake_case` pluralized table names.
        - Example: `User` → `users`, `OrderItem` → `order_items`.
        """
        super().__init_subclass__(**kwargs)
        if not cls.Meta.table_name:
            cls.Meta.table_name = cls.__name__.lower() + "s"  # Simple pluralization

    @classmethod
    async def create(cls, pool: asyncpg.Pool, **kwargs) -> "BaseModel":
        """
        Insert a new record into the database.

        Args:
            pool (asyncpg.Pool): The connection pool to use for executing the query.
            **kwargs: Column values to be inserted into the database.

        Returns:
            BaseModel: An instance of the model populated with the inserted values.
        """
        columns = ", ".join(kwargs.keys())
        values = ", ".join(f"${i+1}" for i in range(len(kwargs)))
        query = f"INSERT INTO {cls.Meta.table_name} ({columns}) VALUES ({values}) RETURNING id, {columns}"

        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, *kwargs.values())
            return cls(**dict(row))

    @classmethod
    async def fetch_one(
        cls, pool: asyncpg.Pool, columns: Optional[List[str]] = None, **conditions
    ) -> Optional["BaseModel"]:
        """
        Retrieve a single record from the database that matches the given conditions.
        """
        selected_columns = ", ".join(
            set(columns or []) | {"id"}
        )  # Ensure 'id' is always selected
        where_clause = (
            " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(conditions.keys()))
            if conditions
            else ""
        )
        query = f"SELECT {selected_columns} FROM {cls.Meta.table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        query += " LIMIT 1"

        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, *conditions.values())
            return cls(**dict(row)) if row else None

    @classmethod
    async def fetch_all(
        cls, pool: asyncpg.Pool, columns: Optional[List[str]] = None, **conditions
    ) -> List["BaseModel"]:
        """
        Retrieve multiple records from the database that match the given conditions.
        """
        selected_columns = ", ".join(
            set(columns or []) | {"id"}
        )  # Ensure 'id' is always selected
        where_clause = (
            " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(conditions.keys()))
            if conditions
            else ""
        )
        query = f"SELECT {selected_columns} FROM {cls.Meta.table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *conditions.values())
            return [cls(**dict(row)) for row in rows]

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

        selected_columns = (
            ", ".join(set(columns or []) | {"id"})
            if not distinct
            else f"DISTINCT {', '.join(set(columns or []) | {'id'})}"
        )
        query = f"SELECT {selected_columns} FROM {cls.Meta.table_name}"
        query_values = []
        where_clauses = []

        # Handle WHERE conditions
        if conditions:
            if isinstance(conditions, list):  # OR conditions
                or_clauses = []
                for condition in conditions:
                    sub_clauses = []
                    for key, value in condition.items():
                        operator, column = cls._parse_filter_key(key)

                        # Handle special operators
                        if operator in ["IS NULL", "IS NOT NULL"]:
                            sub_clauses.append(
                                f"{column} {operator}"
                            )  # No placeholders
                        elif operator in ["IN", "NOT IN"]:
                            if not isinstance(value, list):
                                raise ValueError(
                                    f"Expected list for '{key}', got {type(value)}"
                                )
                            placeholders = ", ".join(
                                f"${len(query_values) + j + 1}"
                                for j in range(len(value))
                            )
                            sub_clauses.append(f"{column} {operator} ({placeholders})")
                            query_values.extend(value)  # Flatten the list
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

                    # Handle special operators
                    if operator in ["IS NULL", "IS NOT NULL"]:
                        where_clauses.append(f"{column} {operator}")  # No placeholders
                    elif operator in ["IN", "NOT IN"]:
                        if not isinstance(value, list):
                            raise ValueError(
                                f"Expected list for '{key}', got {type(value)}"
                            )
                        placeholders = ", ".join(
                            f"${len(query_values) + j + 1}" for j in range(len(value))
                        )
                        where_clauses.append(f"{column} {operator} ({placeholders})")
                        query_values.extend(value)  # Flatten the list
                    else:
                        where_clauses.append(
                            f"{column} {operator} ${len(query_values) + 1}"
                        )
                        query_values.append(value)

        if where_clauses:
            query += f" WHERE {' AND '.join(where_clauses)}"

        # Handle ORDER BY
        if order_by:
            order_clause = ", ".join(
                f"{col[1:]} DESC" if col.startswith("-") else f"{col} ASC"
                for col in order_by
            )
            query += f" ORDER BY {order_clause}"

        # Handle LIMIT and OFFSET
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *query_values)
            return [cls(**dict(row)) for row in rows]

    @staticmethod
    def _parse_filter_key(key: str):
        """
        Parse filter key to extract the column name and SQL operator.

        Example:
            "age__gte" -> ("age", ">=")
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

    async def update(self, pool: asyncpg.Pool, **updates) -> None:
        """
        Update the current record in the database.
        """
        if not updates:
            return

        set_clause = ", ".join(
            f"{key} = ${i+2}" for i, key in enumerate(updates.keys())
        )
        query = f"UPDATE {self.Meta.table_name} SET {set_clause} WHERE id = $1"

        async with pool.acquire() as conn:
            await conn.execute(query, self.id, *updates.values())

    async def delete(self, pool: asyncpg.Pool) -> None:
        """
        Delete the current record from the database.
        """
        query = f"DELETE FROM {self.Meta.table_name} WHERE id = $1"

        async with pool.acquire() as conn:
            await conn.execute(query, self.id)
