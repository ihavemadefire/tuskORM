import asyncpg
import uuid
from typing import Type, Dict, Any, List, Optional
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

    model_config = ConfigDict(extra="allow", from_attributes=True)  # Allows missing fields
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
    async def fetch_one(cls, pool: asyncpg.Pool, columns: Optional[List[str]] = None, **conditions) -> Optional["BaseModel"]:
        """
        Retrieve a single record from the database that matches the given conditions.
        """
        selected_columns = ", ".join(set(columns or []) | {"id"})  # Ensure 'id' is always selected
        where_clause = " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(conditions.keys())) if conditions else ""
        query = f"SELECT {selected_columns} FROM {cls.Meta.table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        query += " LIMIT 1"

        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, *conditions.values())
            return cls(**dict(row)) if row else None

    @classmethod
    async def fetch_all(cls, pool: asyncpg.Pool, columns: Optional[List[str]] = None, **conditions) -> List["BaseModel"]:
        """
        Retrieve multiple records from the database that match the given conditions.
        """
        selected_columns = ", ".join(set(columns or []) | {"id"})  # Ensure 'id' is always selected
        where_clause = " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(conditions.keys())) if conditions else ""
        query = f"SELECT {selected_columns} FROM {cls.Meta.table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *conditions.values())
            return [cls(**dict(row)) for row in rows]

    @classmethod
    async def fetch_filter(cls, pool: asyncpg.Pool, conditions: Dict[str, Any], columns: Optional[List[str]] = None) -> List["BaseModel"]:
        """
        Retrieve multiple records from the database based on filtering criteria.
        """
        selected_columns = ", ".join(set(columns or []) | {"id"})  # Ensure 'id' is always selected
        where_clause = " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(conditions.keys())) if conditions else ""
        query = f"SELECT {selected_columns} FROM {cls.Meta.table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *conditions.values())
            return [cls.model_construct(**dict(row)) for row in rows]

    async def update(self, pool: asyncpg.Pool, **updates) -> None:
        """
        Update the current record in the database.
        """
        if not updates:
            return

        set_clause = ", ".join(f"{key} = ${i+2}" for i, key in enumerate(updates.keys()))
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
