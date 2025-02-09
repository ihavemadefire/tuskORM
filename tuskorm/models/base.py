import asyncpg
import uuid
from typing import Type, Dict, Any
from pydantic import BaseModel as PydanticModel
from dataclasses import dataclass, field

class BaseModel(PydanticModel):
    """
    Base class for ORM models in TuskORM.
    Provides automatic table name inference, field registration, and CRUD operations.
    """

    id: uuid.UUID = field(default_factory=uuid.uuid4)

    class Meta:
        """Meta class for defining additional table properties."""
        table_name: str = ""

    def __init_subclass__(cls, **kwargs):
        """
        Auto-infer table name if not explicitly defined.
        """
        super().__init_subclass__(**kwargs)
        if not cls.Meta.table_name:
            cls.Meta.table_name = cls.__name__.lower() + "s"  # Simple pluralization

    @classmethod
    async def create(cls, pool: asyncpg.Pool, **kwargs) -> "BaseModel":
        """
        Create a new record in the database.
        """
        columns = ", ".join(kwargs.keys())
        values = ", ".join(f"${i+1}" for i in range(len(kwargs)))
        query = f"INSERT INTO {cls.Meta.table_name} ({columns}) VALUES ({values}) RETURNING *"

        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, *kwargs.values())
            return cls(**dict(row))

    @classmethod
    async def fetch_one(cls, pool: asyncpg.Pool, **conditions) -> "BaseModel":
        """
        Fetch a single record by conditions.
        """
        where_clause = " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(conditions.keys()))
        query = f"SELECT * FROM {cls.Meta.table_name} WHERE {where_clause} LIMIT 1"

        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, *conditions.values())
            return cls(**dict(row)) if row else None

    @classmethod
    async def fetch_all(cls, pool: asyncpg.Pool, **conditions) -> list["BaseModel"]:
        """
        Fetch multiple records by conditions.
        """
        where_clause = " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(conditions.keys()))
        query = f"SELECT * FROM {cls.Meta.table_name} WHERE {where_clause}"

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *conditions.values())
            return [cls(**dict(row)) for row in rows]

    async def update(self, pool: asyncpg.Pool, **updates) -> None:
        """
        Update the record in the database.
        """
        set_clause = ", ".join(f"{key} = ${i+1}" for i, key in enumerate(updates.keys()))
        query = f"UPDATE {self.Meta.table_name} SET {set_clause} WHERE id = $1"

        async with pool.acquire() as conn:
            await conn.execute(query, self.id, *updates.values())

    async def delete(self, pool: asyncpg.Pool) -> None:
        """
        Delete the record from the database.
        """
        query = f"DELETE FROM {self.Meta.table_name} WHERE id = $1"

        async with pool.acquire() as conn:
            await conn.execute(query, self.id)
