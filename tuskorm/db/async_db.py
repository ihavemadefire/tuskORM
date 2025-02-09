import asyncpg
from typing import Optional

class AsyncDatabase:
    """
    Manages async PostgreSQL connections and connection pooling for TuskORM.
    """
    
    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 10):
        """
        Initializes the database connection pool.
        
        :param dsn: Database connection string.
        :param min_size: Minimum number of connections in the pool.
        :param max_size: Maximum number of connections in the pool.
        """
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """
        Establishes the connection pool.
        """
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                dsn=self.dsn,
                min_size=self.min_size,
                max_size=self.max_size
            )
            print("Database connected successfully.")

    async def disconnect(self):
        """
        Closes the database connection pool.
        """
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
            print("Database connection closed.")

    async def fetch_one(self, query: str, *args):
        """
        Fetches a single record from the database.
        
        :param query: SQL query string.
        :param args: Query parameters.
        :return: Single database record or None.
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetch_all(self, query: str, *args):
        """
        Fetches multiple records from the database.
        
        :param query: SQL query string.
        :param args: Query parameters.
        :return: List of records.
        """
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def execute(self, query: str, *args):
        """
        Executes an SQL statement (INSERT, UPDATE, DELETE) using a new connection per query.
        Ensures transactional execution to avoid race conditions.
        
        :param query: SQL query string.
        :param args: Query parameters.
        :return: Number of affected rows.
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():  # ✅ Ensure transactional execution
                return await conn.execute(query, *args)