import asyncpg
import logging
import asyncio

class AsyncDatabase:
    """Asynchronous PostgreSQL database connection handler with robust error handling."""
    
    def __init__(self, database: str, user: str, password: str, host: str, port: int = 5432):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.pool = None  # Connection pool
        self.retry_attempts = 3  # Number of retries before failing

    async def connect(self):
        """Establish connection pool with retries."""
        for attempt in range(1, self.retry_attempts + 1):
            try:
                self.pool = await asyncpg.create_pool(
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    min_size=1,  # Minimum number of connections
                    max_size=10, # Maximum number of connections
                    timeout=10   # Connection timeout in seconds
                )
                logging.info(f"✅ Connected to database {self.database} on attempt {attempt}")
                return
            except asyncpg.PostgresError as e:
                logging.error(f"❌ Database connection failed (attempt {attempt}/{self.retry_attempts}): {e}")
                if attempt == self.retry_attempts:
                    raise
                await asyncio.sleep(2)  # Wait before retrying

    async def disconnect(self):
        """Close the database connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None
            logging.info("🔌 Disconnected from database.")

    async def execute(self, query: str, *args) -> str:
        """Execute a query and return status message."""
        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute(query, *args)
                return result
        except asyncpg.PostgresError as e:
            logging.error(f"❗ Database execution error: {e}")
            raise

    async def fetch_one(self, query: str, *args):
        """Fetch a single record."""
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetchrow(query, *args)
        except asyncpg.PostgresError as e:
            logging.error(f"❗ Fetch error: {e}")
            raise

    async def fetch_all(self, query: str, *args):
        """Fetch multiple records."""
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetch(query, *args)
        except asyncpg.PostgresError as e:
            logging.error(f"❗ Fetch all error: {e}")
            raise
