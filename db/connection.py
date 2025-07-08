from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_HOST, DB_NAME, DB_USERNAME, DB_PASSWORD]):
    raise ValueError("Missing required database environment variables")

pw_quoted = quote_plus(DB_PASSWORD)

DATABASE_URL = (
    f"postgresql+asyncpg://"
    f"{DB_USERNAME}:{pw_quoted}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

print("Database connected")

# Create async engine
engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)

# Create async session maker
AsyncSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

# Base class for models
Base = declarative_base()

# Dependency to get the database session
async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()