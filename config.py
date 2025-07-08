from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv


load_dotenv()

class Settings(BaseSettings):
    # SFTP configuration
    SFTP_HOSTS: list[str] = ['snapshotsftp1.nseindia.com', 'snapshotsftp2.nseindia.com']
    SFTP_PORT: int = 6010
    SFTP_USER: str = 'PTCPL_15MINCM'
    SFTP_PASS: str = ''
    SFTP_REMOTE_PATH: str = "/CM30"
    KEY_PATH: str = os.path.join(os.path.dirname(__file__), 'ssh', 'pride_sftp_key')

    # Polling interval for SFTP watcher (in seconds)
    POLL_INTERVAL_SECONDS: int = 60

    # Logging level
    LOG_LEVEL: str = "INFO"

    # Database settings
    DB_HOST: str = os.getenv("DB_HOST")
    DB_PORT: int = int(os.getenv("DB_PORT", 5432))
    DB_NAME: str = os.getenv("DB_NAME")
    DB_USERNAME: str = os.getenv("DB_USERNAME")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD")

    # Additional fields from your .env
    redis_url: str = "redis://localhost:6379"
    market_cache_ttl: int = 30
    api_rate_limit: int = 100
    websocket_max_connections: int = 1000

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"  # This will ignore extra fields

settings = Settings()