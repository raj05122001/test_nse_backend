from pydantic import BaseSettings
import os

class Settings(BaseSettings):
    # SFTP configuration
    SFTP_HOST: str = ['snapshotsftp1.nseindia.com', 'snapshotsftp2.nseindia.com']
    SFTP_PORT: int = 6010
    SFTP_USER: str = 'PTCPL_15MINCM'
    SFTP_REMOTE_PATH: str = "/CM30"
    KEY_PATH = os.path.join(os.path.dirname(__file__), 'ssh', 'pride_sftp_key')

    # Polling interval for SFTP watcher (in seconds)
    POLL_INTERVAL_SECONDS: int = 60

    # Logging level
    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# Instantiate settings; import this `settings` everywhere
settings = Settings()
