import os
import paramiko
from typing import List, Optional
from utils.logger import get_logger
from config import settings

logger = get_logger(__name__)

class SFTPClient:
    """
    Simple wrapper around Paramiko SFTPClient.
    Reads host/port/credentials from app.config.settings.
    """

    def __init__(self):
        self.host: str = settings.SFTP_HOST
        self.port: int = settings.SFTP_PORT or 22
        self.username: str = settings.SFTP_USER
        self.password: str = settings.SFTP_PASS
        self.transport: Optional[paramiko.Transport] = None
        self.client:  Optional[paramiko.SFTPClient] = None

    def connect(self) -> None:
        """
        Establishes the SFTP connection if not already active.
        """
        if self.client and self.transport and self.transport.is_active():
            return

        try:
            logger.info(f"Connecting to SFTP {self.host}:{self.port} as {self.username}")
            self.transport = paramiko.Transport((self.host, self.port))
            self.transport.connect(username=self.username, password=self.password)
            self.client = paramiko.SFTPClient.from_transport(self.transport)
            logger.info("SFTP connection established")
        except Exception as e:
            logger.error(f"Error connecting to SFTP: {e}")
            raise

    def list_files(self, remote_dir: str) -> List[str]:
        """
        List all entries (files & directories) in the given remote directory.
        Returns full remote paths.
        """
        self.connect()
        try:
            names = self.client.listdir(remote_dir)
            paths = [os.path.join(remote_dir, name) for name in names]
            logger.debug(f"Listed {len(paths)} items in {remote_dir}")
            return paths
        except Exception as e:
            logger.error(f"Error listing {remote_dir}: {e}")
            raise

    def download_file(self, remote_path: str) -> bytes:
        """
        Download the file at remote_path and return its raw bytes.
        """
        self.connect()
        try:
            logger.info(f"Downloading SFTP file: {remote_path}")
            with self.client.open(remote_path, 'rb') as rf:
                data = rf.read()
            logger.debug(f"Downloaded {len(data)} bytes from {remote_path}")
            return data
        except Exception as e:
            logger.error(f"Error downloading {remote_path}: {e}")
            raise

    def close(self) -> None:
        """
        Gracefully close the SFTP session and underlying transport.
        """
        if self.client:
            try:
                self.client.close()
            except Exception:
                pass
        if self.transport:
            try:
                self.transport.close()
            except Exception:
                pass
        logger.info("SFTP connection closed")
