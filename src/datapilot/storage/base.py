"""Abstract storage protocol shared by local and S3 backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class StorageBackend(ABC):
    """Storage interface for Medallion artifacts and pipeline reports."""

    @abstractmethod
    def upload_file(self, local_path: Path, remote_key: str) -> str:
        """Persist a single local file and return its target URI."""

    @abstractmethod
    def upload_directory(self, local_dir: Path, remote_prefix: str) -> list[str]:
        """Recursively persist all files under a directory and return target URIs."""

    @abstractmethod
    def download_file(self, remote_key: str, local_path: Path) -> Path:
        """Retrieve a stored object to a local file path."""

    @abstractmethod
    def list_objects(self, prefix: str) -> list[str]:
        """List stored object keys under a prefix."""

    @abstractmethod
    def exists(self, remote_key: str) -> bool:
        """Return True if an object exists at remote_key."""

    @abstractmethod
    def uri_for(self, remote_key: str) -> str:
        """Return canonical URI for a storage key."""
