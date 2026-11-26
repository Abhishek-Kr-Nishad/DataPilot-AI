"""Local filesystem implementation of StorageBackend."""

from __future__ import annotations

import shutil
from pathlib import Path

from datapilot.storage.base import StorageBackend


class LocalStorageBackend(StorageBackend):
    """Stores artifacts under a local base directory mirroring Medallion prefixes."""

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir.resolve()
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _resolve_key(self, remote_key: str) -> Path:
        clean_key = remote_key.strip("/").replace("\\", "/")
        candidate = (self.base_dir / clean_key).resolve()
        if self.base_dir not in candidate.parents and candidate != self.base_dir:
            raise ValueError(f"Path traversal outside base_dir is forbidden: {remote_key!r}")
        return candidate

    def upload_file(self, local_path: Path, remote_key: str) -> str:
        target = self._resolve_key(remote_key)
        target.parent.mkdir(parents=True, exist_ok=True)
        if local_path.resolve() != target:
            shutil.copy2(local_path, target)
        return self.uri_for(remote_key)

    def upload_directory(self, local_dir: Path, remote_prefix: str) -> list[str]:
        uris: list[str] = []
        clean_prefix = remote_prefix.strip("/").replace("\\", "/")
        for file_path in sorted(local_dir.rglob("*")):
            if file_path.is_file():
                rel = file_path.relative_to(local_dir).as_posix()
                key = f"{clean_prefix}/{rel}" if clean_prefix else rel
                uris.append(self.upload_file(file_path, key))
        return uris

    def download_file(self, remote_key: str, local_path: Path) -> Path:
        source = self._resolve_key(remote_key)
        if not source.exists():
            raise FileNotFoundError(f"Object not found in local storage: {source}")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, local_path)
        return local_path

    def list_objects(self, prefix: str) -> list[str]:
        clean_prefix = prefix.strip("/").replace("\\", "/")
        root = self._resolve_key(clean_prefix) if clean_prefix else self.base_dir
        if not root.exists():
            return []
        if root.is_file():
            return [root.relative_to(self.base_dir).as_posix()]
        return sorted(
            p.relative_to(self.base_dir).as_posix()
            for p in root.rglob("*")
            if p.is_file()
        )

    def exists(self, remote_key: str) -> bool:
        return self._resolve_key(remote_key).exists()

    def uri_for(self, remote_key: str) -> str:
        return str(self._resolve_key(remote_key))
