"""Tile versioning — git-for-knowledge."""

import copy, time
from enum import Enum

class MergeStrategy(Enum):
    OURS = "ours"
    THEIRS = "theirs"
    SYNTHESIS = "synthesis"

class TileVersionControl:
    def __init__(self):
        self._versions: dict[str, list[dict]] = {}
        self._branches: dict[str, str] = {}
        self._current: dict[str, str] = {}

    def commit(self, tile_id: str, content: str, metadata: dict = None) -> dict:
        version = {"content": content, "metadata": metadata or {},
                   "timestamp": time.time(), "version": len(self._versions.get(tile_id, [])) + 1}
        if tile_id not in self._versions:
            self._versions[tile_id] = []
        self._versions[tile_id].append(version)
        self._current[tile_id] = tile_id
        return version

    def branch(self, tile_id: str, branch_name: str) -> bool:
        versions = self._versions.get(tile_id, [])
        if not versions: return False
        self._branches[f"{tile_id}:{branch_name}"] = versions[-1]["content"]
        return True

    def checkout(self, tile_id: str, version_num: int = None) -> dict | None:
        versions = self._versions.get(tile_id, [])
        if not versions: return None
        if version_num:
            v = [v for v in versions if v["version"] == version_num]
            return v[0] if v else None
        return versions[-1]

    def merge(self, tile_id: str, their_content: str, strategy: MergeStrategy = MergeStrategy.OURS) -> dict | None:
        ours = self.checkout(tile_id)
        if not ours: return None
        if strategy == MergeStrategy.OURS:
            return ours
        elif strategy == MergeStrategy.THEIRS:
            return self.commit(tile_id, their_content)
        else:
            synthesized = f"{ours['content']}\n{their_content}"
            return self.commit(tile_id, synthesized)

    def rollback(self, tile_id: str, steps: int = 1) -> dict | None:
        versions = self._versions.get(tile_id, [])
        if len(versions) <= steps: return None
        target = versions[-(steps + 1)]
        self._versions[tile_id] = versions[:-steps]
        return target

    def diff(self, tile_id: str, v1: int, v2: int) -> dict | None:
        versions = self._versions.get(tile_id, [])
        a = [v for v in versions if v["version"] == v1]
        b = [v for v in versions if v["version"] == v2]
        if not a or not b: return None
        return {"from": a[0], "to": b[0], "changed": a[0]["content"] != b[0]["content"]}

    def history(self, tile_id: str) -> list[dict]:
        return self._versions.get(tile_id, [])

    @property
    def stats(self) -> dict:
        return {"tiles": len(self._versions), "branches": len(self._branches),
                "total_versions": sum(len(v) for v in self._versions.values())}
