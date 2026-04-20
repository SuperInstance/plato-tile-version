"""Tile versioning — immutable version chains, diff, rollback, branching."""
import time
import hashlib
from dataclasses import dataclass, field
from typing import Optional
from collections import defaultdict

@dataclass
class TileVersion:
    tile_id: str
    version: int
    content: str
    author: str = ""
    message: str = ""
    parent_hash: str = ""
    hash: str = ""
    branch: str = "main"
    created_at: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.hash:
            raw = f"{self.tile_id}:{self.version}:{self.content}:{self.parent_hash}"
            self.hash = hashlib.sha256(raw.encode()).hexdigest()[:16]

@dataclass
class DiffResult:
    tile_id: str
    from_version: int
    to_version: int
    additions: int
    deletions: int
    unchanged: int
    diff_lines: list[str] = field(default_factory=list)

class TileVersionControl:
    def __init__(self, max_versions: int = 100):
        self._chains: dict[str, list[TileVersion]] = defaultdict(list)
        self._branches: dict[str, dict[str, str]] = defaultdict(dict)  # tile_id -> {branch: hash}
        self.max_versions = max_versions

    def commit(self, tile_id: str, content: str, author: str = "",
               message: str = "", branch: str = "main") -> TileVersion:
        chain = self._chains[tile_id]
        parent_hash = chain[-1].hash if chain else ""
        version = len(chain) + 1
        tv = TileVersion(tile_id=tile_id, version=version, content=content,
                        author=author, message=message, parent_hash=parent_hash, branch=branch)
        chain.append(tv)
        self._branches[tile_id][branch] = tv.hash
        # Trim old versions
        if len(chain) > self.max_versions:
            self._chains[tile_id] = chain[-self.max_versions:]
        return tv

    def checkout(self, tile_id: str, version: int = 0) -> Optional[TileVersion]:
        chain = self._chains.get(tile_id, [])
        if not chain:
            return None
        if version <= 0:
            return chain[-1]
        for tv in chain:
            if tv.version == version:
                return tv
        return None

    def head(self, tile_id: str, branch: str = "main") -> Optional[TileVersion]:
        chain = self._chains.get(tile_id, [])
        for tv in reversed(chain):
            if tv.branch == branch:
                return tv
        return chain[-1] if chain else None

    def history(self, tile_id: str, limit: int = 20) -> list[TileVersion]:
        chain = self._chains.get(tile_id, [])
        return list(reversed(chain[-limit:]))

    def diff(self, tile_id: str, from_v: int, to_v: int) -> DiffResult:
        old = self.checkout(tile_id, from_v)
        new = self.checkout(tile_id, to_v)
        if not old or not new:
            return DiffResult(tile_id=tile_id, from_version=from_v, to_version=to_v,
                            additions=0, deletions=0, unchanged=0)
        old_lines = old.content.splitlines()
        new_lines = new.content.splitlines()
        old_set = set(old_lines)
        new_set = set(new_lines)
        additions = len(new_set - old_set)
        deletions = len(old_set - new_set)
        unchanged = len(old_set & new_set)
        diff_lines = []
        for line in new_lines:
            if line not in old_set:
                diff_lines.append(f"+ {line}")
        for line in old_lines:
            if line not in new_set:
                diff_lines.append(f"- {line}")
        return DiffResult(tile_id=tile_id, from_version=from_v, to_version=to_v,
                         additions=additions, deletions=deletions, unchanged=unchanged,
                         diff_lines=diff_lines)

    def rollback(self, tile_id: str, version: int, author: str = "",
                 message: str = "") -> Optional[TileVersion]:
        target = self.checkout(tile_id, version)
        if not target:
            return None
        return self.commit(tile_id, target.content, author=author,
                          message=message or f"Rollback to v{version}")

    def branch(self, tile_id: str, branch_name: str, from_version: int = 0) -> bool:
        target = self.checkout(tile_id, from_version)
        if not target:
            return False
        self._branches[tile_id][branch_name] = target.hash
        return True

    def branches(self, tile_id: str) -> dict[str, str]:
        return dict(self._branches.get(tile_id, {}))

    def merge(self, tile_id: str, source_branch: str, target_branch: str = "main",
              author: str = "") -> Optional[TileVersion]:
        source = self.head(tile_id, source_branch)
        target = self.head(tile_id, target_branch)
        if not source:
            return None
        return self.commit(tile_id, source.content, author=author,
                          message=f"Merge {source_branch} into {target_branch}",
                          branch=target_branch)

    def stats(self, tile_id: str = "") -> dict:
        if tile_id:
            chain = self._chains.get(tile_id, [])
            return {"tile_id": tile_id, "versions": len(chain),
                    "branches": len(self._branches.get(tile_id, {}))}
        return {"tiles": len(self._chains),
                "total_versions": sum(len(c) for c in self._chains.values())}
