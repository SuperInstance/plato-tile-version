"""Tile versioning — immutable version chains, branching, diff, rollback, merge."""
import time
import json
import hashlib
from dataclasses import dataclass, field
from typing import Optional
from collections import defaultdict

@dataclass
class Version:
    number: int
    tile_id: str
    content: str
    author: str = ""
    message: str = ""
    parent: int = 0
    branch: str = "main"
    checksum: str = ""
    timestamp: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.checksum:
            self.checksum = hashlib.sha256(
                f"{self.tile_id}:{self.number}:{self.content}".encode()
            ).hexdigest()[:16]

@dataclass
class VersionDiff:
    from_version: int
    to_version: int
    additions: int = 0
    deletions: int = 0
    changes: list[str] = field(default_factory=list)
    is_revert: bool = False

@dataclass
class BranchInfo:
    name: str
    head: int
    created_at: float = field(default_factory=time.time)
    parent_branch: str = "main"

class TileVersion:
    def __init__(self):
        self._versions: dict[str, dict[int, Version]] = defaultdict(dict)  # tile_id → {version → Version}
        self._branches: dict[str, dict[str, BranchInfo]] = defaultdict(dict)  # tile_id → {branch → BranchInfo}
        self._heads: dict[str, int] = {}  # tile_id → current version

    def commit(self, tile_id: str, content: str, author: str = "",
               message: str = "", branch: str = "main") -> Version:
        tile_versions = self._versions[tile_id]
        version_num = len(tile_versions) + 1
        parent = self._heads.get(tile_id, 0)
        version = Version(number=version_num, tile_id=tile_id, content=content,
                         author=author, message=message, parent=parent, branch=branch)
        tile_versions[version_num] = version
        self._heads[tile_id] = version_num
        # Update branch head
        if branch not in self._branches[tile_id]:
            self._branches[tile_id][branch] = BranchInfo(
                name=branch, head=version_num, parent_branch=branch)
        self._branches[tile_id][branch].head = version_num
        return version

    def get(self, tile_id: str, version: int) -> Optional[Version]:
        return self._versions[tile_id].get(version)

    def head(self, tile_id: str) -> Optional[Version]:
        v = self._heads.get(tile_id)
        return self._versions[tile_id].get(v) if v else None

    def history(self, tile_id: str, limit: int = 50) -> list[Version]:
        versions = list(self._versions[tile_id].values())
        versions.sort(key=lambda v: v.number, reverse=True)
        return versions[:limit]

    def diff(self, tile_id: str, from_v: int, to_v: int) -> VersionDiff:
        v_from = self._versions[tile_id].get(from_v)
        v_to = self._versions[tile_id].get(to_v)
        if not v_from or not v_to:
            return VersionDiff(from_version=from_v, to_version=to_v)
        # Line-level diff
        lines_from = v_from.content.splitlines()
        lines_to = v_to.content.splitlines()
        set_from = set(lines_from)
        set_to = set(lines_to)
        additions = len(set_to - set_from)
        deletions = len(set_from - set_to)
        changes = []
        for line in sorted(set_to - set_from):
            changes.append(f"+ {line}")
        for line in sorted(set_from - set_to):
            changes.append(f"- {line}")
        # Check if this is a revert
        is_revert = v_to.content == self._versions[tile_id].get(v_from.parent, Version(0, "", "")).content if v_from.parent else False
        return VersionDiff(from_version=from_v, to_version=to_v,
                          additions=additions, deletions=deletions,
                          changes=changes[:50], is_revert=is_revert)

    def rollback(self, tile_id: str, to_version: int, author: str = "") -> Optional[Version]:
        target = self._versions[tile_id].get(to_version)
        if not target:
            return None
        return self.commit(tile_id, target.content, author,
                          f"Rollback to v{to_version}")

    def branch(self, tile_id: str, branch_name: str, from_branch: str = "main") -> Optional[BranchInfo]:
        from_head = self._branches[tile_id].get(from_branch)
        if not from_head:
            return None
        info = BranchInfo(name=branch_name, head=from_head.head,
                         parent_branch=from_branch)
        self._branches[tile_id][branch_name] = info
        return info

    def merge(self, tile_id: str, source_branch: str, target_branch: str = "main",
              author: str = "") -> Optional[Version]:
        source = self._branches[tile_id].get(source_branch)
        target = self._branches[tile_id].get(target_branch)
        if not source or not target:
            return None
        source_content = self._versions[tile_id].get(source.head)
        if not source_content:
            return None
        return self.commit(tile_id, source_content.content, author,
                          f"Merge {source_branch} into {target_branch}",
                          branch=target_branch)

    def branches(self, tile_id: str) -> list[BranchInfo]:
        return list(self._branches[tile_id].values())

    def version_count(self, tile_id: str) -> int:
        return len(self._versions[tile_id])

    def all_tile_ids(self) -> list[str]:
        return list(self._versions.keys())

    def export(self, tile_id: str) -> dict:
        versions = self.history(tile_id, limit=1000)
        return {"tile_id": tile_id, "versions": [
            {"number": v.number, "author": v.author, "message": v.message,
             "branch": v.branch, "checksum": v.checksum, "timestamp": v.timestamp,
             "content_length": len(v.content)}
            for v in versions
        ], "branches": [{"name": b.name, "head": b.head, "parent": b.parent_branch}
                        for b in self.branches(tile_id)]}

    @property
    def stats(self) -> dict:
        tiles = len(self._versions)
        versions = sum(len(vs) for vs in self._versions.values())
        branches = sum(len(bs) for bs in self._branches.values())
        return {"tiles": tiles, "versions": versions, "branches": branches}
