"""공부 기록 저장소 (SQLite).

공부 시간은 '구간(segment)' 단위로 저장한다.
카메라/화면 공유가 켜져 있던 연속 구간 하나가 한 행이다.
통계는 조회 범위와 구간이 겹치는 만큼만 더하므로 자정을 넘긴 공부도 날짜별로 정확히 나뉜다.
"""
from __future__ import annotations

from typing import List, Optional, Tuple

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS segments (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id  INTEGER NOT NULL,
    user_id   INTEGER NOT NULL,
    start_at  INTEGER NOT NULL,          -- UTC unix seconds
    end_at    INTEGER NOT NULL,          -- 진행 중이면 마지막 heartbeat 시각
    open      INTEGER NOT NULL DEFAULT 1 -- 1 = 진행 중
);
CREATE INDEX IF NOT EXISTS idx_segments_guild_user
    ON segments (guild_id, user_id, start_at);
"""

# 조회 범위 [:start, :end) 와 구간이 겹치는 초. 진행 중인 구간은 현재 시각(:now)까지로 본다.
_OVERLAP = """
MAX(0,
    MIN(CASE WHEN open = 1 THEN :now ELSE end_at END, :end)
    - MAX(start_at, :start))
"""


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        self.conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        self.conn = await aiosqlite.connect(self.path)
        await self.conn.executescript(SCHEMA)
        await self.conn.commit()

    async def close(self) -> None:
        if self.conn is not None:
            await self.conn.close()
            self.conn = None

    async def close_dangling(self) -> int:
        """봇이 비정상 종료돼 열린 채 남은 구간을 마지막 heartbeat 시각으로 닫는다."""
        cur = await self.conn.execute("UPDATE segments SET open = 0 WHERE open = 1")
        await self.conn.commit()
        return cur.rowcount

    async def start_segment(self, guild_id: int, user_id: int, now: int) -> int:
        cur = await self.conn.execute(
            "INSERT INTO segments (guild_id, user_id, start_at, end_at, open)"
            " VALUES (?, ?, ?, ?, 1)",
            (guild_id, user_id, now, now),
        )
        await self.conn.commit()
        return cur.lastrowid

    async def end_segment(self, segment_id: int, now: int) -> None:
        await self.conn.execute(
            "UPDATE segments SET end_at = ?, open = 0 WHERE id = ?", (now, segment_id)
        )
        await self.conn.commit()

    async def heartbeat(self, now: int) -> None:
        """진행 중인 구간의 end_at을 갱신한다. 봇이 갑자기 꺼져도 최대 1분만 손실된다."""
        await self.conn.execute("UPDATE segments SET end_at = ? WHERE open = 1", (now,))
        await self.conn.commit()

    async def user_total(
        self, guild_id: int, user_id: int, start: int, end: int, now: int
    ) -> int:
        cur = await self.conn.execute(
            f"SELECT COALESCE(SUM({_OVERLAP}), 0) FROM segments"
            " WHERE guild_id = :guild AND user_id = :user AND start_at < :end",
            {"guild": guild_id, "user": user_id, "start": start, "end": end, "now": now},
        )
        (total,) = await cur.fetchone()
        return int(total)

    async def ranking(
        self, guild_id: int, start: int, end: int, now: int, limit: int = 10
    ) -> List[Tuple[int, int]]:
        cur = await self.conn.execute(
            f"SELECT user_id, SUM({_OVERLAP}) AS total FROM segments"
            " WHERE guild_id = :guild AND start_at < :end"
            " GROUP BY user_id HAVING total > 0"
            " ORDER BY total DESC LIMIT :limit",
            {"guild": guild_id, "start": start, "end": end, "now": now, "limit": limit},
        )
        return [(int(uid), int(total)) for uid, total in await cur.fetchall()]
