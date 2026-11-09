"""디스코드 스터디룸 봇.

스터디 음성 채널에서 카메라 또는 화면 공유를 켜고 있는 동안만 공부 시간을 측정한다.
공유가 잠깐 꺼져도 GRACE_SECONDS 안에 다시 켜면 같은 세션으로 이어진다.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import DefaultDict, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import tasks
from dotenv import load_dotenv

from db import Database

load_dotenv()

TOKEN = os.environ.get("DISCORD_TOKEN", "")
STUDY_CHANNEL_IDS = {
    int(x) for x in os.environ.get("STUDY_CHANNEL_IDS", "").split(",") if x.strip()
}
GUILD_ID = os.environ.get("GUILD_ID", "").strip()
GRACE_SECONDS = int(os.environ.get("GRACE_SECONDS", "60"))
DB_PATH = os.environ.get("DB_PATH", "study.db")
KST = ZoneInfo("Asia/Seoul")

log = logging.getLogger("studybot")
NO_PING = discord.AllowedMentions.none()  # 이름은 보이되 알림은 가지 않게

Key = Tuple[int, int]  # (guild_id, user_id)


@dataclass
class StudySession:
    """한 사람의 공부 세션. 유예 시간 안에 다시 켠 구간들은 하나의 세션으로 묶인다."""

    channel_id: int
    closed_seconds: int = 0               # 이미 끝난 구간들의 합
    segment_id: Optional[int] = None      # 진행 중인 구간 (유예 중이면 None)
    segment_start: int = 0
    finalize_task: Optional[asyncio.Task] = None

    def elapsed(self, now: int) -> int:
        running = now - self.segment_start if self.segment_id is not None else 0
        return self.closed_seconds + running


def is_counting(state: Optional[discord.VoiceState]) -> bool:
    """스터디 채널에 있고 카메라 또는 화면 공유를 켜고 있으면 True."""
    return (
        state is not None
        and state.channel is not None
        and state.channel.id in STUDY_CHANNEL_IDS
        and (state.self_video or state.self_stream)
    )


def fmt(seconds: int) -> str:
    hours, minutes = divmod(seconds // 60, 60)
    return f"{hours}시간 {minutes}분" if hours else f"{minutes}분"


def today_range(now: int) -> Tuple[int, int]:
    start = datetime.fromtimestamp(now, KST).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(start.timestamp()), int((start + timedelta(days=1)).timestamp())


def week_range(now: int) -> Tuple[int, int]:
    """월요일 0시(KST)부터 일주일."""
    day_start, _ = today_range(now)
    start = datetime.fromtimestamp(day_start, KST)
    start -= timedelta(days=start.weekday())
    return int(start.timestamp()), int((start + timedelta(days=7)).timestamp())


class StudyBot(discord.Client):
    def __init__(self) -> None:
        # 기본 intents에 voice_states가 포함돼 있어서 특권 intent는 필요 없다.
        super().__init__(intents=discord.Intents.default())
        self.tree = app_commands.CommandTree(self)
        self.db = Database(DB_PATH)
        self.sessions: Dict[Key, StudySession] = {}
        # 같은 사람의 시작/종료가 DB 쓰기 도중 겹치지 않게 순서대로 처리
        self.locks: DefaultDict[Key, asyncio.Lock] = defaultdict(asyncio.Lock)

    # ---------- 수명 주기 ----------

    async def setup_hook(self) -> None:
        await self.db.connect()
        closed = await self.db.close_dangling()
        if closed:
            log.info("지난 실행에서 열린 채 남은 구간 %d개를 정리했어요", closed)

        try:
            if GUILD_ID:  # 테스트 서버에는 슬래시 명령어가 즉시 반영된다
                guild = discord.Object(id=int(GUILD_ID))
                self.tree.copy_global_to(guild=guild)
                await self.tree.sync(guild=guild)
            else:  # 전역 동기화는 반영까지 시간이 걸릴 수 있다
                await self.tree.sync()
        except discord.Forbidden:
            log.error(
                "슬래시 명령어를 등록하지 못했어요 (403 Missing Access). "
                "봇이 GUILD_ID(%s) 서버에 초대됐는지, 초대할 때 applications.commands 권한을 "
                "체크했는지 확인하세요. 시간 측정은 계속 동작해요.",
                GUILD_ID,
            )

        self.heartbeat.start()

    async def on_ready(self) -> None:
        log.info("%s 로 로그인했어요", self.user)
        await self.reconcile()

    async def close(self) -> None:
        now = int(time.time())
        for sess in self.sessions.values():
            if sess.finalize_task:
                sess.finalize_task.cancel()
            if sess.segment_id is not None:
                await self.db.end_segment(sess.segment_id, now)
        self.sessions.clear()
        if self.heartbeat.is_running():
            self.heartbeat.cancel()
        await self.db.close()
        await super().close()

    @tasks.loop(seconds=60)
    async def heartbeat(self) -> None:
        await self.db.heartbeat(int(time.time()))

    async def reconcile(self) -> None:
        """시작하거나 재연결할 때 실제 음성 상태와 기록을 맞춘다."""
        counting_now = set()
        for channel_id in STUDY_CHANNEL_IDS:
            channel = self.get_channel(channel_id)
            if not isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
                log.warning("스터디 채널 %s 을(를) 찾을 수 없어요. ID를 확인하세요.", channel_id)
                continue
            for member in channel.members:
                if member.bot or not is_counting(member.voice):
                    continue
                key = (member.guild.id, member.id)
                counting_now.add(key)
                sess = self.sessions.get(key)
                if sess is None or sess.segment_id is None:
                    await self.start_counting(key, channel)

        # 연결이 끊긴 사이에 공유를 끈 사람
        for key, sess in list(self.sessions.items()):
            if sess.segment_id is not None and key not in counting_now:
                await self.stop_counting(key)

    # ---------- 측정 로직 ----------

    async def on_voice_state_update(
        self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState
    ) -> None:
        if member.bot:
            return
        key = (member.guild.id, member.id)
        was, now = is_counting(before), is_counting(after)

        if not was and now:
            await self.start_counting(key, after.channel)
        elif was and not now:
            await self.stop_counting(key)
        elif was and now and key in self.sessions:  # 스터디 채널끼리 이동
            self.sessions[key].channel_id = after.channel.id

        joined_study = (
            after.channel is not None
            and after.channel.id in STUDY_CHANNEL_IDS
            and (before.channel is None or before.channel.id != after.channel.id)
        )
        if joined_study and not now:
            await self.notify(
                after.channel.id,
                f"👋 {member.mention} 어서 오세요! 📷 카메라나 🖥️ 화면 공유를 켜면 공부 시간 측정이 시작돼요.",
                delete_after=60,
            )

    async def start_counting(self, key: Key, channel: discord.abc.GuildChannel) -> None:
        async with self.locks[key]:
            await self._start_counting(key, channel)

    async def stop_counting(self, key: Key) -> None:
        async with self.locks[key]:
            await self._stop_counting(key)

    async def _start_counting(self, key: Key, channel: discord.abc.GuildChannel) -> None:
        sess = self.sessions.get(key)
        if sess is not None and sess.segment_id is not None:
            return  # 이미 측정 중

        resumed = sess is not None
        if resumed:  # 유예 시간 안에 다시 켬 → 같은 세션으로 이어 가기
            if sess.finalize_task:
                sess.finalize_task.cancel()
                sess.finalize_task = None
            sess.channel_id = channel.id
        else:
            sess = StudySession(channel_id=channel.id)
            self.sessions[key] = sess

        now = int(time.time())
        sess.segment_start = now
        sess.segment_id = await self.db.start_segment(key[0], key[1], now)

        if not resumed:
            await self.notify(channel.id, f"▶️ <@{key[1]}> 공부 시작! 화이팅 💪")

    async def _stop_counting(self, key: Key) -> None:
        sess = self.sessions.get(key)
        if sess is None or sess.segment_id is None:
            return

        now = int(time.time())
        segment_id = sess.segment_id
        sess.segment_id = None
        sess.closed_seconds += now - sess.segment_start
        sess.finalize_task = asyncio.create_task(self.finalize_later(key, sess))
        await self.db.end_segment(segment_id, now)

    async def finalize_later(self, key: Key, sess: StudySession) -> None:
        """유예 시간이 지나도 다시 켜지 않으면 세션을 끝내고 결과를 알려준다."""
        try:
            await asyncio.sleep(GRACE_SECONDS)
        except asyncio.CancelledError:
            return
        if self.sessions.get(key) is not sess:
            return
        del self.sessions[key]

        now = int(time.time())
        today = await self.db.user_total(key[0], key[1], *today_range(now), now)
        await self.notify(
            sess.channel_id,
            f"⏹️ <@{key[1]}> 수고했어요! 이번 세션 **{fmt(sess.closed_seconds)}** · 오늘 총 **{fmt(today)}**",
        )

    async def notify(self, channel_id: int, content: str, delete_after: Optional[float] = None) -> None:
        """음성 채널의 텍스트 채팅에 메시지를 보낸다."""
        channel = self.get_channel(channel_id)
        if channel is None:
            return
        try:
            await channel.send(content, allowed_mentions=NO_PING, delete_after=delete_after)
        except discord.HTTPException as e:
            log.warning("채널 %s 에 메시지를 보내지 못했어요: %s", channel_id, e)


bot = StudyBot()


# ---------- 슬래시 명령어 ----------

@bot.tree.command(name="내시간", description="오늘, 이번 주, 누적 공부 시간을 보여줘요")
@app_commands.guild_only()
async def my_time(interaction: discord.Interaction) -> None:
    now = int(time.time())
    gid, uid = interaction.guild_id, interaction.user.id
    today = await bot.db.user_total(gid, uid, *today_range(now), now)
    week = await bot.db.user_total(gid, uid, *week_range(now), now)
    total = await bot.db.user_total(gid, uid, 0, now + 1, now)

    embed = discord.Embed(title=f"📚 {interaction.user.display_name}님의 공부 시간", color=0x5865F2)
    embed.add_field(name="오늘", value=fmt(today))
    embed.add_field(name="이번 주", value=fmt(week))
    embed.add_field(name="누적", value=fmt(total))

    sess = bot.sessions.get((gid, uid))
    if sess is not None:
        state = "🟢 공부 중" if sess.segment_id is not None else "⏸️ 잠시 멈춤"
        embed.set_footer(text=f"{state} · 이번 세션 {fmt(sess.elapsed(now))}")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="랭킹", description="서버 공부 시간 순위를 보여줘요")
@app_commands.describe(기간="순위를 볼 기간")
@app_commands.choices(
    기간=[
        app_commands.Choice(name="오늘", value="today"),
        app_commands.Choice(name="이번 주", value="week"),
        app_commands.Choice(name="전체", value="all"),
    ]
)
@app_commands.guild_only()
async def ranking(interaction: discord.Interaction, 기간: str = "today") -> None:
    now = int(time.time())
    if 기간 == "week":
        start, end, label = *week_range(now), "이번 주"
    elif 기간 == "all":
        start, end, label = 0, now + 1, "전체"
    else:
        start, end, label = *today_range(now), "오늘"

    rows = await bot.db.ranking(interaction.guild_id, start, end, now)
    if not rows:
        await interaction.response.send_message(f"{label} 기록이 아직 없어요. 첫 번째 주인공이 되어 보세요! ✨")
        return

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines = [f"{medals.get(i, f'**{i}.**')} <@{uid}> — {fmt(sec)}" for i, (uid, sec) in enumerate(rows, 1)]
    embed = discord.Embed(title=f"🏆 {label} 공부 랭킹", description="\n".join(lines), color=0xF1C40F)
    await interaction.response.send_message(embed=embed, allowed_mentions=NO_PING)


@bot.tree.command(name="현황", description="지금 공부 중인 사람을 보여줘요")
@app_commands.guild_only()
async def status(interaction: discord.Interaction) -> None:
    now = int(time.time())
    active = sorted(
        ((uid, sess) for (gid, uid), sess in bot.sessions.items() if gid == interaction.guild_id),
        key=lambda item: item[1].elapsed(now),
        reverse=True,
    )
    if not active:
        await interaction.response.send_message("지금은 공부 중인 사람이 없어요. 📷 카메라나 🖥️ 화면 공유를 켜고 시작해 보세요!")
        return

    lines = [
        f"{'🟢' if sess.segment_id is not None else '⏸️'} <@{uid}> — {fmt(sess.elapsed(now))}"
        for uid, sess in active
    ]
    embed = discord.Embed(title=f"📖 지금 공부 중 ({len(active)}명)", description="\n".join(lines), color=0x2ECC71)
    await interaction.response.send_message(embed=embed, allowed_mentions=NO_PING)


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit(".env 파일에 DISCORD_TOKEN을 설정하세요.")
    if not STUDY_CHANNEL_IDS:
        raise SystemExit(".env 파일에 STUDY_CHANNEL_IDS를 설정하세요.")
    bot.run(TOKEN, log_level=logging.INFO)
