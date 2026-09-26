"""表情包关键词匹配 + 选表情。

Host 的 ``emoji.get_by_description`` 没有相似度阈值：它按 Levenshtein 相似度取前 10 张后随机
返回一张，表情库非空时永不返回空（相似度为 0 也照样返回）。因此"返回了表情"不代表关键词命中：

* **标签匹配**：用 ``emoji.get_emotions``（只有标签字符串、不含图片）做本地匹配，关键词与任一
  标签互为子串才算库里有对应表情；标签缓存按间隔惰性刷新，覆盖表情库增删。
* **相关性校验**：只对匹配上的关键词调用 ``get_by_description``，并按返回表情的 description
  标签复核；不相关的结果仅在 ``allow_random_fallback`` 开启时当作随机兜底。

表情库为空时调 ``get_by_description`` 会让主程序刷 "[获取表情包] 表情包列表为空" warning，
所以标签为空时一律不发这个 RPC。
"""

from __future__ import annotations

import asyncio
import random
import re
import time
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from ..plugin import SmartPokePlugin


# 表情库在 Host 启动后异步加载：探测先等一会，再轮询 get_emotions 直到拿到标签。
EMOJI_PROBE_INITIAL_DELAY_SECONDS = 2.0
EMOJI_READY_POLL_INTERVAL_SECONDS = 1.5
EMOJI_READY_POLL_MAX_ATTEMPTS = 40

# 标签缓存有效期：过期后下一次选表情前重新拉取。
EMOJI_TAGS_REFRESH_INTERVAL_SECONDS = 600.0

# 单次选表情最多调用几次 get_by_description（每次都会传回一张 base64 图片）。
EMOJI_PICK_MAX_ATTEMPTS = 3

# 与 Host 拆分表情标签时使用的分隔符一致。
_TAG_SEPARATORS = re.compile(r"[,，、；]")


def _split_tags(text: Any) -> list[str]:
    return [tag.strip() for tag in _TAG_SEPARATORS.split(str(text or "")) if tag.strip()]


def _keyword_matches_tags(keyword: str, tags: list[str]) -> bool:
    """关键词与任一标签互为子串即视为匹配。"""
    return any(keyword in tag or tag in keyword for tag in tags)


class EmojiKeywordValidator:
    """表情关键词的匹配与选用。"""

    def __init__(self, plugin: "SmartPokePlugin") -> None:
        self._plugin = plugin
        self._emotion_tags: list[str] = []
        self._tags_refreshed_at: float = 0.0
        # 最近一次探测针对的关键词；热更新时据此判断关键词是否变了、要不要重新探测。
        self._probed_keywords: tuple[str, ...] | None = None

    def keywords_changed(self) -> bool:
        return tuple(self._configured_keywords()) != self._probed_keywords

    # ===== 探测 =====

    async def probe_keywords(self) -> None:
        """等表情库就绪后做一次本地匹配，把可用 / 用不上的关键词打到日志里。"""
        ctx = self._plugin.ctx
        keywords = self._configured_keywords()
        self._probed_keywords = tuple(keywords)
        if not keywords:
            return

        if not self._emotion_tags:
            await asyncio.sleep(EMOJI_PROBE_INITIAL_DELAY_SECONDS)
        for attempt in range(1, EMOJI_READY_POLL_MAX_ATTEMPTS + 1):
            if await self._refresh_emotion_tags():
                break
            if attempt < EMOJI_READY_POLL_MAX_ATTEMPTS:
                await asyncio.sleep(EMOJI_READY_POLL_INTERVAL_SECONDS)
        else:
            ctx.logger.info(
                "等待表情库就绪超时（轮询 %d 次仍未拿到表情标签），跳过关键词匹配；运行时选表情前会再次尝试",
                EMOJI_READY_POLL_MAX_ATTEMPTS,
            )
            return

        matched = self._matched_keywords(keywords)
        if not matched:
            ctx.logger.warning(
                "表情关键词在表情库里都找不到对应标签（关键词：%s）；"
                "建议换成库里有的标签，或开启 emoji.allow_random_fallback",
                ", ".join(keywords),
            )
            return
        ctx.logger.info(
            "表情关键词匹配 %d/%d 个：%s", len(matched), len(keywords), ", ".join(matched),
        )
        unmatched = [kw for kw in keywords if kw not in matched]
        if unmatched:
            ctx.logger.info("以下表情关键词在表情库里没有对应标签，运行时会跳过：%s", ", ".join(unmatched))

    # ===== 运行时挑表情 =====

    async def pick_emoji(self) -> dict[str, Any] | None:
        """挑一张与关键词相关的表情；都不相关时按 allow_random_fallback 决定是否改发随机表情。"""
        ctx = self._plugin.ctx
        await self._ensure_emotion_tags()
        if not self._emotion_tags:
            ctx.logger.debug("[emoji] 表情库尚未就绪或为空，跳过本次选表情")
            return None

        fallback: dict[str, Any] | None = None
        matched = self._matched_keywords(self._configured_keywords())
        if matched:
            # 匹配上的关键词各试一次，不够次数再轮一遍：同一关键词每次会从前 10 名里重新随机
            order = random.sample(matched, len(matched))
            for kw in (order * EMOJI_PICK_MAX_ATTEMPTS)[:EMOJI_PICK_MAX_ATTEMPTS]:
                try:
                    emoji = await ctx.emoji.get_by_description(kw, limit=1)
                except Exception:
                    ctx.logger.debug("emoji.get_by_description 失败 (kw=%s)", kw, exc_info=True)
                    continue
                # 失败信封 {"success": False, ...} 没有 base64
                if not isinstance(emoji, dict) or not emoji.get("base64"):
                    continue
                if _keyword_matches_tags(kw, _split_tags(emoji.get("description") or emoji.get("emotion"))):
                    return emoji
                ctx.logger.debug(
                    "[emoji] 关键词 %s 取到的表情与之无关（%s），换一张", kw, emoji.get("description"),
                )
                if fallback is None:
                    fallback = emoji

        if not self._plugin.config.emoji.allow_random_fallback:
            return None
        # 取到过的无关表情本身就是随机的，直接拿来兜底，省一次 get_random
        if fallback is not None:
            return fallback
        try:
            emojis = await ctx.emoji.get_random(1)
        except Exception:
            ctx.logger.debug("emoji.get_random 失败", exc_info=True)
            return None
        if isinstance(emojis, list):
            for item in emojis:
                if isinstance(item, dict) and item.get("base64"):
                    return item
        return None

    # ===== 内部 =====

    def _configured_keywords(self) -> list[str]:
        """配置里的关键词，去空白并去重（重复的词会被多次请求）。"""
        return list(dict.fromkeys(
            str(k).strip() for k in self._plugin.config.emoji.description_keywords if str(k).strip()
        ))

    def _matched_keywords(self, keywords: list[str]) -> list[str]:
        return [kw for kw in keywords if _keyword_matches_tags(kw, self._emotion_tags)]

    async def _ensure_emotion_tags(self) -> None:
        """标签缓存为空（表情库未就绪 / 为空，每次都重试）或已过期时刷新。"""
        if self._emotion_tags and time.time() - self._tags_refreshed_at < EMOJI_TAGS_REFRESH_INTERVAL_SECONDS:
            return
        await self._refresh_emotion_tags()

    async def _refresh_emotion_tags(self) -> bool:
        """拉取表情库标签，拿到非空标签返回 True；调用失败时保留旧缓存。"""
        try:
            result = await self._plugin.ctx.emoji.get_emotions()
        except Exception:
            self._plugin.ctx.logger.debug("emoji.get_emotions 调用失败", exc_info=True)
            return False
        if not isinstance(result, list):
            return False
        self._emotion_tags = [str(tag).strip() for tag in result if str(tag).strip()]
        self._tags_refreshed_at = time.time()
        return bool(self._emotion_tags)
