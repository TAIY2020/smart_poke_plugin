"""表情包关键词验证 + 选表情。

负责：

* **启动期探测**：表情库就绪后用 ``emoji.get_emotions`` 做本地子串匹配，再对未匹配
  的关键词发 ``emoji.get_by_description`` RPC 兜底——必须先确认就绪再发 RPC，
  否则表情库为空时主程序会刷 "[获取表情包] 表情包列表为空" warning。

* **运行时采样**：优先采样已验证关键词以提高 RPC 命中率，但混入未验证关键词作为
  表情库新增表情后的刷新机制。

* **关键词衰退**：已验证关键词连续 miss 到阈值后从验证集移除，应对"表情库被删"场景。
"""

from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from ..plugin import SmartPokePlugin


# 表情库未就绪时 get_by_description 会在主程序里打 "[获取表情包] 表情包列表为空" warning，
# 必须先用无副作用的 get_emotions 轮询确认就绪后再发 RPC。
EMOJI_PROBE_INITIAL_DELAY_SECONDS = 2.0
EMOJI_READY_POLL_INTERVAL_SECONDS = 1.5
EMOJI_READY_POLL_MAX_ATTEMPTS = 40

EMOJI_KEYWORD_PROBE_LIMIT = 3

# 已验证关键词连续 miss 阈值：达到后从验证集移除，应对"表情库后续被删"场景。
EMOJI_KEYWORD_MISS_THRESHOLD = 3


class EmojiKeywordValidator:
    """表情关键词的验证与选用。"""

    def __init__(self, plugin: "SmartPokePlugin") -> None:
        self._plugin = plugin
        # 启动期探测命中的关键词；运行时优先采样以提高 RPC 命中率，
        # 但会混入未验证关键词作为表情库新增表情后的刷新机制。
        self._validated_keywords: list[str] = []
        self._miss_counts: dict[str, int] = {}

    # ===== 启动期探测 =====

    async def probe_keywords_at_startup(self) -> None:
        """启动后探测关键词命中情况。

        必须先用无副作用的 ``emoji.get_emotions`` 轮询表情库就绪，再做本地子串匹配，
        最后对未匹配的关键词发 ``emoji.get_by_description`` 兜底——否则表情库为空时
        ``get_by_description`` 会触发主程序的 "[获取表情包] 表情包列表为空" warning 刷屏。
        """
        ctx = self._plugin.ctx
        keywords = [
            str(k).strip()
            for k in self._plugin.config.emoji.description_keywords
            if str(k).strip()
        ]
        if not keywords:
            return

        await asyncio.sleep(EMOJI_PROBE_INITIAL_DELAY_SECONDS)

        emotions: list[Any] = []
        for attempt in range(1, EMOJI_READY_POLL_MAX_ATTEMPTS + 1):
            try:
                result = await ctx.emoji.get_emotions()
            except Exception:
                ctx.logger.debug(
                    "emoji.get_emotions 第 %d 次调用失败", attempt, exc_info=True,
                )
                result = None
            if isinstance(result, list) and result:
                emotions = result
                if attempt > 1:
                    ctx.logger.debug(
                        "表情库在第 %d 次轮询时就绪（%d 个 emotion 标签）",
                        attempt, len(emotions),
                    )
                break
            if attempt < EMOJI_READY_POLL_MAX_ATTEMPTS:
                await asyncio.sleep(EMOJI_READY_POLL_INTERVAL_SECONDS)
        else:
            ctx.logger.info(
                "等待表情库就绪超时（轮询 %d 次仍未拿到 emotion 标签），跳过启动期关键词探测；"
                "运行时仍会按需采样关键词调用 emoji RPC",
                EMOJI_READY_POLL_MAX_ATTEMPTS,
            )
            return

        emotion_set = {str(e).strip() for e in emotions if str(e).strip()}
        prevalidated: list[str] = []
        for kw in keywords:
            if any(kw == e or kw in e or e in kw for e in emotion_set):
                prevalidated.append(kw)
        if prevalidated:
            for kw in prevalidated:
                if kw not in self._validated_keywords:
                    self._validated_keywords.append(kw)
            ctx.logger.info(
                "emoji.get_emotions 本地匹配命中 %d/%d 个关键词：%s",
                len(prevalidated), len(keywords), ", ".join(prevalidated),
            )

        remaining = [kw for kw in keywords if kw not in prevalidated]
        if not remaining:
            return

        validated_via_rpc: list[str] = []
        for kw in remaining:
            try:
                emoji = await ctx.emoji.get_by_description(kw, limit=1)
            except Exception:
                ctx.logger.debug(
                    "emoji 关键词探测 RPC 失败 (kw=%s)", kw, exc_info=True
                )
                continue
            # 失败信封无 base64 键，必须用实际内容字段判命中，
            # 否则会把 RPC 异常误当成"找到表情"而污染验证集。
            if isinstance(emoji, dict) and emoji.get("base64"):
                validated_via_rpc.append(kw)
        if validated_via_rpc:
            for kw in validated_via_rpc:
                if kw not in self._validated_keywords:
                    self._validated_keywords.append(kw)
            ctx.logger.info(
                "emoji 关键词 RPC 探测追加命中 %d 个：%s",
                len(validated_via_rpc), ", ".join(validated_via_rpc),
            )
            return

        if not self._validated_keywords:
            ctx.logger.warning(
                "emoji 关键词探测全部未命中（关键词：%s）；"
                "建议调整关键词或开启 emoji.allow_random_fallback",
                ", ".join(keywords),
            )

    # ===== 运行时挑表情 =====

    async def pick_emoji(self) -> dict[str, Any] | None:
        """挑一个表情；找不到时按配置决定是否回退随机。

        运行时发现未验证关键词命中（表情库新增表情）会顺手写入验证集；
        已验证关键词连续 miss 到阈值则从验证集移除，应对表情库被删的场景。
        """
        ctx = self._plugin.ctx
        cfg = self._plugin.config.emoji
        probe = self._sample_probe_keywords(cfg.description_keywords)

        for kw in probe:
            try:
                emoji = await ctx.emoji.get_by_description(kw, limit=1)
                # 用 base64 而非"非空 dict"判命中：Host 抛异常时返回的失败信封
                # {"success": False, ...} 是非空 dict 但无 base64，误判会污染验证集、
                # 提前 return 错误信封并跳过随机兜底。
                if isinstance(emoji, dict) and emoji.get("base64"):
                    if kw not in self._validated_keywords:
                        self._validated_keywords.append(kw)
                    self._miss_counts.pop(kw, None)
                    return emoji
                ctx.logger.debug("按关键词 %s 没找到合适表情", kw)
                if kw in self._validated_keywords:
                    misses = self._miss_counts.get(kw, 0) + 1
                    if misses >= EMOJI_KEYWORD_MISS_THRESHOLD:
                        self._validated_keywords.remove(kw)
                        self._miss_counts.pop(kw, None)
                        ctx.logger.debug(
                            "[emoji] 关键词 %s 已连续 %d 次未命中，移出验证集",
                            kw, misses,
                        )
                    else:
                        self._miss_counts[kw] = misses
            except Exception:
                ctx.logger.debug("emoji.get_by_description 失败 (kw=%s)", kw, exc_info=True)

        if not cfg.allow_random_fallback:
            return None

        try:
            emojis = await ctx.emoji.get_random(1)
            if isinstance(emojis, list):
                for item in emojis:
                    if isinstance(item, dict) and item.get("base64"):
                        return item
            # get_random 正常返回 list；返回 dict 只可能是失败信封，用 base64 将其排除
            elif isinstance(emojis, dict) and emojis.get("base64"):
                return emojis
        except Exception:
            ctx.logger.debug("emoji.get_random 失败", exc_info=True)
        return None

    # ===== 内部 =====

    def _sample_probe_keywords(self, keywords: list[str]) -> list[str]:
        """采样关键词：已验证的优先，未验证的作为表情库新增的刷新机制混入。

        先 ``dict.fromkeys`` 去重：用户在配置中误填重复关键词时，否则同一关键词会
        被多次 RPC 探测，miss 时计数翻倍增长导致该词被过早移出验证集。
        """
        cleaned = list(dict.fromkeys(
            str(k).strip() for k in keywords if str(k).strip()
        ))
        if not cleaned:
            return []
        validated_set = set(self._validated_keywords)
        validated = [k for k in cleaned if k in validated_set]
        unvalidated = [k for k in cleaned if k not in validated_set]
        random.shuffle(validated)
        random.shuffle(unvalidated)
        return (validated + unvalidated)[:EMOJI_KEYWORD_PROBE_LIMIT]
