"""反应执行器：戳到麦麦后的反应主流程。

被 ``SmartPokePlugin.handle_poke_event`` 在 ``react_probability`` 命中后调用。
按权重抽样回戳 / 表情 / 文字三种反应；任意路径失败按既定级联回退避免 mark_reacted
了却什么都没发的尴尬。

同时持有 ``silent_reply``——``react_probability`` 未命中时按 ``silent_chat_probability``
偶尔挤一句沉默回复池。
"""

from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING

from .state import PokeContext


if TYPE_CHECKING:
    from .plugin import SmartPokePlugin


class ReactionExecutor:
    """戳到麦麦后的反应主流程。"""

    def __init__(self, plugin: "SmartPokePlugin") -> None:
        self._plugin = plugin

    # ===== 公开入口 =====

    async def silent_reply(self, ctx: PokeContext) -> None:
        """``react_probability`` 未命中时按 ``silent_chat_probability`` 概率挤一句。

        冷却在"确认能挤出一句话"后才消耗（解析 stream_id 成功且回复池非空），
        避免 stream_id 解析失败 / 池为空时白白冷却用户。
        """
        plugin = self._plugin
        try:
            stream_id = await plugin.resolve_stream_id_for_context(ctx)
            if not stream_id:
                plugin.ctx.logger.debug(
                    "[silent_reply] 无法解析 stream_id (group=%s, poker=%s)，静默放弃",
                    ctx.group_id, ctx.poker_id,
                )
                return
            pool = plugin.config.fallback.silent_replies
            if not pool:
                plugin.ctx.logger.debug("[silent_reply] silent 回复池为空，按沉默语义放弃发送")
                return
            # 二次确认滑动窗口：派发到 mark 之间可能被并发事件填满
            max_per_minute = plugin.config.reaction.max_reactions_per_minute
            if max_per_minute > 0 and plugin._state.peek_reaction_window(60) >= max_per_minute:
                plugin.ctx.logger.debug(
                    "[silent_reply] 派发到 mark 之间窗口被填满，静默放弃 (poker=%s)",
                    ctx.poker_id,
                )
                return
            plugin._state.mark_reacted(ctx.cooldown_key, ctx.poker_id)
            await self._delay_a_bit()
            await self._safe_send_text(random.choice(pool), stream_id)
        except Exception:
            plugin.ctx.logger.exception("silent_reply 发送失败")

    async def react_to_poke(self, ctx: PokeContext, is_spam: bool, poke_count: int) -> None:
        """戳到麦麦后的反应主流程：抽样 → 延迟 → 三档回复（poke/emoji/text）+ 级联回退。"""
        plugin = self._plugin
        try:
            # 先解析 stream_id 再延迟：避免选了 text/emoji 但 stream_id 解析失败时
            # 白白等掉几秒思考延迟才回退到回戳。回戳走 send_poke 不依赖 stream_id。
            stream_id = await plugin.resolve_stream_id_for_context(ctx)
            if not stream_id:
                plugin.ctx.logger.debug(
                    "[smart_poke] 无法解析 stream_id (group=%s, poker=%s)，回退到回戳路径",
                    ctx.group_id, ctx.poker_id,
                )

            kind = self._decide_reaction_kind(is_spam)
            if kind in ("emoji", "text") and not stream_id:
                kind = "poke"

            await self._delay_a_bit()

            plugin.ctx.logger.info(
                "[smart_poke] 触发反应: kind=%s, is_spam=%s, poke_count=%d, poker=%s, scene=%s",
                kind,
                is_spam,
                poke_count,
                ctx.poker_name or ctx.poker_id,
                "群聊" if ctx.is_group else "私聊",
            )

            if kind == "poke":
                ok = await self._send_back_poke(ctx, is_spam=is_spam)
                if not ok:
                    if stream_id:
                        await self._send_text(stream_id, is_spam)
                    else:
                        plugin.ctx.logger.debug(
                            "[smart_poke] 回戳失败且无 stream_id 可回退，本次反应放弃 (poker=%s)",
                            ctx.poker_id,
                        )
                return

            if kind == "emoji":
                ok = await self._send_emoji(stream_id)
                if ok:
                    return
                plugin.ctx.logger.debug("表情反应失败，回退到回戳")
                ok_poke = await self._send_back_poke(ctx, is_spam=is_spam)
                if not ok_poke:
                    await self._send_text(stream_id, is_spam)
                return

            sent = await self._send_text(stream_id, is_spam)
            if sent:
                return
            # 兜底回戳，避免 mark 了却什么都没发
            plugin.ctx.logger.debug(
                "[smart_poke] 文字反应未发出，回退到回戳 (poker=%s)", ctx.poker_id,
            )
            await self._send_back_poke(ctx, is_spam=is_spam)
        except Exception:
            plugin.ctx.logger.exception("反应戳一戳时出错")

    # ===== 内部：延迟与抽样 =====

    async def _delay_a_bit(self) -> None:
        cfg = self._plugin.config.reaction
        lo = max(0.0, cfg.min_delay_seconds)
        hi = max(lo, cfg.max_delay_seconds)
        delay = random.uniform(lo, hi) if hi > 0 else 0
        if delay > 0:
            await asyncio.sleep(delay)

    def _decide_reaction_kind(self, is_spam: bool) -> str:
        """按权重抽取反应类型；暴戳态下回戳 ×1.5、表情 ×0.3、文字 ×1.2。

        「真正沉默」由 handle_poke_event 的 react_probability 未命中分支负责，
        与本方法的反应类型抽样无关。
        """
        cfg = self._plugin.config.reaction
        spam_mult_poke = 1.5 if is_spam else 1.0
        spam_mult_emoji = 0.3 if is_spam else 1.0
        spam_mult_text = 1.2 if is_spam else 1.0
        weights = {
            "poke": max(0.0, cfg.back_poke_weight * spam_mult_poke),
            "emoji": max(0.0, cfg.emoji_weight * spam_mult_emoji),
            "text": max(0.0, cfg.text_weight * spam_mult_text),
        }
        total = sum(weights.values())
        if total <= 0:
            return "text"

        roll = random.random() * total
        cumulative = 0.0
        for kind, weight in weights.items():
            cumulative += weight
            if roll < cumulative:
                return kind
        return "text"

    # ===== 内部：发送实现 =====

    async def _send_back_poke(self, ctx: PokeContext, is_spam: bool = False) -> bool:
        """通过 NapCat 适配器发送回戳，支持多次连续戳。

        防自戳死循环：ignore_self_poke=False 时若让麦麦回戳自己，新的 send_poke
        会再触发一条 notify.poke 被本插件接收，导致无限循环。

        每两次之间 0.3~0.8s 随机短延迟避免请求扎堆触发风控；
        任一中途失败立即停止（可能已被风控）。
        """
        plugin = self._plugin
        if ctx.poker_id == ctx.self_id:
            plugin.ctx.logger.warning(
                "拒绝回戳麦麦自己（self_id=%s）以避免戳一戳事件死循环", ctx.self_id,
            )
            return False

        max_times = max(1, plugin.config.reaction.back_poke_max_times)
        times = max_times if is_spam else random.randint(1, max_times)

        any_success = False
        for i in range(times):
            ok = await plugin._napcat.send_poke(
                ctx.poker_id, ctx.group_id, is_group=ctx.is_group, label="back_poke"
            )
            if ok:
                any_success = True
            else:
                break
            if i < times - 1:
                await asyncio.sleep(random.uniform(0.3, 0.8))

        if times > 1 and any_success:
            plugin.ctx.logger.debug(
                "[back_poke] 连戳 %d 次 (is_spam=%s, max=%d)",
                times, is_spam, max_times,
            )
        return any_success

    async def _send_text(self, stream_id: str, is_spam: bool) -> bool:
        """从配置的回复池中随机挑一句发出。

        暴戳态不回落到 silent_replies——"..."与"被烦了"语气冲突；前两档全空时
        让 react_to_poke 走兜底回戳更合适。
        """
        cfg = self._plugin.config.fallback
        if is_spam:
            primary = cfg.spam_replies
            secondary = cfg.normal_replies
            tertiary: list[str] = []
        else:
            primary = cfg.normal_replies
            secondary: list[str] = []
            tertiary = cfg.silent_replies

        for pool, label in ((primary, "primary"), (secondary, "secondary"), (tertiary, "silent")):
            if pool:
                ok = await self._safe_send_text(random.choice(pool), stream_id)
                if ok:
                    return True
                self._plugin.ctx.logger.debug("[_send_text] %s 池发送失败，尝试下一档", label)
        self._plugin.ctx.logger.debug(
            "[_send_text] 所有回复池均为空或发送失败 (is_spam=%s)", is_spam,
        )
        return False

    async def _safe_send_text(self, text: str, stream_id: str) -> bool:
        try:
            await self._plugin.ctx.send.text(text, stream_id)
            return True
        except Exception:
            self._plugin.ctx.logger.exception("发送文字回复失败")
            return False

    async def _send_emoji(self, stream_id: str) -> bool:
        """发送一张表情包。Host 序列化表情时固定输出 ``base64`` 字段。"""
        plugin = self._plugin
        try:
            emoji = await plugin._emoji.pick_emoji()
            if not isinstance(emoji, dict):
                return False

            emoji_data = emoji.get("base64")
            if not isinstance(emoji_data, str) or not emoji_data:
                return False

            await plugin.ctx.send.emoji(emoji_data, stream_id)
            return True
        except Exception:
            plugin.ctx.logger.exception("发送表情失败")
            return False
