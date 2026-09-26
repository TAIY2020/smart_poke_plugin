"""跟风戳：别人之间互戳时，麦麦按概率跟着戳一下熟人。

被 ``SmartPokePlugin.handle_poke_event`` 在"分支一：戳的不是麦麦"路径上调用。
``maybe_trigger`` 是同步的——只做判定 + 派发后台任务，真正出手在 ``_react``。
"""

from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING

from .state import PokeContext


if TYPE_CHECKING:
    from ..plugin import SmartPokePlugin


class BystanderPoker:
    """互戳跟风戳。"""

    def __init__(self, plugin: "SmartPokePlugin") -> None:
        self._plugin = plugin

    def maybe_trigger(self, ctx: PokeContext) -> bool:
        """返回 ``True`` 表示已派发跟风戳任务，调用方据此决定是否吞事件。"""
        plugin = self._plugin
        cfg = plugin.config.bystander
        if not cfg.enabled:
            return False
        if not ctx.is_group:
            return False
        if not plugin.config.reaction.react_in_group:
            return False
        if ctx.poker_id == ctx.self_id or ctx.target_id == ctx.self_id:
            return False
        # 黑名单用户发起的互戳不跟风（否则 victim 策略下麦麦会帮他接着戳别人）
        if ctx.poker_id in plugin._blacklist:
            return False
        if not self._group_allowed(ctx.group_id):
            return False
        bystander_key = ctx.cooldown_key
        if plugin._state.in_bystander_cooldown(bystander_key, cfg.cooldown_seconds):
            return False
        if random.random() > cfg.probability:
            return False

        target_id = self._pick_target(ctx)
        if not target_id:
            return False

        plugin._state.mark_bystander(bystander_key)

        plugin._spawn_background_task(
            self._react(ctx, target_id),
            "bystander",
        )
        return True

    def _group_allowed(self, group_id: str) -> bool:
        """群级名单：黑名单优先；白名单非空时只放行名单内的群。"""
        plugin = self._plugin
        if group_id in plugin._bystander_blacklist_groups:
            return False
        return not plugin._bystander_whitelist_groups or group_id in plugin._bystander_whitelist_groups

    def _pick_target(self, ctx: PokeContext) -> str:
        """按 target_strategy 挑选跟风对象；命中黑名单则返回空串而不退化策略。"""
        plugin = self._plugin
        strategy = plugin.config.bystander.target_strategy
        if strategy == "victim":
            candidates = [ctx.target_id]
        elif strategy == "poker":
            candidates = [ctx.poker_id]
        else:
            candidates = [ctx.target_id, ctx.poker_id]
            random.shuffle(candidates)

        for candidate in candidates:
            if candidate and candidate not in plugin._blacklist:
                return candidate
        return ""

    async def _react(self, ctx: PokeContext, target_id: str) -> None:
        plugin = self._plugin
        cfg = plugin.config.bystander
        lo = max(0.0, cfg.min_delay_seconds)
        hi = max(lo, cfg.max_delay_seconds)
        delay = random.uniform(lo, hi) if hi > 0 else 0
        if delay > 0:
            await asyncio.sleep(delay)

        # 延迟期间配置可能已热更新：关闭跟风 / 群聊响应、群被移出名单、发起者或目标被拉黑，
        # 则放弃在途跟风戳（maybe_trigger 只在派发时检查过一次）。
        cfg = plugin.config.bystander
        if not cfg.enabled or not plugin.config.reaction.react_in_group:
            plugin.ctx.logger.debug("[bystander] 延迟期间跟风戳或群聊响应已关闭，放弃本次跟风")
            return
        if not self._group_allowed(ctx.group_id):
            plugin.ctx.logger.debug("[bystander] 延迟期间群 %s 已不在跟风名单内，放弃本次跟风", ctx.group_id)
            return
        if target_id in plugin._blacklist or ctx.poker_id in plugin._blacklist:
            plugin.ctx.logger.debug("[bystander] 延迟期间发起者或目标已被加入黑名单，放弃本次跟风")
            return

        # 跟风对象是被戳者时按需补解析其群名片（发起者的名字入站消息里已经带了）。
        target_name = ctx.target_name
        if ctx.is_group and ctx.group_id and target_id == ctx.target_id and not target_name:
            resolved = await plugin.resolve_member_name(ctx.group_id, target_id)
            if resolved:
                target_name = resolved

        ok = await plugin._napcat.send_poke(
            target_id, ctx.group_id, is_group=ctx.is_group, label="bystander"
        )
        if ok:
            target_label = target_name if (target_id == ctx.target_id and target_name) else target_id
            plugin.ctx.logger.info(
                "[smart_poke] 跟风戳完成: strategy=%s, target=%s (poker=%s, victim=%s)",
                cfg.target_strategy,
                target_label, ctx.poker_name or ctx.poker_id, target_name or ctx.target_id,
            )
            # target 是 victim 时用已解析的 target_name；是 poker 时用 poker_name；都不命中传空让上层走 resolve
            if target_id == ctx.target_id:
                injected_name = target_name
            elif target_id == ctx.poker_id:
                injected_name = ctx.poker_name
            else:
                injected_name = ""
            # 写入上下文需要该群会话在 Host 侧真实存在（冷群只被互戳过、从未有正常消息时并不存在）：
            # 与被戳反应同口径，带路由身份幂等确保会话后再写；未开启写入时不发这次 RPC。
            stream_id = ctx.stream_id
            if plugin.config.plugin.record_self_poke_to_context:
                stream_id = await plugin.resolve_stream_id_for_context(ctx, allow_open=True) or ctx.stream_id
            await plugin.record_self_poke_to_context(
                label="bystander",
                target_id=target_id,
                target_name=injected_name,
                group_id=ctx.group_id,
                is_group=ctx.is_group,
                stream_id=stream_id,
            )
