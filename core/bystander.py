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

        if ctx.is_group and ctx.group_id and target_id == ctx.target_id and not ctx.target_name:
            resolved = await plugin.resolve_member_name(ctx.group_id, target_id)
            if resolved:
                ctx.target_name = resolved

        ok = await plugin._napcat.send_poke(
            target_id, ctx.group_id, is_group=ctx.is_group, label="bystander"
        )
        if ok:
            target_label = ctx.target_name if (target_id == ctx.target_id and ctx.target_name) else target_id
            plugin.ctx.logger.info(
                "[smart_poke] 跟风戳完成: strategy=%s, target=%s (poker=%s, victim=%s)",
                cfg.target_strategy,
                target_label, ctx.poker_name or ctx.poker_id, ctx.target_name or ctx.target_id,
            )
            # target 是 victim 时用已解析的 target_name；是 poker 时用 poker_name；都不命中传空让上层走 resolve
            if target_id == ctx.target_id:
                injected_name = ctx.target_name
            elif target_id == ctx.poker_id:
                injected_name = ctx.poker_name
            else:
                injected_name = ""
            await plugin.record_self_poke_to_context(
                label="bystander",
                target_id=target_id,
                target_name=injected_name,
                group_id=ctx.group_id,
                is_group=ctx.is_group,
                stream_id=ctx.stream_id,
            )
