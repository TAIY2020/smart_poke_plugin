"""主动戳：群里有人说话时按拟人化概率被勾起来戳一下熟人。

``SmartPokePlugin.handle_poke_event`` 对每条非戳一戳的入站消息调用 ``observe_signal``：
它是同步的，只做廉价快检 + 派发后台任务，重活在 ``_maybe_poke`` 完成。

并发控制不用锁：入口的冷却 / 上限检查只是乐观快检，``get_recent`` 等 await 之后再同步复检并
预占 in-flight（复检与预占之间没有 await，在单事件循环里天然原子）。in-flight 期间任何群的
新任务都会被挡下；send_poke 成功后转正为冷却与每日额度，失败则留一小段全局退避。
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import TYPE_CHECKING, Any

from .common import in_active_hours, to_positive_int


if TYPE_CHECKING:
    from ..plugin import SmartPokePlugin


class ProactivePoker:
    """主动戳的观察 + 判定 + 出手。"""

    def __init__(self, plugin: "SmartPokePlugin") -> None:
        self._plugin = plugin

    # ===== 公开入口 =====

    def observe_signal(self, message: Any) -> None:
        """每条入站群消息都被"考虑"一次（通知类消息由 ``_extract_signal`` 排除）。"""
        plugin = self._plugin
        cfg = plugin.config.proactive
        if not cfg.enabled:
            return
        info = self._extract_signal(message)
        if info is None:
            return
        group_id, speaker_id, stream_id, trigger = info
        # 群级名单、概率骰子、活跃时段都不依赖历史消息，放在派发前：注定出不了手的消息直接
        # return，不必 spawn 空任务白占 PROACTIVE_TASK_QUEUE_LIMIT 槽位。黑名单优先；白名单
        # 非空时只放行名单内的群；localtime 有开销，放在概率命中之后。
        if group_id in plugin._proactive_blacklist_groups:
            return
        if plugin._proactive_whitelist_groups and group_id not in plugin._proactive_whitelist_groups:
            return
        if cfg.probability <= 0 or random.random() > cfg.probability:
            return
        if not in_active_hours(cfg.active_hour_start, cfg.active_hour_end, time.localtime().tm_hour):
            return
        plugin._spawn_background_task(
            self._maybe_poke(group_id, speaker_id, stream_id, trigger), "proactive"
        )

    # ===== 候选信号提取 =====

    def _extract_signal(self, message: Any) -> tuple[str, str, str, dict[str, Any]] | None:
        """快速排除：仅做廉价过滤，重活留给 ``_maybe_poke``。

        返回 ``(group_id, speaker_id, stream_id, trigger)``；``stream_id`` 取消息自带的 session_id
        （Host 派发 Hook 前已按路由身份算好回填），后续直接用它拉历史 / 写上下文，
        省掉一次反查 RPC，多账号同群时也不会串到别的账号的流。

        ``trigger`` 是这条消息按 ``message.get_recent`` 同构格式精简出的记录（只留
        message_id / timestamp / 发言人），交给 ``_maybe_poke`` 补进拉到的历史：Host 要到 Hook
        之后才把入站消息写库，后台任务拉历史时多半还查不到它。只拷这几个字段，免得把
        带图片 base64 的整条消息挂在后台任务上。

        顺便从适配器写入的 ``additional_config.self_id`` 学习当前 bot 账号——
        普通消息也会带，比等 notify.poke 提前得多。
        """
        if not isinstance(message, dict):
            return None
        if message.get("is_notify"):
            return None

        msg_info = message.get("message_info") or {}
        if not isinstance(msg_info, dict):
            return None

        additional = msg_info.get("additional_config") or {}
        if isinstance(additional, dict):
            learned_self_id = str(additional.get("self_id") or "").strip()
            self._plugin._state.set_known_self_id(learned_self_id)

        group_info = msg_info.get("group_info") or {}
        if not isinstance(group_info, dict):
            return None
        group_id_raw = group_info.get("group_id")
        group_int = to_positive_int(group_id_raw)
        if group_int is None:
            return None
        group_id = str(group_int)

        user_info = msg_info.get("user_info") or {}
        if not isinstance(user_info, dict):
            return None
        speaker_id = str(user_info.get("user_id") or "").strip()
        if not speaker_id:
            return None

        known_self_id = self._plugin._state.get_known_self_id()
        if known_self_id and speaker_id == known_self_id:
            return None

        stream_id = str(message.get("session_id") or "").strip()
        trigger = {
            "message_id": str(message.get("message_id") or "").strip(),
            "timestamp": message.get("timestamp"),
            "message_info": {
                "user_info": {
                    "user_id": speaker_id,
                    "user_cardname": user_info.get("user_cardname"),
                    "user_nickname": user_info.get("user_nickname"),
                },
            },
        }
        return group_id, speaker_id, stream_id, trigger

    # ===== 主流程 =====

    def _quota_available(self, group_id: str) -> bool:
        """全局冷却（含 in-flight 预占与失败退避）、同群冷却、每日上限是否都允许出手。"""
        cfg = self._plugin.config.proactive
        state = self._plugin._state
        if state.in_proactive_global_cooldown(cfg.global_cooldown_seconds):
            return False
        if state.in_proactive_chat_cooldown(group_id, cfg.per_chat_cooldown_seconds):
            return False
        return not (cfg.max_pokes_per_day > 0 and state.proactive_daily_count() >= cfg.max_pokes_per_day)

    def _still_allowed(self, group_id: str, target_id: str) -> bool:
        """思考延迟后、出手前复查最新配置：主动戳被关闭、群被拉黑 / 移出白名单、目标被拉黑则放弃。

        派发前的名单 / 开关检查只反映派发那一刻的配置；on_config_update 刷新集合与配置实例后，
        这里读到的是最新值。命中时打 debug 并返回 ``False``，调用方走 in-flight 释放。
        """
        plugin = self._plugin
        cfg = plugin.config.proactive
        if not cfg.enabled:
            reason = "主动戳已关闭"
        elif group_id in plugin._proactive_blacklist_groups:
            reason = "群已加入黑名单"
        elif plugin._proactive_whitelist_groups and group_id not in plugin._proactive_whitelist_groups:
            reason = "群已不在白名单"
        elif target_id in plugin._blacklist:
            reason = "目标用户已加入黑名单"
        else:
            return True
        plugin.ctx.logger.debug(
            "[proactive] 出手前复查未通过（%s），放弃本次主动戳 (group=%s, target=%s)",
            reason, group_id, target_id,
        )
        return False

    async def _maybe_poke(
        self,
        group_id: str,
        speaker_id: str,
        stream_id: str = "",
        trigger: dict[str, Any] | None = None,
    ) -> None:
        """主动戳的完整判定与执行流程（并发控制见模块说明）。"""
        plugin = self._plugin
        cfg = plugin.config.proactive
        state = plugin._state

        if not self._quota_available(group_id):
            return

        # 优先用触发消息自带的 session_id；为空（异常适配器）才只读反查兜底
        if not stream_id:
            stream_id = await plugin.resolve_stream_id_for_group(group_id)
        if not stream_id:
            plugin.ctx.logger.debug(
                "[proactive] 群 %s 无法解析 stream_id，本次跳过", group_id,
            )
            return

        try:
            recent = await plugin.ctx.message.get_recent(
                stream_id, limit=cfg.recent_fetch_limit
            )
        except Exception:
            plugin.ctx.logger.debug(
                "[proactive] message.get_recent 失败 (group=%s)", group_id, exc_info=True
            )
            return
        if not isinstance(recent, list):
            return
        if trigger:
            recent = self._with_trigger(recent, trigger)
        if not recent:
            return

        target_id, target_name, active_count = self._pick_target(
            recent, group_id, speaker_id,
        )
        if active_count < cfg.min_recent_messages or not target_id:
            return
        # 兜底防自戳：_pick_target 已用 known_self_id 过滤 bot 自己的消息，但 self_id 学到之前的
        # 极端窗口里 bot 自己仍可能成为候选；戳到自己只会触发回声，白白浪费一次配额。
        known_self_id = state.get_known_self_id()
        if known_self_id and target_id == known_self_id:
            plugin.ctx.logger.debug(
                "[proactive] 目标解析为 bot 自身 (self_id=%s)，跳过本次主动戳",
                known_self_id,
            )
            return

        # 上面的 await 期间别的任务可能已经出手：复检通过后立即预占 in-flight，两步之间不能有 await。
        if not self._quota_available(group_id):
            return
        # 只预占 in-flight，每日额度与群/全局长冷却等 send_poke 成功后再由 commit_proactive 计入，
        # 避免风控/超时/取消时"没戳出去却扣了配额"。TTL 按思考延迟的实际上界算（误配 min > max 时
        # 实际延迟可达 min），令牌供 commit/abort 校验，防止误清后来者的 in-flight。
        inflight_token = state.begin_proactive_inflight(
            max(cfg.min_delay_seconds, cfg.max_delay_seconds)
        )

        committed = False
        try:
            lo = max(0.0, cfg.min_delay_seconds)
            hi = max(lo, cfg.max_delay_seconds)
            delay = random.uniform(lo, hi) if hi > 0 else 0
            if delay > 0:
                await asyncio.sleep(delay)

            # 延迟期间配置可能已热更新：出手前复查开关 / 群名单 / 目标黑名单，
            # 未通过则放弃（finally 释放 in-flight，不消耗每日额度与长冷却）
            if not self._still_allowed(group_id, target_id):
                return

            if not target_name:
                resolved = await plugin.resolve_member_name(group_id, target_id)
                if resolved:
                    target_name = resolved

            ok = await plugin._napcat.send_poke(
                target_id, group_id, is_group=True, label="proactive",
            )
            if ok:
                # 发送成功，正式占用每日额度与群/全局长冷却（令牌匹配时顺带清 in-flight）
                state.commit_proactive(group_id, inflight_token)
                committed = True
                plugin.ctx.logger.info(
                    "[smart_poke] 主动戳完成: strategy=%s, group=%s, target=%s",
                    cfg.target_strategy, group_id, target_name or target_id,
                )
                await plugin.record_self_poke_to_context(
                    label="proactive",
                    target_id=target_id,
                    target_name=target_name,
                    group_id=group_id,
                    is_group=True,
                    stream_id=stream_id,
                )
        finally:
            if not committed:
                # 失败/异常/取消：释放 in-flight 并留一小段全局失败退避，不消耗每日额度
                state.abort_proactive_inflight(inflight_token)

    # ===== 目标挑选 =====

    @staticmethod
    def _with_trigger(recent: list[Any], trigger: dict[str, Any]) -> list[Any]:
        """把触发消息补进 ``get_recent`` 的结果；按 message_id 判定已在库里就不重复加。

        Host 要到 before_process Hook 之后才把入站消息写库，后台任务拉历史时触发消息多半
        还查不到：不补的话 active_speaker 挑不中刚说话的人（会退到窗口里别的说话者），
        群活跃计数也少算这一条。message_id 为空时无从判重，照样补上。
        """
        trigger_id = trigger.get("message_id")
        if trigger_id and any(
            isinstance(msg, dict) and str(msg.get("message_id") or "").strip() == trigger_id
            for msg in recent
        ):
            return recent
        return [*recent, trigger]

    def _pick_target(
        self,
        recent: list[Any],
        group_id: str,
        speaker_id: str,
    ) -> tuple[str, str, int]:
        """挑选候选戳目标，返回 (target_id, target_name, active_count)。

        active_count 是 ``recent_window_seconds`` 内的非麦麦、非通知消息条数，
        供调用方判断群活跃度。target_id 为空串表示没有合适候选。
        """
        plugin = self._plugin
        cfg = plugin.config.proactive
        now = time.time()
        lookback_cutoff = now - cfg.lookback_seconds
        active_window_cutoff = now - cfg.recent_window_seconds
        self_id = plugin._state.get_known_self_id()

        active_count = 0
        # 每个 uid 只保留其"最新一条消息"的 (ts, uname)，靠 ts 比较实现，
        # 不依赖 message.get_recent 的返回顺序（正序 / 倒序都得到同一结果）。
        candidates: dict[str, tuple[float, str]] = {}

        for msg in recent:
            if not isinstance(msg, dict):
                continue
            if msg.get("is_notify"):
                continue
            try:
                ts = float(msg.get("timestamp") or 0)
            except (TypeError, ValueError):
                continue
            if ts <= 0:
                continue

            msg_info = msg.get("message_info") or {}
            if not isinstance(msg_info, dict):
                continue
            user_info = msg_info.get("user_info") or {}
            if not isinstance(user_info, dict):
                continue
            uid = str(user_info.get("user_id") or "").strip()
            if not uid:
                continue
            if self_id and uid == self_id:
                continue

            if ts >= active_window_cutoff:
                active_count += 1
            if ts < lookback_cutoff:
                continue

            if uid in plugin._blacklist:
                continue
            if cfg.respect_spam_history and self._poked_bot_recently(group_id, uid):
                continue

            uname = str(user_info.get("user_cardname") or user_info.get("user_nickname") or "").strip()
            existing = candidates.get(uid)
            if existing is None or ts > existing[0]:
                candidates[uid] = (ts, uname)

        if not candidates:
            return "", "", active_count

        if cfg.target_strategy == "active_speaker":
            # 优先戳"勾起这次观察的说话人"；它被前面的过滤剔除（如最近戳过麦麦）时，
            # 退到候选里时间戳最新的那个，让 active_speaker 在边界场景也成立。
            if speaker_id in candidates:
                _ts, uname = candidates[speaker_id]
                return speaker_id, uname, active_count
            uid, (_ts, uname) = max(candidates.items(), key=lambda kv: kv[1][0])
            return uid, uname, active_count

        uid = random.choice(list(candidates.keys()))
        _ts, uname = candidates[uid]
        return uid, uname, active_count

    def _poked_bot_recently(self, group_id: str, user_id: str) -> bool:
        """窗口取自 ``proactive.respect_spam_window_seconds``（reaction.spam_window_seconds 太短）。"""
        return self._plugin._state.poked_bot_recently(
            group_id, user_id, self._plugin.config.proactive.respect_spam_window_seconds
        )
