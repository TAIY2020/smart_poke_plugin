"""主动戳：群里有人说话时按拟人化概率被勾起来戳一下熟人。

被 ``SmartPokePlugin.observe_message_for_proactive`` 在每条入站群消息上调用。
``observe_signal`` 同步：仅做廉价快检 + 派发后台任务；重活在 ``_maybe_poke`` 完成。

双层锁：

* per-group lock（``state.get_proactive_lock``）防同群双发；
* global lock 保护"全局冷却二次确认 + mark"临界区——不同群的并发任务可能在各自
  per-group lock 内同时穿过全局乐观快检。

RPC 与延迟都在 per-group lock 内或锁外，避免全局串行所有群的网络往返。
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
        """每条群消息都被"考虑"一次。

        主 BLOCKING handler 对戳一戳事件 ``abort`` 时 dispatcher 会先 ``break``，
        所以戳一戳通知不会触发主动戳，避免事件回声。
        """
        plugin = self._plugin
        cfg = plugin.config.proactive
        if not cfg.enabled:
            return
        info = self._extract_signal(message)
        if info is None:
            return
        # 主动戳观察是高频路径：顺带按时间节流触发一次状态清理（O(1) 检查，实际 _prune
        # 最快每 _PRUNE_MIN_INTERVAL_SECONDS 一次），避免"只主动戳、很少有人戳麦麦"的
        # 环境下 _prune 长期不跑、_proactive_locks 等随群数累积。
        plugin._state.maybe_prune()
        group_id, speaker_id = info
        # 群级名单只依赖 group_id（O(1) set 查找），前移到派发前：名单外的群直接 return，
        # 不必 spawn 一个进 _maybe_poke 立刻退出的空任务、白占 PROACTIVE_TASK_QUEUE_LIMIT 槽位。
        # 黑名单优先；白名单非空时只放行名单内的群。
        if group_id in plugin._proactive_blacklist_groups:
            return
        if plugin._proactive_whitelist_groups and group_id not in plugin._proactive_whitelist_groups:
            return
        # 概率骰子提前到派发前：未中签的群消息直接 return，避免占用队列槽位空跑 _maybe_poke。
        if cfg.probability <= 0:
            return
        if random.random() > cfg.probability:
            return
        # 活跃时段判定带 localtime 开销，放在概率命中之后、仅对入选消息计算（同样前移以免空跑任务）。
        now_struct = time.localtime()
        if not in_active_hours(cfg.active_hour_start, cfg.active_hour_end, now_struct.tm_hour):
            return
        plugin._spawn_background_task(
            self._maybe_poke(group_id, speaker_id), "proactive"
        )

    # ===== 候选信号提取 =====

    def _extract_signal(self, message: Any) -> tuple[str, str] | None:
        """快速排除：仅做廉价过滤，重活留给 ``_maybe_poke``。

        顺便从 napcat codec 注入的 ``additional_config.self_id`` 学习当前 bot 账号——
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

        return group_id, speaker_id

    # ===== 主流程 =====

    async def _maybe_poke(self, group_id: str, speaker_id: str) -> None:
        """主动戳的完整判定与执行流程。

        双层锁：per-group lock 防同群双发，global lock 保护"全局冷却二次确认 + mark"
        临界区——不同群的并发任务可能在各自 per-group lock 内同时穿过全局乐观快检。
        RPC 与延迟都在 per-group lock 内或锁外，避免全局串行所有群的网络往返。
        """
        plugin = self._plugin
        cfg = plugin.config.proactive

        # 概率骰子、群黑/白名单、活跃时段判定都已在 observe_signal 派发前完成
        # （三者均只依赖 group_id，前移可避免为注定 return 的触发 spawn 出空任务），这里不再重复。

        target_id: str = ""
        target_name: str = ""
        # begin_proactive_inflight 发的令牌；仅在锁内成功 begin 后才会走到锁外 try，
        # 届时必为有效值(>0)，此处初始化只为静态可读性与防御。
        inflight_token: int = 0

        async with plugin._state.get_proactive_lock(group_id):
            # 全局冷却是乐观快检，原子性靠后续 _proactive_global_lock 内的二次确认保障
            if plugin._state.in_proactive_global_cooldown(cfg.global_cooldown_seconds):
                return
            if plugin._state.in_proactive_chat_cooldown(group_id, cfg.per_chat_cooldown_seconds):
                return
            if cfg.max_pokes_per_day > 0:
                already = plugin._state.proactive_daily_count()
                if already >= cfg.max_pokes_per_day:
                    return

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
            if not isinstance(recent, list) or not recent:
                return

            target_id, target_name, active_count = self._pick_target(
                recent, group_id, speaker_id,
            )
            if active_count < cfg.min_recent_messages:
                return
            if not target_id:
                return
            # 兜底防自戳：_pick_target 已用 known_self_id 过滤 bot 自己的消息，但
            # self_id 学到之前的极端窗口里 bot 自己仍可能成为候选；这里在消耗
            # 冷却/日上限（mark_proactive）之前再挡一道，避免戳到自己后触发
            # ignore_self_poke 回声、白白浪费一次主动戳配额。
            known_self_id = plugin._state.get_known_self_id()
            if known_self_id and target_id == known_self_id:
                plugin.ctx.logger.debug(
                    "[proactive] 目标解析为 bot 自身 (self_id=%s)，跳过本次主动戳",
                    known_self_id,
                )
                return

            async with plugin._proactive_global_lock:
                if plugin._state.in_proactive_global_cooldown(cfg.global_cooldown_seconds):
                    return
                if cfg.max_pokes_per_day > 0:
                    already = plugin._state.proactive_daily_count()
                    if already >= cfg.max_pokes_per_day:
                        return
                # 锁内只预占 in-flight 防并发双发；每日额度与群/全局长冷却推迟到
                # send_poke 成功后再由 commit_proactive 计入，避免风控/超时/取消时
                # "没戳出去却扣了配额"。in-flight 期间其他群的并发任务会在此退避。
                # 传思考延迟上限做动态 TTL，并接住令牌供 commit/abort 防误清。
                inflight_token = plugin._state.begin_proactive_inflight(cfg.max_delay_seconds)

        # ----- 锁外：思考延迟 + 出手 -----
        committed = False
        try:
            lo = max(0.0, cfg.min_delay_seconds)
            hi = max(lo, cfg.max_delay_seconds)
            delay = random.uniform(lo, hi) if hi > 0 else 0
            if delay > 0:
                await asyncio.sleep(delay)

            if not target_name:
                resolved = await plugin.resolve_member_name(group_id, target_id)
                if resolved:
                    target_name = resolved

            ok = await plugin._napcat.send_poke(
                target_id, group_id, is_group=True, label="proactive",
            )
            if ok:
                # 发送成功，正式占用每日额度与群/全局长冷却（令牌匹配时顺带清 in-flight）
                plugin._state.commit_proactive(group_id, inflight_token)
                committed = True
                plugin.ctx.logger.info(
                    "[smart_poke] 主动戳完成: strategy=%s, group=%s, target=%s",
                    cfg.target_strategy, group_id, target_name or target_id,
                )
                # 复用 _maybe_poke 里已 resolve 的 stream_id，省一次缓存查
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
                plugin._state.abort_proactive_inflight(inflight_token)

    # ===== 目标挑选 =====

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
