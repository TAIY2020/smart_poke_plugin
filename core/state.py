"""Smart Poke 插件的状态层。

包含：
    * ``PokeContext`` —— 从一次戳事件解析出的关键字段。
    * ``PokeStateManager`` —— 冷却、暴戳计数、各种 TTL 缓存、主动戳每日上限/锁的集中持有者。

运行时仅持有内存态；冷却/每日上限等限频字段经 export_persistable /
import_persistable 由插件层在卸载/加载时落盘与恢复，其余缓存热重载丢失（接受）。
所有方法都是同步的；与事件循环交互的部分由
``ProactivePoker`` / ``ReactionExecutor`` 等模块自行处理。
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque

from .common import format_local_date


# ----- 状态层相关常量 -----

MEMBER_NAME_CACHE_TTL_SECONDS = 600.0
MEMBER_NAME_CACHE_MAX_SIZE = 256
MEMBER_NAME_NEGATIVE_CACHE_TTL_SECONDS = 60.0

# notice 消息 session_id 固定为空，stream_id 每次反应都得回查，缓存收益明显。
STREAM_ID_CACHE_TTL_SECONDS = 1800.0
# 与 _name_cache 对齐：给 stream_id 缓存一个即时容量上限，避免接触大量群/私聊时
# 在两次 _prune 之间无界增长（_prune 仅每 _PRUNE_THRESHOLD 次戳才触发一次）。
STREAM_ID_CACHE_MAX_SIZE = 256


# ----- PokeContext -----


class PokeContext:
    """从一次戳一戳事件中提取出的关键信息。"""

    __slots__ = (
        "is_group",
        "self_id",
        "poker_id",
        "poker_name",
        "poke_action",
        "target_id",
        "target_name",
        "group_id",
        "stream_id",
        "cooldown_key",
        "spam_scope_key",
    )

    def __init__(self) -> None:
        self.is_group: bool = False
        self.self_id: str = ""
        self.poker_id: str = ""
        self.poker_name: str = ""
        # QQ 自定义戳一戳动作文本（如"拍了拍"/"捏了捏"）；取不到则空，使用处兜底"戳了戳"
        self.poke_action: str = ""
        self.target_id: str = ""
        self.target_name: str = ""
        self.group_id: str = ""
        self.stream_id: str = ""
        # 冷却维度：群聊用 group_id、私聊用 poker_id（稳定，不混 stream_id 以免漂移分裂）
        self.cooldown_key: str = ""
        # 与 cooldown_key 解耦：proactive 分支查 _poked_bot_recently 只能传 group_id，
        # 必须与 record 端用同一个 scope 才能匹配
        self.spam_scope_key: str = ""

    @property
    def is_poking_bot(self) -> bool:
        return bool(self.self_id) and self.target_id == self.self_id


# ----- PokeStateManager -----


class PokeStateManager:
    """冷却时间戳、暴戳计数、各种 TTL 缓存的集中持有者。"""

    _PRUNE_THRESHOLD = 200
    # 通用 stale 阈值：超过此时长未更新的状态 key 会被 _prune 回收。同时它约束了
    # config.proactive.respect_spam_window_seconds 的有效上限——_poke_records 中更早的
    # 「戳过麦麦」记录会被本阈值回收、不再能被 poked_bot_recently 命中，故 config 侧
    # RESPECT_SPAM_WINDOW_MAX_SECONDS 与此对齐；若调整本值，记得同步 config 侧上限。
    _STALE_AFTER_SECONDS = 3600
    # 主动戳观察等"高频但不戳麦麦"的路径按此最小间隔节流触发 _prune，避免该环境下
    # _prune（原仅靠被动戳计数触发）长期不跑、_proactive_locks/_last_*_at 随群数累积。
    _PRUNE_MIN_INTERVAL_SECONDS = 300.0
    # 单条暴戳计数 deque 的硬上限——暴戳判定只关心 >= spam_threshold，多余的旧记录
    # 会被天然挤出而不影响判定结果。
    _POKE_RECORD_MAXLEN = 200
    # 主动戳 in-flight 预占的基础时长下限：覆盖"锁外思考延迟 + 昵称解析 + send_poke RPC"；
    # 实际 TTL 取 max(本下限, 思考延迟上限 + RPC 余量)，避免 max_delay_seconds 调大后
    # in-flight 在思考延迟期内提前过期、让其他群穿过全局冷却/每日上限（见 begin_proactive_inflight）。
    _PROACTIVE_INFLIGHT_TTL_SECONDS = 30.0
    # send_poke RPC + 安全余量：与思考延迟上限相加，作为动态 in-flight TTL 的候选。
    _PROACTIVE_INFLIGHT_RPC_MARGIN_SECONDS = 20.0
    # send_poke 失败后的短全局退避：失败不消耗每日额度/长冷却，但留一小段冷却，
    # 避免风控期多个群连环重试刷屏。
    _PROACTIVE_FAILURE_BACKOFF_SECONDS = 15.0

    def __init__(self) -> None:
        self._last_react_at: dict[str, float] = {}
        self._poke_records: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=PokeStateManager._POKE_RECORD_MAXLEN)
        )
        self._last_bystander_at: dict[str, float] = {}
        self._record_counter: int = 0
        # _prune 上次执行时间，供 maybe_prune 做时间节流；被动戳计数触发 _prune 时也会更新它。
        self._last_prune_at: float = 0.0
        # 反应频率滑动窗口，按会话维度分桶（群聊=group_id、私聊=poker_id，与 cooldown_key
        # 同口径）：与逐人冷却互补，防同一会话内多人轮番车轮战。分桶而非全局，避免某群被
        # 刷屏时填满唯一的全局窗口、误伤其他群的正常用户。跟风戳/主动戳有各自的冷却+日上限，
        # 不占用此窗口。
        self._reaction_window: dict[str, deque[float]] = defaultdict(deque)
        # name=""为负缓存条目（已知该用户没有可解析昵称），TTL 配更短。
        self._name_cache: dict[str, tuple[str, float]] = {}
        self._stream_id_cache: dict[str, tuple[str, float]] = {}
        self._last_proactive_at_chat: dict[str, float] = {}
        self._last_proactive_global_at: float = 0.0
        # 主动戳 in-flight 预占截止时间：begin 设、commit/abort 仅在令牌匹配时清；
        # in_proactive_global_cooldown 据此把"进行中/失败退避中"也视为全局占用。
        self._proactive_inflight_until: float = 0.0
        # in-flight 令牌：每次 begin 自增发牌，commit/abort 校验自己持有的牌是否仍是
        # 当前牌——避免"旧任务超时后新任务已 begin，旧任务收尾却清掉新任务 in-flight"的误清。
        self._proactive_inflight_token: int = 0
        self._proactive_inflight_seq: int = 0
        self._proactive_daily_count: int = 0
        self._proactive_daily_date: str = ""
        # per-group 主动戳锁。放在这里与 _last_proactive_at_chat 同源 prune，
        # 避免群数量上涨时锁字典无界增长。
        self._proactive_locks: dict[str, asyncio.Lock] = {}
        # OBSERVE 阶段拿不到 self_id，从 napcat additional_config / payload 学到后缓存于此；
        # 用于过滤"自己说话触发主动戳"等边界场景。拿不到也不致命。
        self._known_self_id: str = ""

    # ----- self_id -----

    def set_known_self_id(self, self_id: str) -> None:
        """从消息或 notice payload 学到 bot 自身 self_id 时调用。"""
        if self_id and self_id != self._known_self_id:
            self._known_self_id = self_id

    def get_known_self_id(self) -> str:
        return self._known_self_id

    # ----- proactive 锁 -----

    def get_proactive_lock(self, group_id: str) -> asyncio.Lock:
        """获取或创建 per-group 主动戳锁。

        单一事件循环下 dict 写无 await 切点，操作原子，不需要注册锁。

        **关键不变量（调用约定）**：必须用 ``async with state.get_proactive_lock(g):``
        同表达式立即 acquire，不要拆成两步 ``lock = state.get_proactive_lock(g); ...;
        async with lock:``。三条同步性共同保证 prune 不会删走"已被某 task 拿到引用但
        尚未进入 async with"的 lock 导致同群双发：

            1. 本方法是同步的，调用方求值后无切点
            2. asyncio.Lock.__aenter__ 在未被持有时走快路径，立即设 _locked=True，无 yield
            3. _prune 显式 ``lock.locked()`` 跳过持有中的锁

        拆成两步式写法会在 lock= 与 async with 之间插入潜在 await 切点，让步骤 1 失效。
        """
        lock = self._proactive_locks.get(group_id)
        if lock is None:
            lock = asyncio.Lock()
            self._proactive_locks[group_id] = lock
        return lock

    # ----- 反应冷却 -----

    @staticmethod
    def _cooldown_key(scope_key: str, poker_id: str) -> str:
        if not scope_key:
            return ""
        return f"{scope_key}:{poker_id}" if poker_id else scope_key

    def in_cooldown(self, scope_key: str, poker_id: str, cooldown_seconds: int) -> bool:
        if cooldown_seconds <= 0 or not scope_key:
            return False
        key = self._cooldown_key(scope_key, poker_id)
        last = self._last_react_at.get(key, 0.0)
        return (time.time() - last) < cooldown_seconds

    def mark_reacted(self, scope_key: str, poker_id: str) -> float:
        """标记一次反应，返回写入的时间戳——该时间戳同时充当这次反应的"令牌"。

        反应的真正发送被随机思考延迟推迟，而主链 mark 后立即占冷却。当
        max_delay_seconds > cooldown_seconds 时，同一人在延迟窗口内再次戳会穿过逐人
        冷却、再派发一个反应任务。后台任务在延迟后、发送前用本返回值调
        ``is_reaction_superseded`` 比对，确认自己仍是该 key 最新被授权的反应，否则
        放弃发送，避免两条回复叠着发出。
        """
        key = self._cooldown_key(scope_key, poker_id)
        now = time.time()
        if key:
            self._last_react_at[key] = now
        # 频率窗口按 scope_key（会话维度）分桶，与 peek_reaction_window 同口径；
        # 无 scope_key 时不入窗（无法定位会话维度，本就不参与频率限制）。
        if scope_key:
            self._reaction_window[scope_key].append(now)
        return now

    def is_reaction_superseded(
        self, scope_key: str, poker_id: str, react_token: float
    ) -> bool:
        """该次反应（``react_token`` 为其 ``mark_reacted`` 返回的时间戳）是否已被同一
        key 上更晚的反应取代。

        返回 ``True`` 表示本任务在延迟期间，同一 cooldown_key 又被 mark 了一次更晚的
        反应，本任务应放弃发送以免叠加。无 key（无法定位冷却维度）时返回 ``False``
        放行，与未启用本校验时的旧行为一致。
        """
        key = self._cooldown_key(scope_key, poker_id)
        if not key:
            return False
        return self._last_react_at.get(key, 0.0) > react_token

    def peek_reaction_window(self, scope_key: str, window_seconds: int) -> int:
        """返回该会话（``scope_key``）窗口内累计反应数，顺便清理过期记录。不写入新记录。

        ``scope_key`` 须与 :meth:`mark_reacted` 同口径（群聊 group_id / 私聊 poker_id），
        否则命不中同一个桶。空 ``scope_key`` 或非正窗口视为不限制，返回 0。
        清空后的空桶留给 ``_prune`` 兜底回收（用 ``.get`` 不凭空创建桶）。
        """
        if window_seconds <= 0 or not scope_key:
            return 0
        window = self._reaction_window.get(scope_key)
        if not window:
            return 0
        cutoff = time.time() - window_seconds
        while window and window[0] < cutoff:
            window.popleft()
        return len(window)

    # ----- 跟风戳冷却 -----

    def in_bystander_cooldown(self, scope_key: str, cooldown_seconds: int) -> bool:
        if cooldown_seconds <= 0 or not scope_key:
            return False
        last = self._last_bystander_at.get(scope_key, 0.0)
        return (time.time() - last) < cooldown_seconds

    def mark_bystander(self, scope_key: str) -> None:
        if scope_key:
            self._last_bystander_at[scope_key] = time.time()

    # ----- 暴戳计数 -----

    def record_poke_and_count(
        self, scope_key: str, poker_id: str, window_seconds: int
    ) -> int:
        """记录一次戳，返回窗口期内的累计次数。"""
        if not scope_key:
            return 0
        key = f"{scope_key}:{poker_id}" if poker_id else scope_key
        now = time.time()
        cutoff = now - max(window_seconds, 1)
        records = self._poke_records[key]
        while records and records[0] < cutoff:
            records.popleft()
        records.append(now)

        self._record_counter += 1
        if self._record_counter >= self._PRUNE_THRESHOLD:
            self._record_counter = 0
            self._last_prune_at = time.time()
            self._prune()
        return len(records)

    # ----- 昵称缓存 -----

    @staticmethod
    def _name_cache_key(group_id: str, user_id: str) -> str:
        return f"{group_id}:{user_id}" if group_id else user_id

    def get_cached_name(self, group_id: str, user_id: str) -> str | None:
        if not user_id:
            return None
        key = self._name_cache_key(group_id, user_id)
        entry = self._name_cache.get(key)
        if entry is None:
            return None
        name, expire_at = entry
        if expire_at < time.time():
            self._name_cache.pop(key, None)
            return None
        return name

    def cache_name(self, group_id: str, user_id: str, name: str, ttl: float) -> None:
        """缓存昵称查询结果；``name`` 为空串表示负缓存（已知该用户没有可用昵称）。"""
        if not user_id:
            return
        key = self._name_cache_key(group_id, user_id)
        self._name_cache[key] = (name, time.time() + ttl)
        if len(self._name_cache) > MEMBER_NAME_CACHE_MAX_SIZE:
            now = time.time()
            # value 是 (name, expire_at) 元组；按 expire_at 排序保留最晚到期的一半。
            # 若误写成 kv[1] 会按 (name, expire_at) 元组字典序，先比 name 字符串
            kept = sorted(
                ((k, v) for k, v in self._name_cache.items() if v[1] >= now),
                key=lambda kv: kv[1][1],
                reverse=True,
            )[: MEMBER_NAME_CACHE_MAX_SIZE // 2]
            self._name_cache = dict(kept)

    # ----- stream_id 缓存 -----

    @staticmethod
    def _stream_cache_key(*, group_id: str, user_id: str) -> str:
        if group_id:
            return f"group:{group_id}"
        return f"user:{user_id}" if user_id else ""

    def get_cached_stream_id(self, *, group_id: str, user_id: str) -> str | None:
        key = self._stream_cache_key(group_id=group_id, user_id=user_id)
        if not key:
            return None
        entry = self._stream_id_cache.get(key)
        if entry is None:
            return None
        stream_id, expire_at = entry
        if expire_at < time.time():
            self._stream_id_cache.pop(key, None)
            return None
        return stream_id

    def cache_stream_id(
        self, *, group_id: str, user_id: str, stream_id: str, ttl: float
    ) -> None:
        if not stream_id:
            return
        key = self._stream_cache_key(group_id=group_id, user_id=user_id)
        if not key:
            return
        self._stream_id_cache[key] = (stream_id, time.time() + ttl)
        if len(self._stream_id_cache) > STREAM_ID_CACHE_MAX_SIZE:
            now = time.time()
            # value 是 (stream_id, expire_at)，与 _name_cache 同构：
            # 先丢已过期项，再按 expire_at 降序保留最晚到期的一半。
            kept = sorted(
                ((k, v) for k, v in self._stream_id_cache.items() if v[1] >= now),
                key=lambda kv: kv[1][1],
                reverse=True,
            )[: STREAM_ID_CACHE_MAX_SIZE // 2]
            self._stream_id_cache = dict(kept)

    # ----- 主动戳：冷却与日上限 -----

    def in_proactive_chat_cooldown(self, group_id: str, cooldown_seconds: int) -> bool:
        if cooldown_seconds <= 0 or not group_id:
            return False
        last = self._last_proactive_at_chat.get(group_id, 0.0)
        return (time.time() - last) < cooldown_seconds

    def in_proactive_global_cooldown(self, cooldown_seconds: int) -> bool:
        now = time.time()
        # in-flight 预占期 / 失败退避期一律视为全局占用，挡住其他群并发穿过乐观快检；
        # 即使 global_cooldown_seconds=0 也要靠它防同一时刻多群并发双发。
        if now < self._proactive_inflight_until:
            return True
        if cooldown_seconds <= 0:
            return False
        return (now - self._last_proactive_global_at) < cooldown_seconds

    def mark_proactive(self, group_id: str) -> None:
        """按当前本地日期归零并累计每日计数。

        日期在内部按 ``time.time()`` 当场计算（而非由调用方传入）：主动戳是
        "锁内算日期、思考延迟后锁外才 commit"，若沿用锁内日期，跨午夜会把
        ``_proactive_daily_date`` 拨回前一天、令当日计数被错误重置。
        ``format_local_date`` 与 active_hour_* 同口径（本地时间）。
        """
        now = time.time()
        today = format_local_date(now)
        if group_id:
            self._last_proactive_at_chat[group_id] = now
        self._last_proactive_global_at = now
        if today != self._proactive_daily_date:
            self._proactive_daily_date = today
            self._proactive_daily_count = 0
        self._proactive_daily_count += 1

    def begin_proactive_inflight(self, expected_delay: float = 0.0) -> int:
        """锁内调用：标记一次主动戳进行中（短期预占），阻止其他群并发穿过全局冷却。

        返回本次 in-flight 的令牌（token），调用方须在 commit/abort 时回传：仅当令牌
        仍是当前持有者时才真正清理 in-flight，避免旧任务超时、新任务已接管后旧任务
        收尾误清新任务的占用。

        TTL 取 ``max(基础下限, expected_delay + RPC 余量)``：``expected_delay`` 传思考
        延迟上限（``cfg.max_delay_seconds``），确保延迟调大后 in-flight 不会在思考延迟
        期内提前过期、放任其他群穿过全局冷却/每日上限。

        仅占 in-flight，**不**消耗每日额度与群/全局长冷却；待 send_poke 成功后由
        commit_proactive 转正，或失败/取消时由 abort_proactive_inflight 释放。
        """
        self._proactive_inflight_seq += 1
        token = self._proactive_inflight_seq
        self._proactive_inflight_token = token
        ttl = max(
            self._PROACTIVE_INFLIGHT_TTL_SECONDS,
            max(0.0, expected_delay) + self._PROACTIVE_INFLIGHT_RPC_MARGIN_SECONDS,
        )
        self._proactive_inflight_until = time.time() + ttl
        return token

    def commit_proactive(self, group_id: str, token: int) -> None:
        """send_poke 成功后调用：占用每日额度与群/全局长冷却；仅当 ``token`` 仍是当前
        持有者时才清 in-flight（否则保留新任务的 in-flight 不动）。

        mark_proactive 无论令牌是否仍当前都执行——戳确实发出去了，理应记一次全局/群
        冷却与每日额度（其日期在 mark_proactive 内按当前时刻计算，不受锁内外时差影响）。
        """
        if token == self._proactive_inflight_token:
            self._proactive_inflight_until = 0.0
            self._proactive_inflight_token = 0
        self.mark_proactive(group_id)

    def abort_proactive_inflight(self, token: int) -> None:
        """send_poke 未成功（失败/异常/取消）时调用：不消耗每日额度与长冷却。

        仅当 ``token`` 仍是当前持有者时才把 in-flight 收敛为一小段全局失败退避（避免
        风控期多个群连环重试刷屏）；若已被更晚的任务接管（令牌过期）则什么都不动，
        以免误清新任务的 in-flight 占用。
        """
        if token == self._proactive_inflight_token:
            self._proactive_inflight_until = time.time() + self._PROACTIVE_FAILURE_BACKOFF_SECONDS
            self._proactive_inflight_token = 0

    def proactive_daily_count(self) -> int:
        """返回当日已成功主动戳次数；按当前本地日期判定，跨午夜自动归零。"""
        if format_local_date(time.time()) != self._proactive_daily_date:
            return 0
        return self._proactive_daily_count

    def poked_bot_recently(
        self, scope_key: str, user_id: str, window_seconds: int
    ) -> bool:
        """``user_id`` 是否在 ``window_seconds`` 内于 ``scope_key`` 维度戳过麦麦。

        ``scope_key`` 必须与主 Hook record 端用同一维度（群聊场景下用 group_id）才能命中。
        """
        if not scope_key or not user_id or window_seconds <= 0:
            return False
        records = self._poke_records.get(f"{scope_key}:{user_id}")
        if not records:
            return False
        cutoff = time.time() - window_seconds
        return any(ts >= cutoff for ts in records)

    # ----- 持久化快照 -----

    def export_persistable(self) -> dict:
        """导出值得跨重启保留的限频状态（卸载时由插件写盘）。

        只挑"丢了会造成实际影响"的字段：冷却时间戳防重启后立即连戳、
        每日计数防重启刷新主动戳额度。TTL 缓存 / 暴戳窗口 / in-flight 等
        短命或运行时态不导出，加载后自然重建。
        """
        return {
            "last_react_at": dict(self._last_react_at),
            "last_bystander_at": dict(self._last_bystander_at),
            "last_proactive_at_chat": dict(self._last_proactive_at_chat),
            "last_proactive_global_at": self._last_proactive_global_at,
            "proactive_daily_count": self._proactive_daily_count,
            "proactive_daily_date": self._proactive_daily_date,
        }

    def import_persistable(self, data: dict) -> None:
        """载入持久化快照（on_load 调用）；过期/畸形项静默丢弃，不影响启动。

        冷却时间戳按 ``_STALE_AFTER_SECONDS`` 过滤（与 _prune 同口径）；
        每日计数仅在快照日期仍是"今天"时恢复，跨天自动作废。
        """
        if not isinstance(data, dict):
            return
        cutoff = time.time() - self._STALE_AFTER_SECONDS

        def _load_ts_map(key: str) -> dict[str, float]:
            raw = data.get(key)
            if not isinstance(raw, dict):
                return {}
            result: dict[str, float] = {}
            for k, v in raw.items():
                try:
                    ts = float(v)
                except (TypeError, ValueError):
                    continue
                if ts >= cutoff:
                    result[str(k)] = ts
            return result

        self._last_react_at.update(_load_ts_map("last_react_at"))
        self._last_bystander_at.update(_load_ts_map("last_bystander_at"))
        self._last_proactive_at_chat.update(_load_ts_map("last_proactive_at_chat"))
        try:
            global_at = float(data.get("last_proactive_global_at", 0.0))
        except (TypeError, ValueError):
            global_at = 0.0
        if global_at >= cutoff:
            self._last_proactive_global_at = max(self._last_proactive_global_at, global_at)
        daily_date = str(data.get("proactive_daily_date") or "")
        if daily_date and daily_date == format_local_date(time.time()):
            try:
                count = int(data.get("proactive_daily_count", 0))
            except (TypeError, ValueError):
                count = 0
            if count > self._proactive_daily_count:
                self._proactive_daily_date = daily_date
                self._proactive_daily_count = count

    # ----- prune / clear -----

    def maybe_prune(self) -> None:
        """按时间节流触发 _prune。

        _prune 原本只在 record_poke_and_count（被动戳）按计数触发；主动戳观察这类
        "高频但不戳麦麦"的路径调用本方法，确保 _proactive_locks / _last_*_at 等
        不依赖"有人戳麦麦"也能被定期回收。本方法 O(1)，仅在距上次 _prune 超过
        _PRUNE_MIN_INTERVAL_SECONDS 时才真正执行一次 O(n) 的 _prune。
        """
        now = time.time()
        if now - self._last_prune_at >= self._PRUNE_MIN_INTERVAL_SECONDS:
            self._last_prune_at = now
            self._prune()

    def _prune(self) -> None:
        """删除超过 _STALE_AFTER_SECONDS 未更新的 key，控制字典体积。"""
        cutoff = time.time() - self._STALE_AFTER_SECONDS
        self._last_react_at = {k: v for k, v in self._last_react_at.items() if v >= cutoff}
        self._last_bystander_at = {k: v for k, v in self._last_bystander_at.items() if v >= cutoff}
        self._last_proactive_at_chat = {
            k: v for k, v in self._last_proactive_at_chat.items() if v >= cutoff
        }
        self._poke_records = defaultdict(
            lambda: deque(maxlen=PokeStateManager._POKE_RECORD_MAXLEN),
            {k: v for k, v in self._poke_records.items() if v and v[-1] >= cutoff},
        )
        # 逐桶清理过期反应记录，丢弃清空后的空桶，避免离开的会话长期残留空 deque。
        pruned_windows: dict[str, deque[float]] = {}
        for scope_key, window in self._reaction_window.items():
            while window and window[0] < cutoff:
                window.popleft()
            if window:
                pruned_windows[scope_key] = window
        self._reaction_window = defaultdict(deque, pruned_windows)
        now = time.time()
        self._name_cache = {k: v for k, v in self._name_cache.items() if v[1] >= now}
        self._stream_id_cache = {k: v for k, v in self._stream_id_cache.items() if v[1] >= now}
        # 与 _last_proactive_at_chat 同源 prune：3600s 没主动戳过的群锁可清；
        # 但 lock.locked()=True 时仍跳过，避免移除"正持有中"的锁导致同群双发
        active_groups = set(self._last_proactive_at_chat.keys())
        self._proactive_locks = {
            gid: lock for gid, lock in self._proactive_locks.items()
            if gid in active_groups or lock.locked()
        }

    def clear(self) -> None:
        self._last_react_at.clear()
        self._poke_records.clear()
        self._last_bystander_at.clear()
        self._name_cache.clear()
        self._stream_id_cache.clear()
        self._last_proactive_at_chat.clear()
        self._last_proactive_global_at = 0.0
        self._proactive_inflight_until = 0.0
        self._proactive_inflight_token = 0
        self._proactive_inflight_seq = 0
        self._proactive_daily_count = 0
        self._proactive_daily_date = ""
        self._reaction_window.clear()
        self._record_counter = 0
        self._last_prune_at = 0.0
        self._proactive_locks.clear()
        self._known_self_id = ""
