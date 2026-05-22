"""智能戳一戳插件 — MaiBot SDK v2

通过 @HookHandler 订阅 chat.receive.before_process，识别 napcat 适配器
注入的 notify.poke 事件，按拟人化策略回戳 / 发文字 / 发表情 / 沉默；
另以 OBSERVE 模式观察普通消息，按概率触发主动戳。

本文件为薄入口：仅持有 config schema 绑定、生命周期与两个 Hook 的入口派发。
具体执行链拆在以下 deep module：

* ``state.PokeStateManager`` —— 冷却/计数/缓存的集中持有者
* ``napcat.NapcatPokeClient`` —— ``send_poke`` 调用 + 失败日志抑制
* ``emoji.EmojiKeywordValidator`` —— 关键词探测 + 衰退 + 选表情
* ``reaction.ReactionExecutor`` —— 戳到麦麦的反应主流程（poke / emoji / text）
* ``bystander.BystanderPoker`` —— 别人互戳时跟风
* ``proactive.ProactivePoker`` —— 群消息观察 + 主动戳完整链路
"""

from __future__ import annotations

import asyncio
import random
from typing import Any

from maibot_sdk import HookHandler, MaiBotPlugin
from maibot_sdk.types import ErrorPolicy, HookMode, HookOrder

from .bystander import BystanderPoker
from .common import PLUGIN_VERSION, PROACTIVE_TASK_QUEUE_LIMIT, to_positive_int
from .config import SmartPokeConfig
from .emoji import EmojiKeywordValidator
from .napcat import NapcatPokeClient
from .proactive import ProactivePoker
from .reaction import ReactionExecutor
from .state import (
    MEMBER_NAME_CACHE_TTL_SECONDS,
    MEMBER_NAME_NEGATIVE_CACHE_TTL_SECONDS,
    STREAM_ID_CACHE_TTL_SECONDS,
    PokeContext,
    PokeStateManager,
)


# --- 主插件 ---


class SmartPokePlugin(MaiBotPlugin):
    """智能戳一戳插件主类。"""

    config_model = SmartPokeConfig

    def __init__(self) -> None:
        super().__init__()
        self._blacklist: set[str] = set()
        self._proactive_whitelist_groups: set[str] = set()
        self._proactive_blacklist_groups: set[str] = set()
        self._pending_tasks: set[asyncio.Task] = set()
        self._state = PokeStateManager()
        # global 锁保护"全局冷却二次确认 + mark"临界区，避免不同群并发任务都穿过乐观快检。
        # per-group 锁挂在 self._state.get_proactive_lock(group_id)，与 _last_proactive_at_chat
        # 同源 prune，避免群数量上涨时锁字典无界增长。
        self._proactive_global_lock: asyncio.Lock = asyncio.Lock()
        self._proactive_active_count: int = 0
        # on_unload 入口置 True，_spawn_background_task 据此拒收新任务。
        self._shutting_down: bool = False

        # 5 个协作模块。每个都持 plugin 弱引用以访问 ctx/config/state。
        self._napcat = NapcatPokeClient(self)
        self._emoji = EmojiKeywordValidator(self)
        self._reaction = ReactionExecutor(self)
        self._bystander = BystanderPoker(self)
        self._proactive = ProactivePoker(self)

    # ===== 生命周期 =====

    async def on_load(self) -> None:
        self._refresh_user_sets()
        self.ctx.logger.info("智能戳一戳插件(v%s)初始化完成。", PLUGIN_VERSION)
        self._spawn_background_task(
            self._emoji.probe_keywords_at_startup(), "emoji_keyword_probe"
        )

    async def on_unload(self) -> None:
        # 拒收新任务后再 cancel/gather，避免 gather 完成后又有"漏网之鱼"被孤立
        self._shutting_down = True
        to_cancel = [t for t in self._pending_tasks if not t.done()]
        for task in to_cancel:
            task.cancel()
        if to_cancel:
            await asyncio.gather(*to_cancel, return_exceptions=True)
        self._pending_tasks.clear()
        self._proactive_active_count = 0
        self._state.clear()  # 同时清掉 _state 内的 _proactive_locks

    async def on_config_update(
        self, scope: str, config_data: dict, version: str
    ) -> None:
        if scope == "self":
            self._refresh_user_sets()
            self.ctx.logger.info("配置已热更新完成。")

    def _refresh_user_sets(self) -> None:
        cfg = self.config
        self._blacklist = {str(x).strip() for x in cfg.user_control.blacklist if str(x).strip()}
        self._proactive_whitelist_groups = {
            str(x).strip() for x in cfg.proactive.whitelist_groups if str(x).strip()
        }
        self._proactive_blacklist_groups = {
            str(x).strip() for x in cfg.proactive.blacklist_groups if str(x).strip()
        }

    # ===== 后台任务调度 =====

    def _spawn_background_task(self, coro: Any, label: str, timeout: float = 120.0) -> None:
        """提交后台任务，带超时兜底；卸载期或主动戳超并发时直接 close coroutine。"""
        if self._shutting_down:
            try:
                coro.close()
            except Exception:
                pass
            return

        is_proactive = label == "proactive"
        if is_proactive and self._proactive_active_count >= PROACTIVE_TASK_QUEUE_LIMIT:
            try:
                coro.close()
            except Exception:
                pass
            self.ctx.logger.debug(
                "[proactive] 并发任务已达上限 %d，丢弃本次触发",
                PROACTIVE_TASK_QUEUE_LIMIT,
            )
            return

        async def _runner() -> None:
            try:
                await asyncio.wait_for(coro, timeout=timeout)
            except asyncio.TimeoutError:
                self.ctx.logger.warning("[%s] 后台任务超时 %ss，已取消", label, timeout)
            except Exception:
                self.ctx.logger.exception("[%s] 后台任务异常", label)

        if is_proactive:
            self._proactive_active_count += 1

        task = asyncio.create_task(_runner())
        self._pending_tasks.add(task)

        def _on_done(t: asyncio.Task) -> None:
            self._pending_tasks.discard(t)
            if is_proactive:
                self._proactive_active_count = max(0, self._proactive_active_count - 1)

        task.add_done_callback(_on_done)

    # ===== 名字 / stream_id 解析（公开给协作模块复用，带 TTL 缓存）=====

    async def resolve_member_name(self, group_id: str, user_id: str) -> str:
        """解析群成员昵称，群名片优先于 nickname；带 TTL 缓存与负缓存。"""
        if not user_id:
            return ""
        cached = self._state.get_cached_name(group_id, user_id)
        if cached is not None:
            return cached

        name = ""
        try:
            if group_id:
                user_int = to_positive_int(user_id)
                group_int = to_positive_int(group_id)
                if user_int is None or group_int is None:
                    return ""
                info = await self.ctx.api.call(
                    "adapter.napcat.group.get_group_member_info",
                    group_id=group_int,
                    user_id=user_int,
                    no_cache=False,
                )
                # SDK _normalize_capability_result 已对 api.call 解出 envelope.result，
                # info 直接是 adapter 返回的 dict；失败时是 {"success": False, ...}，
                # .get("card") / .get("nickname") 都为 None，统一落到 name="" 走负缓存。
                if isinstance(info, dict):
                    name = str(info.get("card") or info.get("nickname") or "").strip()
            else:
                user_int = to_positive_int(user_id)
                if user_int is None:
                    return ""
                info = await self.ctx.api.call(
                    "adapter.napcat.account.get_stranger_info",
                    user_id=user_int,
                    no_cache=False,
                )
                if isinstance(info, dict):
                    name = str(info.get("nickname") or info.get("nick") or "").strip()
        except Exception:
            self.ctx.logger.debug(
                "解析昵称失败 (group=%s, user=%s)", group_id, user_id, exc_info=True
            )
            self._state.cache_name(
                group_id, user_id, "", MEMBER_NAME_NEGATIVE_CACHE_TTL_SECONDS
            )
            return ""

        if name:
            self._state.cache_name(group_id, user_id, name, MEMBER_NAME_CACHE_TTL_SECONDS)
        else:
            self._state.cache_name(
                group_id, user_id, "", MEMBER_NAME_NEGATIVE_CACHE_TTL_SECONDS
            )
        return name

    async def resolve_stream_id_for_group(self, group_id: str) -> str:
        """根据群号查 stream_id（带 TTL 缓存）。"""
        if not group_id:
            return ""
        cached = self._state.get_cached_stream_id(group_id=group_id, user_id="")
        if cached is not None:
            return cached
        try:
            stream = await self.ctx.chat.get_stream_by_group_id(group_id, platform="qq")
        except Exception:
            self.ctx.logger.debug(
                "get_stream_by_group_id 失败 (group=%s)", group_id, exc_info=True
            )
            return ""
        stream_id = ""
        if isinstance(stream, dict):
            stream_id = str(stream.get("session_id") or "")
        if stream_id:
            self._state.cache_stream_id(
                group_id=group_id, user_id="", stream_id=stream_id,
                ttl=STREAM_ID_CACHE_TTL_SECONDS,
            )
        return stream_id

    async def resolve_stream_id_for_user(self, user_id: str) -> str:
        """根据用户号查 stream_id（带 TTL 缓存）。"""
        if not user_id:
            return ""
        cached = self._state.get_cached_stream_id(group_id="", user_id=user_id)
        if cached is not None:
            return cached
        try:
            stream = await self.ctx.chat.get_stream_by_user_id(user_id, platform="qq")
        except Exception:
            self.ctx.logger.debug(
                "get_stream_by_user_id 失败 (user=%s)", user_id, exc_info=True
            )
            return ""
        stream_id = ""
        if isinstance(stream, dict):
            stream_id = str(stream.get("session_id") or "")
        if stream_id:
            self._state.cache_stream_id(
                group_id="", user_id=user_id, stream_id=stream_id,
                ttl=STREAM_ID_CACHE_TTL_SECONDS,
            )
        return stream_id

    async def resolve_stream_id_for_context(self, ctx: PokeContext) -> str:
        """notice 消息 session_id 固定为空时按群/用户回查。"""
        if ctx.is_group and ctx.group_id:
            return await self.resolve_stream_id_for_group(ctx.group_id)
        if ctx.poker_id:
            return await self.resolve_stream_id_for_user(ctx.poker_id)
        return ""

    # ===== Maisaka 上下文注入 =====

    _SELF_POKE_ACTION_LABELS = {
        "back_poke": "回戳了",
        "bystander": "跟风戳了",
        "proactive": "主动戳了",
    }

    async def record_self_poke_to_context(
        self,
        *,
        label: str,
        target_id: str,
        target_name: str,
        group_id: str,
        is_group: bool,
        stream_id: str = "",
    ) -> None:
        """把 bot 自己发出的戳行为追加到对应聊天流的 Maisaka 上下文。

        失败仅 debug 不抛——记忆写入失败的严重度低于戳没出去。
        """
        if not self.config.plugin.record_self_poke_to_context:
            return
        if not target_id:
            return

        if not stream_id:
            # notify 路径下 ctx.stream_id 常为空，按群/用户回查
            if is_group and group_id:
                stream_id = await self.resolve_stream_id_for_group(group_id)
            else:
                stream_id = await self.resolve_stream_id_for_user(target_id)
        if not stream_id:
            self.ctx.logger.debug(
                "[%s] 无法解析 stream_id，跳过 Maisaka 上下文注入 (group=%s, target=%s)",
                label, group_id, target_id,
            )
            return

        action = self._SELF_POKE_ACTION_LABELS.get(label, "戳了戳")
        display_name = (target_name or "").strip() or target_id

        text = (
            f"[系统事件] 我刚刚通过 QQ 的「戳一戳」功能{action} \"{display_name}\"。"
        )

        try:
            resp = await self.ctx.maisaka.context.append(
                stream_id=stream_id,
                segments=[{"type": "text", "content": text}],
                visible_text=text,
                source_kind=f"plugin:smart_poke:{label}",
            )
        except Exception:
            self.ctx.logger.warning(
                "[%s] maisaka.context.append 调用异常 (stream=%s)",
                label, stream_id, exc_info=True,
            )
            return

        # host 业务失败统一回 {"success": False, "error": ...}；成功则带 index/visible_text/source_kind
        if isinstance(resp, dict) and resp.get("success") is False:
            self.ctx.logger.warning(
                "[%s] maisaka.context.append 业务失败 (stream=%s): %s",
                label, stream_id, resp.get("error"),
            )
            return

        # 成功时打 info 让用户在日志里能肉眼确认"这次戳已写入麦麦上下文"
        if isinstance(resp, dict) and resp.get("success") is True:
            self.ctx.logger.info(
                "[%s] 已写入 Maisaka 上下文: text=%r, index=%s, stream=%s",
                label, resp.get("visible_text") or text,
                resp.get("index"), stream_id,
            )
        else:
            self.ctx.logger.debug(
                "[%s] maisaka.context.append 返回了非预期结构: %r", label, resp,
            )

    # ===== Hook 入口 =====

    @HookHandler(
        "chat.receive.before_process",
        name="smart_poke_listener",
        description="识别并响应 napcat 注入的戳一戳通知事件",
        mode=HookMode.BLOCKING,
        order=HookOrder.EARLY,
        timeout_ms=3000,
        error_policy=ErrorPolicy.SKIP,
    )
    async def handle_poke_event(self, message: dict | None = None, **kwargs):
        del kwargs

        if not self.config.plugin.enabled:
            return None

        ctx = self._extract_poke_context(message)
        if ctx is None:
            return None

        # ----- 分支一：戳的不是麦麦（别人互戳）-----
        if not ctx.is_poking_bot:
            # send_poke 出去后 napcat 会回灌一条 poker_id=self_id 的事件，提前过滤掉
            # 多次连续回戳产生的 n 倍回声逐一走完整套检查的开销
            if ctx.poker_id == ctx.self_id:
                return None
            triggered = self._bystander.maybe_trigger(ctx)
            if triggered and self.config.bystander.swallow_event:
                return {"action": "abort"}
            return None

        # ----- 分支二：戳的是麦麦本人 -----

        if ctx.poker_id == ctx.self_id and self.config.user_control.ignore_self_poke:
            return {"action": "abort"}

        if ctx.poker_id in self._blacklist:
            self.ctx.logger.debug("黑名单用户 %s 的戳一戳已静默拦截", ctx.poker_id)
            return {"action": "abort"}

        if ctx.is_group and not self.config.reaction.react_in_group:
            return None
        if not ctx.is_group and not self.config.reaction.react_in_private:
            return None

        # 暴戳计数必须在冷却检查之前累计：否则 cooldown_seconds=8 + spam_threshold=5
        # 的默认组合下连戳全被冷却拦截，spam 窗口里只能记到 1 次。
        # 用 spam_scope_key 而非 cooldown_key，确保 proactive 的 poked_bot_recently(group_id) 能命中。
        poke_count = self._state.record_poke_and_count(
            ctx.spam_scope_key,
            ctx.poker_id,
            self.config.reaction.spam_window_seconds,
        )
        is_spam = poke_count >= self.config.reaction.spam_threshold

        if self._state.in_cooldown(
            ctx.cooldown_key, ctx.poker_id, self.config.reaction.cooldown_seconds
        ):
            self.ctx.logger.debug(
                "[%s:%s] 戳一戳冷却中，已拦截",
                ctx.cooldown_key, ctx.poker_id,
            )
            return {"action": "abort"}

        # 滑动窗口频率限制：逐人冷却拦不住"10 个人轮番戳麦麦"
        max_per_minute = self.config.reaction.max_reactions_per_minute
        if max_per_minute > 0:
            window_count = self._state.peek_reaction_window(60)
            if window_count >= max_per_minute:
                self.ctx.logger.debug(
                    "[%s] 60s 内累计反应 %d 次已达上限 %d，静默吞事件",
                    ctx.cooldown_key or ctx.poker_id, window_count, max_per_minute,
                )
                return {"action": "abort"}

        if random.random() > self.config.reaction.react_probability:
            self.ctx.logger.debug(
                "[%s] 戳一戳触发概率未命中，静默拦截", ctx.cooldown_key or ctx.poker_id
            )
            chat_prob = self.config.reaction.silent_chat_probability
            if chat_prob > 0 and random.random() < chat_prob:
                self._spawn_background_task(self._reaction.silent_reply(ctx), "silent_reply")
                return {"action": "abort"}
            if self.config.reaction.swallow_when_silent:
                return {"action": "abort"}
            return None

        self._state.mark_reacted(ctx.cooldown_key, ctx.poker_id)

        self._spawn_background_task(
            self._reaction.react_to_poke(ctx, is_spam, poke_count),
            "react",
        )

        return {"action": "abort"}

    @HookHandler(
        "chat.receive.before_process",
        name="smart_poke_proactive_observer",
        description="观察普通群消息，按拟人化概率被勾起一次主动戳",
        mode=HookMode.OBSERVE,
        order=HookOrder.LATE,
        timeout_ms=2000,
        error_policy=ErrorPolicy.SKIP,
    )
    async def observe_message_for_proactive(self, message: dict | None = None, **kwargs):
        """OBSERVE 旁路：每条入站群消息都被"考虑"一次，再交由 ProactivePoker 层层过滤。

        主 BLOCKING handler 对戳一戳事件 ``abort`` 时 dispatcher 会先 ``break``，
        所以戳一戳通知不会触发主动戳，避免事件回声。
        """
        del kwargs
        if not self.config.plugin.enabled:
            return None
        self._proactive.observe_signal(message)
        return None

    # ===== 信息提取 =====

    def _extract_poke_context(self, message: Any) -> PokeContext | None:
        """从消息 dict 中提取戳一戳信息；不是戳一戳事件时返回 ``None``。"""
        if not isinstance(message, dict):
            return None
        if not message.get("is_notify"):
            return None

        msg_info = message.get("message_info") or {}
        if not isinstance(msg_info, dict):
            return None
        additional = msg_info.get("additional_config") or {}
        if not isinstance(additional, dict):
            return None

        if additional.get("napcat_notice_type") != "notify":
            return None
        if additional.get("napcat_notice_sub_type") != "poke":
            return None

        payload = additional.get("napcat_notice_payload") or {}
        if not isinstance(payload, dict):
            return None

        self_id = str(payload.get("self_id") or "").strip()
        poker_id = str(payload.get("user_id") or "").strip()
        target_id = str(payload.get("target_id") or "").strip()

        if not self_id or not poker_id or not target_id:
            return None

        self._state.set_known_self_id(self_id)

        # 严格判定 group_id：必须正整数才视为群聊，避免 "0" / 0 被误判
        group_info = msg_info.get("group_info") or {}
        raw_group_id = payload.get("group_id")
        if raw_group_id is None or str(raw_group_id).strip() in ("", "0"):
            raw_group_id = group_info.get("group_id")
        group_int = to_positive_int(raw_group_id)

        user_info = msg_info.get("user_info") or {}

        # 群名片优先于 nickname，与 resolve_member_name 保持一致
        poker_cardname = str(user_info.get("user_cardname") or "").strip()
        poker_nickname = str(user_info.get("user_nickname") or "").strip()

        ctx = PokeContext()
        ctx.self_id = self_id
        ctx.poker_id = poker_id
        ctx.poker_name = poker_cardname or poker_nickname
        ctx.target_id = target_id
        # 主分支 target 是麦麦自己不需要昵称；跟风戳分支按需异步补 target_name
        ctx.target_name = ""
        ctx.group_id = str(group_int) if group_int is not None else ""
        ctx.is_group = group_int is not None
        ctx.stream_id = str(message.get("session_id") or "")
        # napcat notice 注入的 session_id 常为空，按 stream_id → group_id → poker_id 回退
        ctx.cooldown_key = ctx.stream_id or ctx.group_id or ctx.poker_id
        ctx.spam_scope_key = ctx.group_id if ctx.is_group else ctx.poker_id
        return ctx


def create_plugin() -> SmartPokePlugin:
    """Runner 调用入口。"""
    return SmartPokePlugin()
