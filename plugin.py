"""智能戳一戳插件 — MaiBot SDK v2

通过 @HookHandler 订阅 chat.receive.before_process，识别 napcat 适配器
注入的 notify.poke 事件，按拟人化策略回戳 / 发文字 / 发表情 / 沉默；
另以 OBSERVE 模式观察普通消息，按概率触发主动戳。

本文件为薄入口：仅持有 config schema 绑定、生命周期与两个 Hook 的入口派发。
具体执行链拆在 ``core`` 子包：

* ``core.state.PokeStateManager`` —— 冷却/计数/缓存的集中持有者
* ``core.napcat.NapcatPokeClient`` —— ``send_poke`` 调用 + 失败日志抑制
* ``core.emoji.EmojiKeywordValidator`` —— 关键词探测 + 衰退 + 选表情
* ``core.reaction.ReactionExecutor`` —— 戳到麦麦的反应主流程（poke / emoji / text）
* ``core.bystander.BystanderPoker`` —— 别人互戳时跟风
* ``core.proactive.ProactivePoker`` —— 群消息观察 + 主动戳完整链路
"""

from __future__ import annotations

import asyncio
import random
from typing import Any

from maibot_sdk import HookHandler, MaiBotPlugin
from maibot_sdk.types import ErrorPolicy, HookMode, HookOrder

from .core.bystander import BystanderPoker
from .core.common import PLUGIN_VERSION, PROACTIVE_TASK_QUEUE_LIMIT, extract_onebot_field, to_positive_int
from .core.config import SmartPokeConfig
from .core.emoji import EmojiKeywordValidator
from .core.napcat import NapcatPokeClient
from .core.proactive import ProactivePoker
from .core.reaction import ReactionExecutor
from .core.state import (
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
        # emoji 关键词探测任务句柄：热更新时取消上一轮未结束的探测，避免累积并发长轮询。
        self._emoji_probe_task: asyncio.Task | None = None
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
        self._shutting_down = False
        self._refresh_user_sets()
        self.ctx.logger.info("智能戳一戳插件(v%s)初始化完成。", PLUGIN_VERSION)
        self._spawn_emoji_probe()

    async def on_unload(self) -> None:
        # 拒收新任务后再 cancel/gather，避免 gather 完成后又有"漏网之鱼"被孤立
        self._shutting_down = True
        to_cancel = [t for t in self._pending_tasks if not t.done()]
        for task in to_cancel:
            task.cancel()
        if to_cancel:
            await asyncio.gather(*to_cancel, return_exceptions=True)
        self._pending_tasks.clear()
        self._emoji_probe_task = None
        self._proactive_active_count = 0
        self._state.clear()  # 同时清掉 _state 内的 _proactive_locks

    async def on_config_update(
        self, scope: str, config_data: dict, version: str
    ) -> None:
        if scope == "self":
            self._refresh_user_sets()
            self._emoji.reset()
            self.ctx.logger.info("配置已热更新完成。")
            self._spawn_emoji_probe()

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

    def _spawn_background_task(self, coro: Any, label: str, timeout: float = 120.0) -> asyncio.Task | None:
        """提交后台任务，带超时兜底；卸载期或主动戳超并发时直接 close coroutine。"""
        if self._shutting_down:
            try:
                coro.close()
            except Exception:
                pass
            return None

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
            return None

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
        return task

    def _spawn_emoji_probe(self) -> None:
        """启动表情关键词探测；先取消上一轮未结束的探测，避免热更新连续保存配置时
        累积多个并发长轮询任务（表情库未就绪时单轮探测最长会轮询约 1 分钟）。"""
        old = self._emoji_probe_task
        if old is not None and not old.done():
            old.cancel()
        self._emoji_probe_task = self._spawn_background_task(
            self._emoji.probe_keywords_at_startup(), "emoji_keyword_probe"
        )

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
                # info 为 adapter 返回的 dict；失败时是 {"success": False, ...}，各字段
                # 取不到统一落到 name="" 走负缓存。extract_onebot_field 兼容 NapCat
                # (data 已剥到顶层)与 SnowLuma 等(card/nickname 裹在 data 里)两类返回。
                name = extract_onebot_field(info, "card", "nickname")
            else:
                user_int = to_positive_int(user_id)
                if user_int is None:
                    return ""
                info = await self.ctx.api.call(
                    "adapter.napcat.account.get_stranger_info",
                    user_id=user_int,
                    no_cache=False,
                )
                # get_stranger_info 仅有 nickname 字段；extract_onebot_field 兼容
                # data 已剥离(NapCat)与未剥离(SnowLuma)两种返回结构。
                name = extract_onebot_field(info, "nickname")
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

    async def resolve_stream_id_for_group(self, group_id: str, *, allow_open: bool = False) -> str:
        """根据群号查 stream_id（带 TTL 缓存）。

        ``allow_open=True`` 时，若 chat_manager 尚无该群会话（get_stream_by_group_id
        落空），回退 ``ctx.chat.open_session`` 打开/创建群聊流——覆盖"冷群从未建过流、
        被戳后想回文字/表情却拿不到 stream_id"的边角场景。主动戳等只读路径保持默认
        False，不在冷群上凭空创建会话。
        """
        if not group_id:
            return ""
        cached = self._state.get_cached_stream_id(group_id=group_id, user_id="")
        if cached is not None:
            return cached
        stream_id = ""
        try:
            stream = await self.ctx.chat.get_stream_by_group_id(group_id, platform="qq")
        except Exception:
            self.ctx.logger.debug(
                "get_stream_by_group_id 失败 (group=%s)", group_id, exc_info=True
            )
            stream = None
        if isinstance(stream, dict):
            stream_id = str(stream.get("session_id") or "")
        if not stream_id and allow_open:
            stream_id = await self._open_session_stream_id(chat_type="group", group_id=group_id)
        if stream_id:
            self._state.cache_stream_id(
                group_id=group_id, user_id="", stream_id=stream_id,
                ttl=STREAM_ID_CACHE_TTL_SECONDS,
            )
        return stream_id

    async def resolve_stream_id_for_user(self, user_id: str, *, allow_open: bool = False) -> str:
        """根据用户号查 stream_id（带 TTL 缓存）。

        ``allow_open=True`` 时 get_stream_by_user_id 落空会回退 ``ctx.chat.open_session``
        打开/创建私聊流，语义同 :meth:`resolve_stream_id_for_group`。
        """
        if not user_id:
            return ""
        cached = self._state.get_cached_stream_id(group_id="", user_id=user_id)
        if cached is not None:
            return cached
        stream_id = ""
        try:
            stream = await self.ctx.chat.get_stream_by_user_id(user_id, platform="qq")
        except Exception:
            self.ctx.logger.debug(
                "get_stream_by_user_id 失败 (user=%s)", user_id, exc_info=True
            )
            stream = None
        if isinstance(stream, dict):
            stream_id = str(stream.get("session_id") or "")
        if not stream_id and allow_open:
            stream_id = await self._open_session_stream_id(chat_type="private", user_id=user_id)
        if stream_id:
            self._state.cache_stream_id(
                group_id="", user_id=user_id, stream_id=stream_id,
                ttl=STREAM_ID_CACHE_TTL_SECONDS,
            )
        return stream_id

    async def resolve_stream_id_for_context(self, ctx: PokeContext, *, allow_open: bool = False) -> str:
        """优先用入站消息已带的 session_id；为空时再按群/用户回查。

        旧实现假设 notice 的 session_id 固定为空而直接回查，但部分 notice 实际带了
        有效 session_id，且 chat_manager 里未必存在对应 group session
        (get_stream_by_group_id 遍历 chat_manager.sessions 找不到时返回 None)，
        导致 react 拿不到 stream_id 时 emoji/text/llm 三档全部回退到 poke。
        故优先采信 ctx.stream_id（与 record_self_poke_to_context 的取值口径一致）。
        """
        if ctx.stream_id:
            return ctx.stream_id
        if ctx.is_group and ctx.group_id:
            return await self.resolve_stream_id_for_group(ctx.group_id, allow_open=allow_open)
        if ctx.poker_id:
            return await self.resolve_stream_id_for_user(ctx.poker_id, allow_open=allow_open)
        return ""

    async def _open_session_stream_id(
        self, *, chat_type: str, group_id: str = "", user_id: str = ""
    ) -> str:
        """get_stream_by_* 落空时回退 ``ctx.chat.open_session`` 打开/创建会话，返回其 session_id。

        open_session 不在 SDK 归一化白名单内，返回 Host 完整结果
        （含 success / created / stream_id / session_id / stream）；失败信封 success=False。
        失败仅 debug 并返回空串，由调用方按"无 stream_id"降级（如回退到回戳）。
        """
        try:
            result = await self.ctx.chat.open_session(
                platform="qq", chat_type=chat_type, group_id=group_id, user_id=user_id,
            )
        except Exception:
            self.ctx.logger.debug(
                "chat.open_session 调用异常 (chat_type=%s, group=%s, user=%s)",
                chat_type, group_id, user_id, exc_info=True,
            )
            return ""
        if not isinstance(result, dict):
            return ""
        if result.get("success") is False:
            self.ctx.logger.debug(
                "chat.open_session 业务失败 (chat_type=%s, group=%s, user=%s): %s",
                chat_type, group_id, user_id, result.get("error"),
            )
            return ""
        stream_id = str(result.get("session_id") or result.get("stream_id") or "")
        if stream_id:
            self.ctx.logger.debug(
                "chat.open_session 已%s会话 (chat_type=%s, group=%s, user=%s, stream=%s)",
                "创建" if result.get("created") else "命中",
                chat_type, group_id, user_id, stream_id,
            )
        return stream_id

    # ===== Maisaka 上下文注入 =====

    async def _append_self_event_to_context(
        self, *, stream_id: str, text: str, label: str
    ) -> None:
        """底层：把一条文本事件追加进 Maisaka 上下文，统一异常 / resp 处理与日志。"""
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

        display_name = (target_name or "").strip() or target_id
        # 用 QQ 原生戳一戳提示风格融入上下文，而非生硬的「系统事件」旁白；
        # 整个 prompt 里 LLM 扮演麦麦，"我"即指麦麦，与"X戳了戳我"成对、语义清晰。
        text = f"我戳了戳{display_name}"

        await self._append_self_event_to_context(
            stream_id=stream_id, text=text, label=label,
        )

    async def record_poked_by_to_context(self, ctx: PokeContext) -> None:
        """开启写入上下文时，把"对方戳了麦麦"先记入上下文，作为麦麦随后回复 / 回戳的前因，
        避免上下文里只剩麦麦的回复（如"干嘛戳我"）却不知道在回应谁。

        用 QQ 原生戳一戳提示风格（"X戳了戳我"）记一行文本——戳一戳走 adapter API、没有对应
        的真实消息，只能 append 而非走 send 同步。经 maisaka.context.append(get_or_create)
        会确保该会话 runtime 存在，从而后续回复的 ctx.send sync 也一定能记入。
        """
        if not self.config.plugin.record_self_poke_to_context:
            return
        stream_id = await self.resolve_stream_id_for_context(ctx)
        if not stream_id:
            self.ctx.logger.debug(
                "[poked] 无法解析 stream_id，跳过被戳事件注入 (poker=%s)", ctx.poker_id,
            )
            return
        poker = (ctx.poker_name or "").strip() or ctx.poker_id or "对方"
        action = (ctx.poke_action or "").strip() or "戳了戳"
        text = f"{poker}{action}我"
        await self._append_self_event_to_context(
            stream_id=stream_id, text=text, label="poked",
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

        # 戳事件（戳麦麦 / 别人互戳）是会写入冷却、暴戳计数、跟风冷却等状态字典的唯一入口，
        # 且与 proactive.enabled 无关：在此按时间节流触发一次状态清理（O(1) 检查，实际
        # _prune 最快每 _PRUNE_MIN_INTERVAL_SECONDS 一次）。兜底覆盖 observe_signal 的
        # maybe_prune 够不到的路径——主动戳关闭、或纯「别人互戳跟风」场景下，否则 _prune
        # 只能靠「戳麦麦累计 _PRUNE_THRESHOLD 次」触发，这些状态字典的清理会长期停滞。
        self._state.maybe_prune()

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
            window_count = self._state.peek_reaction_window(ctx.cooldown_key, 60)
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

        # 接住 mark 返回的时间戳令牌透传给后台反应：当 max_delay_seconds > cooldown_seconds
        # 时，同一人在思考延迟窗口内再戳会派发新的反应任务，react_to_poke 据此在发送前
        # 确认自己未被更晚的反应取代，避免叠加回复。
        react_token = self._state.mark_reacted(ctx.cooldown_key, ctx.poker_id)

        self._spawn_background_task(
            self._reaction.react_to_poke(ctx, is_spam, poke_count, react_token),
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
        # group_info / user_info 仅是辅助来源（group_id 回退源、poker 昵称）；异常适配器可能
        # 把它们填成非 dict 真值（字符串/列表），用 isinstance 降级为空 dict 而非 return None：
        # 戳事件的关键字段 self_id/poker_id/target_id 已从 payload 取得并校验，不应因辅助字段
        # 类型异常而丢弃整条合法戳事件（与 _extract_signal 缺 group_info 必须 return None 不同）。
        group_info = msg_info.get("group_info")
        if not isinstance(group_info, dict):
            group_info = {}
        raw_group_id = payload.get("group_id")
        if raw_group_id is None or str(raw_group_id).strip() in ("", "0"):
            raw_group_id = group_info.get("group_id")
        group_int = to_positive_int(raw_group_id)

        user_info = msg_info.get("user_info")
        if not isinstance(user_info, dict):
            user_info = {}

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
        # 冷却维度用稳定的 group_id（群聊）/ poker_id（私聊），不混入 stream_id：
        # notice 的 session_id 偶尔缺失会让 cooldown_key 在 stream_id 与 group_id 间漂移、
        # 逐人冷却分裂成两个 key 而短暂失效。stream_id 只用于发送，不参与冷却 key。
        ctx.cooldown_key = ctx.group_id or ctx.poker_id
        ctx.spam_scope_key = ctx.group_id if ctx.is_group else ctx.poker_id
        ctx.poke_action = self._extract_poke_action(payload)
        return ctx

    @staticmethod
    def _extract_poke_action(payload: dict) -> str:
        """从 napcat poke notice 的 raw_info 提取自定义戳一戳动作文本（如"拍了拍""捏了捏"）。

        napcat 在 poke notice 的 raw_info 里以
        ``[{"type":"nor","txt":"拍了拍"}, {"type":"qq", ...}, {"type":"nor","txt":"的脸"}]``
        形式给出，第一个 ``nor`` 文本即动作词；取不到（旧版 / 无该字段）返回空串，
        由调用方兜底为"戳了戳"，因此 napcat 不发 raw_info 时也不会回归。
        """
        raw_info = payload.get("raw_info")
        if not isinstance(raw_info, list):
            return ""
        for col in raw_info:
            if isinstance(col, dict) and col.get("type") == "nor":
                txt = str(col.get("txt") or "").strip()
                if txt:
                    return txt
        return ""


def create_plugin() -> SmartPokePlugin:
    """Runner 调用入口。"""
    return SmartPokePlugin()
