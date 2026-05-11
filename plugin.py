"""智能戳一戳插件 — MaiBot SDK v2

监听 QQ 戳一戳事件：
- 戳麦麦本人时按概率作出回戳 / 文字 / 表情 / 沉默反应。
- 别人之间互戳时，麦麦也可按概率「跟风」戳一下（目标可配置为被戳者 / 发起者 / 随机）。

实现要点：
    依赖 MaiBot-Napcat-Adapter 注入的 notice 事件。当 napcat 收到
    notify.poke 事件时，会把 payload 透传到消息 dict 的
    message_info.additional_config 中（napcat_notice_type=notify、
    napcat_notice_sub_type=poke、napcat_notice_payload=<原始 payload>）。
    本插件通过 @HookHandler 订阅 chat.receive.before_process，
    在该 Hook 中识别戳一戳事件，分两种情况：
      - 戳麦麦本人 → 拦截事件 + 异步触发反应任务
      - 戳别人 → 不拦截事件，仅按概率异步触发跟风戳任务
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import defaultdict
from typing import Any, Dict, List, Literal, Optional

from maibot_sdk import Field, HookHandler, MaiBotPlugin, PluginConfigBase
from maibot_sdk.types import ErrorPolicy, HookMode, HookOrder
from pydantic import field_validator


logger = logging.getLogger("plugin.smart_poke")

PLUGIN_VERSION = "1.1.0"


# --- 配置模型 ---


class PluginSection(PluginConfigBase):
    """插件基础元信息。"""

    __ui_label__ = "插件设置"

    name: str = Field(
        default="smart_poke_plugin",
        description="插件名称",
        json_schema_extra={"disabled": True},
    )
    version: str = Field(
        default=PLUGIN_VERSION,
        description="插件版本",
        json_schema_extra={"disabled": True},
    )
    config_version: str = Field(
        default=PLUGIN_VERSION,
        description="配置文件版本",
        json_schema_extra={"disabled": True},
    )
    enabled: bool = Field(
        default=True,
        description="是否启用插件",
        json_schema_extra={"label": "启用插件"},
    )


class ReactionSection(PluginConfigBase):
    """反应行为配置。"""

    __ui_label__ = "反应行为"

    react_probability: float = Field(
        default=0.85,
        ge=0.0,
        le=1.0,
        description="被戳时整体作出反应的概率",
        json_schema_extra={
            "label": "反应概率",
            "hint": "0~1，越大越爱搭理",
            "x-widget": "slider",
            "min": 0.0,
            "max": 1.0,
            "step": 0.05,
        },
    )
    back_poke_probability: float = Field(
        default=0.45,
        ge=0.0,
        le=1.0,
        description="反应时选择回戳的占比（与表情、文字、沉默互斥分配）",
        json_schema_extra={"label": "回戳占比"},
    )
    emoji_probability: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="反应时选择发表情包的占比",
        json_schema_extra={"label": "表情占比"},
    )
    silent_probability: float = Field(
        default=0.05,
        ge=0.0,
        le=1.0,
        description="反应时选择沉默的占比",
        json_schema_extra={"label": "沉默占比"},
    )
    text_probability: float = Field(
        default=0.2,
        ge=0.0,
        le=1.0,
        description="反应时选择文字回复的占比；四种反应权重会按总和归一化，不要求加起来等于 1",
        json_schema_extra={"label": "文字占比"},
    )

    min_delay_seconds: float = Field(
        default=1.0,
        ge=0.0,
        le=30.0,
        description="作出反应的最小延迟（秒），模拟「人在思考」",
        json_schema_extra={"label": "最小延迟（秒）"},
    )
    max_delay_seconds: float = Field(
        default=3.5,
        ge=0.0,
        le=60.0,
        description="作出反应的最大延迟（秒）",
        json_schema_extra={"label": "最大延迟（秒）"},
    )

    cooldown_seconds: int = Field(
        default=8,
        ge=0,
        le=600,
        description="同一聊天的反应冷却时间（秒）；冷却期内被戳一律忽略",
        json_schema_extra={"label": "冷却时长（秒）"},
    )

    spam_threshold: int = Field(
        default=3,
        ge=2,
        le=50,
        description="在判定窗口内被同一人戳到该次数后进入「被烦」状态",
        json_schema_extra={"label": "暴戳阈值"},
    )
    spam_window_seconds: int = Field(
        default=30,
        ge=5,
        le=600,
        description="暴戳判定窗口长度（秒）",
        json_schema_extra={"label": "暴戳窗口（秒）"},
    )

    react_in_group: bool = Field(
        default=True,
        description="是否在群聊中响应戳一戳",
        json_schema_extra={"label": "群聊响应"},
    )
    react_in_private: bool = Field(
        default=True,
        description="是否在私聊中响应戳一戳",
        json_schema_extra={"label": "私聊响应"},
    )


class FallbackSection(PluginConfigBase):
    """文字回复随机池。"""

    __ui_label__ = "文字回复"

    normal_replies: List[str] = Field(
        default_factory=lambda: [
            "干嘛戳我",
            "诶诶诶，戳什么戳",
            "？？？",
            "干啥",
        ],
        description="普通情况下的文字回复随机池",
        json_schema_extra={"label": "普通回复池"},
    )
    spam_replies: List[str] = Field(
        default_factory=lambda: [
            "你戳够了没",
            "好烦别戳了",
            "你是不是没事干",
            "停！手！",
            "烦不烦",
            "SB吧",
        ],
        description="被暴戳后的文字回复随机池",
        json_schema_extra={"label": "暴戳回复池"},
    )
    silent_replies: List[str] = Field(
        default_factory=lambda: [
            "...",
            "地铁老人手机.jpg",
            "懒得理",
        ],
        description="选择「沉默」反应时偶尔会发出的极简内容",
        json_schema_extra={"label": "沉默回复池"},
    )


class UserControlSection(PluginConfigBase):
    """用户控制：黑名单 / 自戳处理。"""

    __ui_label__ = "用户控制"

    blacklist: List[str] = Field(
        default_factory=list,
        description="黑名单 QQ 列表，被戳后插件会静默（不反应，不让事件继续传播）",
        json_schema_extra={"label": "黑名单 QQ"},
    )
    ignore_self_poke: bool = Field(
        default=True,
        description="是否忽略麦麦戳麦麦自己（避免回声）",
        json_schema_extra={"label": "忽略自戳"},
    )


class EmojiSection(PluginConfigBase):
    """表情包反应配置。"""

    __ui_label__ = "表情包反应"

    description_keywords: List[str] = Field(
        default_factory=lambda: ["疑惑", "无奈", "生气", "无语", "哼", "瞪"],
        description="选择表情包时使用的描述关键词，将随机抽取其一调用 emoji.get_by_description",
        json_schema_extra={"label": "表情关键词"},
    )
    min_similarity: float = Field(
        default=0.45,
        ge=0.0,
        le=1.0,
        description="按关键词匹配到的表情包相似度阈值，低于此值将放弃发表情、回退到回戳",
        json_schema_extra={
            "label": "表情相似度阈值",
            "x-widget": "slider",
            "min": 0.0,
            "max": 1.0,
            "step": 0.05,
        },
    )
    allow_random_fallback: bool = Field(
        default=False,
        description="按关键词没找到合适表情时是否回退到随机表情（默认关闭，更倾向于回戳）",
        json_schema_extra={"label": "允许随机表情"},
    )


class BystanderSection(PluginConfigBase):
    """跟风戳：别人之间互戳时，麦麦也跟着戳一下。"""

    __ui_label__ = "跟风戳"

    enabled: bool = Field(
        default=True,
        description="是否启用跟风戳：检测到别人互戳时麦麦也跟着戳一下",
        json_schema_extra={"label": "启用跟风戳"},
    )
    probability: float = Field(
        default=0.15,
        ge=0.0,
        le=1.0,
        description="别人互戳时麦麦跟风戳的概率",
        json_schema_extra={
            "label": "跟风戳概率",
            "x-widget": "slider",
            "min": 0.0,
            "max": 1.0,
            "step": 0.05,
        },
    )
    target_strategy: Literal["victim", "poker", "random"] = Field(
        default="victim",
        description=(
            "跟风目标策略："
            "victim=戳被戳者（跟着欺负他）；"
            "poker=戳发起者（替被戳者还击）；"
            "random=两者随机各一半"
        ),
        json_schema_extra={
            "label": "目标策略",
            "x-widget": "select",
            "options": [
                {"value": "victim", "label": "戳被戳者"},
                {"value": "poker", "label": "戳发起者"},
                {"value": "random", "label": "随机选一个"},
            ],
        },
    )
    cooldown_seconds: int = Field(
        default=30,
        ge=0,
        le=600,
        description="同一聊天的跟风戳冷却（独立于戳麦麦的冷却）",
        json_schema_extra={"label": "跟风冷却（秒）"},
    )
    min_delay_seconds: float = Field(
        default=1.5,
        ge=0.0,
        le=30.0,
        description="跟风戳的最小延迟，避免戳一戳消息列里挤一起",
        json_schema_extra={"label": "最小延迟（秒）"},
    )
    max_delay_seconds: float = Field(
        default=4.0,
        ge=0.0,
        le=60.0,
        description="跟风戳的最大延迟",
        json_schema_extra={"label": "最大延迟（秒）"},
    )

    @field_validator("target_strategy", mode="before")
    @classmethod
    def _normalize_target_strategy(cls, value: Any) -> Literal["victim", "poker", "random"]:
        normalized = "" if value is None else str(value).strip().lower()
        if normalized in ("victim", "poker", "random"):
            return normalized  # type: ignore[return-value]
        return "victim"


class SmartPokeConfig(PluginConfigBase):
    """智能戳一戳插件完整配置。"""

    plugin: PluginSection = Field(default_factory=PluginSection)
    reaction: ReactionSection = Field(default_factory=ReactionSection)
    fallback: FallbackSection = Field(default_factory=FallbackSection)
    user_control: UserControlSection = Field(default_factory=UserControlSection)
    emoji: EmojiSection = Field(default_factory=EmojiSection)
    bystander: BystanderSection = Field(default_factory=BystanderSection)


# --- 数据结构 ---


class PokeContext:
    """从一次戳一戳事件中提取出的关键信息。"""

    __slots__ = (
        "is_valid",
        "is_poking_bot",
        "is_group",
        "self_id",
        "poker_id",
        "poker_name",
        "target_id",
        "target_name",
        "group_id",
        "group_name",
        "stream_id",
        "raw_payload",
    )

    def __init__(self) -> None:
        self.is_valid: bool = False
        self.is_poking_bot: bool = False
        self.is_group: bool = False
        self.self_id: str = ""
        self.poker_id: str = ""
        self.poker_name: str = ""
        self.target_id: str = ""
        self.target_name: str = ""
        self.group_id: str = ""
        self.group_name: str = ""
        self.stream_id: str = ""
        self.raw_payload: Dict[str, Any] = {}


class PokeStateManager:
    """跨实例共享的状态：冷却时间戳与每用户戳次数窗口。"""

    _last_react_at: Dict[str, float] = {}
    _poke_records: Dict[str, List[float]] = defaultdict(list)
    _last_bystander_at: Dict[str, float] = {}

    @classmethod
    def in_cooldown(cls, stream_id: str, cooldown_seconds: int) -> bool:
        if cooldown_seconds <= 0 or not stream_id:
            return False
        last = cls._last_react_at.get(stream_id, 0.0)
        return (time.time() - last) < cooldown_seconds

    @classmethod
    def mark_reacted(cls, stream_id: str) -> None:
        if stream_id:
            cls._last_react_at[stream_id] = time.time()

    @classmethod
    def in_bystander_cooldown(cls, stream_id: str, cooldown_seconds: int) -> bool:
        if cooldown_seconds <= 0 or not stream_id:
            return False
        last = cls._last_bystander_at.get(stream_id, 0.0)
        return (time.time() - last) < cooldown_seconds

    @classmethod
    def mark_bystander(cls, stream_id: str) -> None:
        if stream_id:
            cls._last_bystander_at[stream_id] = time.time()

    @classmethod
    def record_poke_and_count(
        cls, stream_id: str, poker_id: str, window_seconds: int
    ) -> int:
        """记录一次戳，返回窗口期内的累计次数。"""
        key = f"{stream_id}:{poker_id}"
        now = time.time()
        cutoff = now - max(window_seconds, 1)
        records = [t for t in cls._poke_records[key] if t >= cutoff]
        records.append(now)
        cls._poke_records[key] = records
        return len(records)

    @classmethod
    def clear(cls) -> None:
        cls._last_react_at.clear()
        cls._poke_records.clear()
        cls._last_bystander_at.clear()


# --- 辅助函数 ---


def _safe_int(value: Any) -> Optional[int]:
    """把任意输入安全转成正整数，失败返回 None。"""
    try:
        text = str(value).strip()
        if not text:
            return None
        result = int(text)
        if result <= 0:
            return None
        return result
    except (TypeError, ValueError):
        return None


# --- 主插件 ---


class SmartPokePlugin(MaiBotPlugin):
    """智能戳一戳插件主类。

    工作流程：
        1. 收到 chat.receive.before_process Hook。
        2. 从 message.message_info.additional_config 判断是否 napcat 的 notify.poke 事件。
        3. 只在 target_id == self_id（戳麦麦本人）时拦截事件并触发反应。
        4. 异步任务按概率执行回戳 / 文字 / 表情 / 沉默。
    """

    config_model = SmartPokeConfig

    def __init__(self) -> None:
        super().__init__()
        self._blacklist: set[str] = set()
        self._pending_tasks: set[asyncio.Task] = set()

    # ===== 生命周期 =====

    async def on_load(self) -> None:
        self._refresh_user_sets()
        logger.info("智能戳一戳插件 v%s 已就绪。", PLUGIN_VERSION)

    async def on_unload(self) -> None:
        # 取消未完成的反应任务，避免 Runner 卸载后还在调用 capability
        for task in list(self._pending_tasks):
            if not task.done():
                task.cancel()
        self._pending_tasks.clear()
        PokeStateManager.clear()

    async def on_config_update(
        self, scope: str, config_data: dict, version: str
    ) -> None:
        if scope == "self":
            self._refresh_user_sets()
            logger.info("配置已热更新到 v%s。", version)

    def _refresh_user_sets(self) -> None:
        """把配置中的黑名单归一化成字符串集合，便于 O(1) 查询。"""
        cfg = self.config
        self._blacklist = {str(x).strip() for x in cfg.user_control.blacklist if str(x).strip()}

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
    async def handle_poke_event(self, message: Optional[dict] = None, **kwargs):
        del kwargs

        if not self.config.plugin.enabled:
            return None

        ctx = self._extract_poke_context(message)
        if not ctx.is_valid:
            # 不是戳一戳事件 → 放行
            return None

        # ----- 分支一：戳的不是麦麦（别人互戳）-----
        if not ctx.is_poking_bot:
            # 不拦截事件，仅尝试触发跟风戳
            self._maybe_trigger_bystander(ctx)
            return None

        # ----- 分支二：戳的是麦麦本人 -----

        # 麦麦戳麦麦自己：根据开关决定
        if ctx.poker_id == ctx.self_id and self.config.user_control.ignore_self_poke:
            return {"action": "abort"}

        # 黑名单：拦截但不反应
        if ctx.poker_id in self._blacklist:
            logger.debug("黑名单用户 %s 的戳一戳已静默拦截", ctx.poker_id)
            return {"action": "abort"}

        # 群聊/私聊总开关
        if ctx.is_group and not self.config.reaction.react_in_group:
            return {"action": "abort"}
        if not ctx.is_group and not self.config.reaction.react_in_private:
            return {"action": "abort"}

        # 冷却
        if PokeStateManager.in_cooldown(
            ctx.stream_id, self.config.reaction.cooldown_seconds
        ):
            logger.debug("[%s] 戳一戳冷却中，已拦截", ctx.stream_id or ctx.group_id or ctx.poker_id)
            return {"action": "abort"}

        # 反应概率
        if random.random() > self.config.reaction.react_probability:
            logger.debug("[%s] 戳一戳触发概率未命中，静默拦截", ctx.stream_id or ctx.poker_id)
            return {"action": "abort"}

        # 记录一次戳并判定是否进入「被烦」
        poke_count = PokeStateManager.record_poke_and_count(
            ctx.stream_id or ctx.group_id or ctx.poker_id,
            ctx.poker_id,
            self.config.reaction.spam_window_seconds,
        )
        is_spam = poke_count >= self.config.reaction.spam_threshold
        PokeStateManager.mark_reacted(ctx.stream_id)

        # 异步执行反应（避免阻塞 Hook 链）
        task = asyncio.create_task(self._react(ctx, is_spam, poke_count))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

        # 拦截事件，避免 Host 把 "XX 发起了戳一戳" 当成普通消息再走一遍消息流程
        return {"action": "abort"}

    # ===== 跟风戳 =====

    def _maybe_trigger_bystander(self, ctx: PokeContext) -> None:
        """检查是否应当对一次"别人互戳"事件做跟风反应。"""
        cfg = self.config.bystander
        if not cfg.enabled:
            return
        # 跟风戳只在群聊场景成立：私聊本来就只有用户和麦麦两人，没有第三方目标。
        if not ctx.is_group:
            return
        # 戳的人或被戳的人是麦麦自己：交给主分支处理
        if ctx.poker_id == ctx.self_id or ctx.target_id == ctx.self_id:
            return
        # 黑名单中的人发起的戳不响应
        if ctx.poker_id in self._blacklist:
            return
        # 冷却
        bystander_key = ctx.stream_id or ctx.group_id
        if PokeStateManager.in_bystander_cooldown(bystander_key, cfg.cooldown_seconds):
            return
        # 概率
        if random.random() > cfg.probability:
            return

        # 选定跟风目标
        target_id = self._pick_bystander_target(ctx)
        if not target_id:
            return
        # 目标在黑名单也不戳，避免被反向"恶意操控"
        if target_id in self._blacklist:
            return

        PokeStateManager.mark_bystander(bystander_key)

        task = asyncio.create_task(self._react_bystander(ctx, target_id))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    def _pick_bystander_target(self, ctx: PokeContext) -> str:
        """根据策略挑选要跟风戳的对象。"""
        strategy = self.config.bystander.target_strategy
        if strategy == "victim":
            return ctx.target_id
        if strategy == "poker":
            return ctx.poker_id
        # random
        return random.choice([ctx.target_id, ctx.poker_id])

    async def _react_bystander(self, ctx: PokeContext, target_id: str) -> None:
        """跟风戳：延迟后给指定目标戳一下，纯戳不发文字。"""
        try:
            cfg = self.config.bystander
            lo = max(0.0, cfg.min_delay_seconds)
            hi = max(lo, cfg.max_delay_seconds)
            delay = random.uniform(lo, hi) if hi > 0 else 0
            if delay > 0:
                await asyncio.sleep(delay)

            target_int = _safe_int(target_id)
            if target_int is None:
                return
            call_kwargs: Dict[str, Any] = {"user_id": target_int}
            if ctx.is_group:
                group_int = _safe_int(ctx.group_id)
                if group_int is None:
                    return
                call_kwargs["group_id"] = group_int

            try:
                resp = await self.ctx.api.call(
                    "adapter.napcat.message.send_poke", **call_kwargs
                )
                if isinstance(resp, dict) and resp.get("success") is False:
                    logger.warning("跟风戳调用失败: %s", resp.get("error"))
                    return
                logger.info(
                    "[smart_poke] 跟风戳完成: strategy=%s, target=%s (poker=%s, victim=%s)",
                    self.config.bystander.target_strategy,
                    target_id, ctx.poker_id, ctx.target_id,
                )
            except Exception:
                logger.exception("跟风戳调用异常")
        except Exception:
            logger.exception("跟风戳任务异常")

    # ===== 反应主流程 =====

    async def _react(self, ctx: PokeContext, is_spam: bool, poke_count: int) -> None:
        """决定反应类型并执行；外层捕获所有异常防止背景任务挂掉。"""
        try:
            await self._delay_a_bit()

            # napcat 注入的 notice 消息 session_id 形如 "napcat-notice-xxx"，
            # 用它发消息会让 Host 内部 hook 找不到合法的 group_info，
            # 因此群聊/私聊场景都强制按 group_id / user_id 回查真实 stream_id。
            stream_id = await self._resolve_stream_id(ctx)
            if not stream_id:
                stream_id = ctx.stream_id  # 兜底，避免完全发不出
            if not stream_id and ctx.is_group:
                logger.warning("无法解析群 %s 的 stream_id，放弃反应", ctx.group_id)
                return

            kind = self._decide_reaction_kind(is_spam)
            logger.info(
                "[smart_poke] 触发反应: kind=%s, is_spam=%s, poke_count=%d, poker=%s, scene=%s",
                kind,
                is_spam,
                poke_count,
                ctx.poker_name or ctx.poker_id,
                "群聊" if ctx.is_group else "私聊",
            )

            if kind == "poke":
                ok = await self._send_back_poke(ctx)
                if not ok and stream_id:
                    # 回戳失败 → 回退文字
                    await self._send_text(ctx, stream_id, is_spam, poke_count)
                return

            if kind == "emoji":
                ok = await self._send_emoji(stream_id) if stream_id else False
                if ok:
                    return
                # 表情匹配不到或相似度太低 → 回退到回戳
                logger.debug("表情反应失败/被相似度阈值过滤，回退到回戳")
                ok_poke = await self._send_back_poke(ctx)
                if not ok_poke and stream_id:
                    # 回戳也失败再退到文字
                    await self._send_text(ctx, stream_id, is_spam, poke_count)
                return

            if kind == "silent":
                # 小概率挤出一句极简回复，否则真的沉默
                if stream_id and random.random() < 0.3:
                    pool = self.config.fallback.silent_replies
                    if pool:
                        await self._safe_send_text(random.choice(pool), stream_id)
                return

            # 默认：文字
            if stream_id:
                await self._send_text(ctx, stream_id, is_spam, poke_count)
        except Exception:
            logger.exception("反应戳一戳时出错")

    async def _delay_a_bit(self) -> None:
        """按配置范围随机延迟，模拟思考时间。"""
        cfg = self.config.reaction
        lo = max(0.0, cfg.min_delay_seconds)
        hi = max(lo, cfg.max_delay_seconds)
        delay = random.uniform(lo, hi) if hi > 0 else 0
        if delay > 0:
            await asyncio.sleep(delay)

    def _decide_reaction_kind(self, is_spam: bool) -> str:
        """按配置权重选择反应类型。返回 'poke' / 'emoji' / 'silent' / 'text'。

        四个权重独立配置，不要求加起来等于 1：取它们的总和后归一化分配。
        暴戳时会削弱回戳权重、抬高沉默和文字权重，让麦麦更倾向"嫌烦"而不是回戳。
        """
        cfg = self.config.reaction
        spam_mult_text = 1.5 if is_spam else 1.0
        weights = {
            "poke": max(0.0, cfg.back_poke_probability * (0.5 if is_spam else 1.0)),
            "emoji": max(0.0, cfg.emoji_probability),
            "silent": max(0.0, cfg.silent_probability + (0.15 if is_spam else 0.0)),
            "text": max(0.0, cfg.text_probability * spam_mult_text),
        }
        total = sum(weights.values())
        if total <= 0:
            return "text"  # 全部权重为 0 时兜底为文字

        roll = random.random() * total
        cumulative = 0.0
        for kind, weight in weights.items():
            cumulative += weight
            if roll < cumulative:
                return kind
        return "text"

    # ===== 反应实现 =====

    async def _send_back_poke(self, ctx: PokeContext) -> bool:
        """通过 NapCat 适配器发送回戳，返回是否成功。

        群聊和私聊统一调用 ``adapter.napcat.message.send_poke``：
        - 群聊：同时传 ``user_id``（被戳者）和 ``group_id``。
        - 私聊：仅传 ``user_id``，省略 ``group_id``。
        """
        poker_int = _safe_int(ctx.poker_id)
        if poker_int is None:
            return False

        call_kwargs: Dict[str, Any] = {"user_id": poker_int}
        if ctx.is_group:
            group_int = _safe_int(ctx.group_id)
            if group_int is None:
                return False
            call_kwargs["group_id"] = group_int

        try:
            resp = await self.ctx.api.call(
                "adapter.napcat.message.send_poke", **call_kwargs
            )
            if isinstance(resp, dict) and resp.get("success") is False:
                logger.warning("回戳调用失败: %s", resp.get("error"))
                return False
            return True
        except Exception:
            logger.exception("发送回戳异常")
            return False

    async def _send_text(
        self,
        ctx: PokeContext,
        stream_id: str,
        is_spam: bool,
        poke_count: int,
    ) -> None:
        """从配置的回复池中随机挑一句文字发出。"""
        del ctx, poke_count  # 当前实现不依赖 ctx 和计数
        pool = (
            self.config.fallback.spam_replies
            if is_spam
            else self.config.fallback.normal_replies
        )
        text = random.choice(pool) if pool else "..."
        await self._safe_send_text(text, stream_id)

    async def _safe_send_text(self, text: str, stream_id: str) -> None:
        try:
            await self.ctx.send.text(text, stream_id)
        except Exception:
            logger.exception("发送文字回复失败")

    async def _send_emoji(self, stream_id: str) -> bool:
        """发送一张表情包。优先按关键词搜索，找不到则随机。"""
        try:
            emoji = await self._pick_emoji()
            if not emoji:
                return False

            emoji_data = ""
            if isinstance(emoji, dict):
                for key in (
                    "emoji_base64",
                    "base64",
                    "image_base64",
                    "data",
                    "content",
                ):
                    value = emoji.get(key)
                    if isinstance(value, str) and value:
                        emoji_data = value
                        break

            if not emoji_data:
                return False

            await self.ctx.send.emoji(emoji_data, stream_id)
            return True
        except Exception:
            logger.exception("发送表情失败")
            return False

    async def _pick_emoji(self) -> Optional[Dict[str, Any]]:
        """按关键词搜表情包，相似度低于阈值或没结果时返回 None。

        默认不回退到随机表情，因为随机表情与"被戳"语境无关，
        让 _react 在 None 时回退去回戳更合理。可在配置里打开 allow_random_fallback。
        """
        keywords = [k for k in self.config.emoji.description_keywords if str(k).strip()]
        threshold = self.config.emoji.min_similarity

        if keywords:
            try:
                kw = random.choice(keywords)
                hit = await self.ctx.emoji.get_by_description(kw, limit=1)
                candidate: Optional[Dict[str, Any]] = None
                if isinstance(hit, list) and hit and isinstance(hit[0], dict):
                    candidate = hit[0]
                elif isinstance(hit, dict):
                    candidate = hit

                if candidate is not None:
                    similarity = self._extract_emoji_similarity(candidate)
                    if similarity >= threshold:
                        return candidate
                    logger.debug(
                        "表情匹配相似度 %.3f 低于阈值 %.2f（关键词=%s），放弃发表情",
                        similarity, threshold, kw,
                    )
            except Exception:
                logger.debug("emoji.get_by_description 失败", exc_info=True)

        if not self.config.emoji.allow_random_fallback:
            return None

        try:
            rand = await self.ctx.emoji.get_random(1)
            if isinstance(rand, list) and rand:
                return rand[0] if isinstance(rand[0], dict) else None
            if isinstance(rand, dict):
                return rand
        except Exception:
            logger.debug("emoji.get_random 失败", exc_info=True)
        return None

    @staticmethod
    def _extract_emoji_similarity(emoji: Dict[str, Any]) -> float:
        """从表情字典里尽量找到相似度数值；找不到视为 0。"""
        for key in ("similarity", "score", "similarity_score", "match_score"):
            value = emoji.get(key)
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                try:
                    return float(value)
                except ValueError:
                    continue
        return 0.0

    # ===== 信息提取 =====

    def _extract_poke_context(self, message: Any) -> PokeContext:
        """从消息 dict 中提取戳一戳信息。

        ``is_valid`` 表示这是一次成功识别的戳一戳事件；
        ``is_poking_bot`` 进一步区分目标是不是麦麦本人。
        """
        ctx = PokeContext()
        if not isinstance(message, dict):
            return ctx
        if not message.get("is_notify"):
            return ctx

        msg_info = message.get("message_info") or {}
        if not isinstance(msg_info, dict):
            return ctx
        additional = msg_info.get("additional_config") or {}
        if not isinstance(additional, dict):
            return ctx

        if additional.get("napcat_notice_type") != "notify":
            return ctx
        if additional.get("napcat_notice_sub_type") != "poke":
            return ctx

        payload = additional.get("napcat_notice_payload") or {}
        if not isinstance(payload, dict):
            return ctx

        self_id = str(payload.get("self_id") or additional.get("self_id") or "").strip()
        poker_id = str(payload.get("user_id") or "").strip()
        target_id = str(payload.get("target_id") or "").strip()
        group_id = str(payload.get("group_id") or "").strip()

        if not self_id or not poker_id or not target_id:
            return ctx

        user_info = msg_info.get("user_info") or {}
        group_info = msg_info.get("group_info") or {}

        ctx.is_valid = True
        ctx.is_poking_bot = target_id == self_id
        ctx.self_id = self_id
        ctx.poker_id = poker_id
        ctx.poker_name = str(user_info.get("user_nickname") or user_info.get("nickname") or "")
        ctx.target_id = target_id
        # napcat 注入的 user_info 是发起方的，target 这里没法直接拿到昵称，
        # 后续如有需要可以通过 ctx.api.call("adapter.napcat.group.get_group_member_info") 查询，
        # 但戳一戳本身不需要昵称，这里留空即可。
        ctx.target_name = ""
        ctx.group_id = group_id or str(group_info.get("group_id") or "")
        ctx.group_name = str(group_info.get("group_name") or "")
        ctx.is_group = bool(ctx.group_id)
        ctx.stream_id = str(message.get("session_id") or "")
        ctx.raw_payload = payload
        return ctx

    async def _resolve_stream_id(self, ctx: PokeContext) -> str:
        """当 message.session_id 为空时，按群/用户回查 stream_id。"""
        try:
            stream: Any
            if ctx.is_group and ctx.group_id:
                stream = await self.ctx.chat.get_stream_by_group_id(
                    ctx.group_id, platform="qq"
                )
            elif ctx.poker_id:
                stream = await self.ctx.chat.get_stream_by_user_id(
                    ctx.poker_id, platform="qq"
                )
            else:
                return ""
            if isinstance(stream, dict):
                return str(
                    stream.get("session_id")
                    or stream.get("stream_id")
                    or stream.get("id")
                    or ""
                )
        except Exception:
            logger.debug("回查 stream_id 失败", exc_info=True)
        return ""


def create_plugin() -> SmartPokePlugin:
    """Runner 调用入口。"""
    return SmartPokePlugin()
