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
import json
import random
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from maibot_sdk import Field, HookHandler, MaiBotPlugin, PluginConfigBase
from maibot_sdk.types import ErrorPolicy, HookMode, HookOrder
from pydantic import field_validator


def _load_manifest_version() -> str:
    """从 _manifest.json 读取版本号，保持插件元数据单一来源。"""
    try:
        manifest_path = Path(__file__).parent / "_manifest.json"
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        version = data.get("version")
        if isinstance(version, str) and version.strip():
            return version.strip()
    except Exception:
        pass
    return "0.0.0"


PLUGIN_VERSION = _load_manifest_version()


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
    back_poke_weight: float = Field(
        default=0.4,
        ge=0.0,
        le=1.0,
        description="反应时选择回戳的权重（与表情、文字按总和归一化分配）",
        json_schema_extra={"label": "回戳权重"},
    )
    emoji_weight: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="反应时选择发表情包的权重",
        json_schema_extra={"label": "表情权重"},
    )
    silent_chat_probability: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description=(
            "react_probability 未命中时仍然挤一句 silent_replies 的概率。"
            "意义上等价于「装看不见但偶尔抱怨一下」；命中后会消耗冷却，避免短时间内反复挤话"
        ),
        json_schema_extra={
            "label": "沉默时发言概率",
            "hint": "未命中反应概率时，挤一句 fallback.silent_replies 的概率（会消耗冷却）",
            "x-widget": "slider",
            "min": 0.0,
            "max": 1.0,
            "step": 0.05,
        },
    )
    text_weight: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="反应时选择文字回复的权重；三种权重按总和归一化，不要求加起来等于 1",
        json_schema_extra={"label": "文字权重"},
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
        default=0.85,
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
        "stream_id",
        "cooldown_key",
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
        self.stream_id: str = ""
        # napcat 注入的 notice 消息 session_id 常为空，需要按 stream_id → group_id → poker_id 回退
        self.cooldown_key: str = ""


class PokeStateManager:
    """每个插件实例独立持有的状态：冷却时间戳与每用户戳次数窗口。

    设计要点：
    - 戳麦麦冷却按 ``scope_key:poker_id`` 维度计：不同人独立冷却，A 触发冷却不阻挡 B。
    - 跟风戳冷却按 ``scope_key`` 维度计：避免麦麦在群里跟风刷屏。
    - 暴戳计数同样按 ``scope_key:poker_id`` 维度，与戳麦麦冷却一致。
    - ``scope_key`` 由调用方决定（stream_id → group_id → poker_id 回退），
      避免 napcat 注入的 notice 消息 session_id 为空时冷却完全失效。
    - 字典 key 不会自动消失，因此在 record_poke_and_count 中按计数触发 _prune。
    """

    _PRUNE_THRESHOLD = 200
    _STALE_AFTER_SECONDS = 3600

    def __init__(self) -> None:
        self._last_react_at: Dict[str, float] = {}
        self._poke_records: Dict[str, List[float]] = defaultdict(list)
        self._last_bystander_at: Dict[str, float] = {}
        self._record_counter: int = 0

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

    def mark_reacted(self, scope_key: str, poker_id: str) -> None:
        key = self._cooldown_key(scope_key, poker_id)
        if key:
            self._last_react_at[key] = time.time()

    def in_bystander_cooldown(self, scope_key: str, cooldown_seconds: int) -> bool:
        if cooldown_seconds <= 0 or not scope_key:
            return False
        last = self._last_bystander_at.get(scope_key, 0.0)
        return (time.time() - last) < cooldown_seconds

    def mark_bystander(self, scope_key: str) -> None:
        if scope_key:
            self._last_bystander_at[scope_key] = time.time()

    def record_poke_and_count(
        self, scope_key: str, poker_id: str, window_seconds: int
    ) -> int:
        """记录一次戳，返回窗口期内的累计次数。"""
        if not scope_key:
            return 0
        key = f"{scope_key}:{poker_id}" if poker_id else scope_key
        now = time.time()
        cutoff = now - max(window_seconds, 1)
        records = [t for t in self._poke_records[key] if t >= cutoff]
        records.append(now)
        self._poke_records[key] = records

        self._record_counter += 1
        if self._record_counter >= self._PRUNE_THRESHOLD:
            self._record_counter = 0
            self._prune()
        return len(records)

    def _prune(self) -> None:
        """删除超过 _STALE_AFTER_SECONDS 未更新的 key，控制字典体积。"""
        cutoff = time.time() - self._STALE_AFTER_SECONDS
        self._last_react_at = {k: v for k, v in self._last_react_at.items() if v >= cutoff}
        self._last_bystander_at = {k: v for k, v in self._last_bystander_at.items() if v >= cutoff}
        self._poke_records = defaultdict(
            list,
            {k: v for k, v in self._poke_records.items() if v and max(v) >= cutoff},
        )

    def clear(self) -> None:
        self._last_react_at.clear()
        self._poke_records.clear()
        self._last_bystander_at.clear()
        self._record_counter = 0


# --- 辅助函数 ---


def _to_positive_int(value: Any) -> Optional[int]:
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
        self._state = PokeStateManager()

    # ===== 生命周期 =====

    async def on_load(self) -> None:
        self._refresh_user_sets()
        self.ctx.logger.info("智能戳一戳插件(v%s)初始化完成。", PLUGIN_VERSION)
        # 探测一次表情库的 emotion 标签，提示用户关键词命中情况
        self._spawn_background_task(self._check_emoji_emotions(), "emoji_emotion_check")

    async def on_unload(self) -> None:
        # 取消未完成的反应任务并等待真正终止，避免 Runner 卸载后还在调用 capability
        to_cancel = [t for t in self._pending_tasks if not t.done()]
        for task in to_cancel:
            task.cancel()
        if to_cancel:
            await asyncio.gather(*to_cancel, return_exceptions=True)
        self._pending_tasks.clear()
        self._state.clear()

    async def on_config_update(
        self, scope: str, config_data: dict, version: str
    ) -> None:
        if scope == "self":
            self._refresh_user_sets()
            self.ctx.logger.info("配置已热更新到 v%s。", version)

    def _refresh_user_sets(self) -> None:
        """把配置中的黑名单归一化成字符串集合，便于 O(1) 查询。"""
        cfg = self.config
        self._blacklist = {str(x).strip() for x in cfg.user_control.blacklist if str(x).strip()}

    def _spawn_background_task(self, coro: Any, label: str, timeout: float = 60.0) -> None:
        """提交后台任务并登记到 _pending_tasks；带超时兜底防止挂死。

        子协程内部理应已有 try/except，本封装的超时只是最后一道防护：
        - 60 秒应当足够覆盖任何戳反应路径（最大延迟 + 几次 RPC）。
        - 触发 TimeoutError 时 wait_for 会取消子协程，输出 warning。
        """

        async def _runner() -> None:
            try:
                await asyncio.wait_for(coro, timeout=timeout)
            except asyncio.TimeoutError:
                self.ctx.logger.warning("[%s] 后台任务超时 %ss，已取消", label, timeout)
            except Exception:
                self.ctx.logger.exception("[%s] 后台任务异常", label)

        task = asyncio.create_task(_runner())
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _check_emoji_emotions(self) -> None:
        """启动时探测一次表情库 emotion 标签，与配置的 description_keywords 求交集。

        若完全不交集，说明按关键词搜表情会一直失败，应当提示用户调整 keywords
        或允许随机回退。SDK 已自动解包 emoji.get_emotions，直接返回 emotion 字符串列表。
        启动后稍等数秒再查，避免表情库尚未装载完成时误报。
        """
        await asyncio.sleep(5.0)
        try:
            emotions = await self.ctx.emoji.get_emotions()
            if not isinstance(emotions, list):
                self.ctx.logger.debug("表情库尚未提供 emotion 标签，跳过交集校验")
                return
            emotion_set = {str(e).strip() for e in emotions if str(e).strip()}
            if not emotion_set:
                return

            configured = {
                str(k).strip()
                for k in self.config.emoji.description_keywords
                if str(k).strip()
            }
            if not configured:
                return

            intersection = configured & emotion_set
            if intersection:
                self.ctx.logger.info(
                    "emoji.description_keywords 命中 %d/%d 个 emotion 标签",
                    len(intersection), len(configured),
                )
                return

            sample = ", ".join(sorted(emotion_set)[:10])
            self.ctx.logger.warning(
                "配置的 emoji.description_keywords 与表情库 emotion 标签无交集，"
                "按关键词搜表情会持续失败。可用 emotion 示例: %s",
                sample or "(空)",
            )
        except Exception:
            self.ctx.logger.debug("emoji emotion 校验失败", exc_info=True)

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
            self.ctx.logger.debug("黑名单用户 %s 的戳一戳已静默拦截", ctx.poker_id)
            return {"action": "abort"}

        # 群聊/私聊总开关：关闭场景下放行事件，让 Host 自行处理
        if ctx.is_group and not self.config.reaction.react_in_group:
            return None
        if not ctx.is_group and not self.config.reaction.react_in_private:
            return None

        # 暴戳计数：通过黑名单/场景检查后立即累计，与是否反应解耦，
        # 否则连续戳但概率没命中时永远进不了「被烦」状态。
        poke_count = self._state.record_poke_and_count(
            ctx.cooldown_key,
            ctx.poker_id,
            self.config.reaction.spam_window_seconds,
        )
        is_spam = poke_count >= self.config.reaction.spam_threshold

        # 冷却（按 cooldown_key + poker_id 维度：不同人独立冷却）
        if self._state.in_cooldown(
            ctx.cooldown_key, ctx.poker_id, self.config.reaction.cooldown_seconds
        ):
            self.ctx.logger.debug(
                "[%s:%s] 戳一戳冷却中，已拦截",
                ctx.cooldown_key, ctx.poker_id,
            )
            return {"action": "abort"}

        # 反应概率
        if random.random() > self.config.reaction.react_probability:
            self.ctx.logger.debug(
                "[%s] 戳一戳触发概率未命中，静默拦截", ctx.cooldown_key or ctx.poker_id
            )
            # 未命中也按 silent_chat_probability 概率偶尔挤一句 silent_replies
            chat_prob = self.config.reaction.silent_chat_probability
            if chat_prob > 0 and random.random() < chat_prob:
                # silent_reply 也消耗冷却，避免短时间内被同一人连戳时反复挤话
                self._state.mark_reacted(ctx.cooldown_key, ctx.poker_id)
                self._spawn_background_task(self._silent_reply(ctx), "silent_reply")
            return {"action": "abort"}

        self._state.mark_reacted(ctx.cooldown_key, ctx.poker_id)

        # 异步执行反应（避免阻塞 Hook 链）
        self._spawn_background_task(
            self._react(ctx, is_spam, poke_count),
            "react",
        )

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
        # 冷却：按 cooldown_key 维度（群聊场景下回退到 group_id）
        bystander_key = ctx.cooldown_key
        if self._state.in_bystander_cooldown(bystander_key, cfg.cooldown_seconds):
            return
        # 概率
        if random.random() > cfg.probability:
            return

        # 选定跟风目标
        target_id = self._pick_bystander_target(ctx)
        if not target_id:
            return
        # 黑名单仅决定"麦麦不主动戳此人"——
        # 发起者 (poker) 在黑名单里也不阻止跟风戳被戳者的场景，
        # 只在最终目标命中黑名单时跳过。
        if target_id in self._blacklist:
            return

        self._state.mark_bystander(bystander_key)

        self._spawn_background_task(
            self._react_bystander(ctx, target_id),
            "bystander",
        )

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
        cfg = self.config.bystander
        lo = max(0.0, cfg.min_delay_seconds)
        hi = max(lo, cfg.max_delay_seconds)
        delay = random.uniform(lo, hi) if hi > 0 else 0
        if delay > 0:
            await asyncio.sleep(delay)

        ok = await self._invoke_send_poke(
            target_id, ctx.group_id, is_group=ctx.is_group, label="bystander"
        )
        if ok:
            self.ctx.logger.info(
                "[smart_poke] 跟风戳完成: strategy=%s, target=%s (poker=%s, victim=%s)",
                cfg.target_strategy,
                target_id, ctx.poker_id, ctx.target_id,
            )

    # ===== 反应主流程 =====

    async def _silent_reply(self, ctx: PokeContext) -> None:
        """react_probability 未命中时按 silent_chat_probability 概率挤出的一句轻反应。

        触发前已由 handle_poke_event 调用 mark_reacted 消耗冷却，避免短时间内被同一人
        连戳时反复挤话。与正常反应分支不同：不参与反应类型抽样，只挑一句 silent_replies。
        """
        try:
            await self._delay_a_bit()
            stream_id = await self._resolve_stream_id(ctx)
            if not stream_id:
                return
            pool = self.config.fallback.silent_replies
            if not pool:
                return
            await self._safe_send_text(random.choice(pool), stream_id)
        except Exception:
            self.ctx.logger.exception("silent_reply 发送失败")

    async def _react(self, ctx: PokeContext, is_spam: bool, poke_count: int) -> None:
        """决定反应类型并执行；外层捕获所有异常防止背景任务挂掉。"""
        try:
            await self._delay_a_bit()

            # napcat 注入的 notice 消息 session_id 字段是空字符串，
            # 必须按 group_id / user_id 回查真实 stream_id 才能发文字/表情。
            # 回戳走适配器 send_poke 直接用 group_id/user_id，不依赖 stream_id，
            # 因此即便 stream_id 解析失败，回戳路径仍然可用。
            stream_id = await self._resolve_stream_id(ctx)
            if not stream_id:
                self.ctx.logger.debug(
                    "[smart_poke] 无法解析 stream_id (group=%s, poker=%s)，回退到回戳路径",
                    ctx.group_id, ctx.poker_id,
                )

            kind = self._decide_reaction_kind(is_spam)
            # 没有 stream_id 时，文字/表情类型无法发出，统一回落到回戳
            if kind in ("emoji", "text") and not stream_id:
                kind = "poke"

            self.ctx.logger.info(
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
                    await self._send_text(stream_id, is_spam)
                return

            if kind == "emoji":
                ok = await self._send_emoji(stream_id)
                if ok:
                    return
                # 表情发送失败 → 回退到回戳
                self.ctx.logger.debug("表情反应失败，回退到回戳")
                ok_poke = await self._send_back_poke(ctx)
                if not ok_poke:
                    # 回戳也失败再退到文字
                    await self._send_text(stream_id, is_spam)
                return

            # kind == "text"
            await self._send_text(stream_id, is_spam)
        except Exception:
            self.ctx.logger.exception("反应戳一戳时出错")

    async def _delay_a_bit(self) -> None:
        """按配置范围随机延迟，模拟思考时间。"""
        cfg = self.config.reaction
        lo = max(0.0, cfg.min_delay_seconds)
        hi = max(lo, cfg.max_delay_seconds)
        delay = random.uniform(lo, hi) if hi > 0 else 0
        if delay > 0:
            await asyncio.sleep(delay)

    def _decide_reaction_kind(self, is_spam: bool) -> str:
        """按配置权重选择反应类型。返回 'poke' / 'emoji' / 'text'。

        三个权重独立配置，不要求加起来等于 1：取它们的总和后归一化分配。
        暴戳时会削弱回戳权重、抬高文字权重，让麦麦更倾向"嫌烦"地说话而不是回戳。
        「真正沉默」由 handle_poke_event 中的 react_probability 未命中分支负责，
        与本方法的反应类型抽样无关。
        """
        cfg = self.config.reaction
        spam_mult_poke = 0.5 if is_spam else 1.0
        spam_mult_emoji = 1.0
        spam_mult_text = 1.5 if is_spam else 1.0
        weights = {
            "poke": max(0.0, cfg.back_poke_weight * spam_mult_poke),
            "emoji": max(0.0, cfg.emoji_weight * spam_mult_emoji),
            "text": max(0.0, cfg.text_weight * spam_mult_text),
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

    async def _invoke_send_poke(
        self,
        target_id: str,
        group_id: str,
        *,
        is_group: bool,
        label: str,
    ) -> bool:
        """统一封装 adapter.napcat.message.send_poke 调用。

        - 私聊：仅传 ``user_id``。
        - 群聊：同时传 ``user_id`` / ``group_id`` / ``target_id``（按 NapCat 隐藏 schema
          要求），其中 ``target_id`` 与 ``user_id`` 同值，明确指向被戳对象。

        返回 True 表示调用成功（NapCat 已接受请求）。
        """
        target_int = _to_positive_int(target_id)
        if target_int is None:
            return False

        call_kwargs: Dict[str, Any] = {"user_id": target_int}
        if is_group:
            group_int = _to_positive_int(group_id)
            if group_int is None:
                return False
            call_kwargs["group_id"] = group_int
            call_kwargs["target_id"] = target_int

        try:
            resp = await self.ctx.api.call(
                "adapter.napcat.message.send_poke", **call_kwargs
            )
            if isinstance(resp, dict) and resp.get("success") is False:
                self.ctx.logger.warning(
                    "[%s] send_poke 调用失败: %s", label, resp.get("error")
                )
                return False
            return True
        except Exception:
            self.ctx.logger.exception("[%s] send_poke 调用异常", label)
            return False

    async def _send_back_poke(self, ctx: PokeContext) -> bool:
        """通过 NapCat 适配器发送回戳，返回是否成功。"""
        return await self._invoke_send_poke(
            ctx.poker_id, ctx.group_id, is_group=ctx.is_group, label="back_poke"
        )

    async def _send_text(self, stream_id: str, is_spam: bool) -> None:
        """从配置的回复池中随机挑一句文字发出。"""
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
            self.ctx.logger.exception("发送文字回复失败")

    async def _send_emoji(self, stream_id: str) -> bool:
        """发送一张表情包。Host 序列化表情时固定输出 ``base64`` 字段。"""
        try:
            emoji = await self._pick_emoji()
            if not isinstance(emoji, dict):
                return False

            emoji_data = emoji.get("base64")
            if not isinstance(emoji_data, str) or not emoji_data:
                return False

            await self.ctx.send.emoji(emoji_data, stream_id)
            return True
        except Exception:
            self.ctx.logger.exception("发送表情失败")
            return False

    async def _pick_emoji(self) -> Optional[Dict[str, Any]]:
        """挑一张表情包。优先按关键词搜，找不到时按配置回退随机。

        SDK 已经按 ``_CAPABILITY_RESULT_KEYS`` 表自动解包 RPC 响应：
        - ``emoji.get_by_description`` 直接返回 ``{"base64": ..., "description": ..., "emotion": ...}`` 或 ``None``。
        - ``emoji.get_random`` 直接返回 ``[{...}, ...]`` 列表。

        会按随机顺序最多遍历 3 个关键词，遇到首个命中即返回。
        """
        keywords = [k for k in self.config.emoji.description_keywords if str(k).strip()]

        if keywords:
            shuffled = keywords[:]
            random.shuffle(shuffled)
            for kw in shuffled[:3]:
                try:
                    emoji = await self.ctx.emoji.get_by_description(kw, limit=1)
                    if isinstance(emoji, dict) and emoji:
                        return emoji
                    self.ctx.logger.debug("按关键词 %s 没找到合适表情", kw)
                except Exception:
                    self.ctx.logger.debug("emoji.get_by_description 失败 (kw=%s)", kw, exc_info=True)

        if not self.config.emoji.allow_random_fallback:
            return None

        try:
            emojis = await self.ctx.emoji.get_random(1)
            if isinstance(emojis, list):
                for item in emojis:
                    if isinstance(item, dict) and item:
                        return item
            elif isinstance(emojis, dict) and emojis:
                return emojis
        except Exception:
            self.ctx.logger.debug("emoji.get_random 失败", exc_info=True)
        return None

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

        if not self_id or not poker_id or not target_id:
            return ctx

        # 严格判定 group_id：必须是正整数才视为群聊，避免 "0" / 0 等被误判
        group_info = msg_info.get("group_info") or {}
        raw_group_id = payload.get("group_id")
        if raw_group_id is None or str(raw_group_id).strip() in ("", "0"):
            raw_group_id = group_info.get("group_id")
        group_int = _to_positive_int(raw_group_id)

        user_info = msg_info.get("user_info") or {}

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
        ctx.group_id = str(group_int) if group_int is not None else ""
        ctx.is_group = group_int is not None
        ctx.stream_id = str(message.get("session_id") or "")
        # 冷却 key fallback：napcat notice 注入的 session_id 常为空，
        # 必须按 stream_id → group_id → poker_id 回退才能保证冷却生效。
        ctx.cooldown_key = ctx.stream_id or ctx.group_id or ctx.poker_id
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
            self.ctx.logger.debug("回查 stream_id 失败", exc_info=True)
        return ""


def create_plugin() -> SmartPokePlugin:
    """Runner 调用入口。"""
    return SmartPokePlugin()
