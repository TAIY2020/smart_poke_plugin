"""智能戳一戳插件 — MaiBot SDK v2

通过 @HookHandler 订阅 chat.receive.before_process，识别 napcat 适配器
注入的 notify.poke 事件，按拟人化策略回戳 / 发文字 / 发表情 / 沉默；
另以 OBSERVE 模式观察普通消息，按概率触发主动戳。
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Literal

from maibot_sdk import Field, HookHandler, MaiBotPlugin, PluginConfigBase
from maibot_sdk.types import ErrorPolicy, HookMode, HookOrder
from pydantic import field_validator


_module_logger = logging.getLogger(__name__)


def _load_manifest_version() -> str:
    """从 _manifest.json 读取版本号，保持插件元数据单一来源。"""
    try:
        manifest_path = Path(__file__).parent / "_manifest.json"
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        version = data.get("version")
        if isinstance(version, str) and version.strip():
            return version.strip()
        _module_logger.warning(
            "_manifest.json 中 version 字段缺失或非法 (%r)，回落到 0.0.0", version,
        )
    except Exception:
        _module_logger.warning("读取 _manifest.json 失败，回落到 0.0.0", exc_info=True)
    return "0.0.0"


PLUGIN_VERSION = _load_manifest_version()

CONFIG_SCHEMA_VERSION = "1.5.0"

# 表情库未就绪时 get_by_description 会在主程序里打 "[获取表情包] 表情包列表为空" warning，
# 必须先用无副作用的 get_emotions 轮询确认就绪后再发 RPC。
EMOJI_PROBE_INITIAL_DELAY_SECONDS = 2.0
EMOJI_READY_POLL_INTERVAL_SECONDS = 1.5
EMOJI_READY_POLL_MAX_ATTEMPTS = 40
EMOJI_PROBE_RPC_MAX_ATTEMPTS = 1

EMOJI_KEYWORD_PROBE_LIMIT = 3

# 已验证关键词连续 miss 阈值：达到后从验证集移除，应对"表情库后续被删"场景。
EMOJI_KEYWORD_MISS_THRESHOLD = 3

MEMBER_NAME_CACHE_TTL_SECONDS = 600.0
MEMBER_NAME_CACHE_MAX_SIZE = 256
MEMBER_NAME_NEGATIVE_CACHE_TTL_SECONDS = 60.0

# notice 消息 session_id 固定为空，stream_id 每次反应都得回查，缓存收益明显。
STREAM_ID_CACHE_TTL_SECONDS = 1800.0

# 主动戳后台任务并发上限：群高速刷屏时按背压丢弃新任务，避免任务风暴。
PROACTIVE_TASK_QUEUE_LIMIT = 64

# send_poke 失败抑制窗口：风控/频控时同 label 的 warning 只打一条，其余降级为 debug。
SEND_POKE_FAILURE_LOG_SUPPRESS_SECONDS = 15.0


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
        default=CONFIG_SCHEMA_VERSION,
        description="配置 schema 版本（与插件版本独立，仅在配置字段结构变更时手动上调）",
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
            "hint": (
                "0~1，越大越爱搭理。注意：单类反应（回戳/表情/文字）的期望触发率 ≈ "
                "react_probability × weight / sum(weights)，再被冷却与每分钟上限进一步削减"
            ),
            "x-widget": "slider",
            "min": 0.0,
            "max": 1.0,
            "step": 0.05,
        },
    )
    back_poke_weight: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="反应时选择回戳的权重；三种权重按总和归一化，不要求加起来等于 1",
        json_schema_extra={
            "label": "回戳权重",
            "hint": "三种权重按和归一化抽取，不要求加起来等于 1",
            "x-widget": "slider",
            "min": 0.0,
            "max": 1.0,
            "step": 0.05,
        },
    )
    emoji_weight: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="反应时选择发表情包的权重；三种权重按总和归一化，不要求加起来等于 1",
        json_schema_extra={
            "label": "表情权重",
            "hint": "三种权重按和归一化抽取，不要求加起来等于 1",
            "x-widget": "slider",
            "min": 0.0,
            "max": 1.0,
            "step": 0.05,
        },
    )
    text_weight: float = Field(
        default=0.1,
        ge=0.0,
        le=1.0,
        description="反应时选择文字回复的权重；三种权重按总和归一化，不要求加起来等于 1",
        json_schema_extra={
            "label": "文字权重",
            "hint": "三种权重按和归一化抽取，不要求加起来等于 1",
            "x-widget": "slider",
            "min": 0.0,
            "max": 1.0,
            "step": 0.05,
        },
    )
    silent_chat_probability: float = Field(
        default=0.1,
        ge=0.0,
        le=1.0,
        description=(
            "react_probability 未命中时仍然挤一句沉默回复池的概率。"
            "意义上等价于「装看不见但偶尔抱怨一下」；命中后会消耗冷却，避免短时间内反复挤话"
        ),
        json_schema_extra={
            "label": "沉默时发言概率",
            "hint": "未命中反应概率时，挤一句沉默回复池的概率（会消耗冷却）",
            "x-widget": "slider",
            "min": 0.0,
            "max": 1.0,
            "step": 0.05,
        },
    )
    swallow_when_silent: bool = Field(
        default=True,
        description=(
            "react_probability 未命中、插件什么都不发时，是否仍拦截戳一戳事件。"
            "默认 True：吞掉事件，让 Host 完全感知不到这次戳；"
            "改为 False 则放行事件，主程序可以接续自带的戳一戳人设回应"
        ),
        json_schema_extra={
            "label": "沉默时吞事件",
            "hint": "关闭时未反应的戳会照常传给主程序",
        },
    )

    min_delay_seconds: float = Field(
        default=1.0,
        ge=0.0,
        le=30.0,
        description="作出反应的最小延迟（秒），模拟「人在思考」",
        json_schema_extra={"label": "最小延迟（秒）"},
    )
    max_delay_seconds: float = Field(
        default=2.5,
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
    max_reactions_per_minute: int = Field(
        default=6,
        ge=0,
        le=60,
        description=(
            "过去 60 秒内累计反应（含 silent_reply）次数上限，"
            "超过即静默吞事件——与逐人冷却不同维度，专治『多人轮番戳麦麦』场景下"
            "麦麦像永动机一样事事回应的违和感。0 表示完全不限制"
        ),
        json_schema_extra={
            "label": "每分钟反应上限",
            "hint": "0 关闭；逐人冷却拦不住多人轮番车轮战，这里再加一层全局上限",
        },
    )
    back_poke_max_times: int = Field(
        default=3,
        ge=1,
        le=5,
        description=(
            "选择『回戳』反应时连续戳几下的上限（包含本次）。"
            "普通状态下随机抽 1~max；进入暴戳状态后固定为 max 次——"
            "拟人化语义『被烦了就连戳几下还回去』，比单戳一下更带情绪。"
            "每次戳之间会有 0.3~0.8s 短延迟，避免请求扎堆触发风控。"
            "默认 3 让暴戳态至少多戳一下；调到 1 则关闭连戳"
        ),
        json_schema_extra={
            "label": "回戳次数上限",
            "hint": "默认 3 让暴戳态连戳两下；调到 1 退化为单次回戳，调到 4~5 更狠",
        },
    )

    spam_threshold: int = Field(
        default=5,
        ge=2,
        le=50,
        description="在判定窗口内被同一人戳到该次数后进入「被烦」状态",
        json_schema_extra={"label": "暴戳阈值"},
    )
    spam_window_seconds: int = Field(
        default=45,
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

    normal_replies: list[str] = Field(
        default_factory=lambda: [
            "干嘛戳我",
            "戳什么戳",
            "干啥",
        ],
        description="普通情况下的文字回复随机池",
        json_schema_extra={"label": "普通回复池"},
    )
    spam_replies: list[str] = Field(
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
    silent_replies: list[str] = Field(
        default_factory=lambda: [
            "...",
            "？？？",
            "，，，",
        ],
        description="选择「沉默」反应时偶尔会发出的极简内容",
        json_schema_extra={"label": "沉默回复池"},
    )


class UserControlSection(PluginConfigBase):
    """用户控制：黑名单 / 自戳处理。"""

    __ui_label__ = "用户控制"

    blacklist: list[str] = Field(
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

    description_keywords: list[str] = Field(
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
        json_schema_extra={
            "label": "启用跟风戳",
            "hint": "需要同时启用 reaction.react_in_group 才会真正生效",
        },
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
            "hint": "victim：戳被戳者（跟着欺负他）| poker：戳发起者（替被戳者还击）| random：随机选一个",
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
    swallow_event: bool = Field(
        default=False,
        description=(
            "决定跟风戳触发后是否拦截当前的『别人互戳』事件。"
            "默认 False：不拦截，Host 仍会按普通消息流处理这次互戳；"
            "True：拦截事件，避免主程序自带的戳一戳人设回应再插一脚"
        ),
        json_schema_extra={
            "label": "跟风时吞事件",
            "hint": "True 时麦麦跟风戳后会吞掉这次互戳事件，避免 Host 再生成一遍回应",
        },
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
        _module_logger.warning(
            "bystander.target_strategy 配置值 %r 非法，已回落到 'victim'。"
            "合法取值: victim / poker / random",
            value,
        )
        return "victim"


class ProactiveSection(PluginConfigBase):
    """主动戳：群里有人说话时，按概率被勾起来戳一下熟人。
    """

    __ui_label__ = "主动戳"

    enabled: bool = Field(
        default=True,
        description=(
            "是否启用主动戳：群里有新消息进来时按低概率被勾起来戳一下熟人。"
            "仅群聊生效；私聊场景永远不会主动戳"
        ),
        json_schema_extra={
            "label": "启用主动戳",
            "hint": "事件驱动：群里每条新消息都会被『考虑』一次，再叠加多重约束",
        },
    )
    probability: float = Field(
        default=0.02,
        ge=0.0,
        le=1.0,
        description=(
            "群里每收到一条新消息时被勾起主动戳的基础概率。"
            "活跃群每分钟有十几条消息，0.02 大约对应『几分钟可能出一次手』，"
            "再被冷却/日上限/活跃度等约束削减后实际频率会更低"
        ),
        json_schema_extra={
            "label": "出手概率",
            "hint": "每条新消息触发的基础概率；建议 0.01~0.05",
            "x-widget": "slider",
            "min": 0.0,
            "max": 0.3,
            "step": 0.005,
        },
    )
    active_hour_start: int = Field(
        default=9,
        ge=0,
        le=23,
        description="活跃时段起点（24h 制，本地时间）；与 active_hour_end 共同界定『允许主动戳』的小时区间",
        json_schema_extra={"label": "活跃时段开始（小时）"},
    )
    active_hour_end: int = Field(
        default=24,
        ge=0,
        le=24,
        description=(
            "活跃时段终点（24h 制，本地时间，开区间）。"
            "支持跨午夜：当 end ≤ start 时认定为跨午夜，例如 start=22 / end=2 表示晚 22 点到次日 2 点。"
            "把 start 和 end 设成同一个值（含 0/0）即表示全天活跃"
        ),
        json_schema_extra={"label": "活跃时段结束（小时）"},
    )
    per_chat_cooldown_seconds: int = Field(
        default=600,
        ge=0,
        le=86400,
        description="同一群聊两次主动戳的最小间隔（秒）；避免在同一群里短时间内反复出手",
        json_schema_extra={"label": "同群冷却（秒）"},
    )
    global_cooldown_seconds: int = Field(
        default=90,
        ge=0,
        le=86400,
        description="任意两次主动戳之间的全局最小间隔（秒），避免在多个群间同时撒野",
        json_schema_extra={"label": "全局冷却（秒）"},
    )
    max_pokes_per_day: int = Field(
        default=3,
        ge=0,
        le=1000,
        description="每天主动戳的次数上限（按本地日期归零，0 表示不限制）；超出后当天不再出手",
        json_schema_extra={"label": "每日上限"},
    )
    lookback_seconds: int = Field(
        default=1800,
        ge=60,
        le=86400,
        description="候选用户『最近活跃』的回溯窗口（秒）；只有在窗口内说过话的用户才会被纳入候选",
        json_schema_extra={"label": "候选活跃窗口（秒）"},
    )
    recent_window_seconds: int = Field(
        default=300,
        ge=30,
        le=3600,
        description="判定『群是否够活跃』的统计窗口（秒）；窗口内消息数 < min_recent_messages 则跳过",
        json_schema_extra={"label": "群活跃统计窗口（秒）"},
    )
    recent_fetch_limit: int = Field(
        default=100,
        ge=10,
        le=500,
        description=(
            "每次主动戳时通过 message.get_recent 拉取多少条最近消息，用于候选与活跃度判断。"
            "默认 100：在活跃群里能覆盖到 lookback_seconds (默认 1800s) 内的绝大多数说话者；"
            "调小可节省 RPC 与序列化，但候选池会变窄，可能漏掉早些说话的熟人"
        ),
        json_schema_extra={
            "label": "拉取消息条数",
            "hint": "建议保持与「候选活跃窗口」/「群活跃统计窗口」的预期密度匹配",
        },
    )
    min_recent_messages: int = Field(
        default=3,
        ge=1,
        le=100,
        description=(
            "群活跃门槛：recent_window_seconds 窗口内有效消息（去掉麦麦自己、通知事件）"
            "至少要达到该数量才考虑出手，避免在死群里偶发触发"
        ),
        json_schema_extra={"label": "群活跃门槛（条）"},
    )
    respect_spam_history: bool = Field(
        default=True,
        description=(
            "是否避开『最近戳过麦麦的用户』——开启后会跳过 respect_spam_window_seconds "
            "窗口内戳过麦麦的人，避免在用户刚戳完麦麦时立即反过去骚扰对方（容易显得报复性）"
        ),
        json_schema_extra={"label": "避开骚扰过麦麦的人"},
    )
    respect_spam_window_seconds: int = Field(
        default=600,
        ge=30,
        le=86400,
        description=(
            "避开『戳过麦麦的人』时使用的回溯窗口（秒）。"
            "与 reaction.spam_window_seconds（默认 45s，用于暴戳态判定）独立——"
            "后者的窗口太短，不足以让对方『刚戳完麦麦立刻被反戳』显得不报复性；"
            "默认 600 秒（10 分钟）让『避开』更保守一些"
        ),
        json_schema_extra={
            "label": "避开戳过麦麦的窗口（秒）",
            "hint": "推荐 300~1800；越长越保守，越短越容易反戳",
        },
    )
    target_strategy: Literal["active_speaker", "random_recent"] = Field(
        default="active_speaker",
        description=(
            "目标挑选策略："
            "active_speaker=戳『最新一条非通知消息的发送者』，最贴合『被这个人说话勾起戳意』的语义；"
            "random_recent=从所有 lookback_seconds 窗口内活跃用户里随机挑一个"
        ),
        json_schema_extra={
            "label": "目标策略",
            "hint": "active_speaker：戳最近说话的人 | random_recent：随机选一个最近活跃用户",
            "x-widget": "select",
            "options": [
                {"value": "active_speaker", "label": "刚说话的人"},
                {"value": "random_recent", "label": "随机活跃用户"},
            ],
        },
    )
    min_delay_seconds: float = Field(
        default=2.0,
        ge=0.0,
        le=30.0,
        description="决定出手到真正发出戳之间的最小思考延迟（秒），避免被识破为机械触发",
        json_schema_extra={"label": "最小延迟（秒）"},
    )
    max_delay_seconds: float = Field(
        default=6.0,
        ge=0.0,
        le=60.0,
        description="主动戳的最大思考延迟（秒）；与 min 一起按均匀分布抽样",
        json_schema_extra={"label": "最大延迟（秒）"},
    )
    whitelist_groups: list[str] = Field(
        default_factory=list,
        description=(
            "群白名单：只在这些群里允许主动戳；为空表示『没有白名单限制』。"
            "白名单与黑名单同时存在时，黑名单优先生效（黑名单内永远不戳）"
        ),
        json_schema_extra={"label": "群白名单"},
    )
    blacklist_groups: list[str] = Field(
        default_factory=list,
        description="群黑名单：永不在这些群里主动戳",
        json_schema_extra={"label": "群黑名单"},
    )

    @field_validator("target_strategy", mode="before")
    @classmethod
    def _normalize_proactive_target_strategy(
        cls, value: Any
    ) -> Literal["active_speaker", "random_recent"]:
        normalized = "" if value is None else str(value).strip().lower()
        if normalized in ("active_speaker", "random_recent"):
            return normalized  # type: ignore[return-value]
        _module_logger.warning(
            "proactive.target_strategy 配置值 %r 非法，已回落到 'active_speaker'。"
            "合法取值: active_speaker / random_recent",
            value,
        )
        return "active_speaker"


class SmartPokeConfig(PluginConfigBase):
    """智能戳一戳插件完整配置。"""

    plugin: PluginSection = Field(default_factory=PluginSection)
    reaction: ReactionSection = Field(default_factory=ReactionSection)
    fallback: FallbackSection = Field(default_factory=FallbackSection)
    user_control: UserControlSection = Field(default_factory=UserControlSection)
    emoji: EmojiSection = Field(default_factory=EmojiSection)
    bystander: BystanderSection = Field(default_factory=BystanderSection)
    proactive: ProactiveSection = Field(default_factory=ProactiveSection)


# --- 数据结构 ---


class PokeContext:
    """从一次戳一戳事件中提取出的关键信息。"""

    __slots__ = (
        "is_group",
        "self_id",
        "poker_id",
        "poker_name",
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
        self.target_id: str = ""
        self.target_name: str = ""
        self.group_id: str = ""
        self.stream_id: str = ""
        # napcat notice 的 session_id 常为空，按 stream_id → group_id → poker_id 回退
        self.cooldown_key: str = ""
        # 与 cooldown_key 解耦：proactive 分支查 _poked_bot_recently 只能传 group_id，
        # 必须与 record 端用同一个 scope 才能匹配
        self.spam_scope_key: str = ""

    @property
    def is_poking_bot(self) -> bool:
        return bool(self.self_id) and self.target_id == self.self_id


class PokeStateManager:
    """冷却时间戳、暴戳计数、各种 TTL 缓存的集中持有者。"""

    _PRUNE_THRESHOLD = 200
    _STALE_AFTER_SECONDS = 3600
    # 单条暴戳计数 deque 的硬上限——暴戳判定只关心 >= spam_threshold，多余的旧记录
    # 会被天然挤出而不影响判定结果。
    _POKE_RECORD_MAXLEN = 200

    def __init__(self) -> None:
        self._last_react_at: dict[str, float] = {}
        self._poke_records: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=PokeStateManager._POKE_RECORD_MAXLEN)
        )
        self._last_bystander_at: dict[str, float] = {}
        self._record_counter: int = 0
        # 反应总数滑动窗口：与逐人冷却互补，防多人轮番车轮战；
        # 跟风戳/主动戳有各自的冷却+日上限，不占用此窗口。
        self._reaction_window: deque[float] = deque()
        # name=""为负缓存条目（已知该用户没有可解析昵称），TTL 配更短。
        self._name_cache: dict[str, tuple[str, float]] = {}
        self._stream_id_cache: dict[str, tuple[str, float]] = {}
        self._last_proactive_at_chat: dict[str, float] = {}
        self._last_proactive_global_at: float = 0.0
        self._proactive_daily_count: int = 0
        self._proactive_daily_date: str = ""

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
        now = time.time()
        if key:
            self._last_react_at[key] = now
        self._reaction_window.append(now)

    def peek_reaction_window(self, window_seconds: int) -> int:
        """返回窗口内累计反应数，顺便清理过期记录。不写入新记录。"""
        if window_seconds <= 0:
            return 0
        cutoff = time.time() - window_seconds
        while self._reaction_window and self._reaction_window[0] < cutoff:
            self._reaction_window.popleft()
        return len(self._reaction_window)

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
        records = self._poke_records[key]
        while records and records[0] < cutoff:
            records.popleft()
        records.append(now)

        self._record_counter += 1
        if self._record_counter >= self._PRUNE_THRESHOLD:
            self._record_counter = 0
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
            kept = sorted(
                ((k, v) for k, v in self._name_cache.items() if v[1] >= now),
                key=lambda kv: kv[1],
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

    # ----- 主动戳：冷却与日上限 -----

    def in_proactive_chat_cooldown(self, group_id: str, cooldown_seconds: int) -> bool:
        if cooldown_seconds <= 0 or not group_id:
            return False
        last = self._last_proactive_at_chat.get(group_id, 0.0)
        return (time.time() - last) < cooldown_seconds

    def in_proactive_global_cooldown(self, cooldown_seconds: int) -> bool:
        if cooldown_seconds <= 0:
            return False
        return (time.time() - self._last_proactive_global_at) < cooldown_seconds

    def mark_proactive(self, group_id: str, today: str) -> None:
        """``today`` 由调用方按本地日期生成，保持与 active_hour_* 的口径一致。"""
        now = time.time()
        if group_id:
            self._last_proactive_at_chat[group_id] = now
        self._last_proactive_global_at = now
        if today != self._proactive_daily_date:
            self._proactive_daily_date = today
            self._proactive_daily_count = 0
        self._proactive_daily_count += 1

    def proactive_daily_count(self, today: str) -> int:
        if today != self._proactive_daily_date:
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
        while self._reaction_window and self._reaction_window[0] < cutoff:
            self._reaction_window.popleft()
        now = time.time()
        self._name_cache = {k: v for k, v in self._name_cache.items() if v[1] >= now}
        self._stream_id_cache = {k: v for k, v in self._stream_id_cache.items() if v[1] >= now}

    def clear(self) -> None:
        self._last_react_at.clear()
        self._poke_records.clear()
        self._last_bystander_at.clear()
        self._name_cache.clear()
        self._stream_id_cache.clear()
        self._last_proactive_at_chat.clear()
        self._last_proactive_global_at = 0.0
        self._proactive_daily_count = 0
        self._proactive_daily_date = ""
        self._reaction_window.clear()
        self._record_counter = 0


# --- 辅助函数 ---


def _to_positive_int(value: Any) -> int | None:
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


def _in_active_hours(start: int, end: int, now_hour: int) -> bool:
    """判断当前小时是否落在 [start, end) 活跃区间内（本地时间，24h 制）。

    - ``start == end``：全天活跃；
    - ``start < end``：普通区间；
    - ``start > end``：跨午夜区间（如 22 ~ 2 表示晚 22 到次日 2 点）。

    ``end == 24`` 不做 ``% 24`` 归一化——否则默认配置 ``start=9, end=24``
    会被误判成跨午夜区间。
    """
    start = start % 24
    end_normalized = end if end == 24 else end % 24
    if start == end_normalized:
        return True
    if start < end_normalized:
        return start <= now_hour < end_normalized
    return now_hour >= start or now_hour < end_normalized


def _format_local_date(timestamp: float) -> str:
    return time.strftime("%Y-%m-%d", time.localtime(timestamp))


# --- 主插件 ---


class SmartPokePlugin(MaiBotPlugin):
    """智能戳一戳插件主类。"""

    config_model = SmartPokeConfig

    def __init__(self) -> None:
        super().__init__()
        self._blacklist: set[str] = set()
        self._pending_tasks: set[asyncio.Task] = set()
        self._state = PokeStateManager()
        # 启动期探测命中的关键词；运行时优先采样以提高 RPC 命中率，
        # 但会混入未验证关键词作为表情库新增表情后的刷新机制。
        self._validated_emoji_keywords: list[str] = []
        self._validated_emoji_miss_counts: dict[str, int] = {}
        self._proactive_whitelist_groups: set[str] = set()
        self._proactive_blacklist_groups: set[str] = set()
        # OBSERVE 阶段拿不到 self_id；从 napcat additional_config / payload 学到后缓存，
        # 用于过滤"自己说话触发主动戳"等边界。拿不到也不致命。
        self._last_known_self_id: str = ""
        # 主动戳锁结构：per-group 防同群双发，global 防跨群同时穿过乐观快检。
        self._proactive_locks: dict[str, asyncio.Lock] = {}
        self._proactive_global_lock: asyncio.Lock = asyncio.Lock()
        self._proactive_active_count: int = 0
        self._send_poke_failure_warned_at: dict[str, float] = {}
        # on_unload 入口置 True，_spawn_background_task 据此拒收新任务。
        self._shutting_down: bool = False

    # ===== 生命周期 =====

    async def on_load(self) -> None:
        self._refresh_user_sets()
        # 把 manifest 版本号同步回 config.plugin.version：避免 config.toml 与 _manifest.json 漂移
        try:
            if self.config.plugin.version != PLUGIN_VERSION:
                self.config.plugin.version = PLUGIN_VERSION
        except Exception:
            self.ctx.logger.debug("同步 plugin.version 到 manifest 失败", exc_info=True)
        self.ctx.logger.info("智能戳一戳插件(v%s)初始化完成。", PLUGIN_VERSION)
        self._spawn_background_task(self._probe_emoji_keywords(), "emoji_keyword_probe")

    async def on_unload(self) -> None:
        # 拒收新任务后再 cancel/gather，避免 gather 完成后又有"漏网之鱼"被孤立
        self._shutting_down = True
        to_cancel = [t for t in self._pending_tasks if not t.done()]
        for task in to_cancel:
            task.cancel()
        if to_cancel:
            await asyncio.gather(*to_cancel, return_exceptions=True)
        self._pending_tasks.clear()
        self._proactive_locks.clear()
        self._proactive_active_count = 0
        self._state.clear()

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

    def _sample_probe_keywords(self, keywords: list[str]) -> list[str]:
        """采样关键词：已验证的优先，未验证的作为表情库新增的刷新机制混入。"""
        cleaned = [str(k).strip() for k in keywords if str(k).strip()]
        if not cleaned:
            return []
        validated_set = set(self._validated_emoji_keywords)
        validated = [k for k in cleaned if k in validated_set]
        unvalidated = [k for k in cleaned if k not in validated_set]
        random.shuffle(validated)
        random.shuffle(unvalidated)
        return (validated + unvalidated)[:EMOJI_KEYWORD_PROBE_LIMIT]

    async def _probe_emoji_keywords(self) -> None:
        """启动后探测关键词命中情况。

        必须先用无副作用的 ``emoji.get_emotions`` 轮询表情库就绪，再做本地子串匹配，
        最后对未匹配的关键词发 ``emoji.get_by_description`` 兜底——否则表情库为空时
        ``get_by_description`` 会触发主程序的 "[获取表情包] 表情包列表为空" warning 刷屏。
        """
        keywords = [str(k).strip() for k in self.config.emoji.description_keywords if str(k).strip()]
        if not keywords:
            return

        await asyncio.sleep(EMOJI_PROBE_INITIAL_DELAY_SECONDS)

        emotions: list[Any] = []
        for attempt in range(1, EMOJI_READY_POLL_MAX_ATTEMPTS + 1):
            try:
                result = await self.ctx.emoji.get_emotions()
            except Exception:
                self.ctx.logger.debug(
                    "emoji.get_emotions 第 %d 次调用失败", attempt, exc_info=True,
                )
                result = None
            if isinstance(result, list) and result:
                emotions = result
                if attempt > 1:
                    self.ctx.logger.debug(
                        "表情库在第 %d 次轮询时就绪（%d 个 emotion 标签）",
                        attempt, len(emotions),
                    )
                break
            if attempt < EMOJI_READY_POLL_MAX_ATTEMPTS:
                await asyncio.sleep(EMOJI_READY_POLL_INTERVAL_SECONDS)
        else:
            self.ctx.logger.info(
                "等待表情库就绪超时（轮询 %d 次仍未拿到 emotion 标签），跳过启动期关键词探测；"
                "运行时仍会按需采样关键词调用 emoji RPC",
                EMOJI_READY_POLL_MAX_ATTEMPTS,
            )
            return

        emotion_set = {str(e).strip() for e in emotions if str(e).strip()}
        prevalidated: list[str] = []
        for kw in keywords:
            if any(kw == e or kw in e or e in kw for e in emotion_set):
                prevalidated.append(kw)
        if prevalidated:
            for kw in prevalidated:
                if kw not in self._validated_emoji_keywords:
                    self._validated_emoji_keywords.append(kw)
            self.ctx.logger.info(
                "emoji.get_emotions 本地匹配命中 %d/%d 个关键词：%s",
                len(prevalidated), len(keywords), ", ".join(prevalidated),
            )

        remaining = [kw for kw in keywords if kw not in prevalidated]
        if not remaining:
            return

        validated_via_rpc: list[str] = []
        for kw in remaining:
            try:
                emoji = await self.ctx.emoji.get_by_description(kw, limit=1)
            except Exception:
                self.ctx.logger.debug(
                    "emoji 关键词探测 RPC 失败 (kw=%s)", kw, exc_info=True
                )
                continue
            if isinstance(emoji, dict) and emoji:
                validated_via_rpc.append(kw)
        if validated_via_rpc:
            for kw in validated_via_rpc:
                if kw not in self._validated_emoji_keywords:
                    self._validated_emoji_keywords.append(kw)
            self.ctx.logger.info(
                "emoji 关键词 RPC 探测追加命中 %d 个：%s",
                len(validated_via_rpc), ", ".join(validated_via_rpc),
            )
            return

        if not self._validated_emoji_keywords:
            self.ctx.logger.warning(
                "emoji 关键词探测全部未命中（关键词：%s）；"
                "建议调整关键词或开启 emoji.allow_random_fallback",
                ", ".join(keywords),
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
            triggered = self._maybe_trigger_bystander(ctx)
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
        # 用 spam_scope_key 而非 cooldown_key，确保 proactive 的 _poked_bot_recently(group_id) 能命中。
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
                self._spawn_background_task(self._silent_reply(ctx), "silent_reply")
                return {"action": "abort"}
            if self.config.reaction.swallow_when_silent:
                return {"action": "abort"}
            return None

        self._state.mark_reacted(ctx.cooldown_key, ctx.poker_id)

        self._spawn_background_task(
            self._react(ctx, is_spam, poke_count),
            "react",
        )

        return {"action": "abort"}

    # ===== 跟风戳 =====

    def _maybe_trigger_bystander(self, ctx: PokeContext) -> bool:
        """返回 ``True`` 表示已派发跟风戳任务，调用方据此决定是否吞事件。"""
        cfg = self.config.bystander
        if not cfg.enabled:
            return False
        if not ctx.is_group:
            return False
        if not self.config.reaction.react_in_group:
            return False
        if ctx.poker_id == ctx.self_id or ctx.target_id == ctx.self_id:
            return False
        bystander_key = ctx.cooldown_key
        if self._state.in_bystander_cooldown(bystander_key, cfg.cooldown_seconds):
            return False
        if random.random() > cfg.probability:
            return False

        target_id = self._pick_bystander_target(ctx)
        if not target_id:
            return False

        self._state.mark_bystander(bystander_key)

        self._spawn_background_task(
            self._react_bystander(ctx, target_id),
            "bystander",
        )
        return True

    def _pick_bystander_target(self, ctx: PokeContext) -> str:
        """按 target_strategy 挑选跟风对象；命中黑名单则返回空串而不退化策略。"""
        strategy = self.config.bystander.target_strategy
        if strategy == "victim":
            candidates = [ctx.target_id]
        elif strategy == "poker":
            candidates = [ctx.poker_id]
        else:
            candidates = [ctx.target_id, ctx.poker_id]
            random.shuffle(candidates)

        for candidate in candidates:
            if candidate and candidate not in self._blacklist:
                return candidate
        return ""

    async def _react_bystander(self, ctx: PokeContext, target_id: str) -> None:
        cfg = self.config.bystander
        lo = max(0.0, cfg.min_delay_seconds)
        hi = max(lo, cfg.max_delay_seconds)
        delay = random.uniform(lo, hi) if hi > 0 else 0
        if delay > 0:
            await asyncio.sleep(delay)

        if ctx.is_group and ctx.group_id and target_id == ctx.target_id and not ctx.target_name:
            resolved = await self._resolve_member_name(ctx.group_id, target_id)
            if resolved:
                ctx.target_name = resolved

        ok = await self._invoke_send_poke(
            target_id, ctx.group_id, is_group=ctx.is_group, label="bystander"
        )
        if ok:
            target_label = ctx.target_name if (target_id == ctx.target_id and ctx.target_name) else target_id
            self.ctx.logger.info(
                "[smart_poke] 跟风戳完成: strategy=%s, target=%s (poker=%s, victim=%s)",
                cfg.target_strategy,
                target_label, ctx.poker_name or ctx.poker_id, ctx.target_name or ctx.target_id,
            )

    # ===== 主动戳 =====

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
        """OBSERVE 旁路：每条入站群消息都被"考虑"一次，再交由判定函数层层过滤。

        主 BLOCKING handler 对戳一戳事件 ``abort`` 时 dispatcher 会先 ``break``，
        所以戳一戳通知不会触发主动戳，避免事件回声。
        """
        del kwargs

        if not self.config.plugin.enabled:
            return None
        if not self.config.proactive.enabled:
            return None

        info = self._extract_proactive_signal(message)
        if info is None:
            return None

        group_id, speaker_id = info
        self._spawn_background_task(
            self._maybe_proactive_poke(group_id, speaker_id), "proactive"
        )
        return None

    def _extract_proactive_signal(self, message: Any) -> tuple[str, str] | None:
        """快速排除：仅做廉价过滤，重活留给 ``_maybe_proactive_poke``。

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
            if learned_self_id and learned_self_id != self._last_known_self_id:
                self._last_known_self_id = learned_self_id

        group_info = msg_info.get("group_info") or {}
        if not isinstance(group_info, dict):
            return None
        group_id_raw = group_info.get("group_id")
        group_int = _to_positive_int(group_id_raw)
        if group_int is None:
            return None
        group_id = str(group_int)

        user_info = msg_info.get("user_info") or {}
        if not isinstance(user_info, dict):
            return None
        speaker_id = str(user_info.get("user_id") or "").strip()
        if not speaker_id:
            return None

        if self._last_known_self_id and speaker_id == self._last_known_self_id:
            return None

        return group_id, speaker_id

    def _get_proactive_lock(self, group_id: str) -> asyncio.Lock:
        """单一事件循环下 setdefault 自身原子（无 await 切点），不需要注册锁。"""
        lock = self._proactive_locks.get(group_id)
        if lock is None:
            lock = asyncio.Lock()
            self._proactive_locks[group_id] = lock
        return lock

    async def _maybe_proactive_poke(self, group_id: str, speaker_id: str) -> None:
        """主动戳的完整判定与执行流程。

        双层锁：per-group lock 防同群双发，global lock 保护"全局冷却二次确认 + mark"
        临界区——不同群的并发任务可能在各自 per-group lock 内同时穿过全局乐观快检。
        RPC 与延迟都在 per-group lock 内或锁外，避免全局串行所有群的网络往返。
        """
        cfg = self.config.proactive

        # ----- 锁外早期过滤 -----
        now_struct = time.localtime()
        if not _in_active_hours(cfg.active_hour_start, cfg.active_hour_end, now_struct.tm_hour):
            return
        if group_id in self._proactive_blacklist_groups:
            return
        if self._proactive_whitelist_groups and group_id not in self._proactive_whitelist_groups:
            return
        if cfg.probability <= 0:
            return
        if random.random() > cfg.probability:
            return

        target_id: str = ""
        target_name: str = ""

        async with self._get_proactive_lock(group_id):
            # 全局冷却是乐观快检，原子性靠后续 _proactive_global_lock 内的二次确认保障
            if self._state.in_proactive_global_cooldown(cfg.global_cooldown_seconds):
                return
            if self._state.in_proactive_chat_cooldown(group_id, cfg.per_chat_cooldown_seconds):
                return
            today = _format_local_date(time.time())
            if cfg.max_pokes_per_day > 0:
                already = self._state.proactive_daily_count(today)
                if already >= cfg.max_pokes_per_day:
                    return

            stream_id = await self._resolve_stream_id_for_group(group_id)
            if not stream_id:
                self.ctx.logger.debug(
                    "[proactive] 群 %s 无法解析 stream_id，本次跳过", group_id,
                )
                return

            try:
                recent = await self.ctx.message.get_recent(
                    stream_id, limit=cfg.recent_fetch_limit
                )
            except Exception:
                self.ctx.logger.debug(
                    "[proactive] message.get_recent 失败 (group=%s)", group_id, exc_info=True
                )
                return
            if not isinstance(recent, list) or not recent:
                return

            target_id, target_name, active_count = self._pick_proactive_target(
                recent, group_id, speaker_id,
            )
            if active_count < cfg.min_recent_messages:
                return
            if not target_id:
                return

            async with self._proactive_global_lock:
                if self._state.in_proactive_global_cooldown(cfg.global_cooldown_seconds):
                    return
                if cfg.max_pokes_per_day > 0:
                    already = self._state.proactive_daily_count(today)
                    if already >= cfg.max_pokes_per_day:
                        return
                self._state.mark_proactive(group_id, today)

        # ----- 锁外：思考延迟 + 出手 -----
        lo = max(0.0, cfg.min_delay_seconds)
        hi = max(lo, cfg.max_delay_seconds)
        delay = random.uniform(lo, hi) if hi > 0 else 0
        if delay > 0:
            await asyncio.sleep(delay)

        if not target_name:
            resolved = await self._resolve_member_name(group_id, target_id)
            if resolved:
                target_name = resolved

        ok = await self._invoke_send_poke(
            target_id, group_id, is_group=True, label="proactive",
        )
        if ok:
            self.ctx.logger.info(
                "[smart_poke] 主动戳完成: strategy=%s, group=%s, target=%s",
                cfg.target_strategy, group_id, target_name or target_id,
            )

    def _pick_proactive_target(
        self,
        recent: list[Any],
        group_id: str,
        speaker_id: str,
    ) -> tuple[str, str, int]:
        """挑选候选戳目标，返回 (target_id, target_name, active_count)。

        active_count 是 ``recent_window_seconds`` 内的非麦麦、非通知消息条数，
        供调用方判断群活跃度。target_id 为空串表示没有合适候选。
        """
        cfg = self.config.proactive
        now = time.time()
        lookback_cutoff = now - cfg.lookback_seconds
        active_window_cutoff = now - cfg.recent_window_seconds
        self_id = self._last_known_self_id

        active_count = 0
        candidates: dict[str, tuple[float, str]] = {}
        latest_speaker_id = ""
        latest_speaker_name = ""

        # message.get_recent 通常按时间正序返回，反向遍历以挑"最新说话者"
        for msg in reversed(recent):
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

            if uid in self._blacklist:
                continue
            if cfg.respect_spam_history and self._poked_bot_recently(group_id, uid):
                continue

            uname = str(user_info.get("user_cardname") or user_info.get("user_nickname") or "").strip()
            if uid not in candidates:
                candidates[uid] = (ts, uname)
            if not latest_speaker_id:
                latest_speaker_id = uid
                latest_speaker_name = uname

        if not candidates:
            return "", "", active_count

        if cfg.target_strategy == "active_speaker":
            # speaker_id 被前面的过滤剔除时退到 latest_speaker_id；
            # 再不行取候选里时间戳最新的那个让 active_speaker 在边界场景也成立
            if speaker_id in candidates:
                ts_unused, uname = candidates[speaker_id]
                del ts_unused
                return speaker_id, uname, active_count
            if latest_speaker_id and latest_speaker_id in candidates:
                return latest_speaker_id, latest_speaker_name, active_count
            uid, (_ts_unused, uname) = max(
                candidates.items(), key=lambda kv: kv[1][0]
            )
            return uid, uname, active_count

        uid = random.choice(list(candidates.keys()))
        ts_unused, uname = candidates[uid]
        del ts_unused
        return uid, uname, active_count

    def _poked_bot_recently(self, group_id: str, user_id: str) -> bool:
        """窗口取自 ``proactive.respect_spam_window_seconds``（reaction.spam_window_seconds 太短）。"""
        return self._state.poked_bot_recently(
            group_id, user_id, self.config.proactive.respect_spam_window_seconds
        )

    async def _resolve_stream_id_for_group(self, group_id: str) -> str:
        """专用于 proactive 路径的 stream_id 解析；缓存共用 PokeStateManager。"""
        if not group_id:
            return ""
        cached = self._state.get_cached_stream_id(group_id=group_id, user_id="")
        if cached is not None:
            return cached
        try:
            stream = await self.ctx.chat.get_stream_by_group_id(group_id, platform="qq")
        except Exception:
            self.ctx.logger.debug(
                "[proactive] get_stream_by_group_id 失败 (group=%s)", group_id, exc_info=True
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

    async def _resolve_member_name(self, group_id: str, user_id: str) -> str:
        """解析群成员昵称，群名片优先于 nickname；带 TTL 缓存与负缓存。"""
        if not user_id:
            return ""
        cached = self._state.get_cached_name(group_id, user_id)
        if cached is not None:
            return cached

        name = ""
        try:
            if group_id:
                user_int = _to_positive_int(user_id)
                group_int = _to_positive_int(group_id)
                if user_int is None or group_int is None:
                    return ""
                info = await self.ctx.api.call(
                    "adapter.napcat.group.get_group_member_info",
                    group_id=group_int,
                    user_id=user_int,
                    no_cache=False,
                )
                if isinstance(info, dict):
                    name = str(info.get("card") or info.get("nickname") or "").strip()
            else:
                user_int = _to_positive_int(user_id)
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

    # ===== 反应主流程 =====

    async def _silent_reply(self, ctx: PokeContext) -> None:
        """react_probability 未命中时按 silent_chat_probability 概率挤一句。

        冷却在"确认能挤出一句话"后才消耗（解析 stream_id 成功且回复池非空），
        避免 stream_id 解析失败 / 池为空时白白冷却用户。
        """
        try:
            stream_id = await self._resolve_stream_id(ctx)
            if not stream_id:
                self.ctx.logger.debug(
                    "[silent_reply] 无法解析 stream_id (group=%s, poker=%s)，静默放弃",
                    ctx.group_id, ctx.poker_id,
                )
                return
            pool = self.config.fallback.silent_replies
            if not pool:
                self.ctx.logger.debug("[silent_reply] silent 回复池为空，按沉默语义放弃发送")
                return
            # 二次确认滑动窗口：派发到 mark 之间可能被并发事件填满
            max_per_minute = self.config.reaction.max_reactions_per_minute
            if max_per_minute > 0 and self._state.peek_reaction_window(60) >= max_per_minute:
                self.ctx.logger.debug(
                    "[silent_reply] 派发到 mark 之间窗口被填满，静默放弃 (poker=%s)",
                    ctx.poker_id,
                )
                return
            self._state.mark_reacted(ctx.cooldown_key, ctx.poker_id)
            await self._delay_a_bit()
            await self._safe_send_text(random.choice(pool), stream_id)
        except Exception:
            self.ctx.logger.exception("silent_reply 发送失败")

    async def _react(self, ctx: PokeContext, is_spam: bool, poke_count: int) -> None:
        try:
            # 先解析 stream_id 再延迟：避免选了 text/emoji 但 stream_id 解析失败时
            # 白白等掉几秒思考延迟才回退到回戳。回戳走 send_poke 不依赖 stream_id。
            stream_id = await self._resolve_stream_id(ctx)
            if not stream_id:
                self.ctx.logger.debug(
                    "[smart_poke] 无法解析 stream_id (group=%s, poker=%s)，回退到回戳路径",
                    ctx.group_id, ctx.poker_id,
                )

            kind = self._decide_reaction_kind(is_spam)
            if kind in ("emoji", "text") and not stream_id:
                kind = "poke"

            await self._delay_a_bit()

            self.ctx.logger.info(
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
                        self.ctx.logger.debug(
                            "[smart_poke] 回戳失败且无 stream_id 可回退，本次反应放弃 (poker=%s)",
                            ctx.poker_id,
                        )
                return

            if kind == "emoji":
                ok = await self._send_emoji(stream_id)
                if ok:
                    return
                self.ctx.logger.debug("表情反应失败，回退到回戳")
                ok_poke = await self._send_back_poke(ctx, is_spam=is_spam)
                if not ok_poke:
                    await self._send_text(stream_id, is_spam)
                return

            sent = await self._send_text(stream_id, is_spam)
            if sent:
                return
            # 兜底回戳，避免 mark 了却什么都没发
            self.ctx.logger.debug(
                "[smart_poke] 文字反应未发出，回退到回戳 (poker=%s)", ctx.poker_id,
            )
            await self._send_back_poke(ctx, is_spam=is_spam)
        except Exception:
            self.ctx.logger.exception("反应戳一戳时出错")

    async def _delay_a_bit(self) -> None:
        cfg = self.config.reaction
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
        cfg = self.config.reaction
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

    # ===== 反应实现 =====

    def _log_send_poke_failure(self, label: str, reason: str, *, exc: bool = False) -> None:
        """同 label 的失败在抑制窗口内只打一条 warning，避免风控期相同栈刷屏。"""
        now = time.time()
        last_warned = self._send_poke_failure_warned_at.get(label, 0.0)
        if now - last_warned >= SEND_POKE_FAILURE_LOG_SUPPRESS_SECONDS:
            self.ctx.logger.warning(
                "[%s] send_poke %s", label, reason, exc_info=exc,
            )
            self._send_poke_failure_warned_at[label] = now
        else:
            self.ctx.logger.debug(
                "[%s] send_poke %s（已被抑制窗口降级）", label, reason, exc_info=exc,
            )

    async def _invoke_send_poke(
        self,
        target_id: str,
        group_id: str,
        *,
        is_group: bool,
        label: str,
    ) -> bool:
        """统一封装 adapter.napcat.message.send_poke 调用。

        群聊场景按 NapCat 隐藏 schema 同时传 user_id / group_id / target_id，
        其中 target_id 与 user_id 同值。返回 True 表示 NapCat 已接受请求。
        """
        target_int = _to_positive_int(target_id)
        if target_int is None:
            return False

        call_kwargs: dict[str, Any] = {"user_id": target_int}
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
            # 宿主层 RPC 无响应 / 反序列化失败时返回 None，按"未成功"处理让上层走兜底
            if resp is None:
                self._log_send_poke_failure(label, "send_poke 无响应 (resp=None)")
                return False
            if isinstance(resp, dict):
                if resp.get("success") is False:
                    self._log_send_poke_failure(
                        label, f"宿主调用失败: {resp.get('error')}"
                    )
                    return False
                # NapCat 业务级失败：resp 直接是 NapCat 原始响应
                status = str(resp.get("status") or "").strip().lower()
                retcode = resp.get("retcode")
                if (status and status not in ("ok", "async")) or (
                    isinstance(retcode, int) and retcode != 0
                ):
                    self._log_send_poke_failure(
                        label,
                        f"NapCat 业务失败: status={status or '<none>'} retcode={retcode} "
                        f"message={resp.get('message') or resp.get('wording')}",
                    )
                    return False
            return True
        except Exception:
            self._log_send_poke_failure(label, "调用异常", exc=True)
            return False

    async def _send_back_poke(self, ctx: PokeContext, is_spam: bool = False) -> bool:
        """通过 NapCat 适配器发送回戳，支持多次连续戳。

        防自戳死循环：ignore_self_poke=False 时若让麦麦回戳自己，新的 send_poke
        会再触发一条 notify.poke 被本插件接收，导致无限循环。

        每两次之间 0.3~0.8s 随机短延迟避免请求扎堆触发风控；
        任一中途失败立即停止（可能已被风控）。
        """
        if ctx.poker_id == ctx.self_id:
            self.ctx.logger.warning(
                "拒绝回戳麦麦自己（self_id=%s）以避免戳一戳事件死循环", ctx.self_id,
            )
            return False

        max_times = max(1, self.config.reaction.back_poke_max_times)
        times = max_times if is_spam else random.randint(1, max_times)

        any_success = False
        for i in range(times):
            ok = await self._invoke_send_poke(
                ctx.poker_id, ctx.group_id, is_group=ctx.is_group, label="back_poke"
            )
            if ok:
                any_success = True
            else:
                break
            if i < times - 1:
                await asyncio.sleep(random.uniform(0.3, 0.8))

        if times > 1 and any_success:
            self.ctx.logger.debug(
                "[back_poke] 连戳 %d 次 (is_spam=%s, max=%d)",
                times, is_spam, max_times,
            )
        return any_success

    async def _send_text(self, stream_id: str, is_spam: bool) -> bool:
        """从配置的回复池中随机挑一句发出。

        暴戳态不回落到 silent_replies——"..."与"被烦了"语气冲突；前两档全空时
        让 _react 走兜底回戳更合适。
        """
        if is_spam:
            primary = self.config.fallback.spam_replies
            secondary = self.config.fallback.normal_replies
            tertiary: list[str] = []
        else:
            primary = self.config.fallback.normal_replies
            secondary: list[str] = []
            tertiary = self.config.fallback.silent_replies

        for pool, label in ((primary, "primary"), (secondary, "secondary"), (tertiary, "silent")):
            if pool:
                ok = await self._safe_send_text(random.choice(pool), stream_id)
                if ok:
                    return True
                self.ctx.logger.debug("[_send_text] %s 池发送失败，尝试下一档", label)
        self.ctx.logger.debug(
            "[_send_text] 所有回复池均为空或发送失败 (is_spam=%s)", is_spam,
        )
        return False

    async def _safe_send_text(self, text: str, stream_id: str) -> bool:
        try:
            await self.ctx.send.text(text, stream_id)
            return True
        except Exception:
            self.ctx.logger.exception("发送文字回复失败")
            return False

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

    async def _pick_emoji(self) -> dict[str, Any] | None:
        """优先按关键词搜，找不到时按配置回退随机。

        运行时发现未验证关键词命中（表情库新增表情）会顺手写入验证集；
        已验证关键词连续 miss 到阈值则从验证集移除，应对表情库被删的场景。
        """
        probe = self._sample_probe_keywords(self.config.emoji.description_keywords)

        for kw in probe:
            try:
                emoji = await self.ctx.emoji.get_by_description(kw, limit=1)
                if isinstance(emoji, dict) and emoji:
                    if kw not in self._validated_emoji_keywords:
                        self._validated_emoji_keywords.append(kw)
                    self._validated_emoji_miss_counts.pop(kw, None)
                    return emoji
                self.ctx.logger.debug("按关键词 %s 没找到合适表情", kw)
                if kw in self._validated_emoji_keywords:
                    misses = self._validated_emoji_miss_counts.get(kw, 0) + 1
                    if misses >= EMOJI_KEYWORD_MISS_THRESHOLD:
                        self._validated_emoji_keywords.remove(kw)
                        self._validated_emoji_miss_counts.pop(kw, None)
                        self.ctx.logger.debug(
                            "[emoji] 关键词 %s 已连续 %d 次未命中，移出验证集",
                            kw, misses,
                        )
                    else:
                        self._validated_emoji_miss_counts[kw] = misses
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

        if self_id and self_id != self._last_known_self_id:
            self._last_known_self_id = self_id

        # 严格判定 group_id：必须正整数才视为群聊，避免 "0" / 0 被误判
        group_info = msg_info.get("group_info") or {}
        raw_group_id = payload.get("group_id")
        if raw_group_id is None or str(raw_group_id).strip() in ("", "0"):
            raw_group_id = group_info.get("group_id")
        group_int = _to_positive_int(raw_group_id)

        user_info = msg_info.get("user_info") or {}

        # 群名片优先于 nickname，与 _resolve_member_name 保持一致
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

    async def _resolve_stream_id(self, ctx: PokeContext) -> str:
        """notice 消息 session_id 固定为空时按群/用户回查，TTL 缓存。"""
        if ctx.is_group and ctx.group_id:
            cache_group, cache_user = ctx.group_id, ""
        elif ctx.poker_id:
            cache_group, cache_user = "", ctx.poker_id
        else:
            return ""

        cached = self._state.get_cached_stream_id(
            group_id=cache_group, user_id=cache_user
        )
        if cached is not None:
            return cached

        stream_id = ""
        try:
            stream: Any
            if cache_group:
                stream = await self.ctx.chat.get_stream_by_group_id(
                    cache_group, platform="qq"
                )
            else:
                stream = await self.ctx.chat.get_stream_by_user_id(
                    cache_user, platform="qq"
                )
            if isinstance(stream, dict):
                stream_id = str(stream.get("session_id") or "")
        except Exception:
            self.ctx.logger.debug("回查 stream_id 失败", exc_info=True)
            return ""

        if stream_id:
            self._state.cache_stream_id(
                group_id=cache_group,
                user_id=cache_user,
                stream_id=stream_id,
                ttl=STREAM_ID_CACHE_TTL_SECONDS,
            )
        return stream_id


def create_plugin() -> SmartPokePlugin:
    """Runner 调用入口。"""
    return SmartPokePlugin()
