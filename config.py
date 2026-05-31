"""Smart Poke 插件的强类型配置 Schema。

7 大 Section（plugin / reaction / fallback / user_control / emoji / bystander / proactive）
聚合到 ``SmartPokeConfig``，由 ``SmartPokePlugin.config_model`` 绑定。
"""

from __future__ import annotations

import logging
from typing import Any, Literal

from maibot_sdk import Field, PluginConfigBase
from pydantic import field_validator


_logger = logging.getLogger(__name__)


# 配置 schema 版本（与插件版本独立，仅在配置字段结构变更时手动上调）
CONFIG_SCHEMA_VERSION = "1.6.0"


# --- 配置模型 ---


class PluginSection(PluginConfigBase):
    """插件基础元信息。"""

    __ui_label__ = "插件设置"

    name: str = Field(
        default="smart_poke_plugin",
        description="插件名称",
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
    record_self_poke_to_context: bool = Field(
        default=False,
        description=(
            "是否把这次戳行为追加到对应聊天流的 "
        ),
        json_schema_extra={
            "label": "[实验性] 把自己的戳写入上下文",
            "hint": "默认关闭；若开启会通过硬性「系统事件」前缀 + 第三人称 + 显式禁止复述指令降低风险",
        },
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
        default=0.6,
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
        default=0.2,
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
        default=0.6,
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
        default=60,
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
        _logger.warning(
            "bystander.target_strategy 配置值 %r 非法，已回落到 'victim'。"
            "合法取值: victim / poker / random",
            value,
        )
        return "victim"


class ProactiveSection(PluginConfigBase):
    """主动戳：群里有人说话时，按概率被勾起来戳一下熟人。"""

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
        default=0.035,
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
        _logger.warning(
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
