"""Smart Poke 插件的强类型配置 Schema。

7 大 Section（plugin / reaction / fallback / user_control / emoji / bystander / proactive）
聚合到 ``SmartPokeConfig``，由 ``SmartPokePlugin.config_model`` 绑定。
"""

from __future__ import annotations

import logging
from typing import Any, Literal

from maibot_sdk import Field, PluginConfigBase
from pydantic import field_validator, model_validator


_logger = logging.getLogger(__name__)


# 配置 schema 版本（与插件版本独立，仅在配置字段结构变更时手动上调）
CONFIG_SCHEMA_VERSION = "1.7.5"


def _warn_if_delay_range_inverted(section: str, min_delay: float, max_delay: float) -> None:
    """配置告警：``max_delay < min_delay`` 时打一条 warning 提示。

    各模块的延迟抽样都用 ``hi = max(lo, cfg.max_delay_seconds)`` 兜底，误配 max<min
    不会崩溃，但会静默退化为"固定取 min 值"。这里只补一条提示、刻意不修正字段：
    ``PluginConfigBase`` 启用了 ``validate_assignment=True``，在 ``model_validator``
    内赋值会叠加触发额外校验，而运行时兜底已保证安全。
    """
    if max_delay < min_delay:
        _logger.warning(
            "%s 的 max_delay_seconds(%s) 小于 min_delay_seconds(%s)，"
            "运行时思考延迟将退化为固定 %ss；建议调整为 max_delay >= min_delay",
            section, max_delay, min_delay, min_delay,
        )


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
            "是否把麦麦自己的戳一戳互动写入对应聊天流的 Maisaka 上下文"
        ),
        json_schema_extra={
            "label": "[实验性] 把自己的戳与回复写入上下文",
            "hint": "默认关闭；开启后回复内容以正常聊天记录形式进上下文，戳动作以 QQ 原生提示风格记录",
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
                "0~1，越大越爱搭理。"
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
        description="反应时选择回戳的权重；各档权重按总和归一化，不要求加起来等于 1",
        json_schema_extra={
            "label": "回戳权重",
            "hint": "各档权重按和归一化抽取，不要求加起来等于 1",
            "x-widget": "slider",
            "min": 0.0,
            "max": 1.0,
            "step": 0.05,
        },
    )
    llm_weight: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description=(
            "反应时选择『调用 LLM 生成一句拟人回复』的权重，与回戳/表情/文字按总和归一化。"
            "0 表示关闭该档；该档经 ctx.llm.generate 直接生成、不走主链路，"
            "并把思考延迟与生成并行以吸收延迟，失败会自动回退到文字回复池"
        ),
        json_schema_extra={
            "label": "LLM 回复权重",
            "hint": "与回戳/表情/文字按总和归一化抽取；0 关闭。该档不走主链路、自带延迟吸收与回退兜底",
            "x-widget": "slider",
            "min": 0.0,
            "max": 1.0,
            "step": 0.05,
        },
    )
    emoji_weight: float = Field(
        default=0.15,
        ge=0.0,
        le=1.0,
        description="反应时选择发表情包的权重；各档权重按总和归一化，不要求加起来等于 1",
        json_schema_extra={
            "label": "表情权重",
            "hint": "各档权重按和归一化抽取，不要求加起来等于 1",
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
        description="反应时选择文字回复的权重；各档权重按总和归一化，不要求加起来等于 1",
        json_schema_extra={
            "label": "文字权重",
            "hint": "各档权重按和归一化抽取，不要求加起来等于 1",
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
        description=(
            "同一聊天内同一人的反应冷却时间（秒）；冷却期内该用户再戳一律忽略。"
            "换人戳不受此冷却影响——多人轮番场景由「每分钟反应上限」兜底"
        ),
        json_schema_extra={
            "label": "冷却时长（秒）",
            "hint": "按「聊天 + 戳的人」逐人冷却；换人不受影响，多人场景看「每分钟反应上限」",
        },
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

    # ----- LLM 反应档参数（仅当 LLM 回复权重 > 0 时生效）-----

    llm_model: Literal["utils", "replyer", "planner"] = Field(
        default="planner",
        description=(
            "LLM 反应档使用的模型任务槽位（对应 Host model_task_config 下的任务名，"
            "Host 按 task 名路由到该槽位配置的模型）。"
            "utils=通用快模型；"
            "replyer=主回复模型(最贴人设但可能较慢)；planner=规划快模型。"
            "只能在这几个槽位里选，不要手填具体模型名"
        ),
        json_schema_extra={
            "label": "LLM 模型槽位",
            "hint": "戳一戳要快可选 utils，要最贴人设可选 replyer",
            "x-widget": "select",
            "options": [
                {"value": "utils", "label": "utils（通用快模型）"},
                {"value": "replyer", "label": "replyer（主回复模型，最贴人设）"},
                {"value": "planner", "label": "planner（规划快模型）"},
            ],
        },
    )
    llm_max_tokens: int = Field(
        default=256,
        ge=0,
        le=512,
        description=(
            "LLM 反应档生成的最大 token 数。戳一戳回复通常一两句，设小以加快生成；"
            "0 表示不覆盖、改用 Host 模型配置"
        ),
        json_schema_extra={
            "label": "LLM 最大 token",
            "hint": "戳回复一两句即可，默认 256；调小更快，0 表示用 Host 配置",
        },
    )
    llm_temperature: float = Field(
        default=1.0,
        ge=0.0,
        le=2.0,
        description="LLM 反应档的采样温度，高一点让回复更随机有梗",
        json_schema_extra={
            "label": "LLM 温度",
            "x-widget": "slider",
            "min": 0.0,
            "max": 2.0,
            "step": 0.05,
        },
    )
    llm_persona: str = Field(
        default="",
        description=(
            "LLM 反应档的人设 / 说话风格描述，作为 system 提示注入。"
            "留空时自动读取麦麦的全局人设（personality.personality），与主聊天保持一致；"
            "填写则覆盖，可给戳一戳单独定制更冲 / 更皮的语气。一两句话点明性格即可"
        ),
        json_schema_extra={
            "label": "LLM 人设",
            "hint": "留空=自动同步麦麦全局人设；填写=给戳一戳单独定制语气",
            "placeholder": "留空将自动使用麦麦的全局人设",
            "x-widget": "textarea",
        },
    )
    llm_response_style: str = Field(
        default="",
        description=(
            "LLM 戳一戳回复的额外风格要求。留空时不额外指定风格，"
            "只由麦麦全局人设或上方 LLM 人设决定语气；填写后会追加到提示词约束里"
        ),
        json_schema_extra={
            "label": "LLM 额外回复风格",
            "hint": "可选；留空则不强行指定讽刺/毒舌等风格，让人设自然发挥",
            "placeholder": "例如：语气温和一点，像朋友随口吐槽",
            "x-widget": "textarea",
        },
    )
    llm_context_messages: int = Field(
        default=0,
        ge=0,
        le=20,
        description=(
            "LLM 反应档生成回复前，额外拉取最近多少条聊天记录作为氛围上下文注入提示词。"
            "0 表示不拉取（默认，最快，戳一戳作为独立轻量交互）；"
            ">0 时先 message.get_recent 拉取、build_readable 格式化后注入 system，"
            "让被戳回复能接住群里 / 对话正在聊的话题，代价是生成前多一次拉取延迟。"
            "仅在 LLM 回复权重 > 0 时生效；建议 5~10"
        ),
        json_schema_extra={
            "label": "LLM 上下文消息数",
            "hint": "0=不拉历史(最快、默认)；填几条让戳回复更连贯，会略增延迟，建议 5~10",
        },
    )

    @field_validator("llm_model", mode="before")
    @classmethod
    def _normalize_llm_model(cls, value: Any) -> str:
        normalized = "" if value is None else str(value).strip()
        if normalized in ("utils", "replyer", "planner"):
            return normalized
        # 空串（含旧配置遗留的 ""）/ 非法值统一回落到默认 planner——必须落在 Literal
        # 取值集内，否则 pydantic 校验失败会导致插件配置加载崩溃。
        if normalized:
            _logger.warning(
                "reaction.llm_model 配置值 %r 非法，已回落到 'planner'。"
                "合法取值: utils / replyer / planner",
                value,
            )
        return "planner"

    @model_validator(mode="after")
    def _check_delay_range(self) -> "ReactionSection":
        _warn_if_delay_range_inverted(
            "reaction", self.min_delay_seconds, self.max_delay_seconds
        )
        return self


class FallbackSection(PluginConfigBase):
    """文字回复随机池。"""

    __ui_label__ = "文字回复"

    normal_replies: list[str] = Field(
        default_factory=lambda: [
            "何意味",
            "你是不是没事干",
            "干啥",
        ],
        description="普通情况下的文字回复随机池",
        json_schema_extra={"label": "普通回复池"},
    )
    spam_replies: list[str] = Field(
        default_factory=lambda: [
            "够了",
            "好烦",
            "停！手！",
            "烦不烦",
            "再戳生气了",
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
        default=0.35,
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

    @model_validator(mode="after")
    def _check_delay_range(self) -> "BystanderSection":
        _warn_if_delay_range_inverted(
            "bystander", self.min_delay_seconds, self.max_delay_seconds
        )
        return self


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
        description="每天成功主动戳的次数上限（按本地日期归零，0 表示不限制）；仅统计真正发出去的戳，失败/取消不计入、当天可重试；达到上限后当天不再出手",
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

    @model_validator(mode="after")
    def _check_delay_range(self) -> "ProactiveSection":
        _warn_if_delay_range_inverted(
            "proactive", self.min_delay_seconds, self.max_delay_seconds
        )
        return self


class SmartPokeConfig(PluginConfigBase):
    """智能戳一戳插件完整配置。"""

    plugin: PluginSection = Field(default_factory=PluginSection)
    reaction: ReactionSection = Field(default_factory=ReactionSection)
    fallback: FallbackSection = Field(default_factory=FallbackSection)
    user_control: UserControlSection = Field(default_factory=UserControlSection)
    emoji: EmojiSection = Field(default_factory=EmojiSection)
    bystander: BystanderSection = Field(default_factory=BystanderSection)
    proactive: ProactiveSection = Field(default_factory=ProactiveSection)
