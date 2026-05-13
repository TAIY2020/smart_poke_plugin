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
import logging
import random
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Literal

from maibot_sdk import Field, HookHandler, MaiBotPlugin, PluginConfigBase
from maibot_sdk.types import ErrorPolicy, HookMode, HookOrder
from pydantic import field_validator


# 模块级 stdlib logger：在 ctx.logger 不可用的位置兜底。
# field_validator 是 classmethod，构造期还拿不到 self.ctx；
# _load_manifest_version() 在模块导入期就要打日志，也走它。
# 用 __name__ 让 logger 落在与 Runner IPC handler 对齐的命名空间下，
# 主进程侧按 logger 名做过滤/分组时能正确归属到本插件。
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

# 配置 schema 版本：与 PLUGIN_VERSION 独立追踪，仅在 SmartPokeConfig 字段结构变更时手动 bump。
# 与插件版本绑定会让每次发版都把用户的 config_version 顶到新值，将来想在 on_config_update
# 里做配置迁移就失去了可比对的锚点。
CONFIG_SCHEMA_VERSION = "1.3.0"

# 启动后探测关键词命中前的等待时间（秒）：表情库装载完成前提前探测会全部 miss。
# 改为「先短等一次，未命中则按 EMOJI_PROBE_RETRY_INTERVAL 重试 EMOJI_PROBE_MAX_ATTEMPTS 次」，
# 避免冷启动慢的部署里固定 5 秒还赶不上、或冷启动快时白等 5 秒。
EMOJI_PROBE_INITIAL_DELAY_SECONDS = 2.0
EMOJI_PROBE_RETRY_INTERVAL_SECONDS = 2.0
EMOJI_PROBE_MAX_ATTEMPTS = 3

# 选表情时按描述关键词探测的次数上限，避免关键词池很大时把所有 RPC 全打一遍。
# 启动期 _probe_emoji_keywords 会把命中过的关键词记入 _validated_emoji_keywords，
# 运行时优先采样这些已验证关键词，让 LIMIT 内的 RPC 命中率显著提高。
EMOJI_KEYWORD_PROBE_LIMIT = 3

# 已验证表情关键词的连续 miss 阈值：达到该次数后自动从验证集中移除，
# 应对「表情库后续被删除导致曾经命中的关键词再也查不到」的场景，
# 避免每次反应都白白把 RPC 配额消耗在已经失效的关键词上。
EMOJI_KEYWORD_MISS_THRESHOLD = 3

# 昵称缓存 TTL（秒）与容量上限：群里同一用户在短时间内被反复戳，避免重复 RPC
MEMBER_NAME_CACHE_TTL_SECONDS = 600.0
MEMBER_NAME_CACHE_MAX_SIZE = 256
# 昵称解析失败的负缓存 TTL：避免连戳同一无昵称用户时反复打 RPC
MEMBER_NAME_NEGATIVE_CACHE_TTL_SECONDS = 60.0

# stream_id 回查缓存 TTL：notice 消息的 session_id 固定为空，每次反应都得回查；
# stream_id 在 Host 端基本稳定，可以长一点
STREAM_ID_CACHE_TTL_SECONDS = 1800.0

# 主动戳后台任务并发上限：群高速刷屏时 OBSERVE 每条都派发一次任务，
# 绝大多数会在早期 return，但瞬时仍会堆积。超过该阈值后丢弃新任务（背压），
# 避免极端流量下任务风暴拖累 Runner 调度。
PROACTIVE_TASK_QUEUE_LIMIT = 64
# 主动戳 self_id 的兜底：观察消息阶段拿不到 self_id 时，至少能从最近一次 notify.poke 事件里学到。
# 模块级缓存让该信息跨多个聊天流共享，避免每个群独立踩坑。
# 它只是个"软约束"——拿不到 self_id 也只是无法过滤掉麦麦自己发的消息，
# 不会造成功能性 bug（麦麦自己发消息也走 send.* 出站，理论上不会再被 receive Hook 接到）。


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
        description="反应时选择回戳的权重；三种权重按总和归一化，不要求加起来等于 1",
        json_schema_extra={
            "label": "回戳权重",
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
            "x-widget": "slider",
            "min": 0.0,
            "max": 1.0,
            "step": 0.05,
        },
    )
    text_weight: float = Field(
        default=0.2,
        ge=0.0,
        le=1.0,
        description="反应时选择文字回复的权重；三种权重按总和归一化，不要求加起来等于 1",
        json_schema_extra={
            "label": "文字权重",
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
            "react_probability 未命中时仍然挤一句 silent_replies 的概率。"
            "意义上等价于「装看不见但偶尔抱怨一下」；命中后会消耗冷却，避免短时间内反复挤话"
        ),
        json_schema_extra={
            "label": "沉默时发言概率",
            "hint": "未命中反应概率时，挤一句 silent_replies 的概率（会消耗冷却）",
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
            "hint": "False 时未反应的戳会照常传给主程序",
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
            "hint": "默认 3 让暴戳态连戳两下；调到 1 退化为单次回戳，调到 3~5 更狠",
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
            "诶诶诶，戳什么戳",
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
        default=30,
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
            "hint": "建议保持与 lookback_seconds / recent_window_seconds 的预期密度匹配",
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
            "是否避开『最近戳过麦麦的用户』——开启后会跳过 spam_window_seconds 内戳过麦麦的人，"
            "避免在用户刚戳完麦麦时立即反过去骚扰对方（容易显得报复性）"
        ),
        json_schema_extra={"label": "尊重 spam 历史"},
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
    """从一次戳一戳事件中提取出的关键信息。

    ``_extract_poke_context`` 在识别失败时直接返回 ``None``，因此一旦构造出
    ``PokeContext`` 实例，里面的字段一定是「成功识别」的状态。是否戳麦麦本人
    由 ``is_poking_bot`` 派生计算，不再单独存字段。
    """

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
        # napcat 注入的 notice 消息 session_id 常为空，需要按 stream_id → group_id → poker_id 回退
        self.cooldown_key: str = ""
        # 暴戳计数专用 scope：群聊固定用 group_id、私聊固定用 poker_id。
        # 与 cooldown_key 解耦的原因：proactive 分支查询 _poked_bot_recently 时只能传
        # group_id，必须与 record 端用同一个 scope 才能匹配；不能依赖
        # "napcat notice 的 session_id 永远为空" 这种上游隐式事实。
        self.spam_scope_key: str = ""

    @property
    def is_poking_bot(self) -> bool:
        """目标是否为麦麦本人。"""
        return bool(self.self_id) and self.target_id == self.self_id


class PokeStateManager:
    """每个插件实例独立持有的状态：冷却时间戳与每用户戳次数窗口。

    设计要点：
    - 戳麦麦冷却按 ``scope_key:poker_id`` 维度计：不同人独立冷却，A 触发冷却不阻挡 B。
    - 跟风戳冷却按 ``scope_key`` 维度计：避免麦麦在群里跟风刷屏。
    - 暴戳计数同样按 ``scope_key:poker_id`` 维度，与戳麦麦冷却一致。
    - ``scope_key`` 由调用方决定（stream_id → group_id → poker_id 回退），
      避免 napcat 注入的 notice 消息 session_id 为空时冷却完全失效。
    - 暴戳窗口用 ``deque`` 存时间戳，popleft 把过期项整体移出，避免每次戳重建 list。
    - 字典 key 不会自动消失，因此在 record_poke_and_count 中按计数触发 _prune。
    """

    _PRUNE_THRESHOLD = 200
    _STALE_AFTER_SECONDS = 3600

    def __init__(self) -> None:
        self._last_react_at: dict[str, float] = {}
        self._poke_records: dict[str, deque[float]] = defaultdict(deque)
        self._last_bystander_at: dict[str, float] = {}
        self._record_counter: int = 0
        # 戳麦麦反应的滑动窗口：所有"真的反应了"（mark_reacted 调用）的时间戳，
        # 用于实现"过去 N 秒内反应总数上限"。与逐人冷却互补——
        # 后者防同一人短时刷屏，前者防多人轮番车轮战。
        # 跟风戳 / 主动戳不占用这个窗口，因为它们已有各自的冷却 + 日上限。
        self._reaction_window: deque[float] = deque()
        # 昵称缓存：key = "group_id:user_id" 或 "user_id"；value = (nickname, expire_at)。
        # 允许 nickname 为空串，表示「已知该用户没有可解析的昵称」的负缓存条目。
        self._name_cache: dict[str, tuple[str, float]] = {}
        # stream_id 回查缓存：notice 消息的 session_id 固定为空，每次反应都得回查。
        # key = "group:{id}" / "user:{id}"；value = (stream_id, expire_at)。
        self._stream_id_cache: dict[str, tuple[str, float]] = {}
        # 主动戳：每群最近一次出手时间（按 group_id 维度，不带 user_id —— 主动戳的冷却约束的是
        # "这个群被骚扰的密度"，而不是"某个具体用户在这个群里多久没被戳过"）
        self._last_proactive_at_chat: dict[str, float] = {}
        # 主动戳：全局最近一次出手时间。避免在多个群间无间隔连续戳，"撒野"感太重
        self._last_proactive_global_at: float = 0.0
        # 主动戳：当日次数 + 日期标签。日期变化时归零；这里用本地日期串，
        # 与配置项 active_hour_start/end 的"本地时间"语义保持一致
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
        # 同时把本次反应推进滑动窗口；调用方不需要单独 push。
        # silent_reply 也走这个方法，所以"挤一句话"也会占一个窗口槽，与拟人语义一致。
        self._reaction_window.append(now)

    def peek_reaction_window(self, window_seconds: int) -> int:
        """只读式查询：窗口内当前累计反应数（顺便清理过期记录）。

        与 ``record_*_and_count`` 不同：本方法不写入新记录，仅用于"检查是否超限"。
        真正的"记录一次反应"由 ``mark_reacted`` 内部 push 完成，避免重复计数。
        """
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
        """缓存昵称查询结果。

        ``name`` 允许为空串：表示「已查询过但该用户没有可用昵称」的负缓存条目，
        避免连戳同一无昵称用户时反复打 RPC。负缓存通常配更短的 TTL。
        """
        if not user_id:
            return
        key = self._name_cache_key(group_id, user_id)
        self._name_cache[key] = (name, time.time() + ttl)
        if len(self._name_cache) > MEMBER_NAME_CACHE_MAX_SIZE:
            # 超过容量上限时，丢弃已过期或最早过期的一半，保持复杂度可控
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
        """缓存 stream_id 回查结果。``stream_id`` 为空串时不缓存（避免污染）。"""
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
        """记录一次主动戳：刷新群冷却 + 全局冷却 + 日计数。

        ``today`` 由调用方按本地日期生成（``time.strftime('%Y-%m-%d')``），
        让"日切归零"语义与配置项 ``active_hour_*`` 的本地时间口径一致。
        """
        now = time.time()
        if group_id:
            self._last_proactive_at_chat[group_id] = now
        self._last_proactive_global_at = now
        if today != self._proactive_daily_date:
            self._proactive_daily_date = today
            self._proactive_daily_count = 0
        self._proactive_daily_count += 1

    def proactive_daily_count(self, today: str) -> int:
        """返回当前『今天』的主动戳计数；日期变化时返回 0（但不主动清零，等下次 mark 时清）。"""
        if today != self._proactive_daily_date:
            return 0
        return self._proactive_daily_count

    def poked_bot_recently(
        self, scope_key: str, user_id: str, window_seconds: int
    ) -> bool:
        """``user_id`` 是否在 ``window_seconds`` 内于 ``scope_key`` 维度戳过麦麦。

        复用主分支累计的 _poke_records（戳麦麦事件累计起来的窗口记录），
        让主动戳能尊重「对方刚戳过麦麦」的记忆，避免立即反过去骚扰对方。
        - ``scope_key`` 与主 Hook 保持一致：群聊场景下用 group_id；
        - 任一参数缺失或窗口非正都视为「没戳过」，调用方据此跳过过滤。
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
            deque,
            {k: v for k, v in self._poke_records.items() if v and v[-1] >= cutoff},
        )
        # 反应滑动窗口：清掉 _STALE_AFTER_SECONDS 之前的陈旧记录。
        # peek_reaction_window 已经会按调用方传入的窗口长度清理，但很少触发反应的部署里
        # 窗口可能长时间不被读，借 _prune 兜一下避免内存悄悄涨。
        while self._reaction_window and self._reaction_window[0] < cutoff:
            self._reaction_window.popleft()
        # 昵称缓存与 stream_id 缓存按自身过期时间清理
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


def _in_active_hours(start: int, end: int, now_hour: int) -> bool:
    """判断当前小时是否落在 [start, end) 活跃区间内（本地时间，24h 制）。

    - ``start == end``：视为「全天活跃」，便于用户禁用时段限制时只填同一个值；
    - ``start < end``：普通区间，例如 9 ~ 24 表示早 9 到晚 24（即跨整个白天）；
    - ``start > end``：跨午夜区间，例如 22 ~ 2 表示晚 22 到次日 2 点。

    end 是开区间，与 Python ``range`` 一致，避免「24:00 算不算」的歧义。
    特别地，``end == 24`` 不做 ``% 24`` 归一化——这样默认配置 ``start=9, end=24``
    才能正确表达「从早 9 点直到午夜」，否则 24 → 0 后会被误判成跨午夜区间。
    """
    start = start % 24
    end_normalized = end if end == 24 else end % 24
    if start == end_normalized:
        return True
    if start < end_normalized:
        return start <= now_hour < end_normalized
    return now_hour >= start or now_hour < end_normalized


def _format_local_date(timestamp: float) -> str:
    """按本地时区把 UNIX 时间戳格式化成 ``YYYY-MM-DD``，用于主动戳的日切判断。"""
    return time.strftime("%Y-%m-%d", time.localtime(timestamp))


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
        # 启动期 _probe_emoji_keywords 探测出的「真的能查到表情」的关键词。
        # 运行时 _sample_probe_keywords 优先采样这些已验证关键词，命中率更高。
        # 注意：表情库可能后续注册新表情，因此「未验证」不代表「永远不命中」，
        # 采样时仍会混入未验证关键词作为补充与刷新机制。
        self._validated_emoji_keywords: list[str] = []
        # 已验证关键词的连续 miss 计数：累计达到 EMOJI_KEYWORD_MISS_THRESHOLD 后
        # 自动从验证集中移除，处理「表情库后来被删，关键词长期失效」的场景。
        self._validated_emoji_miss_counts: dict[str, int] = {}
        # 主动戳的群白名单 / 黑名单字符串集合，与 _blacklist 一同在 _refresh_user_sets 里刷新
        self._proactive_whitelist_groups: set[str] = set()
        self._proactive_blacklist_groups: set[str] = set()
        # 缓存最近一次观测到的 self_id：proactive observe 阶段拿不到 self_id，
        # 而第一次戳一戳事件触达后能从 napcat payload 学到它，可用于过滤"自己说话触发主动戳"等边界。
        # 拿不到也不致命：麦麦自己发的消息从 send.* 出站，理论上不会再被 receive Hook 接到。
        self._last_known_self_id: str = ""
        # 主动戳的 per-group asyncio.Lock：保证"冷却检查 + mark"是原子的，
        # 避免高速消息流下同一群多个并发任务同时穿过冷却 → 双发。
        # 锁惰性创建；正常使用下群数量是常数，不主动清理（即使万级群也只占几 MB）。
        self._proactive_locks: dict[str, asyncio.Lock] = {}
        # 主动戳后台任务实时计数：_spawn_background_task 自带的 _pending_tasks 不区分 label，
        # 这里单独计数 proactive，便于按 PROACTIVE_TASK_QUEUE_LIMIT 背压。
        self._proactive_active_count: int = 0

    # ===== 生命周期 =====

    async def on_load(self) -> None:
        self._refresh_user_sets()
        self.ctx.logger.info("智能戳一戳插件(v%s)初始化完成。", PLUGIN_VERSION)
        # 探测一次表情库的 emotion 标签，提示用户关键词命中情况
        self._spawn_background_task(self._probe_emoji_keywords(), "emoji_keyword_probe")

    async def on_unload(self) -> None:
        # 取消未完成的反应任务并等待真正终止，避免 Runner 卸载后还在调用 capability
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
            self.ctx.logger.info("配置已热更新到 v%s。", version)

    def _refresh_user_sets(self) -> None:
        """把配置中的黑名单 / 主动戳群白名单 / 黑名单归一化成字符串集合，便于 O(1) 查询。"""
        cfg = self.config
        self._blacklist = {str(x).strip() for x in cfg.user_control.blacklist if str(x).strip()}
        self._proactive_whitelist_groups = {
            str(x).strip() for x in cfg.proactive.whitelist_groups if str(x).strip()
        }
        self._proactive_blacklist_groups = {
            str(x).strip() for x in cfg.proactive.blacklist_groups if str(x).strip()
        }

    def _spawn_background_task(self, coro: Any, label: str, timeout: float = 120.0) -> None:
        """提交后台任务并登记到 _pending_tasks；带超时兜底防止挂死。

        子协程内部理应已有 try/except，本封装的超时只是最后一道防护：
        - 默认 120 秒：max_delay_seconds 上限 60s + 几次 RPC 调用，留出充足缓冲；
          将原本的 60s 提到 120s 是为了避免「max_delay 接近上限时正常路径自身
          刚好顶到超时」的边界。
        - 触发 TimeoutError 时 wait_for 会取消子协程，输出 warning。

        ``label == "proactive"`` 时启用背压：若当前主动戳并发任务已达 ``PROACTIVE_TASK_QUEUE_LIMIT``，
        直接 close 掉新提交的 coroutine 并打 debug。这样在群高速刷屏时也只是丢弃后续触发，
        不会拖垮 Runner；其余 label（react / silent / bystander）不受此限制。
        """
        is_proactive = label == "proactive"
        if is_proactive and self._proactive_active_count >= PROACTIVE_TASK_QUEUE_LIMIT:
            # 显式关闭避免 "coroutine was never awaited" 警告
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
                # 计数可能因热重载等极端情况错位，保底不让它跑成负数
                self._proactive_active_count = max(0, self._proactive_active_count - 1)

        task.add_done_callback(_on_done)

    def _sample_probe_keywords(self, keywords: list[str]) -> list[str]:
        """挑出本次用于「按描述搜表情」的关键词子集。

        采样策略：
        1. 已验证关键词（启动期探测命中过的）优先随机放在前面；
        2. 未验证关键词随机洗牌后接在后面，作为补充与刷新——表情库可能后续
           注册了新表情让某些关键词突然变得能命中；
        3. 最终截到 ``EMOJI_KEYWORD_PROBE_LIMIT``。

        因此每次反应最多发 LIMIT 次 RPC，但命中率显著高于纯随机洗牌。
        """
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
        """启动后试探一次按关键词搜表情，记录命中关键词到验证集。

        分两步：
        1. 先调一次 ``emoji.get_emotions()`` 拿到全量 emotion 标签集合，与配置关键词做
           子串双向匹配——既能命中"关键词等于标签"，也能命中"标签包含关键词"或
           "关键词包含标签"。本地命中的关键词直接进验证集，零 RPC 开销。
        2. 仅对本地未匹配的关键词再走 ``emoji.get_by_description`` 兜底探测；
           启动期表情库可能尚未装载完毕，使用"短延迟 + 重试"策略，
           每轮 ``EMOJI_PROBE_MAX_ATTEMPTS`` 次，发现至少一个命中即停止。

        SDK 对 ``emoji.get_by_description`` 与 ``emoji.get_emotions`` 的语义并不保证
        完全同集合：前者按描述模糊搜，后者只返回 emotion 标签。这里把"本地匹配"
        作为快速命中通道，把"逐个 RPC 探测"作为完整兜底，两条路径互补。
        """
        keywords = [str(k).strip() for k in self.config.emoji.description_keywords if str(k).strip()]
        if not keywords:
            return

        await asyncio.sleep(EMOJI_PROBE_INITIAL_DELAY_SECONDS)

        # 步骤 1：通过 get_emotions 一次性拿到 emotion 标签集做本地匹配
        prevalidated: list[str] = []
        try:
            emotions = await self.ctx.emoji.get_emotions()
        except Exception:
            self.ctx.logger.debug("emoji.get_emotions 调用失败，将退化到逐关键词探测", exc_info=True)
            emotions = None
        if isinstance(emotions, list) and emotions:
            emotion_set = {str(e).strip() for e in emotions if str(e).strip()}
            for kw in keywords:
                # 双向子串匹配：kw 直接是标签，或被标签包含，或包含某个标签
                if any(kw == e or kw in e or e in kw for e in emotion_set):
                    prevalidated.append(kw)
            if prevalidated:
                # 与步骤 2 的 extend + 去重一致，避免未来引入持久化验证集时被启动探测覆盖
                for kw in prevalidated:
                    if kw not in self._validated_emoji_keywords:
                        self._validated_emoji_keywords.append(kw)
                self.ctx.logger.info(
                    "emoji.get_emotions 本地匹配命中 %d/%d 个关键词：%s",
                    len(prevalidated), len(keywords), ", ".join(prevalidated),
                )

        # 步骤 2：未本地命中的关键词走 RPC 兜底；若已全部命中则直接结束
        remaining = [kw for kw in keywords if kw not in prevalidated]
        if not remaining:
            return

        for attempt in range(1, EMOJI_PROBE_MAX_ATTEMPTS + 1):
            validated_this_round: list[str] = []
            for kw in remaining:
                try:
                    emoji = await self.ctx.emoji.get_by_description(kw, limit=1)
                except Exception:
                    self.ctx.logger.debug(
                        "emoji 关键词探测 RPC 失败 (kw=%s)", kw, exc_info=True
                    )
                    continue
                if isinstance(emoji, dict) and emoji:
                    validated_this_round.append(kw)
            if validated_this_round:
                for kw in validated_this_round:
                    if kw not in self._validated_emoji_keywords:
                        self._validated_emoji_keywords.append(kw)
                self.ctx.logger.info(
                    "emoji 关键词 RPC 探测追加命中 %d 个：%s",
                    len(validated_this_round), ", ".join(validated_this_round),
                )
                return
            if attempt < EMOJI_PROBE_MAX_ATTEMPTS:
                self.ctx.logger.debug(
                    "emoji 关键词探测第 %d 轮全部未命中，%.1fs 后重试",
                    attempt, EMOJI_PROBE_RETRY_INTERVAL_SECONDS,
                )
                await asyncio.sleep(EMOJI_PROBE_RETRY_INTERVAL_SECONDS)

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
            # 不是戳一戳事件 → 放行
            return None

        # ----- 分支一：戳的不是麦麦（别人互戳）-----
        if not ctx.is_poking_bot:
            # 麦麦自己发起的戳（含主动戳 / 回戳 / 跟风戳触发的 notify 回声）：
            # send_poke 出去后 napcat 会回灌一条 poker_id=self_id 的事件，这里立即放行而不进入
            # _maybe_trigger_bystander —— 后者内部本来也会 return False，但提前过滤能省掉
            # 多次连续回戳 (back_poke_max_times > 1) 时 n 倍回声逐一走完整套检查的开销。
            if ctx.poker_id == ctx.self_id:
                return None
            # 仅尝试触发跟风戳；是否拦截事件由 bystander.swallow_event 决定
            triggered = self._maybe_trigger_bystander(ctx)
            if triggered and self.config.bystander.swallow_event:
                return {"action": "abort"}
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

        # 冷却（按 cooldown_key + poker_id 维度：不同人独立冷却）
        # 注意：冷却检查必须在 spam 计数之前，否则冷却期内的"无效戳"也会推动 spam 状态形成
        if self._state.in_cooldown(
            ctx.cooldown_key, ctx.poker_id, self.config.reaction.cooldown_seconds
        ):
            self.ctx.logger.debug(
                "[%s:%s] 戳一戳冷却中，已拦截",
                ctx.cooldown_key, ctx.poker_id,
            )
            return {"action": "abort"}

        # 暴戳计数：通过黑名单/场景/冷却检查后才累计，避免在冷却期内被无声推进。
        # 与反应概率解耦：即使本次因概率没命中没有动作，也会推动「被烦」状态形成。
        # scope_key 用 spam_scope_key（群聊=group_id、私聊=poker_id），与 cooldown_key 解耦，
        # 确保 proactive 分支的 _poked_bot_recently(group_id) 能稳定查到记录。
        poke_count = self._state.record_poke_and_count(
            ctx.spam_scope_key,
            ctx.poker_id,
            self.config.reaction.spam_window_seconds,
        )
        is_spam = poke_count >= self.config.reaction.spam_threshold

        # 滑动窗口频率限制：与逐人冷却互补——逐人冷却拦不住"10 个人轮番戳麦麦"，
        # 这里在通过冷却但即将进入反应分支前再做一道"过去 60 秒内总反应数"检查。
        # 静默 abort，不消耗 silent_chat_probability 也不推 spam 状态（spam 已经在上面记过了）。
        max_per_minute = self.config.reaction.max_reactions_per_minute
        if max_per_minute > 0:
            window_count = self._state.peek_reaction_window(60)
            if window_count >= max_per_minute:
                self.ctx.logger.debug(
                    "[%s] 60s 内累计反应 %d 次已达上限 %d，静默吞事件",
                    ctx.cooldown_key or ctx.poker_id, window_count, max_per_minute,
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
                # 冷却由 _silent_reply 内部在「确认能挤出一句话」（stream_id 解析成功
                # 且回复池非空）后再消耗，避免白白冷却用户。
                self._spawn_background_task(self._silent_reply(ctx), "silent_reply")
                # silent_reply 也算"做出了反应"，统一拦截事件
                return {"action": "abort"}
            # 什么都没发：由 swallow_when_silent 决定是否吞事件
            if self.config.reaction.swallow_when_silent:
                return {"action": "abort"}
            return None

        self._state.mark_reacted(ctx.cooldown_key, ctx.poker_id)

        # 异步执行反应（避免阻塞 Hook 链）
        self._spawn_background_task(
            self._react(ctx, is_spam, poke_count),
            "react",
        )

        # 拦截事件，避免 Host 把 "XX 发起了戳一戳" 当成普通消息再走一遍消息流程
        return {"action": "abort"}

    # ===== 跟风戳 =====

    def _maybe_trigger_bystander(self, ctx: PokeContext) -> bool:
        """检查是否应当对一次「别人互戳」事件做跟风反应。

        返回 ``True`` 表示已经派发跟风戳任务（调用方据此决定是否吞事件）；
        返回 ``False`` 表示本次跳过（任何前置检查未通过、或概率未命中）。
        """
        cfg = self.config.bystander
        if not cfg.enabled:
            return False
        # 跟风戳只在群聊场景成立：私聊本来就只有用户和麦麦两人，没有第三方目标。
        if not ctx.is_group:
            return False
        # 群聊响应总开关：用户关掉群聊响应通常也意味着「群里别凑热闹」，跟着禁用跟风戳
        if not self.config.reaction.react_in_group:
            return False
        # 戳的人或被戳的人是麦麦自己：交给主分支处理
        if ctx.poker_id == ctx.self_id or ctx.target_id == ctx.self_id:
            return False
        # 冷却：按 cooldown_key 维度（群聊场景下回退到 group_id）
        bystander_key = ctx.cooldown_key
        if self._state.in_bystander_cooldown(bystander_key, cfg.cooldown_seconds):
            return False
        # 概率
        if random.random() > cfg.probability:
            return False

        # 选定跟风目标
        target_id = self._pick_bystander_target(ctx)
        if not target_id:
            return False
        # 黑名单仅决定"麦麦不主动戳此人"——
        # 发起者 (poker) 在黑名单里也不阻止跟风戳被戳者的场景，
        # 只在最终目标命中黑名单时跳过。
        if target_id in self._blacklist:
            return False

        self._state.mark_bystander(bystander_key)

        self._spawn_background_task(
            self._react_bystander(ctx, target_id),
            "bystander",
        )
        return True

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

        # 延迟期间顺带解析一下 target 昵称，方便后续日志/文字模板使用
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
        """OBSERVE 旁路：每条入站群消息都会被「考虑」一次，再交由判定函数层层过滤。

        - ``OBSERVE`` 模式不会阻塞主消息链，也不会影响插件自身的戳一戳处理逻辑；
        - 当主 BLOCKING handler 对戳一戳事件 ``abort`` 时，dispatcher 在切到本 OBSERVE
          之前就 ``break`` 了 —— 因此戳一戳通知不会触发主动戳，避免事件回声；
        - 普通消息时主 BLOCKING handler ``return None``，本 OBSERVE 会被调度。
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
        """从入站消息中提取「群 ID + 说话人 ID」二元组，不满足主动戳前置条件则返回 ``None``。

        这里只负责「快速排除」，把真正的多重过滤留给 ``_maybe_proactive_poke``，避免在
        OBSERVE handler 的同步路径上做重活：
          - 必须是群聊（用户配置仅群聊触发，私聊永远不戳）；
          - 必须是真实文本消息，跳过通知事件（含 napcat notify.poke 自身），避免回声；
          - 说话人不能是麦麦自己 —— 通过 ``_last_known_self_id`` 兜底过滤；
          - 群 ID 必须是正整数；user_id 必须非空。
        """
        if not isinstance(message, dict):
            return None
        # 通知类事件（戳一戳 / 入群退群等）一律跳过：它们不是「群里有人说话」的语义信号
        if message.get("is_notify"):
            return None

        msg_info = message.get("message_info") or {}
        if not isinstance(msg_info, dict):
            return None

        group_info = msg_info.get("group_info") or {}
        if not isinstance(group_info, dict):
            return None
        group_id_raw = group_info.get("group_id")
        group_int = _to_positive_int(group_id_raw)
        if group_int is None:
            # 私聊或群号异常 → 不触发主动戳
            return None
        group_id = str(group_int)

        user_info = msg_info.get("user_info") or {}
        if not isinstance(user_info, dict):
            return None
        speaker_id = str(user_info.get("user_id") or "").strip()
        if not speaker_id:
            return None

        # 防御性过滤：理论上麦麦自己发的消息不会再走 receive Hook，
        # 但若 self_id 已知就额外加一道保险。
        if self._last_known_self_id and speaker_id == self._last_known_self_id:
            return None

        return group_id, speaker_id

    def _get_proactive_lock(self, group_id: str) -> asyncio.Lock:
        """惰性获取/创建 per-group asyncio.Lock。

        - 单一事件循环下 setdefault 自身是原子的（无 await 切点），
          因此不需要额外的注册锁；
        - 锁不主动清理：群数量在正常使用下是常数，每个空闲 ``Lock`` 仅几十字节，
          即使到上万规模总开销也只在 MB 级。
        """
        lock = self._proactive_locks.get(group_id)
        if lock is None:
            lock = asyncio.Lock()
            self._proactive_locks[group_id] = lock
        return lock

    async def _maybe_proactive_poke(self, group_id: str, speaker_id: str) -> None:
        """对一次群消息触发主动戳的完整判定与执行流程。

        在 OBSERVE handler 中以后台任务派发，所有耗时操作（昵称解析、最近消息回查、
        send_poke RPC）都在这里发生，避免拖慢主消息链。

        关键设计：把"冷却检查 → 候选筛选 → mark_proactive"包进 per-group ``asyncio.Lock``，
        保证同群多条消息并发触发时严格串行——第一个任务 mark 完后立刻设置冷却，
        排队中的后续任务再次检查时会直接被冷却拒绝。延迟与 send_poke 留在锁外，
        临界区只覆盖"决策与计数"，避免锁持有时间随网络延迟增长。
        """
        cfg = self.config.proactive

        # ----- 锁外早期过滤：把廉价、不可变的拒绝条件放在抢锁之前，避免无谓排队 -----
        now_struct = time.localtime()
        if not _in_active_hours(cfg.active_hour_start, cfg.active_hour_end, now_struct.tm_hour):
            return
        if group_id in self._proactive_blacklist_groups:
            return
        if self._proactive_whitelist_groups and group_id not in self._proactive_whitelist_groups:
            return
        if cfg.probability <= 0:
            return
        # 概率筛先于锁：大部分调用本就该 return，没必要排队浪费临界区时间
        if random.random() > cfg.probability:
            return

        target_id: str = ""
        target_name: str = ""

        # ----- 锁内：冷却 / 日上限 / RPC / 候选筛选 / mark -----
        async with self._get_proactive_lock(group_id):
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

            # 仍在锁内 → mark 把冷却/日计数推上去，让排队中的并发任务进入临界区时立即被冷却拒绝
            self._state.mark_proactive(group_id, today)

        # ----- 锁外：思考延迟 + 出手 -----
        lo = max(0.0, cfg.min_delay_seconds)
        hi = max(lo, cfg.max_delay_seconds)
        delay = random.uniform(lo, hi) if hi > 0 else 0
        if delay > 0:
            await asyncio.sleep(delay)

        # 延迟期间昵称可能还没解析，临时补一下方便日志
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
        """从最近消息列表里挑选候选戳目标。

        返回 ``(target_id, target_name, active_count)``：
        - ``target_id`` 为空串表示没有合适候选；
        - ``active_count`` 是 ``recent_window_seconds`` 内非麦麦、非通知消息条数，
          供调用方判断群活跃度。

        过滤规则（按拟人化要求层层叠加）：
        1. 跳过通知事件、麦麦自己说的话、user_id 为空的异常消息；
        2. 候选必须在 ``lookback_seconds`` 窗口内有过发言；
        3. ``respect_spam_history`` 打开时跳过近期戳过麦麦的人，避免报复性骚扰；
        4. 命中插件主黑名单 ``user_control.blacklist`` 的用户永远不戳；
        5. 根据 ``target_strategy`` 决定从候选里挑谁（最新说话者 / 随机活跃用户）。
        """
        cfg = self.config.proactive
        now = time.time()
        lookback_cutoff = now - cfg.lookback_seconds
        active_window_cutoff = now - cfg.recent_window_seconds
        self_id = self._last_known_self_id

        active_count = 0
        # 用 dict 保留每个用户「最近说话时间」与昵称；按时间倒序处理，第一次见到即最新
        candidates: dict[str, tuple[float, str]] = {}
        latest_speaker_id = ""
        latest_speaker_name = ""

        # message.get_recent 通常按时间正序返回；为了挑「最新说话者」反向遍历
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
            # 跳过麦麦自己
            if self_id and uid == self_id:
                continue

            if ts >= active_window_cutoff:
                active_count += 1
            if ts < lookback_cutoff:
                continue

            # 命中黑名单 / 暴戳历史的候选直接剔除
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
            # speaker_id 是触发本次 observe 的那一条消息的发送者，优先用它（与"刚说话"语义一致）；
            # 若 speaker_id 被前面的过滤剔除（黑名单 / 暴戳 / 时间过早），退到 latest_speaker_id。
            if speaker_id in candidates:
                ts_unused, uname = candidates[speaker_id]
                del ts_unused
                return speaker_id, uname, active_count
            if latest_speaker_id and latest_speaker_id in candidates:
                return latest_speaker_id, latest_speaker_name, active_count
            # 三层兜底：取候选里时间戳最新的那个，让 active_speaker 语义在边界场景也成立
            uid, (_ts_unused, uname) = max(
                candidates.items(), key=lambda kv: kv[1][0]
            )
            return uid, uname, active_count

        # random_recent
        uid = random.choice(list(candidates.keys()))
        ts_unused, uname = candidates[uid]
        del ts_unused
        return uid, uname, active_count

    def _poked_bot_recently(self, group_id: str, user_id: str) -> bool:
        """是否在 spam 窗口内戳过麦麦：薄封装，让 _pick_proactive_target 调用点更直观。"""
        return self._state.poked_bot_recently(
            group_id, user_id, self.config.reaction.spam_window_seconds
        )

    async def _resolve_stream_id_for_group(self, group_id: str) -> str:
        """专用于 proactive 路径的 stream_id 解析：仅按 group_id 查询、带 TTL 缓存。

        与 ``_resolve_stream_id`` 不同的是这里不需要 PokeContext，避免为了主动戳
        造一个空 ctx，逻辑上更直白；缓存共用 PokeStateManager 的 stream_id 表。
        """
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
        """解析群成员昵称，群名片优先于 nickname；带 TTL 缓存避免重复 RPC。

        - 缓存命中（含负缓存）直接返回；负缓存的值是空串，调用方据此知道「曾试过解析但失败」。
        - 群聊调用 ``adapter.napcat.group.get_group_member_info``。
        - 私聊场景退化到 ``adapter.napcat.account.get_stranger_info``。
        - 任何异常或解析为空都返回空串，并写入短 TTL 负缓存，避免短时间内重复 RPC。
        """
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
                    # 群名片优先于昵称
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
            # 失败也写入负缓存，避免连戳同一用户时反复重试
            self._state.cache_name(
                group_id, user_id, "", MEMBER_NAME_NEGATIVE_CACHE_TTL_SECONDS
            )
            return ""

        if name:
            self._state.cache_name(group_id, user_id, name, MEMBER_NAME_CACHE_TTL_SECONDS)
        else:
            # 查询成功但拿不到任何可用字段：同样写负缓存，复用短 TTL
            self._state.cache_name(
                group_id, user_id, "", MEMBER_NAME_NEGATIVE_CACHE_TTL_SECONDS
            )
        return name

    # ===== 反应主流程 =====

    async def _silent_reply(self, ctx: PokeContext) -> None:
        """react_probability 未命中时按 silent_chat_probability 概率挤出的一句轻反应。

        与正常反应分支不同：不参与反应类型抽样，只挑一句 silent_replies；
        silent_replies 池为空时回落到 normal_replies，避免「想挤话却没词可挤」的尴尬。

        冷却由本函数内部在「确认能挤出一句话」后才消耗——先解析 stream_id 与回复池，
        都成立才标记冷却。这样 stream_id 解析失败 / 回复池为空时不会冷却用户，
        避免「想挤话却挤不出还白白冷却，下一次戳又被冷却拦截」的尴尬。
        """
        try:
            # 先解析 stream_id：失败立即放弃，且不消耗冷却
            stream_id = await self._resolve_stream_id(ctx)
            if not stream_id:
                self.ctx.logger.debug(
                    "[silent_reply] 无法解析 stream_id (group=%s, poker=%s)，静默放弃",
                    ctx.group_id, ctx.poker_id,
                )
                return
            pool = self.config.fallback.silent_replies or self.config.fallback.normal_replies
            if not pool:
                self.ctx.logger.debug("[silent_reply] silent / normal 回复池均为空，放弃发送")
                return
            # 二次确认滑动窗口：handle_poke_event 派发本任务时窗口尚未满，
            # 但延迟期间可能被并发事件填满；mark 之前再 peek 一次保证总反应数严格不超上限。
            max_per_minute = self.config.reaction.max_reactions_per_minute
            if max_per_minute > 0 and self._state.peek_reaction_window(60) >= max_per_minute:
                self.ctx.logger.debug(
                    "[silent_reply] 派发到 mark 之间窗口被填满，静默放弃 (poker=%s)",
                    ctx.poker_id,
                )
                return
            # 已确认能发出一句话 → 才占用冷却，避免短时间内被同一人连戳时反复挤话
            self._state.mark_reacted(ctx.cooldown_key, ctx.poker_id)
            await self._delay_a_bit()
            await self._safe_send_text(random.choice(pool), stream_id)
        except Exception:
            self.ctx.logger.exception("silent_reply 发送失败")

    async def _react(self, ctx: PokeContext, is_spam: bool, poke_count: int) -> None:
        """决定反应类型并执行；外层捕获所有异常防止背景任务挂掉。"""
        try:
            # 先解析 stream_id 再延迟：与 _silent_reply 保持一致，避免选了 text/emoji
            # 但 stream_id 解析失败时白白等掉几秒思考延迟才回退到回戳。
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
                        # 回戳失败 → 回退文字
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
                # 表情发送失败 → 回退到回戳
                self.ctx.logger.debug("表情反应失败，回退到回戳")
                ok_poke = await self._send_back_poke(ctx, is_spam=is_spam)
                if not ok_poke:
                    # 回戳也失败再退到文字
                    await self._send_text(stream_id, is_spam)
                return

            # kind == "text"
            sent = await self._send_text(stream_id, is_spam)
            if sent:
                return
            # 文字池全空或发送失败 → 兜底回戳，避免「mark 了却什么都没发」的失声观感
            self.ctx.logger.debug(
                "[smart_poke] 文字反应未发出，回退到回戳 (poker=%s)", ctx.poker_id,
            )
            await self._send_back_poke(ctx, is_spam=is_spam)
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
        暴戳时会削弱回戳和表情权重、抬高文字权重，让麦麦更倾向「嫌烦」地说话——
        表情同样按 0.5 衰减，避免在「被烦」状态下还频繁发可爱表情造成违和。
        「真正沉默」由 handle_poke_event 中的 react_probability 未命中分支负责，
        与本方法的反应类型抽样无关。
        """
        cfg = self.config.reaction
        spam_mult_poke = 0.5 if is_spam else 1.0
        spam_mult_emoji = 0.5 if is_spam else 1.0
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
            if isinstance(resp, dict):
                # 宿主层 RPC 失败：resp = {"success": False, "error": "..."}
                if resp.get("success") is False:
                    self.ctx.logger.warning(
                        "[%s] send_poke 宿主调用失败: %s", label, resp.get("error")
                    )
                    return False
                # NapCat 业务级失败：resp 直接是 NapCat 原始响应，含 status / retcode
                status = str(resp.get("status") or "").strip().lower()
                retcode = resp.get("retcode")
                if (status and status not in ("ok", "async")) or (
                    isinstance(retcode, int) and retcode != 0
                ):
                    self.ctx.logger.warning(
                        "[%s] send_poke NapCat 业务失败: status=%s retcode=%s message=%s",
                        label,
                        status or "<none>",
                        retcode,
                        resp.get("message") or resp.get("wording"),
                    )
                    return False
            return True
        except Exception:
            # 风控 / 频率限制场景下 send_poke 会反复失败，stack trace 会刷屏，
            # 用 warning + exc_info 让用户按需开 DEBUG 看完整堆栈即可。
            self.ctx.logger.warning("[%s] send_poke 调用异常", label, exc_info=True)
            return False

    async def _send_back_poke(self, ctx: PokeContext, is_spam: bool = False) -> bool:
        """通过 NapCat 适配器发送回戳，支持多次连续戳。

        防御自戳死循环：当 user_control.ignore_self_poke=False 且麦麦戳了自己时，
        主分支会进入反应流程；若再回戳麦麦自己，新的 send_poke 会触发又一条
        notify.poke 事件被本插件接收，导致无限循环。这里兜底拒绝，让 _react
        回退到文字（如果 stream_id 可用）或直接放弃本次反应。

        多次回戳：
        - ``back_poke_max_times == 1`` 时（默认）行为与单次完全一致；
        - 普通状态下随机 1~max 次，给行为引入波动避免被识别为定时器；
        - ``is_spam=True``（暴戳状态）固定为 max 次，对应"被烦了一连串戳回去"的拟人语义；
        - 每两次之间 0.3~0.8s 随机短延迟：保留"连戳"节奏，又不会扎堆把 NapCat 风控点燃；
        - 任一中途失败立即停止（可能已经被风控或被对方屏蔽），返回 True 当且仅当至少有一次成功。
        """
        if ctx.poker_id == ctx.self_id:
            self.ctx.logger.warning(
                "拒绝回戳麦麦自己（self_id=%s）以避免戳一戳事件死循环", ctx.self_id,
            )
            return False

        max_times = max(1, self.config.reaction.back_poke_max_times)
        # 暴戳：直接戳到上限，"狠"一点；非暴戳：随机抽，给行为加扰动
        times = max_times if is_spam else random.randint(1, max_times)

        any_success = False
        for i in range(times):
            ok = await self._invoke_send_poke(
                ctx.poker_id, ctx.group_id, is_group=ctx.is_group, label="back_poke"
            )
            if ok:
                any_success = True
            else:
                # 中途失败立即停止：避免被风控时还继续刷请求加剧问题
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
        """从配置的回复池中随机挑一句文字发出。

        池回落链：暴戳态先 spam_replies；不论是不是暴戳，spam/normal 都空时回落到 silent_replies。
        这样用户清空 spam_replies 也不会出现『抽中 text 却什么都没发』的失声观感。
        返回是否成功投递，便于上层做兜底回戳。
        """
        if is_spam:
            primary = self.config.fallback.spam_replies
            secondary = self.config.fallback.normal_replies
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
        """挑一张表情包。优先按关键词搜，找不到时按配置回退随机。

        SDK 已经按 ``_CAPABILITY_RESULT_KEYS`` 表自动解包 RPC 响应：
        - ``emoji.get_by_description`` 直接返回 ``{"base64": ..., "description": ..., "emotion": ...}`` 或 ``None``。
        - ``emoji.get_random`` 直接返回 ``[{...}, ...]`` 列表。

        关键词采样策略与启动期探测共用 ``_sample_probe_keywords``，保证两边一致。
        运行时若发现未验证关键词也能命中（表情库注册了新表情），会顺手把它写入验证集，
        让后续采样优先复用。
        """
        probe = self._sample_probe_keywords(self.config.emoji.description_keywords)

        for kw in probe:
            try:
                emoji = await self.ctx.emoji.get_by_description(kw, limit=1)
                if isinstance(emoji, dict) and emoji:
                    if kw not in self._validated_emoji_keywords:
                        self._validated_emoji_keywords.append(kw)
                    # 命中 → 清零 miss 计数，让该关键词继续留在验证集中优先采样
                    self._validated_emoji_miss_counts.pop(kw, None)
                    return emoji
                self.ctx.logger.debug("按关键词 %s 没找到合适表情", kw)
                # 已验证关键词连续 miss 累计到阈值 → 从验证集中剔除，
                # 避免「表情库被删后该关键词永远 miss 但每次仍被优先尝试」
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

        # self_id：napcat codec 同时把 self_id 塞进 additional_config 和原 payload，
        # payload 是直接 dict(payload)，所以两边其实是同一份，直接读 payload 即可。
        self_id = str(payload.get("self_id") or "").strip()
        poker_id = str(payload.get("user_id") or "").strip()
        target_id = str(payload.get("target_id") or "").strip()

        if not self_id or not poker_id or not target_id:
            return None

        # 顺手把识别到的 self_id 缓存起来，给 proactive 分支提供过滤"自己说话"的兜底依据
        if self_id and self_id != self._last_known_self_id:
            self._last_known_self_id = self_id

        # 严格判定 group_id：必须是正整数才视为群聊，避免 "0" / 0 等被误判
        group_info = msg_info.get("group_info") or {}
        raw_group_id = payload.get("group_id")
        if raw_group_id is None or str(raw_group_id).strip() in ("", "0"):
            raw_group_id = group_info.get("group_id")
        group_int = _to_positive_int(raw_group_id)

        user_info = msg_info.get("user_info") or {}

        # 群名片优先于昵称：与 _resolve_member_name() 保持一致，避免主反应分支
        # 使用 nickname、跟风戳分支使用 card 的字段不一致。
        # napcat 适配器（codecs/notice/enricher.py）会同时填充 user_nickname / user_cardname。
        poker_cardname = str(user_info.get("user_cardname") or "").strip()
        poker_nickname = str(user_info.get("user_nickname") or "").strip()

        ctx = PokeContext()
        ctx.self_id = self_id
        ctx.poker_id = poker_id
        ctx.poker_name = poker_cardname or poker_nickname
        ctx.target_id = target_id
        # napcat 注入的 user_info 字段始终描述发起方，target 端没有现成昵称。
        # 主分支（戳麦麦）的 target 一定是麦麦自己，不需要昵称；
        # 跟风戳分支 (_react_bystander) 才会按需通过 _resolve_member_name() 异步补全 ctx.target_name。
        ctx.target_name = ""
        ctx.group_id = str(group_int) if group_int is not None else ""
        ctx.is_group = group_int is not None
        ctx.stream_id = str(message.get("session_id") or "")
        # 冷却 key fallback：napcat notice 注入的 session_id 常为空，
        # 必须按 stream_id → group_id → poker_id 回退才能保证冷却生效。
        ctx.cooldown_key = ctx.stream_id or ctx.group_id or ctx.poker_id
        # spam 计数 scope：群聊固定 group_id、私聊固定 poker_id。
        # 与 cooldown_key 解耦，确保 proactive 的 _poked_bot_recently(group_id) 能命中记录。
        ctx.spam_scope_key = ctx.group_id if ctx.is_group else ctx.poker_id
        return ctx

    async def _resolve_stream_id(self, ctx: PokeContext) -> str:
        """当 message.session_id 为空时，按群/用户回查 stream_id，并做 TTL 缓存。

        napcat 注入的 notice 消息 session_id 固定为空，每次反应都要回查；
        但 Host 端 stream_id 基本稳定，缓存收益明显。

        SDK 返回的聊天流字典字段名是 ``session_id``（见 guide.md 中的能力代理示例），
        这里直接读取该字段，不做多重 fallback。
        """
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
