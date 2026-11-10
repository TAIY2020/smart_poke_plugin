"""Smart Poke 插件的通用辅助函数、常量与 manifest 版本读取。

跨模块共享的纯函数与跨模块常量，不依赖 SDK ctx。
"""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Any


_logger = logging.getLogger(__name__)


# 主动戳后台任务并发上限：群高速刷屏时按背压丢弃新任务，避免任务风暴。
PROACTIVE_TASK_QUEUE_LIMIT = 64


def _load_manifest_version() -> str:
    """从 _manifest.json 读取版本号，保持插件元数据单一来源。"""
    try:
        manifest_path = Path(__file__).resolve().parent.parent / "_manifest.json"
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        version = data.get("version")
        if isinstance(version, str) and version.strip():
            return version.strip()
        _logger.warning(
            "_manifest.json 中 version 字段缺失或非法 (%r)，回落到 0.0.0", version,
        )
    except Exception:
        _logger.warning("读取 _manifest.json 失败，回落到 0.0.0", exc_info=True)
    return "0.0.0"


PLUGIN_VERSION = _load_manifest_version()


def to_positive_int(value: Any) -> int | None:
    """把任意值解析为正整数；非正或不可解析时返 ``None``。"""
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


def extract_onebot_field(payload: Any, *keys: str) -> str:
    """按顺序取第一个非空字段值，兼容 ``data`` 已剥离与未剥离两种适配器返回。

    NapCat 与 SnowLuma ≥ 1.0.0 的查询型 API 已把 OneBot 响应的 ``data`` 剥到顶层；旧版
    SnowLuma 原样返回完整响应、字段包在 ``data`` 里。这里先在顶层按 ``keys`` 顺序找，
    全落空再钻一层 ``data``。``payload`` 非 dict（如失败信封 / None）时返回 ""。
    """
    if not isinstance(payload, dict):
        return ""
    for source in (payload, payload.get("data")):
        if not isinstance(source, dict):
            continue
        for key in keys:
            value = str(source.get(key) or "").strip()
            if value:
                return value
    return ""


# Host 提取路由身份 ``(account_id, scope)`` 所用的候选键（``RouteKeyFactory.ACCOUNT_ID_KEYS``
# / ``SCOPE_KEYS``），按顺序取第一个非空值；消息顶层取不到时才回退 ``additional_config``。
# 插件调 chat.open_session 必须传同样的 account_id / scope，算出的 session_id 才与入站
# 消息同一条流；否则会凭空创建一条无账号归属的"影子会话"。若 Host 调整该键序，需同步。
ROUTE_ACCOUNT_ID_KEYS = ("platform_io_account_id", "account_id", "self_id", "bot_account")
ROUTE_SCOPE_KEYS = ("platform_io_scope", "route_scope", "adapter_scope", "connection_id")


def _pick_first_non_empty(mapping: dict, keys: tuple[str, ...]) -> str:
    for key in keys:
        value = mapping.get(key)
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            return normalized
    return ""


def extract_route_components(additional_config: Any) -> tuple[str, str]:
    """从 ``additional_config`` 提取 ``(account_id, scope)`` 路由身份（兼容回退路径）。

    口径与 Host ``RouteKeyFactory.extract_components`` 相同（同样的候选键与优先级、
    ``str().strip()`` 后取第一个非空），取不到的分量返回空串。当前适配器把路由身份
    写在消息顶层（见 ``extract_message_route_components``），这里仅作旧适配器的兼容
    回退；Host 注入时还会补 ``platform_io_account_id`` / ``platform_io_scope``。
    """
    if not isinstance(additional_config, dict):
        return "", ""
    return (
        _pick_first_non_empty(additional_config, ROUTE_ACCOUNT_ID_KEYS),
        _pick_first_non_empty(additional_config, ROUTE_SCOPE_KEYS),
    )


def extract_message_route_components(message: Any, additional_config: Any) -> tuple[str, str]:
    """从入站消息提取 ``(account_id, scope)`` 路由身份，优先级与 Host 相同。

    当前适配器（如 SnowLuma）把 ``account_id`` / ``scope`` 写在消息顶层，
    ``additional_config`` 仅作旧适配器的兼容回退：顶层取不到非空值时才回退。
    """
    fallback_account_id, fallback_scope = extract_route_components(additional_config)
    if not isinstance(message, dict):
        return fallback_account_id, fallback_scope
    account_id = str(message.get("account_id") or "").strip() or fallback_account_id
    scope = str(message.get("scope") or "").strip() or fallback_scope
    return account_id, scope


def in_active_hours(start: int, end: int, now_hour: int) -> bool:
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


# 静音时段写法：``HH:MM-HH:MM``。容忍全角冒号与常见的连接符（- – — ~ ～ 至 到）。
_QUIET_PERIOD_PATTERN = re.compile(
    r"^\s*(\d{1,2})\s*[:：]\s*(\d{2})\s*[-–—~～至到]\s*(\d{1,2})\s*[:：]\s*(\d{2})\s*$"
)
_MINUTES_PER_DAY = 24 * 60


def _to_minute_of_day(hour: int, minute: int) -> int | None:
    """``HH:MM`` 转为当天第几分钟；``24:00`` 返回 1440，越界返回 ``None``。"""
    if minute > 59:
        return None
    if hour == 24:
        return _MINUTES_PER_DAY if minute == 0 else None
    if hour > 23:
        return None
    return hour * 60 + minute


def parse_quiet_period(value: Any) -> tuple[int, int] | None:
    """把一条 ``HH:MM-HH:MM`` 解析成 ``(start, end)`` 分钟数；写法非法返回 ``None``。

    起点的 ``24:00`` 视同 ``00:00``；终点的 ``24:00`` 保留为 1440（``00:00-24:00`` 即全天）。
    起止相同（如 ``08:00-08:00``）不构成时段，同样返回 ``None``——全天不响应应关掉
    群聊 / 私聊响应开关，而不是靠一条容易误配的"零长度"时段。
    """
    match = _QUIET_PERIOD_PATTERN.match(str(value or ""))
    if match is None:
        return None
    start_hour, start_minute, end_hour, end_minute = (int(part) for part in match.groups())
    start = _to_minute_of_day(start_hour, start_minute)
    end = _to_minute_of_day(end_hour, end_minute)
    if start is None or end is None:
        return None
    start %= _MINUTES_PER_DAY
    if start == end:
        return None
    return start, end


def format_quiet_period(period: tuple[int, int]) -> str:
    """把 ``(start, end)`` 分钟数格式化回规范写法 ``HH:MM-HH:MM``。"""
    start, end = period
    return f"{start // 60:02d}:{start % 60:02d}-{end // 60:02d}:{end % 60:02d}"


def parse_quiet_periods(values: Any) -> tuple[tuple[int, int], ...]:
    """批量解析静音时段，跳过非法项（配置校验器已归一化，这里只是防御）。"""
    if not isinstance(values, (list, tuple)):
        return ()
    periods = (parse_quiet_period(value) for value in values)
    return tuple(period for period in periods if period is not None)


def in_quiet_periods(periods: tuple[tuple[int, int], ...], now_minute: int) -> bool:
    """判断当天第 ``now_minute`` 分钟是否落在任一静音时段 [start, end) 内（本地时间）。

    ``start > end`` 为跨午夜时段（如 23:30-07:00）；终点不含，07:00 整即恢复。
    """
    for start, end in periods:
        if start < end:
            if start <= now_minute < end:
                return True
        elif now_minute >= start or now_minute < end:
            return True
    return False


def local_minute_of_day() -> int:
    """宿主本地时间的当天第几分钟，与 active_hour_* 同口径。"""
    now = time.localtime()
    return now.tm_hour * 60 + now.tm_min


def format_local_date(timestamp: float) -> str:
    """格式化为 ``YYYY-MM-DD`` 本地日期串。"""
    return time.strftime("%Y-%m-%d", time.localtime(timestamp))
