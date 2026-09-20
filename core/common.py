"""Smart Poke 插件的通用辅助函数、常量与 manifest 版本读取。

跨模块共享的纯函数与跨模块常量，不依赖 SDK ctx。
"""

from __future__ import annotations

import json
import logging
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

    NapCat 查询型 API 已把 OneBot 响应的 ``data`` 剥到顶层（``card`` / ``nickname``
    直接可取）；部分 NapCat 兼容适配器（如 SnowLuma）则原样返回完整 OneBot 响应、
    字段裹在 ``data`` 里。这里先在顶层按 ``keys`` 顺序找，全落空再钻一层 ``data``，
    使昵称解析对两类适配器都成立。``payload`` 非 dict（如失败信封 / None）时返回 ""。
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


# Host 计算入站 session_id 时从 message_info.additional_config 提取路由身份所用的候选键，
# 与 Host ``RouteKeyFactory.ACCOUNT_ID_KEYS`` / ``SCOPE_KEYS`` 一致、按顺序取第一个非空值。
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
    """从入站消息的 ``additional_config`` 提取 ``(account_id, scope)`` 路由身份。

    口径与 Host ``RouteKeyFactory.extract_components`` 相同（同样的候选键与优先级、
    ``str().strip()`` 后取第一个非空），取不到的分量返回空串。NapCat / SnowLuma 两个
    适配器都会把 ``self_id``（及可选的 ``connection_id``）写进 additional_config，
    Host 注入时还会补 ``platform_io_account_id`` / ``platform_io_scope``。
    """
    if not isinstance(additional_config, dict):
        return "", ""
    return (
        _pick_first_non_empty(additional_config, ROUTE_ACCOUNT_ID_KEYS),
        _pick_first_non_empty(additional_config, ROUTE_SCOPE_KEYS),
    )


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


def format_local_date(timestamp: float) -> str:
    """格式化为 ``YYYY-MM-DD`` 本地日期串。"""
    return time.strftime("%Y-%m-%d", time.localtime(timestamp))
