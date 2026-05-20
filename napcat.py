"""NapCat 戳一戳 RPC 客户端。

封装 ``adapter.napcat.message.send_poke`` 调用：

* NapCat 业务级失败识别（status / retcode 双判定）
* 失败日志按 label + 时间窗口抑制，避免风控期相同栈刷屏

调用方拿到 ``True`` 即可认为请求已被 NapCat 接受；返回 ``False`` 时已自动打过日志，
调用方按"未送达"走兜底（如回戳失败回退到文字回复）。
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from .common import to_positive_int


if TYPE_CHECKING:
    from .plugin import SmartPokePlugin


# 同 label 的失败在该窗口内只打一条 warning；其余降级为 debug。
SEND_POKE_FAILURE_LOG_SUPPRESS_SECONDS = 15.0


class NapcatPokeClient:
    """统一的 NapCat send_poke RPC 客户端。"""

    def __init__(self, plugin: "SmartPokePlugin") -> None:
        self._plugin = plugin
        self._failure_warned_at: dict[str, float] = {}

    async def send_poke(
        self,
        target_id: str,
        group_id: str,
        *,
        is_group: bool,
        label: str,
    ) -> bool:
        """发起一次 send_poke 调用。

        群聊场景按 NapCat 隐藏 schema 同时传 user_id / group_id / target_id，
        其中 target_id 与 user_id 同值。返回 ``True`` 表示 NapCat 已接受请求。

        Args:
            label: 日志标签，用于在失败抑制窗口里区分调用来源（``back_poke`` /
                ``bystander`` / ``proactive`` 等）。
        """
        target_int = to_positive_int(target_id)
        if target_int is None:
            return False

        call_kwargs: dict[str, Any] = {"user_id": target_int}
        if is_group:
            group_int = to_positive_int(group_id)
            if group_int is None:
                return False
            call_kwargs["group_id"] = group_int
            call_kwargs["target_id"] = target_int

        try:
            resp = await self._plugin.ctx.api.call(
                "adapter.napcat.message.send_poke", **call_kwargs
            )
            # 宿主层 RPC 无响应 / 反序列化失败时返回 None，按"未成功"处理让上层走兜底
            if resp is None:
                self._log_failure(label, "send_poke 无响应 (resp=None)")
                return False
            if isinstance(resp, dict) and resp.get("success") is False:
                # NapCat 业务失败由 adapter raise → Host _cap_api_call 包装为 success=False，
                # 错误信息含 adapter 抛出的 "NapCat 动作返回失败: action=xxx message=yyy"。
                self._log_failure(
                    label, f"宿主调用失败: {resp.get('error')}"
                )
                return False
            return True
        except Exception:
            self._log_failure(label, "调用异常", exc=True)
            return False

    def _log_failure(self, label: str, reason: str, *, exc: bool = False) -> None:
        """同 label 的失败在抑制窗口内只打一条 warning，避免风控期相同栈刷屏。"""
        now = time.time()
        last_warned = self._failure_warned_at.get(label, 0.0)
        logger = self._plugin.ctx.logger
        if now - last_warned >= SEND_POKE_FAILURE_LOG_SUPPRESS_SECONDS:
            logger.warning("[%s] send_poke %s", label, reason, exc_info=exc)
            self._failure_warned_at[label] = now
        else:
            logger.debug(
                "[%s] send_poke %s（已被抑制窗口降级）", label, reason, exc_info=exc,
            )
