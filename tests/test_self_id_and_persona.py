import asyncio
import unittest
from types import SimpleNamespace


from core.proactive import ProactivePoker
from core.reaction import _DEFAULT_LLM_PERSONA, ReactionExecutor
from core.state import PokeStateManager


class _Logger:
    def debug(self, *args, **kwargs):
        pass


def _fake_plugin(*, persona="", personality=None, config_values=None):
    config_values = config_values or {}

    async def config_get(key, default=None):
        if isinstance(config_values.get(key), Exception):
            raise config_values[key]
        return config_values.get(key, default)

    ctx = SimpleNamespace(logger=_Logger(), config=SimpleNamespace(get=config_get))
    if personality is not None:
        async def get_current():
            if isinstance(personality, Exception):
                raise personality
            return personality

        ctx.personality = SimpleNamespace(get_current=get_current)
    config = SimpleNamespace(
        reaction=SimpleNamespace(llm_persona=persona),
        proactive=SimpleNamespace(enabled=False),
    )
    return SimpleNamespace(ctx=ctx, config=config, _state=PokeStateManager())


class LearnSelfIdTests(unittest.TestCase):
    def _message(self, **top):
        return {
            "message_info": {
                "group_info": {"group_id": "123"},
                "user_info": {"user_id": "999"},
                "additional_config": {},
            },
            **top,
        }

    def test_learns_from_top_level_account_id(self):
        plugin = _fake_plugin()
        ProactivePoker(plugin)._extract_signal(self._message(account_id="10001"))
        self.assertEqual(plugin._state.get_known_self_id(), "10001")

    def test_falls_back_to_additional_config_self_id(self):
        plugin = _fake_plugin()
        message = self._message()
        message["message_info"]["additional_config"] = {"self_id": "10002"}
        ProactivePoker(plugin)._extract_signal(message)
        self.assertEqual(plugin._state.get_known_self_id(), "10002")

    def test_bot_own_message_is_excluded(self):
        plugin = _fake_plugin()
        message = self._message(account_id="999")
        self.assertIsNone(ProactivePoker(plugin)._extract_signal(message))


class ResolvePersonaTests(unittest.TestCase):
    def _resolve(self, plugin):
        return asyncio.run(ReactionExecutor(plugin)._resolve_persona())

    def test_configured_persona_wins(self):
        plugin = _fake_plugin(persona="自定义", personality={"personality": "全局"})
        self.assertEqual(self._resolve(plugin), "自定义")

    def test_uses_get_current_with_name_and_style(self):
        plugin = _fake_plugin(
            personality={"personality": "是猫娘", "reply_style": "简短", "multiple_probability": 0},
            config_values={"bot.nickname": "麦麦"},
        )
        self.assertEqual(self._resolve(plugin), "你的名字是麦麦。\n是猫娘\n你说话的风格：简短")

    def test_alternate_style_by_probability(self):
        plugin = _fake_plugin(personality={
            "personality": "p", "reply_style": "主", "multiple_reply_style": ["备"], "multiple_probability": 1,
        })
        self.assertEqual(self._resolve(plugin), "p\n你说话的风格：备")

    def test_falls_back_to_config_get_when_capability_missing(self):
        # 旧 SDK 没有 ctx.personality，旧 Host 调用会抛异常，两种都要回退
        for personality in (None, RuntimeError("denied")):
            plugin = _fake_plugin(
                personality=personality,
                config_values={"personality.personality": "旧人设"},
            )
            self.assertEqual(self._resolve(plugin), "旧人设")

    def test_default_when_everything_fails(self):
        plugin = _fake_plugin(config_values={"personality.personality": RuntimeError("x")})
        self.assertEqual(self._resolve(plugin), _DEFAULT_LLM_PERSONA)


if __name__ == "__main__":
    unittest.main()
