import asyncio
import unittest
from types import SimpleNamespace

from core.common import (
    format_quiet_period,
    in_quiet_periods,
    parse_quiet_period,
    parse_quiet_periods,
)
from core.config import ReactionSection
from core.reaction import ReactionExecutor
from core.state import PokeContext


def _minute(text):
    hour, minute = text.split(":")
    return int(hour) * 60 + int(minute)


class ParseQuietPeriodTests(unittest.TestCase):
    def test_parses_plain_and_cross_midnight(self):
        self.assertEqual(parse_quiet_period("12:30-13:30"), (750, 810))
        self.assertEqual(parse_quiet_period("23:30-07:00"), (1410, 420))

    def test_tolerates_common_separators_and_spacing(self):
        for text in ("23:30–07:00", "23：30 ~ 7:00", " 23:30至07:00 ", "23:30—07:00"):
            self.assertEqual(parse_quiet_period(text), (1410, 420), text)

    def test_end_of_day_24_00(self):
        self.assertEqual(parse_quiet_period("22:00-24:00"), (1320, 1440))
        self.assertEqual(parse_quiet_period("00:00-24:00"), (0, 1440))
        self.assertEqual(parse_quiet_period("24:00-06:00"), (0, 360))

    def test_rejects_invalid(self):
        for text in ("", None, "abc", "25:00-07:00", "23:60-07:00", "24:30-07:00", "23:30", "08:00-08:00", 2330):
            self.assertIsNone(parse_quiet_period(text), text)

    def test_format_round_trip(self):
        self.assertEqual(format_quiet_period((1410, 420)), "23:30-07:00")
        self.assertEqual(format_quiet_period((1320, 1440)), "22:00-24:00")

    def test_parse_many_skips_invalid(self):
        self.assertEqual(parse_quiet_periods(["23:30-07:00", "bad", "12:30-13:30"]), ((1410, 420), (750, 810)))
        self.assertEqual(parse_quiet_periods(None), ())


class InQuietPeriodsTests(unittest.TestCase):
    PERIODS = parse_quiet_periods(["23:30-07:00", "12:30-13:30"])

    def test_inside(self):
        for text in ("23:30", "23:59", "00:00", "00:30", "06:59", "12:30", "12:45", "13:29"):
            self.assertTrue(in_quiet_periods(self.PERIODS, _minute(text)), text)

    def test_outside_and_end_exclusive(self):
        for text in ("07:00", "13:30", "23:29", "12:29", "18:00"):
            self.assertFalse(in_quiet_periods(self.PERIODS, _minute(text)), text)

    def test_empty_never_quiet(self):
        for now_minute in (0, 720, 1439):
            self.assertFalse(in_quiet_periods((), now_minute))

    def test_whole_day(self):
        periods = parse_quiet_periods(["00:00-24:00"])
        for now_minute in (0, 720, 1439):
            self.assertTrue(in_quiet_periods(periods, now_minute))


class QuietPeriodsConfigTests(unittest.TestCase):
    def test_default_is_empty(self):
        self.assertEqual(ReactionSection().quiet_periods, [])

    def test_normalizes_drops_invalid_and_dedupes(self):
        with self.assertLogs("core.config", level="WARNING") as logs:
            section = ReactionSection(
                quiet_periods=["23：30 ~ 7:00", "12:30-13:30", "", "08:00-08:00", "oops", "23:30-07:00"]
            )
        self.assertEqual(section.quiet_periods, ["23:30-07:00", "12:30-13:30"])
        self.assertEqual(len(logs.records), 2)

    def test_accepts_single_string_and_none(self):
        self.assertEqual(ReactionSection(quiet_periods="12:30-13:30").quiet_periods, ["12:30-13:30"])
        self.assertEqual(ReactionSection(quiet_periods=None).quiet_periods, [])


class _Logger:
    def debug(self, *args, **kwargs):
        pass

    info = warning = exception = debug


class _Harness:
    """最小假插件：记录每一次真正落地的发送动作，``quiet`` 可在运行中翻转。"""

    def __init__(self, *, quiet=False, emoji_ok=True, **reaction):
        self.quiet = quiet
        self.sent: list[str] = []
        harness = self

        async def send_text(text, stream_id, **kwargs):
            harness.sent.append("text")
            return True

        async def send_emoji(data, stream_id, **kwargs):
            harness.sent.append("emoji")
            # 表情发送失败的那一刻跨进静音起点
            if not emoji_ok:
                harness.quiet = True
            return emoji_ok

        async def send_poke(*args, **kwargs):
            harness.sent.append("poke")
            return True

        async def pick_emoji():
            return {"base64": "xx"}

        async def resolve_stream(ctx, allow_open=False):
            return "stream"

        async def noop(*args, **kwargs):
            return None

        weights = {"back_poke_weight": 0.0, "emoji_weight": 0.0, "text_weight": 0.0, "llm_weight": 0.0}
        weights.update(reaction)
        self.plugin = SimpleNamespace(
            ctx=SimpleNamespace(logger=_Logger(), send=SimpleNamespace(text=send_text, emoji=send_emoji)),
            config=SimpleNamespace(
                plugin=SimpleNamespace(record_self_poke_to_context=False),
                reaction=SimpleNamespace(
                    min_delay_seconds=0.0, max_delay_seconds=0.0, back_poke_max_times=1,
                    react_in_group=True, react_in_private=True, **weights,
                ),
                fallback=SimpleNamespace(normal_replies=["a"], spam_replies=["b"], silent_replies=["c"]),
            ),
            _blacklist=set(),
            _napcat=SimpleNamespace(send_poke=send_poke),
            _emoji=SimpleNamespace(pick_emoji=pick_emoji),
            in_quiet_period=lambda: harness.quiet,
            resolve_stream_id_for_context=resolve_stream,
            record_poked_by_to_context=noop,
            record_self_poke_to_context=noop,
        )
        self.executor = ReactionExecutor(self.plugin)

    def react(self):
        ctx = PokeContext()
        ctx.self_id, ctx.poker_id, ctx.group_id, ctx.is_group = "1", "2", "3", True
        ctx.cooldown_key = "3"
        asyncio.run(self.executor.react_to_poke(ctx, False, 1, None))
        return self.sent


class QuietPeriodReactionTests(unittest.TestCase):
    def test_reacts_normally_outside_quiet_period(self):
        self.assertEqual(_Harness(back_poke_weight=1.0).react(), ["poke"])
        self.assertEqual(_Harness(text_weight=1.0).react(), ["text"])
        self.assertEqual(_Harness(emoji_weight=1.0).react(), ["emoji"])

    def test_delayed_reaction_dropped_once_quiet(self):
        # 入口放行后、发送前进入静音：各档都不应发出任何东西，也不触发级联回退
        for kind in ("back_poke_weight", "text_weight", "emoji_weight"):
            self.assertEqual(_Harness(quiet=True, **{kind: 1.0}).react(), [], kind)

    def test_fallback_chain_stays_silent_when_quiet_starts_mid_send(self):
        # 表情发送失败本应回退到回戳 / 文字；失败时已进入静音，回退链整体落空
        self.assertEqual(_Harness(emoji_weight=1.0, emoji_ok=False).react(), ["emoji"])

    def test_fallback_chain_still_works_outside_quiet_period(self):
        harness = _Harness(emoji_weight=1.0)

        async def failing_emoji(data, stream_id, **kwargs):
            harness.sent.append("emoji")
            return False

        harness.plugin.ctx.send.emoji = failing_emoji
        self.assertEqual(harness.react(), ["emoji", "poke"])


if __name__ == "__main__":
    unittest.main()
