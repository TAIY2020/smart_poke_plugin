# 智能戳一戳插件 (SmartPoke Plugin)

👆 **一个让麦麦在 QQ 戳一戳里像真人一样反应的拟人化交互插件。**

被戳时，麦麦不会千篇一律地复读「XX 戳了我一下」，而是按可配置的概率随机选择回戳、文字调侃、发表情包或保持沉默；并且当群里其他人互戳时，麦麦还会有一定概率「跟风」也戳一下，制造群聊乐子。

> 本版本 (v1.1.0) 基于 **MaiBot SDK v2** 开发，使用 `@HookHandler` 装饰器订阅 `chat.receive.before_process` 钩子，配合 `PluginConfigBase` 强类型配置模型，支持配置热重载和 Web UI 配置。

## ✨ 功能特性

- **拟人化反应**: 被戳时按权重随机执行回戳、文字调侃、发表情三种反应中的一种；命中 `react_probability` 未命中时则按概率挤一句简短回复或彻底沉默。
- **权重独立可调**: 三种反应权重独立配置，加起来不必等于 1，插件内部自动按总和归一化分配。
- **跟风戳**: 当群里别人互戳时，麦麦会按概率「跟风」也戳一下，目标可选「跟着欺负被戳者」「替被戳者还击发起者」或「两者随机」。
- **暴戳检测**: 被同一人在判定窗口内多次戳后切换到「被烦」回复池，语气会更冲；计数与是否反应解耦，连续戳即使没命中概率也会累积。
- **思考延迟**: 反应前有随机延迟，避免机器人秒回的违和感。
- **冷却限频**: 戳麦麦冷却按 `stream_id + poker_id` 维度计（不同人独立冷却），跟风戳冷却按 `stream_id` 维度计（避免群刷屏）。
- **黑名单**: 被列入黑名单的用户戳麦麦本人时事件会被静默拦截；跟风戳场景下仅当跟风目标命中黑名单才跳过。
- **场景开关**: 群聊响应、私聊响应、跟风仅群聊均可独立配置；关闭对应场景时戳事件会照常传给主程序，不会被插件吞掉。
- **后台任务限时**: 反应任务带 60 秒超时兜底，避免外部 RPC 异常拖死协程。
- **状态自清理**: 冷却与暴戳计数字典每累积一定次数会自动 prune 掉一小时未活动的 key，防止长跑积累。
- **配置热重载**: 通过 Web UI 修改配置后无需重启，插件会自动应用新配置。

---

## 🚀 快速开始

### 1. 安装

- 手动安装：下载 `smart_poke_plugin` 文件夹放入麦麦主程序的 `plugins` 目录下，然后重启主程序即可完成插件的注册和加载。
- 自动安装：通过 Web UI 在插件市场下载安装。

### 2. 环境要求

- **MaiBot 主程序**: v1.0.0+
- **MaiBot SDK**: v2.0.0+

### 3. 配置

首次启动麦麦后，插件会在其目录下自动生成 `config.toml` 文件，开箱即用。你也可以通过 **Web UI** 在线修改配置，修改后会自动热重载生效。

**默认配置示例**:

```toml
[plugin]
name = "smart_poke_plugin"
version = "1.1.0"
config_version = "1.1.0"
enabled = true

[reaction]
react_probability = 0.85
back_poke_weight = 0.4
emoji_weight = 0.3
text_weight = 0.3
silent_chat_probability = 0.3
min_delay_seconds = 1.0
max_delay_seconds = 3.5
cooldown_seconds = 8
spam_threshold = 3
spam_window_seconds = 30
react_in_group = true
react_in_private = true

[fallback]
normal_replies = ["干嘛戳我", "诶诶诶，戳什么戳", "？？？", "干啥"]
spam_replies = ["你戳够了没", "好烦别戳了", "你是不是没事干", "停！手！", "烦不烦", "SB吧"]
silent_replies = ["...", "地铁老人手机.jpg", "懒得理"]

[user_control]
blacklist = []
ignore_self_poke = true

[emoji]
description_keywords = ["疑惑", "无奈", "生气", "无语", "哼", "瞪"]
min_similarity = 0.45
allow_random_fallback = false

[bystander]
enabled = true
probability = 0.85
target_strategy = "victim"
cooldown_seconds = 30
min_delay_seconds = 1.5
max_delay_seconds = 4.0
```

**⚠️ 重要安全提示**:

- **`user_control.blacklist`**: 黑名单 QQ 列表（字符串格式）。被列入此名单的用户**戳麦麦本人时**事件会被静默拦截；他们发起的别人互戳事件仍可能触发跟风戳，但跟风目标若命中黑名单依然会被跳过。例如：`blacklist = ["12345", "67890"]`。
- **`user_control.ignore_self_poke`**: 是否忽略麦麦戳自己的事件（避免回声）。默认 `true`。
- **`reaction.cooldown_seconds` / `bystander.cooldown_seconds`**: 两个冷却独立计时。前者按 `stream_id + poker_id` 维度，后者按 `stream_id` 维度。

---

### 🎯 戳麦麦本人

任何用户在群聊或私聊中戳麦麦本人时，插件会：

1. 命中黑名单/命中冷却：**静默拦截**事件，麦麦没动作。
2. 关闭对应场景开关（`react_in_group=false` 或 `react_in_private=false`）：**事件照常放行**，让主程序后续流程自行处理。
3. 通过以上检查后立即累计一次暴戳计数（与"是否反应"解耦）。
4. `react_probability` 未命中：拦截事件，但仍有 `silent_chat_probability` 概率挤出一句 `fallback.silent_replies` 池中的极简内容（"装看不见但还是嘀咕了一句"）。
5. `react_probability` 命中：按权重抽一种反应执行：
   - **回戳**：调用 NapCat 适配器戳回去（群聊带 `group_id`，私聊省略）。失败时回退到文字。
   - **文字调侃**：从 `fallback.normal_replies` 池中随机挑一句发出去；进入暴戳状态时改从 `fallback.spam_replies` 池中挑。
   - **发表情包**：从表情包库里按 `emoji.description_keywords` 随机顺序最多遍历 3 个关键词，调用 `emoji.get_by_description`（底层基于 emotion 标签模糊匹配，命中即用）；都没命中就回退到回戳。

### 🎲 跟风戳（别人互戳时）

当群里 A 戳了 B（且 B 不是麦麦本人），插件会：

1. `bystander.enabled = false`、当前是私聊、命中跟风冷却、概率未命中：**不做反应**，事件正常放行。
2. 否则按 `target_strategy` 决定戳谁：
   - `"victim"`：戳被戳者 B（跟着欺负他）。
   - `"poker"`：戳发起者 A（替被戳者还击）。
   - `"random"`：在 A、B 中随机选一个。
3. 选定 target 后若该 target 在黑名单则放弃（避免戳到不想戳的人）；发起者 A 在黑名单不会单独导致跟风跳过。
4. 延迟 `bystander.min_delay_seconds ~ max_delay_seconds` 秒后发出戳。

跟风戳事件**不会被拦截**，仅触发跟风行为。

### 🌪️ 暴戳模式

被同一个人在 `spam_window_seconds` 秒内戳到 `spam_threshold` 次后，插件进入「被烦」状态：

- 反应权重抽样会偏向文字：`back_poke_weight × 0.5`、`text_weight × 1.5`、`emoji_weight` 不变。
- 文字回复改从 `fallback.spam_replies` 池里抽，语气更冲。
- 跟风戳逻辑不受影响。

> 暴戳计数与"是否反应"解耦：只要事件通过了黑名单/场景检查，每次戳都会被累计，即使本次因为冷却或反应概率未命中而没有动作，也会推动「被烦」状态的形成。

### 配置项说明

| 配置项 | 类型 | 默认值 | 说明 |
| ------ | ---- | ------ | ---- |
| `reaction.react_probability` | float | `0.85` | 戳麦麦时整体作出反应的概率（未命中则进入 silent_chat_probability 分支） |
| `reaction.back_poke_weight` | float | `0.4` | 反应抽样里「回戳」的权重 |
| `reaction.emoji_weight` | float | `0.3` | 反应抽样里「表情包」的权重 |
| `reaction.text_weight` | float | `0.3` | 反应抽样里「文字」的权重（三个权重独立配置，按总和归一化） |
| `reaction.silent_chat_probability` | float | `0.3` | `react_probability` 未命中时，仍然挤出一句 `silent_replies` 的概率 |
| `reaction.min_delay_seconds` / `max_delay_seconds` | float | `1.0` / `3.5` | 反应前的随机延迟范围（秒） |
| `reaction.cooldown_seconds` | int | `8` | 戳麦麦本人的反应冷却时长（秒），按 `stream_id + poker_id` 维度计 |
| `reaction.spam_threshold` | int | `3` | 暴戳判定阈值 |
| `reaction.spam_window_seconds` | int | `30` | 暴戳判定窗口长度（秒） |
| `reaction.react_in_group` / `react_in_private` | bool | `true` / `true` | 群聊 / 私聊响应开关；关闭后事件 `return None` 放行 |
| `fallback.normal_replies` | list | 见示例 | 普通情景下的文字回复随机池 |
| `fallback.spam_replies` | list | 见示例 | 暴戳情境下的文字回复随机池 |
| `fallback.silent_replies` | list | 见示例 | `react_probability` 未命中时挤出的极简回复池 |
| `user_control.blacklist` | list | `[]` | 黑名单 QQ 号字符串列表 |
| `user_control.ignore_self_poke` | bool | `true` | 是否忽略麦麦戳麦麦自己 |
| `emoji.description_keywords` | list | `["疑惑","无奈","生气","无语","哼","瞪"]` | 选表情时使用的描述关键词随机池（启动时会与表情库 emotion 标签求交集，无交集会 warn） |
| `emoji.allow_random_fallback` | bool | `false` | 关键词都没匹配上时是否回退到随机表情 |
| `bystander.enabled` | bool | `true` | 是否启用跟风戳 |
| `bystander.probability` | float | `0.85` | 别人互戳时跟风戳的触发概率 |
| `bystander.target_strategy` | str | `"victim"` | 跟风目标：`victim` / `poker` / `random` |
| `bystander.cooldown_seconds` | int | `30` | 跟风戳冷却（独立于戳麦麦的冷却，按 `stream_id` 维度） |
| `bystander.min_delay_seconds` / `max_delay_seconds` | float | `1.5` / `4.0` | 跟风戳延迟范围（秒） |

---

## 📝 注意事项

- **冷却维度**: 戳麦麦本人冷却按 `stream_id + poker_id` 维度计——A 触发冷却不会阻挡 B 同时段戳麦麦；跟风戳冷却按 `stream_id` 维度计，避免在同一群里跟风刷屏。
- **状态隔离**: `PokeStateManager` 作为插件实例属性持有，热重载后状态会被重建，不会跨实例残留。
- **表情按 emotion 标签命中即用**: Host 的 `emoji.get_by_description` 底层是 emotion 标签模糊匹配，命中后直接返回单张表情，不向调用方暴露相似度数值。启动时插件会调用 `emoji.get_emotions` 与配置关键词求交集，无交集时 warn 提示。

Enjoy! 🎉
