# 智能戳一戳插件 (SmartPoke Plugin)

👆 **一个让麦麦在 QQ 戳一戳里像真人一样反应的拟人化交互插件。**

被戳时，麦麦不会千篇一律地复读「XX 戳了我一下」，而是按可配置的概率随机选择回戳、文字调侃、发表情包或保持沉默；并且当群里其他人互戳时，麦麦还会有一定概率「跟风」也戳一下，制造群聊乐子。

> 本版本 (v1.1.0) 基于 **MaiBot SDK v2** 开发，使用 `@HookHandler` 装饰器订阅 `chat.receive.before_process` 钩子，配合 `PluginConfigBase` 强类型配置模型，支持配置热重载和 Web UI 配置。

## ✨ 功能特性

- **拟人化反应**: 被戳时按权重随机执行回戳、文字调侃、发表情或沉默四种反应中的一种，不再每次都一样。
- **权重独立可调**: 四种反应的概率独立配置，加起来不必等于 1，插件内部自动按总和归一化分配。
- **跟风戳**: 当群里别人互戳时，麦麦会按概率「跟风」也戳一下，目标可选「跟着欺负被戳者」「替被戳者还击发起者」或「两者随机」。
- **暴戳检测**: 被同一人在判定窗口内多次戳后切换到「被烦」回复池，语气会更冲。
- **思考延迟**: 反应前有随机延迟（默认 1~3.5 秒），避免机器人秒回的违和感。
- **冷却限频**: 同一聊天的反应间隔可配置，跟风戳独立计时，互不干扰。
- **表情相似度阈值**: 用关键词搜表情时若匹配相似度不达标，自动回退到回戳，避免发出风马牛不相及的表情。
- **黑名单**: 被列入黑名单的用户戳过来会被静默拦截，不反应也不让事件继续传播。
- **场景开关**: 群聊响应、私聊响应、跟风仅群聊均可独立配置。
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
back_poke_probability = 0.45
emoji_probability = 0.3
silent_probability = 0.05
text_probability = 0.2
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
probability = 0.15
target_strategy = "victim"
cooldown_seconds = 30
min_delay_seconds = 1.5
max_delay_seconds = 4.0
```

**⚠️ 重要安全提示**:

- **`user_control.blacklist`**: 黑名单 QQ 列表（字符串格式）。被列入此名单的用户戳麦麦时事件会被静默拦截，不会触发任何反应；他们发起的「互戳」事件也不会触发跟风戳。例如：`blacklist = ["12345", "67890"]`。
- **`user_control.ignore_self_poke`**: 是否忽略麦麦戳自己的事件（避免回声）。默认 `true`。
- **`emoji.min_similarity`**: 表情包关键词匹配的相似度阈值。低于此值时不发送表情，而是回退到回戳，避免发出与情境无关的表情包。
- **`reaction.cooldown_seconds` / `bystander.cooldown_seconds`**: 两个冷却独立计时。冷却期内的事件直接被忽略，不会被拦截后再无反应（戳麦麦本人时除外，那一类事件必拦截）。

---

### 🎯 戳麦麦本人

任何用户在群聊或私聊中戳麦麦本人时，插件会：

1. 命中黑名单/关闭对应场景开关/命中冷却/概率未命中：**静默拦截**事件，麦麦没动作。
2. 否则按权重随机抽一种反应执行：
   - **回戳**：调用 NapCat 适配器戳回去（群聊带 `group_id`，私聊省略）。
   - **文字调侃**：从 `fallback.normal_replies` 池中随机挑一句发出去；进入暴戳状态时改从 `fallback.spam_replies` 池中挑。
   - **发表情包**：从表情包库里按 `emoji.description_keywords` 随机关键词搜一张相似度 ≥ `emoji.min_similarity` 的表情发出去；找不到合适的就回退到回戳。
   - **沉默不理**：直接装看不见；偶尔会发一句 `fallback.silent_replies` 池中的极简内容。

### 🎲 跟风戳（别人互戳时）

当群里 A 戳了 B（且 B 不是麦麦本人），插件会：

1. `bystander.enabled = false`、当前是私聊、命中跟风冷却、概率未命中：**不做反应**，事件正常放行。
2. 否则按 `target_strategy` 决定戳谁：
   - `"victim"`：戳被戳者 B（跟着欺负他）。
   - `"poker"`：戳发起者 A（替被戳者还击）。
   - `"random"`：在 A、B 中随机选一个。
3. 延迟 `bystander.min_delay_seconds ~ max_delay_seconds` 秒后发出戳。

跟风戳事件**不会被拦截**，仅触发跟风行为；如果黑名单里的人发起戳，或者跟风目标在黑名单里，跟风也会被跳过。

### 🌪️ 暴戳模式

被同一个人在 `spam_window_seconds` 秒内戳到 `spam_threshold` 次后，插件进入「被烦」状态：

- 回戳权重减半，沉默权重 +0.15，文字权重 ×1.5。
- 文字回复改从 `fallback.spam_replies` 池里抽，语气更冲。
- 跟风戳逻辑不受影响。

### 配置项说明

| 配置项 | 类型 | 默认值 | 说明 |
| ------ | ---- | ------ | ---- |
| `reaction.react_probability` | float | `0.85` | 戳麦麦时整体作出反应的概率（未命中则静默拦截） |
| `reaction.back_poke_probability` | float | `0.45` | 反应抽样里「回戳」的权重 |
| `reaction.emoji_probability` | float | `0.3` | 反应抽样里「表情包」的权重 |
| `reaction.silent_probability` | float | `0.05` | 反应抽样里「沉默」的权重 |
| `reaction.text_probability` | float | `0.2` | 反应抽样里「文字」的权重（四个权重独立配置，会自动按总和归一化） |
| `reaction.min_delay_seconds` / `max_delay_seconds` | float | `1.0` / `3.5` | 反应前的随机延迟范围（秒） |
| `reaction.cooldown_seconds` | int | `8` | 同一聊天的反应冷却时长（秒） |
| `reaction.spam_threshold` | int | `3` | 暴戳判定阈值 |
| `reaction.spam_window_seconds` | int | `30` | 暴戳判定窗口长度（秒） |
| `reaction.react_in_group` / `react_in_private` | bool | `true` / `true` | 群聊 / 私聊响应开关 |
| `fallback.normal_replies` | list | 见示例 | 普通情景下的文字回复随机池 |
| `fallback.spam_replies` | list | 见示例 | 暴戳情境下的文字回复随机池 |
| `fallback.silent_replies` | list | 见示例 | 沉默反应偶尔发出的极简回复 |
| `user_control.blacklist` | list | `[]` | 黑名单 QQ 号字符串列表 |
| `user_control.ignore_self_poke` | bool | `true` | 是否忽略麦麦戳麦麦自己 |
| `emoji.description_keywords` | list | `["疑惑","无奈","生气","无语","哼","瞪"]` | 选表情时使用的描述关键词随机池 |
| `emoji.min_similarity` | float | `0.45` | 表情匹配相似度阈值，不达标回退回戳 |
| `emoji.allow_random_fallback` | bool | `false` | 关键词没匹配上时是否回退到随机表情 |
| `bystander.enabled` | bool | `true` | 是否启用跟风戳 |
| `bystander.probability` | float | `0.15` | 别人互戳时跟风戳的触发概率 |
| `bystander.target_strategy` | str | `"victim"` | 跟风目标：`victim` / `poker` / `random` |
| `bystander.cooldown_seconds` | int | `30` | 跟风戳冷却（独立于戳麦麦的冷却） |
| `bystander.min_delay_seconds` / `max_delay_seconds` | float | `1.5` / `4.0` | 跟风戳延迟范围（秒） |

---

## 📝 注意事项

- **冷却仅按 stream 隔离**: 跟风戳和戳麦麦本人的冷却都以 `stream_id` 为键，不区分用户。这意味着如果 A 刚被跟风戳了，B 在冷却期内即使发起互戳也不会触发新的跟风戳。
- **表情包字段兼容**: 插件尝试从表情字典的 `similarity` / `score` / `similarity_score` / `match_score` 任一字段读取相似度；不同版本的 emoji 服务返回字段名可能略有差异。

Enjoy! 🎉
