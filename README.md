# WPR QQ 控制插件

`astrbot_plugin_wpr_client` 是 AstrBot 插件。它通过 WPR 的 WebSocket 接口，把 QQ 命令转换为云游戏控制任务，并把任务完成、失败或取消结果主动回复到发起会话。

## 功能

- 使用 `/wpr start` 建立 QQ 控制会话，后续直接发送任务文本。
- 提交 `task` 命名模板或自由参数任务。
- 提交 OpenCV 模板匹配与 OCR 单任务。
- 每次提交立即回复任务 ID。
- 查询 WPR 服务状态和单个任务状态，并返回当前截图。
- 取消排队任务，或取消正在执行的可中断任务。
- 在客户端保存 JSON 任务链，顺序创建多个 WebSocket 任务，支持整链或单节点取消。
- 任务结束时主动发送状态、耗时和结束截图。
- 连接失败后停止并显示原因；再次发送 `/wpr start` 可手动重试。
- 可配置允许使用插件的 QQ 用户 ID。

## 前置条件

1. WPR 服务已启动，并已安装 `websockets` 依赖。
2. WPR 的 WebSocket 地址可从 AstrBot 所在机器访问。
3. 本插件已通过 AstrBot 插件管理器安装或放入 `data/plugins/` 目录。

默认地址：

```text
ws://127.0.0.1:8765/ws
```

如果 AstrBot 和 WPR 不在同一台机器，需在插件配置的 `ws_url` 中填入 WPR 主机的局域网地址，例如：

```text
ws://192.168.1.20:8765/ws
```

## 配置

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `ws_url` | `ws://127.0.0.1:8765/ws` | WPR WebSocket 地址 |
| `http_url` | 留空 | WPR HTTP 地址；留空时由 `ws_url` 自动推导为 `http://主机:端口/remote` |
| `connect_timeout` | `10` | 连接和指令响应等待超时，单位秒 |
| `notify_completion` | `true` | 任务进入终态时是否主动通知 QQ 会话 |
| `capture_screenshot` | `true` | 请求任务终态和状态查询截图，并作为 QQ 图片发送 |
| `allowed_users` | `[]` | 允许使用插件的 QQ 用户 ID 列表；留空不限制 |

`allowed_users` 建议在实际控制游戏时配置为你的 QQ 号，避免群聊中的其他成员执行点击、启动或关闭命令。

## 启动控制会话

```text
/wpr start
```

该命令建立与 WPR 的 WebSocket 连接，并在当前 QQ 会话开启控制模式。进入后，无需继续输入 `/wpr` 前缀，直接发送任务文本即可；发送 `quit` 会退出控制模式并主动断开 WebSocket。

```text
run
click x=0.5 y=0.5
start
status 任务ID
quit
```

会话内的 `start` 才是提交给 WPR 的浏览器/云游戏启动任务；`/wpr start` 本身只启动 QQ 控制会话，不会打开云游戏。无参数 `start` 会直接打开明日方舟云游戏入口，不会检查、注入或等待登录状态。

## QQ 命令

### 帮助

```text
/wpr help
```

该命令会显示会话控制、OpenCV/OCR 任务、状态查询、取消、紧急关闭与浏览器标签页任务的简要用法。

### 直接提交命名模板任务

```text
/wpr task run
/wpr task center_click
```

未执行 `/wpr start` 时，带 `/wpr` 前缀的单次 `task`、`cv`、`ocr` 和无参数 `status` 会直接请求 WPR HTTP 接口；无需建立 WebSocket。执行 `/wpr start` 后，同样的命令改为通过当前 WebSocket 会话提交，以获得任务 ID、实时状态和结束通知。`/wpr auto` 无论是否已有会话都始终使用 WebSocket。

### 直接提交自由参数任务

参数格式为 `key=value`。数值和 `true` / `false` 会自动转换为 JSON 对应类型。

```text
/wpr task click x=0.5 y=0.5
/wpr task wait seconds=20
/wpr task swipe x1=0.1 y1=0.5 x2=0.9 y2=0.5 duration=200
/wpr task start headless=false
/wpr task pipeline pipeline_name=mall
```

带空格的文本请使用引号：

```text
/wpr task text text="hello world"
```

### 客户端任务链

任务链保存在插件目录的 `auto/<名称>.json` 中。每个 JSON 文件包含多个 WPR WebSocket 任务；插件只会在前一任务收到完成、失败或取消终态后，才创建下一项任务。

```text
/wpr create run click&x=1&y=1 pipeline&pipeline_name=startup2
/wpr show run
/wpr auto run
```

上例会创建 `run.json`，其中第 1 项为 `click`，第 2 项为 `pipeline`。`/wpr auto run` 会先回复完整任务列表，随后逐项发送 WebSocket 任务创建消息；每项任务终态仍按普通任务发送完成、失败或取消消息与截图。

可用命令：

```text
/wpr chains
/wpr show run
/wpr update run click&x=0.5&y=0.5 wait&seconds=3
/wpr delete run

/wpr auto run
/wpr auto status run
/wpr auto cancel run
/wpr auto cancel run 2
```

`auto cancel run` 会取消当前 WebSocket 任务，并且不再创建后续任务。`auto cancel run 2` 会取消或跳过第 2 项；若该项正在执行，收到取消终态后会自动继续第 3 项；若尚未创建，则在执行到该项时直接跳过。任务失败时整条链会停止，避免在未知页面状态下继续操作。

任务链节点使用 `任务名&key=value` 格式；支持 `cv:模板路径` 和 `ocr:目标文字` 作为节点开头。例如：

```text
/wpr create wakeup cv:WakeUp/StartToWakeUp.png&threshold=0.8 ocr:确认&timeout=20
```

### 浏览器标签页

```text
/wpr task tab_list
/wpr task tab_new
/wpr task tab_open url=https://example.com
/wpr task tab_switch tab_index=1
/wpr task tab_close tab_index=1
```

进入控制会话后，也可以去掉 `/wpr task` 前缀直接发送同样的任务文本。

### OpenCV 与 OCR 单任务

```text
/wpr cv WakeUp/StartToWakeUp.png threshold=0.8 timeout=30
/wpr ocr 确认 threshold=0.7
```

### 查询状态

```text
/wpr status
/wpr status 任务ID
```

查询单任务状态会同时返回状态文字和一张当前游戏截图。

### 取消任务

```text
/wpr cancel 任务ID
```

每次提交会立即返回任务 ID。任务完成、失败或取消时，插件会自动向发起命令的 QQ 私聊或群聊会话发送状态、执行耗时和任务结束截图；文字和截图会在同一条消息中依次发送。

浏览器窗口被手动关闭或启动流程卡住时，直接提交 `shutdown`。该任务会强制取消未完成任务、关闭残留浏览器并重置 WPR 状态；收到完成通知后可手动提交新的 `start`，插件和服务端不会自动重新打开浏览器。

## 取消行为

- 排队任务会立即取消。
- `wait` 在约 0.1 秒内响应取消。
- 多步模板在步骤之间响应取消。
- 已开始的点击、截图、浏览器启动或视觉识别无法安全地强制中断，会在当前原子操作结束后停止后续步骤。

## 安装

使用 AstrBot 插件市场或仓库地址安装：

```text
https://github.com/kamicry/astrbot_plugin_WPR_client
```

本地开发时，将仓库放入 AstrBot 的 `data/plugins/` 目录后，在 AstrBot WebUI 的插件管理页面重载插件。

## 协议

插件依赖 WPR 的 WebSocket 协议。任务消息格式、状态机和错误语义请查看 WPR 项目的 `WEBSOCKET_API.md`。
