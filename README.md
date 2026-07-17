# CGRA QQ 控制插件

`astrbot_plugin_cgra_client` 是 AstrBot 插件。它通过 CGRA 的 WebSocket 接口，把 QQ 命令转换为云游戏控制任务，并把任务完成、失败或取消结果主动回复到发起会话。

## 功能

- 使用 `/cgra start` 建立 QQ 控制会话，后续直接发送任务文本。
- 提交 `task` 命名模板或自由参数任务。
- 提交 Maa `TemplateMatch` 与 `OcrDetect` 单任务。
- 每次提交立即回复任务 ID、任务名称、参数和大致流程。
- 查询 CGRA 服务状态和单个任务状态，并返回当前截图。
- 取消排队任务，或取消正在执行的可中断任务。
- 任务结束时主动发送状态、耗时和结束截图。
- 连接失败后停止并显示原因；再次发送 `/cgra start` 可手动重试。
- 可配置允许使用插件的 QQ 用户 ID。

## 前置条件

1. CGRA 服务已启动，并已安装 `websockets` 依赖。
2. CGRA 的 WebSocket 地址可从 AstrBot 所在机器访问。
3. 本插件已通过 AstrBot 插件管理器安装或放入 `data/plugins/` 目录。

默认地址：

```text
ws://127.0.0.1:8765/ws
```

如果 AstrBot 和 CGRA 不在同一台机器，需在插件配置的 `ws_url` 中填入 CGRA 主机的局域网地址，例如：

```text
ws://192.168.1.20:8765/ws
```

## 配置

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `ws_url` | `ws://127.0.0.1:8765/ws` | CGRA WebSocket 地址 |
| `connect_timeout` | `10` | 连接和指令响应等待超时，单位秒 |
| `notify_completion` | `true` | 任务进入终态时是否主动通知 QQ 会话 |
| `capture_screenshot` | `true` | 请求任务终态和状态查询截图，并作为 QQ 图片发送 |
| `allowed_users` | `[]` | 允许使用插件的 QQ 用户 ID 列表；留空不限制 |

`allowed_users` 建议在实际控制游戏时配置为你的 QQ 号，避免群聊中的其他成员执行点击、启动或关闭命令。

## 启动控制会话

```text
/cgra start
```

该命令建立与 CGRA 的 WebSocket 连接，并在当前 QQ 会话开启控制模式。进入后，无需继续输入 `/cgra` 前缀，直接发送任务文本即可；发送 `quit` 会退出控制模式并主动断开 WebSocket。

```text
run
click x=0.5 y=0.5
start game=mrfz
status 任务ID
quit
```

会话内的 `start game=mrfz` 才是提交给 CGRA 的浏览器/云游戏启动任务；`/cgra start` 本身只启动 QQ 控制会话，不会打开云游戏。

## QQ 命令

### 帮助

```text
/cgra help
```

### 直接提交命名模板任务

```text
/cgra task run
/cgra task center_click
```

### 直接提交自由参数任务

参数格式为 `key=value`。数值和 `true` / `false` 会自动转换为 JSON 对应类型。

```text
/cgra task click x=0.5 y=0.5
/cgra task wait seconds=20
/cgra task swipe x1=0.1 y1=0.5 x2=0.9 y2=0.5 duration=200
/cgra task start game=mrfz headless=false
```

带空格的文本请使用引号：

```text
/cgra task text text="hello world"
```

### Maa 资源单任务

```text
/cgra cv StartUp
/cgra ocr GameStartUpdateOCR
```

### 查询状态

```text
/cgra status
/cgra status 任务ID
```

查询单任务状态会同时返回状态文字和一张当前游戏截图。

### 取消任务

```text
/cgra cancel 任务ID
```

每次提交会立即返回任务 ID、服务端解析出的任务名称、参数与大致流程。任务完成、失败或取消时，插件会自动向发起命令的 QQ 私聊或群聊会话发送状态、执行耗时和任务结束截图。

## 取消行为

- 排队任务会立即取消。
- `wait` 在约 0.1 秒内响应取消。
- 多步模板在步骤之间响应取消。
- 已开始的点击、截图、浏览器启动或视觉识别无法安全地强制中断，会在当前原子操作结束后停止后续步骤。

## 安装

使用 AstrBot 插件市场或仓库地址安装：

```text
https://github.com/kamicry/astrbot_plugin_CGRA_client
```

本地开发时，将仓库放入 AstrBot 的 `data/plugins/` 目录后，在 AstrBot WebUI 的插件管理页面重载插件。

## 协议

插件依赖 CGRA 的 WebSocket 协议。任务消息格式、状态机和错误语义请查看 CGRA 项目的 `WEBSOCKET_API.md`。
