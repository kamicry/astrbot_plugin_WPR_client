# 更新日志

## v0.4.5

- 新增 `wpr_get_task_status` 与 `wpr_wait_task_result` Tool，使大模型可读取单任务状态或等待终态后继续决策。
- `wpr_wait_task_result` 单次最长等待 30 秒，超时后返回当前状态并提示模型查询或再次等待。
- README 补充任务链白名单配置，以及确定性任务链与模型自主决策的使用边界。

## v0.4.4

- 新增 `wpr_run_chain` Tool；大模型只能运行 `llm_allowed_chains` 中已审核的预定义任务链，可安全封装点击与浏览器控制。
- 新增 AstrBot Function Calling / Tools 接口：`wpr_get_status`、`wpr_execute_task` 与 `wpr_cancel_task`。
- 大模型任务提交复用现有 WPR WebSocket、用户权限校验和 QQ 终态通知。
- 新增 `llm_tool_enabled` 与 `llm_allowed_tasks`，默认只开放高阶游戏任务，不开放任意点击或浏览器操作。
## v0.4.2

- 客户端任务链 JSON 改为保存在插件目录的 `auto/` 下，便于直接查看与维护。

## v0.4.1

- 未进入 `/wpr start` 会话时，`/wpr task`、`/wpr cv`、`/wpr ocr` 和 `/wpr status` 改用 WPR HTTP 接口直接执行。
- 进入 `/wpr start` 会话后，上述命令继续使用 WebSocket；`/wpr auto` 始终使用 WebSocket。

## v0.4.0

- 新增客户端任务链：一个 JSON 保存多个 WebSocket 任务，按顺序等待前一任务终态后再创建下一任务。
- 新增 `create`、`update`、`chains`、`show`、`delete` 与 `auto` 命令，支持从 QQ 创建、查看和运行任务链。
- 支持取消整条任务链，或按序号取消/跳过单个正在执行或尚未创建的节点；单项取消后会继续后续节点。

## v0.3.3

- 适配 WPR 无参数快速 `start`：直接进入云游戏，不执行登录检测或等待游戏画面。
- 更新 OpenCV、OCR 与 Pipeline 的命令说明及任务结果摘要。

## v0.3.2

- `/wpr cv` 改为接收模板图片路径，`/wpr ocr` 改为接收目标文字，并支持可选参数。

## v0.3.1

- 更新插件仓库地址为 `kamicry/astrbot_plugin_WPR_client`。

## v0.3.0

- 项目更名为 WebPiplineRemote，AstrBot 客户端缩写更名为 WPR。
- 命令前缀由 `/cgra` 更名为 `/wpr`，插件 ID 更名为 `astrbot_plugin_wpr_client`。

## v0.2.3

- 补充 `/wpr help` 帮助入口，加入默认启动、紧急关闭和浏览器标签页任务说明。

## v0.2.2

- `shutdown` 改为紧急关闭：可取消卡住的启动任务、关闭残留浏览器并重置 WPR 状态。
- 浏览器窗口被手动关闭后，`shutdown` 不再因已关闭的 Playwright 上下文失败。

## v0.2.1

- 任务提交回执仅显示任务 ID，不再显示任务详情、执行流程或查询、取消提示。
- 任务终态通知明确使用同一条消息链，文字在前、截图在后。
- 修复提交任务时 `capture_screenshot` 未传入 WPR 服务端的问题。

## v0.2.0

- 新增 `/wpr start` 会话控制模式，`quit` 退出并断开连接。
- 每次任务提交返回任务名称、参数、流程与任务 ID。
- 任务终态通知新增状态、耗时和结束截图。
- 单任务状态查询新增当前截图。
- 连接失败后停止自动重连，改为由 `/wpr start` 手动重试。
- 修复 JSON 回包原样展示和截图字段未读取的问题。

## v0.1.0

- 初始发布 WPR AstrBot QQ 控制插件。
- 支持 WebSocket 任务提交、状态查询和取消。
- 支持命名任务、自由参数任务与 Maa CV/OCR 单任务。
- 支持任务终态主动通知 QQ 会话和连接自动重连。
