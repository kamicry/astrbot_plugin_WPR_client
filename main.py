"""CGRA 的 AstrBot QQ 控制插件。"""

from __future__ import annotations

import asyncio
import json
import shlex
from typing import Any

import websockets
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register


TERMINAL_STATES = frozenset({"completed", "failed", "cancelled"})


@register(
    "astrbot_plugin_cgra_client",
    "kamicry",
    "通过 QQ 控制 CGRA 云游戏任务，并接收状态与取消结果。",
    "v0.1.0",
)
class CGRAClientPlugin(Star):
    """维护一个到 CGRA 的 WebSocket 连接，并把任务状态回传 QQ。"""

    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.config = config or {}
        self.ws_url = str(self._config("ws_url", "ws://127.0.0.1:8765/ws"))
        self.connect_timeout = float(self._config("connect_timeout", 10))
        self.notify_completion = bool(self._config("notify_completion", True))
        self.allowed_users = {str(value) for value in self._config("allowed_users", []) if str(value)}

        self._ws: Any = None
        self._connected = asyncio.Event()
        self._stopping = False
        self._connection_task: asyncio.Task | None = None
        self._send_lock = asyncio.Lock()
        self._submit_lock = asyncio.Lock()
        self._accepted_waiter: asyncio.Future[dict[str, Any]] | None = None
        self._task_waiters: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._server_status_waiter: asyncio.Future[dict[str, Any]] | None = None
        self._task_states: dict[str, dict[str, Any]] = {}
        self._task_origins: dict[str, Any] = {}

    async def initialize(self):
        self._connection_task = asyncio.create_task(
            self._connection_loop(),
            name="cgra-astrbot-websocket",
        )
        logger.info("CGRA client plugin initialized: %s", self.ws_url)

    async def terminate(self):
        self._stopping = True
        self._connected.clear()
        if self._connection_task is not None:
            self._connection_task.cancel()
            try:
                await self._connection_task
            except asyncio.CancelledError:
                pass
        self._connection_task = None
        self._ws = None
        self._task_states.clear()
        self._task_origins.clear()
        logger.info("CGRA client plugin stopped")

    def _config(self, key: str, default: Any) -> Any:
        if isinstance(self.config, dict):
            return self.config.get(key, default)
        getter = getattr(self.config, "get", None)
        return getter(key, default) if callable(getter) else default

    async def _connection_loop(self):
        while not self._stopping:
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    open_timeout=self.connect_timeout,
                ) as websocket:
                    self._ws = websocket
                    self._connected.set()
                    logger.info("Connected to CGRA WebSocket: %s", self.ws_url)
                    async for raw_message in websocket:
                        await self._handle_message(raw_message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._stopping:
                    logger.warning("CGRA WebSocket disconnected: %s", exc)
            finally:
                self._ws = None
                self._connected.clear()
            if not self._stopping:
                await asyncio.sleep(3)

    async def _handle_message(self, raw_message: str | bytes):
        try:
            message = json.loads(raw_message)
        except (TypeError, json.JSONDecodeError):
            logger.warning("Ignored invalid CGRA WebSocket message")
            return
        if not isinstance(message, dict):
            return

        event = message.get("event")
        if event == "connected":
            return
        if event == "accepted" and self._accepted_waiter is not None:
            if not self._accepted_waiter.done():
                self._accepted_waiter.set_result(message)
            return
        if event == "server_status" and self._server_status_waiter is not None:
            if not self._server_status_waiter.done():
                self._server_status_waiter.set_result(message)
            return
        if event != "task_status":
            if event == "error":
                logger.warning("CGRA WebSocket error: %s", message.get("error", "unknown error"))
            return

        task_id = str(message.get("task_id", ""))
        if not task_id:
            return
        self._task_states[task_id] = message
        waiter = self._task_waiters.pop(task_id, None)
        if waiter is not None and not waiter.done():
            waiter.set_result(message)

        if message.get("status") in TERMINAL_STATES:
            origin = self._task_origins.pop(task_id, None)
            if origin is not None and self.notify_completion:
                asyncio.create_task(self._notify_task_terminal(origin, message))

    async def _wait_for_connection(self):
        if self._ws is not None and self._connected.is_set():
            return
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=self.connect_timeout)
        except asyncio.TimeoutError as exc:
            raise RuntimeError(f"无法连接 CGRA WebSocket：{self.ws_url}") from exc

    async def _send(self, message: dict[str, Any]):
        await self._wait_for_connection()
        async with self._send_lock:
            if self._ws is None:
                raise RuntimeError("CGRA WebSocket 已断开")
            await self._ws.send(json.dumps(message, ensure_ascii=False))

    async def _submit(self, event: AstrMessageEvent, payload: dict[str, Any]) -> str:
        async with self._submit_lock:
            loop = asyncio.get_running_loop()
            accepted_waiter: asyncio.Future[dict[str, Any]] = loop.create_future()
            self._accepted_waiter = accepted_waiter
            try:
                await self._send({"action": "submit", **payload})
                accepted = await asyncio.wait_for(accepted_waiter, timeout=self.connect_timeout)
            finally:
                self._accepted_waiter = None
        task_id = str(accepted["task_id"])
        self._task_origins[task_id] = event.unified_msg_origin
        return task_id

    async def _query_task(self, task_id: str) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._task_waiters[task_id] = waiter
        try:
            await self._send({"action": "status", "task_id": task_id})
            return await asyncio.wait_for(waiter, timeout=self.connect_timeout)
        finally:
            self._task_waiters.pop(task_id, None)

    async def _query_server(self) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._server_status_waiter = waiter
        try:
            await self._send({"action": "status"})
            return await asyncio.wait_for(waiter, timeout=self.connect_timeout)
        finally:
            self._server_status_waiter = None

    async def _notify_task_terminal(self, origin: Any, message: dict[str, Any]):
        task_id = message["task_id"]
        status = message.get("status", "unknown")
        if status == "completed":
            text = f"CGRA 任务完成\n任务 ID：{task_id}\n{self._format_result(message.get('result'))}"
        elif status == "cancelled":
            text = f"CGRA 任务已取消\n任务 ID：{task_id}"
        else:
            text = f"CGRA 任务失败\n任务 ID：{task_id}\n错误：{message.get('error', '未知错误')}"
        try:
            await self.context.send_message(origin, MessageChain().message(text))
        except Exception as exc:
            logger.warning("Failed to notify CGRA task result to QQ: %s", exc)

    @staticmethod
    def _format_result(result: Any) -> str:
        if not isinstance(result, dict):
            return str(result)
        safe_result = dict(result)
        nested = safe_result.get("result")
        if isinstance(nested, dict) and isinstance(nested.get("image_base64"), str):
            nested = dict(nested)
            nested["image_base64"] = "[截图 Base64 已省略]"
            safe_result["result"] = nested
        text = json.dumps(safe_result, ensure_ascii=False, indent=2)
        return text[:1200] + ("\n..." if len(text) > 1200 else "")

    def _is_allowed(self, event: AstrMessageEvent) -> bool:
        return not self.allowed_users or str(event.get_sender_id()) in self.allowed_users

    @staticmethod
    def _parse_params(parts: list[str]) -> dict[str, Any]:
        params: dict[str, Any] = {}
        for part in parts:
            if "=" not in part:
                raise ValueError(f"参数格式错误：{part}，应为 key=value")
            key, value = part.split("=", 1)
            key = key.strip()
            if not key:
                raise ValueError("参数名不能为空")
            lowered = value.lower()
            if lowered in {"true", "false"}:
                params[key] = lowered == "true"
            else:
                try:
                    params[key] = int(value)
                except ValueError:
                    try:
                        params[key] = float(value)
                    except ValueError:
                        params[key] = value
        return params

    @staticmethod
    def _command_args(event: AstrMessageEvent) -> list[str]:
        text = event.message_str.strip()
        for prefix in ("/cgra", "cgra"):
            if text.lower().startswith(prefix):
                text = text[len(prefix):].strip()
                break
        return shlex.split(text) if text else []

    @filter.command("cgra")
    async def cgra_command(self, event: AstrMessageEvent):
        """CGRA 云游戏控制命令。"""
        if not self._is_allowed(event):
            yield event.plain_result("你没有使用 CGRA 控制插件的权限。")
            return

        try:
            args = self._command_args(event)
        except ValueError as exc:
            yield event.plain_result(f"命令解析失败：{exc}")
            return
        if not args or args[0].lower() == "help":
            yield event.plain_result(self._help_text())
            return

        action = args[0].lower()
        try:
            if action == "task":
                if len(args) < 2:
                    raise ValueError("用法：/cgra task <任务名> [key=value ...]")
                task_id = await self._submit(event, {
                    "task": args[1],
                    "params": self._parse_params(args[2:]),
                })
                yield event.plain_result(f"CGRA 任务已提交\n任务 ID：{task_id}\n可用 /cgra status {task_id} 查询，或 /cgra cancel {task_id} 取消。")
            elif action in {"cv", "ocr"}:
                if len(args) != 2:
                    raise ValueError(f"用法：/cgra {action} <Maa任务名>")
                task_id = await self._submit(event, {
                    "cvtask" if action == "cv" else "ocrtask": args[1],
                    "params": {},
                })
                yield event.plain_result(f"Maa {action.upper()} 任务已提交\n任务 ID：{task_id}")
            elif action == "status":
                if len(args) == 1:
                    server = await self._query_server()
                    yield event.plain_result(f"CGRA 服务状态\n{self._format_result(server.get('status'))}")
                else:
                    task = await self._query_task(args[1])
                    yield event.plain_result(f"CGRA 任务状态\n{self._format_result(task)}")
            elif action == "cancel":
                if len(args) != 2:
                    raise ValueError("用法：/cgra cancel <任务ID>")
                await self._send({"action": "cancel", "task_id": args[1]})
                yield event.plain_result(f"已发送取消请求：{args[1]}")
            else:
                yield event.plain_result(self._help_text())
        except Exception as exc:
            logger.exception("CGRA command failed")
            yield event.plain_result(f"CGRA 操作失败：{exc}")

    @staticmethod
    def _help_text() -> str:
        return """CGRA 云游戏控制

/cgra task <任务名> [key=value ...]
  例：/cgra task run
  例：/cgra task click x=0.5 y=0.5
  例：/cgra task wait seconds=20
/cgra cv <Maa TemplateMatch 任务名>
/cgra ocr <Maa OcrDetect 任务名>
/cgra status [任务ID]
/cgra cancel <任务ID>

提交后会返回任务 ID；任务完成、失败或取消时会自动通知当前 QQ 会话。"""
