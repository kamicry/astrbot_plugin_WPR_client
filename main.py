"""CGRA 的 AstrBot QQ 控制插件。"""

from __future__ import annotations

import asyncio
import base64
import json
import shlex
from pathlib import Path
from typing import Any

import websockets
from astrbot.api import AstrBotConfig, logger
import astrbot.api.message_components as Comp
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.core.utils.astrbot_path import get_astrbot_data_path
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
        self.capture_screenshot = bool(self._config("capture_screenshot", True))
        self.allowed_users = {str(value) for value in (self._config("allowed_users", []) or []) if str(value)}
        self.sessions: dict[str, str] = {}
        self.screenshot_dir = Path(get_astrbot_data_path()) / "plugin_data" / self.name / "screenshots"

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
        self.screenshot_dir.mkdir(parents=True, exist_ok=True)
        logger.info("CGRA client plugin initialized; use /cgra start to connect: %s", self.ws_url)

    async def terminate(self):
        await self._disconnect()
        self.sessions.clear()
        self._task_states.clear()
        self._task_origins.clear()
        logger.info("CGRA client plugin stopped")

    async def _start_connection(self):
        self._stopping = False
        if self._connection_task is None or self._connection_task.done():
            self._connection_task = asyncio.create_task(
                self._connection_loop(),
                name="cgra-astrbot-websocket",
            )
        await self._wait_for_connection()

    async def _disconnect(self):
        self._stopping = True
        self._connected.clear()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._connection_task is not None:
            self._connection_task.cancel()
            try:
                await self._connection_task
            except asyncio.CancelledError:
                pass
        self._connection_task = None
        self._ws = None

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
                    max_size=None,
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
        if self._connection_task is None or self._connection_task.done():
            raise RuntimeError("尚未连接 CGRA，请先发送 /cgra start")
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

    async def _submit(self, event: AstrMessageEvent, payload: dict[str, Any]) -> dict[str, Any]:
        async with self._submit_lock:
            loop = asyncio.get_running_loop()
            accepted_waiter: asyncio.Future[dict[str, Any]] = loop.create_future()
            self._accepted_waiter = accepted_waiter
            try:
                await self._send({
                    "action": "submit",
                    "capture_screenshot": self.capture_screenshot,
                    **payload,
                })
                accepted = await asyncio.wait_for(accepted_waiter, timeout=self.connect_timeout)
            finally:
                self._accepted_waiter = None
        task_id = str(accepted["task_id"])
        self._task_origins[task_id] = event.unified_msg_origin
        cached = self._task_states.get(task_id)
        if cached is not None and cached.get("status") in TERMINAL_STATES and self.notify_completion:
            asyncio.create_task(self._notify_task_terminal(event.unified_msg_origin, cached))
        return accepted

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
        elapsed = self._format_elapsed(message.get("elapsed_ms"))
        if status == "completed":
            text = f"CGRA 任务完成\n任务 ID：{task_id}\n耗时：{elapsed}\n{self._format_result(message.get('result'))}"
        elif status == "cancelled":
            text = f"CGRA 任务已取消\n任务 ID：{task_id}\n耗时：{elapsed}"
        else:
            text = f"CGRA 任务失败\n任务 ID：{task_id}\n耗时：{elapsed}\n错误：{message.get('error', '未知错误')}"
        try:
            screenshot = await self._save_screenshot(task_id, message.get("screenshot_base64"))
            chain = MessageChain().message(text)
            if screenshot is not None:
                chain = chain.file_image(str(screenshot))
            await self.context.send_message(origin, chain)
        except Exception as exc:
            logger.warning("Failed to notify CGRA task result to QQ: %s", exc)

    async def _save_screenshot(self, task_id: str, encoded_image: Any) -> Path | None:
        if not isinstance(encoded_image, str) or not encoded_image:
            return None
        try:
            raw = base64.b64decode(encoded_image, validate=True)
            path = self.screenshot_dir / f"{task_id}.png"
            await asyncio.to_thread(path.write_bytes, raw)
            return path
        except (ValueError, OSError) as exc:
            logger.warning("Failed to save CGRA task screenshot: %s", exc)
            return None

    @staticmethod
    def _format_elapsed(elapsed_ms: Any) -> str:
        try:
            return f"{float(elapsed_ms) / 1000:.2f} 秒"
        except (TypeError, ValueError):
            return "未知"

    async def _task_status_response(self, event: AstrMessageEvent, task: dict[str, Any]):
        text = f"CGRA 任务状态\n{self._format_result(task)}"
        task_id = str(task.get("task_id", "status"))
        screenshot = await self._save_screenshot(f"status-{task_id}", task.get("screenshot_base64"))
        if screenshot is None:
            return event.plain_result(text)
        return event.chain_result([
            Comp.Plain(text=text),
            Comp.Image.fromFileSystem(str(screenshot)),
        ])

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
            if action == "start":
                await self._start_connection()
                self.sessions[event.unified_msg_origin] = str(event.get_sender_id())
                yield event.plain_result("CGRA 控制会话已启动并连接 WebSocket。\n直接发送任务名或命令，例如：run、click x=0.5 y=0.5、start game=mrfz。\n发送 quit 退出会话并断开连接。")
            elif action == "task":
                if len(args) < 2:
                    raise ValueError("用法：/cgra task <任务名> [key=value ...]")
                accepted = await self._submit(event, {
                    "task": args[1],
                    "params": self._parse_params(args[2:]),
                })
                yield event.plain_result(self._accepted_text(accepted))
            elif action in {"cv", "ocr"}:
                if len(args) != 2:
                    raise ValueError(f"用法：/cgra {action} <Maa任务名>")
                accepted = await self._submit(event, {
                    "cvtask" if action == "cv" else "ocrtask": args[1],
                    "params": {},
                })
                yield event.plain_result(self._accepted_text(accepted))
            elif action == "status":
                if len(args) == 1:
                    server = await self._query_server()
                    yield event.plain_result(f"CGRA 服务状态\n{self._format_result(server.get('status'))}")
                else:
                    task = await self._query_task(args[1])
                    yield await self._task_status_response(event, task)
            elif action == "cancel":
                if len(args) != 2:
                    raise ValueError("用法：/cgra cancel <任务ID>")
                await self._send({"action": "cancel", "task_id": args[1]})
                yield event.plain_result(f"已发送取消请求：{args[1]}")
            elif action == "quit":
                self.sessions.pop(event.unified_msg_origin, None)
                await self._disconnect()
                yield event.plain_result("CGRA 控制会话已退出，WebSocket 已断开。")
            else:
                yield event.plain_result(self._help_text())
        except Exception as exc:
            logger.exception("CGRA command failed")
            yield event.plain_result(f"CGRA 操作失败：{exc}")

    @filter.regex(r".*")
    async def session_message(self, event: AstrMessageEvent):
        """处理 /cgra start 后同一 QQ 会话内的控制文本。"""
        origin = event.unified_msg_origin
        owner_id = self.sessions.get(origin)
        if owner_id is None or owner_id != str(event.get_sender_id()):
            return
        message = event.message_str.strip()
        if not message or message.startswith("/"):
            return
        if message.lower() == "quit":
            self.sessions.pop(origin, None)
            await self._disconnect()
            yield event.plain_result("CGRA 控制会话已退出，WebSocket 已断开。")
            return
        try:
            parts = shlex.split(message)
            if not parts:
                return
            action = parts[0].lower()
            if action == "help":
                yield event.plain_result(self._help_text())
            elif action == "status":
                if len(parts) == 1:
                    server = await self._query_server()
                    yield event.plain_result(f"CGRA 服务状态\n{self._format_result(server.get('status'))}")
                else:
                    task = await self._query_task(parts[1])
                    yield await self._task_status_response(event, task)
            elif action == "cancel":
                if len(parts) != 2:
                    raise ValueError("用法：cancel <任务ID>")
                await self._send({"action": "cancel", "task_id": parts[1]})
                yield event.plain_result(f"已发送取消请求：{parts[1]}")
            elif action in {"cv", "ocr"}:
                if len(parts) != 2:
                    raise ValueError(f"用法：{action} <Maa任务名>")
                accepted = await self._submit(event, {
                    "cvtask" if action == "cv" else "ocrtask": parts[1],
                    "params": {},
                })
                yield event.plain_result(self._accepted_text(accepted))
            else:
                task_name = parts[1] if action == "task" and len(parts) > 1 else parts[0]
                param_parts = parts[2:] if action == "task" else parts[1:]
                accepted = await self._submit(event, {
                    "task": task_name,
                    "params": self._parse_params(param_parts),
                })
                yield event.plain_result(self._accepted_text(accepted))
        except Exception as exc:
            logger.exception("CGRA session command failed")
            yield event.plain_result(f"CGRA 操作失败：{exc}")

    @staticmethod
    def _accepted_text(accepted: dict[str, Any]) -> str:
        flow = accepted.get("flow", [])
        flow_text = "\n".join(
            f"{index}. {step.get('description', step.get('type', '未知步骤'))}"
            for index, step in enumerate(flow, 1)
        ) or "服务端未提供流程"
        task_json = json.dumps(accepted.get("task", {}), ensure_ascii=False)
        return (
            f"CGRA 任务已提交\n任务 ID：{accepted['task_id']}\n"
            f"任务 JSON：{task_json}\n大致流程：\n{flow_text}\n"
            f"可发送 status {accepted['task_id']} 查询，或 cancel {accepted['task_id']} 取消。"
        )

    @staticmethod
    def _help_text() -> str:
        return """CGRA 云游戏控制

 /cgra start
  建立 WebSocket 控制会话；会话中直接发送任务文本，quit 退出并断开。
/cgra task <任务名> [key=value ...]
  例：/cgra task run
  例：/cgra task click x=0.5 y=0.5
  例：/cgra task wait seconds=20
/cgra cv <Maa TemplateMatch 任务名>
/cgra ocr <Maa OcrDetect 任务名>
/cgra status [任务ID]
/cgra cancel <任务ID>

任务完成、失败或取消时会自动通知当前 QQ 会话，包含耗时和结束截图。"""
