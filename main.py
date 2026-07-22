"""WPR 的 AstrBot QQ 控制插件。"""

from __future__ import annotations

import asyncio
import base64
import json
import re
import shlex
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

import websockets
from astrbot.api import AstrBotConfig, logger
import astrbot.api.message_components as Comp
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.core.utils.astrbot_path import get_astrbot_data_path
from astrbot.api.star import Context, Star, register


TERMINAL_STATES = frozenset({"completed", "failed", "cancelled"})
CHAIN_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
DEFAULT_LLM_ALLOWED_TASKS = frozenset({
    "start", "shutdown", "screenshot", "pipeline",
    "recruit_prepare", "recruit_refresh_once", "recruit_fill_normal", "recruit_verify",
    "shop_prepare", "shop_buy", "shop_verify",
    "friend_prepare", "friend_visit_all", "friend_verify", "startup_close_popups",
})


@dataclass
class ClientTaskChain:
    """客户端侧按顺序创建 WPR WebSocket 任务的一次运行实例。"""

    name: str
    tasks: list[dict[str, Any]]
    origin: Any
    status: str = "running"
    current_index: int | None = None
    task_ids: dict[int, str] = field(default_factory=dict)
    task_states: dict[int, str] = field(default_factory=dict)
    skipped: set[int] = field(default_factory=set)
    stop_requested: bool = False
    runner: asyncio.Task | None = None


@register(
    "astrbot_plugin_wpr_client",
    "kamicry",
    "通过 QQ 控制 WPR 云游戏任务，并接收状态与取消结果。",
    "v0.4.3",
)
class WPRClientPlugin(Star):
    """维护一个到 WPR 的 WebSocket 连接，并把任务状态回传 QQ。"""

    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.config = config or {}
        self.ws_url = str(self._config("ws_url", "ws://127.0.0.1:8765/ws"))
        configured_http_url = str(self._config("http_url", "")).strip()
        self.http_url = configured_http_url or self._derive_http_url(self.ws_url)
        self.connect_timeout = float(self._config("connect_timeout", 10))
        self.notify_completion = bool(self._config("notify_completion", True))
        self.capture_screenshot = bool(self._config("capture_screenshot", True))
        self.allowed_users = {str(value) for value in (self._config("allowed_users", []) or []) if str(value)}
        self.llm_tool_enabled = bool(self._config("llm_tool_enabled", True))
        self.llm_allowed_tasks = {
            str(value).strip()
            for value in (self._config("llm_allowed_tasks", sorted(DEFAULT_LLM_ALLOWED_TASKS)) or [])
            if str(value).strip()
        }
        self.sessions: dict[str, str] = {}
        self.screenshot_dir = Path(get_astrbot_data_path()) / "plugin_data" / self.name / "screenshots"
        self.chain_dir = Path(__file__).resolve().parent / "auto"

        self._ws: Any = None
        self._connected = asyncio.Event()
        self._stopping = False
        self._last_connection_error: str | None = None
        self._connection_task: asyncio.Task | None = None
        self._send_lock = asyncio.Lock()
        self._submit_lock = asyncio.Lock()
        self._accepted_waiter: asyncio.Future[dict[str, Any]] | None = None
        self._task_waiters: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._terminal_waiters: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._server_status_waiter: asyncio.Future[dict[str, Any]] | None = None
        self._task_states: dict[str, dict[str, Any]] = {}
        self._task_origins: dict[str, Any] = {}
        self._chains: dict[str, ClientTaskChain] = {}
        self._chain_lock = asyncio.Lock()

    async def initialize(self):
        self.screenshot_dir.mkdir(parents=True, exist_ok=True)
        self.chain_dir.mkdir(parents=True, exist_ok=True)
        logger.info("WPR client plugin initialized; use /wpr start to connect: %s", self.ws_url)

    async def terminate(self):
        await self._stop_all_chains()
        await self._disconnect()
        self.sessions.clear()
        self._task_states.clear()
        self._task_origins.clear()
        logger.info("WPR client plugin stopped")

    async def _start_connection(self):
        self._stopping = False
        self._last_connection_error = None
        if self._connection_task is None or self._connection_task.done():
            self._connection_task = asyncio.create_task(
                self._connection_loop(),
                name="wpr-astrbot-websocket",
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

    @staticmethod
    def _derive_http_url(ws_url: str) -> str:
        parsed = urlsplit(ws_url)
        scheme = "https" if parsed.scheme == "wss" else "http"
        path = "/remote" if parsed.path in {"", "/", "/ws"} else parsed.path
        return urlunsplit((scheme, parsed.netloc, path, "", ""))

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
                    logger.info("Connected to WPR WebSocket: %s", self.ws_url)
                    async for raw_message in websocket:
                        await self._handle_message(raw_message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._stopping:
                    self._last_connection_error = str(exc)
                    logger.warning("WPR WebSocket disconnected: %s", exc)
            finally:
                self._ws = None
                self._connected.clear()
            return

    async def _handle_message(self, raw_message: str | bytes):
        try:
            message = json.loads(raw_message)
        except (TypeError, json.JSONDecodeError):
            logger.warning("Ignored invalid WPR WebSocket message")
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
                logger.warning("WPR WebSocket error: %s", message.get("error", "unknown error"))
            return

        task_id = str(message.get("task_id", ""))
        if not task_id:
            return
        self._task_states[task_id] = message
        waiter = self._task_waiters.pop(task_id, None)
        if waiter is not None and not waiter.done():
            waiter.set_result(message)

        if message.get("status") in TERMINAL_STATES:
            terminal_waiter = self._terminal_waiters.pop(task_id, None)
            if terminal_waiter is not None and not terminal_waiter.done():
                terminal_waiter.set_result(message)
            origin = self._task_origins.pop(task_id, None)
            if origin is not None and self.notify_completion:
                asyncio.create_task(self._notify_task_terminal(origin, message))

    async def _wait_for_connection(self):
        if self._ws is not None and self._connected.is_set():
            return
        if self._connection_task is None or self._connection_task.done():
            raise RuntimeError("尚未连接 WPR，请先发送 /wpr start")
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=self.connect_timeout)
        except asyncio.TimeoutError as exc:
            detail = f"：{self._last_connection_error}" if self._last_connection_error else ""
            raise RuntimeError(f"无法连接 WPR WebSocket：{self.ws_url}{detail}") from exc

    async def _send(self, message: dict[str, Any]):
        await self._wait_for_connection()
        async with self._send_lock:
            if self._ws is None:
                raise RuntimeError("WPR WebSocket 已断开")
            await self._ws.send(json.dumps(message, ensure_ascii=False))

    @staticmethod
    def _http_param_value(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (list, tuple)):
            return ",".join(str(item) for item in value)
        return str(value)

    async def _http_remote(self, query: dict[str, Any]) -> dict[str, Any]:
        params = {key: self._http_param_value(value) for key, value in query.items() if value is not None}
        separator = "&" if "?" in self.http_url else "?"
        url = f"{self.http_url}{separator}{urlencode(params)}"

        def request() -> dict[str, Any]:
            try:
                with urlopen(Request(url, headers={"Accept": "application/json, image/png"}), timeout=150) as response:
                    body = response.read()
                    content_type = response.headers.get_content_type()
            except HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"WPR HTTP 返回 {exc.code}: {detail}") from exc
            except URLError as exc:
                raise RuntimeError(f"无法连接 WPR HTTP：{exc.reason}") from exc
            if content_type.startswith("image/"):
                return {"image_bytes": body, "content_type": content_type}
            try:
                result = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RuntimeError("WPR HTTP 返回了无效响应") from exc
            if not isinstance(result, dict):
                raise RuntimeError("WPR HTTP 返回格式错误")
            return result

        return await asyncio.to_thread(request)

    async def _http_task_response(self, event: AstrMessageEvent, title: str, query: dict[str, Any]):
        result = await self._http_remote(query)
        image = result.get("image_bytes")
        if isinstance(image, bytes):
            path = self.screenshot_dir / f"http-{int(time.time() * 1000)}.png"
            await asyncio.to_thread(path.write_bytes, image)
            return event.chain_result([
                Comp.Plain(text=f"WPR HTTP 任务完成：{title}"),
                Comp.Image.fromFileSystem(str(path)),
            ])
        if result.get("success") is False or result.get("error"):
            raise RuntimeError(str(result.get("error", "WPR HTTP 任务失败")))
        return event.plain_result(f"WPR HTTP 任务完成：{title}\n{self._format_result(result)}")

    async def _submit(self, event: AstrMessageEvent, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._submit_for_origin(event.unified_msg_origin, payload)

    async def _submit_for_origin(self, origin: Any, payload: dict[str, Any]) -> dict[str, Any]:
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
        self._task_origins[task_id] = origin
        cached = self._task_states.get(task_id)
        if cached is not None and cached.get("status") in TERMINAL_STATES and self.notify_completion:
            asyncio.create_task(self._notify_task_terminal(origin, cached))
        return accepted

    async def _wait_task_terminal(self, task_id: str) -> dict[str, Any]:
        cached = self._task_states.get(task_id)
        if cached is not None and cached.get("status") in TERMINAL_STATES:
            return cached
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._terminal_waiters[task_id] = waiter
        cached = self._task_states.get(task_id)
        if cached is not None and cached.get("status") in TERMINAL_STATES and not waiter.done():
            waiter.set_result(cached)
        try:
            return await waiter
        finally:
            if self._terminal_waiters.get(task_id) is waiter:
                self._terminal_waiters.pop(task_id, None)

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
            text = f"WPR 任务完成\n任务 ID：{task_id}\n耗时：{elapsed}\n{self._format_task_result(message)}"
        elif status == "cancelled":
            text = f"WPR 任务已取消\n任务 ID：{task_id}\n耗时：{elapsed}"
        else:
            text = f"WPR 任务失败\n任务 ID：{task_id}\n耗时：{elapsed}\n错误：{message.get('error', '未知错误')}"
        try:
            encoded_image = self._extract_screenshot(message)
            if encoded_image is None:
                try:
                    latest = await self._query_task(task_id)
                    encoded_image = self._extract_screenshot(latest)
                except Exception as exc:
                    logger.warning("Failed to fetch WPR task status screenshot: %s", exc)
            screenshot = await self._save_screenshot(task_id, encoded_image)
            components: list[Any] = [Comp.Plain(text=text)]
            if screenshot is not None:
                components.append(Comp.Image.fromFileSystem(str(screenshot)))
            await self.context.send_message(origin, MessageChain(components))
        except Exception as exc:
            logger.warning("Failed to notify WPR task result to QQ: %s", exc)

    async def _save_screenshot(self, task_id: str, encoded_image: Any) -> Path | None:
        if not isinstance(encoded_image, str) or not encoded_image:
            return None
        try:
            raw = base64.b64decode(encoded_image, validate=True)
            path = self.screenshot_dir / f"{task_id}.png"
            await asyncio.to_thread(path.write_bytes, raw)
            return path
        except (ValueError, OSError) as exc:
            logger.warning("Failed to save WPR task screenshot: %s", exc)
            return None

    @staticmethod
    def _format_elapsed(elapsed_ms: Any) -> str:
        try:
            return f"{float(elapsed_ms) / 1000:.2f} 秒"
        except (TypeError, ValueError):
            return "未知"

    async def _task_status_response(self, event: AstrMessageEvent, task: dict[str, Any]):
        task_info = task.get("task", {})
        task_name = task_info.get("name", "未知任务") if isinstance(task_info, dict) else "未知任务"
        text = (
            f"WPR 任务状态\n任务：{task_name}\n"
            f"状态：{task.get('status', '未知')}\n"
            f"耗时：{self._format_elapsed(task.get('elapsed_ms'))}\n"
            f"{self._format_task_result(task)}"
        )
        task_id = str(task.get("task_id", "status"))
        screenshot = await self._save_screenshot(f"status-{task_id}", self._extract_screenshot(task))
        if screenshot is None:
            return event.plain_result(text)
        return event.chain_result([
            Comp.Plain(text=text),
            Comp.Image.fromFileSystem(str(screenshot)),
        ])

    @staticmethod
    def _extract_screenshot(message: dict[str, Any]) -> str | None:
        image = message.get("screenshot_base64")
        if isinstance(image, str) and image:
            return image
        result = message.get("result")
        if not isinstance(result, dict):
            return None
        inner = result.get("result")
        if isinstance(inner, dict):
            image = inner.get("image_base64")
            if isinstance(image, str) and image:
                return image
        return None

    @staticmethod
    def _format_result(result: Any) -> str:
        if not isinstance(result, dict):
            return str(result)
        task_type = result.get("type")
        nested = result.get("result")
        if task_type == "task_template" and isinstance(nested, dict):
            return f"已执行模板：{nested.get('template', '未知')}"
        if task_type == "cv_task" and isinstance(nested, dict):
            return (
                f"模板：{nested.get('template', '未知')}，"
                f"分数：{float(nested.get('score', 0)):.3f}，"
                f"点击：({nested.get('x', '?')}, {nested.get('y', '?')})"
            )
        if task_type == "ocr_task" and isinstance(nested, dict):
            return (
                f"文字：{nested.get('text', '未知')}，"
                f"分数：{float(nested.get('score', 0)):.3f}，"
                f"点击：({nested.get('x', '?')}, {nested.get('y', '?')})"
            )
        if task_type == "pipeline" and isinstance(nested, dict):
            return f"流程：{nested.get('pipeline', '未知')}，已执行 {len(nested.get('nodes', []))} 个节点"
        if task_type == "screenshot":
            return "截图已获取"
        if isinstance(nested, dict):
            keys = ", ".join(str(key) for key in nested if key != "image_base64")
            return f"任务类型：{task_type or '未知'}" + (f"，结果字段：{keys}" if keys else "")
        return f"任务类型：{task_type or '未知'}"

    def _format_task_result(self, message: dict[str, Any]) -> str:
        task = message.get("task", {})
        name = task.get("name", "未知任务") if isinstance(task, dict) else "未知任务"
        return f"任务：{name}\n{self._format_result(message.get('result'))}"

    # ------------------------------------------------------------------
    # 客户端任务链
    # ------------------------------------------------------------------
    def _chain_path(self, name: str) -> Path:
        if not CHAIN_NAME_PATTERN.fullmatch(name):
            raise ValueError("任务链名称只能包含字母、数字、下划线和连字符，长度不超过 64")
        return self.chain_dir / f"{name}.json"

    @staticmethod
    def _validate_chain_task(payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("任务链中的每一项必须是对象")
        kinds = [key for key in ("task", "cvtask", "ocrtask") if isinstance(payload.get(key), str) and payload[key]]
        if len(kinds) != 1:
            raise ValueError("每个任务链节点必须且只能包含 task、cvtask 或 ocrtask 之一")
        params = payload.get("params", {})
        if not isinstance(params, dict):
            raise ValueError("任务链节点的 params 必须是对象")
        return {kinds[0]: payload[kinds[0]], "params": params}

    def _load_chain(self, name: str) -> list[dict[str, Any]]:
        path = self._chain_path(name)
        if not path.is_file():
            raise ValueError(f"任务链不存在：{name}")
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"无法读取任务链 {name}") from exc
        if not isinstance(document, dict) or document.get("name") != name:
            raise ValueError(f"任务链文件格式错误：{name}")
        tasks = document.get("tasks")
        if not isinstance(tasks, list) or not tasks:
            raise ValueError(f"任务链没有任务：{name}")
        return [self._validate_chain_task(task) for task in tasks]

    async def _save_chain(self, name: str, tasks: list[dict[str, Any]]) -> None:
        normalized = [self._validate_chain_task(task) for task in tasks]
        if not normalized:
            raise ValueError("任务链至少需要一个任务")
        path = self._chain_path(name)
        document = {"version": 1, "name": name, "tasks": normalized}
        content = json.dumps(document, ensure_ascii=False, indent=2) + "\n"
        await asyncio.to_thread(path.write_text, content, encoding="utf-8")

    def _parse_chain_steps(self, parts: list[str]) -> list[dict[str, Any]]:
        if not parts:
            raise ValueError("至少提供一个任务，例如：run click&x=0.5&y=0.5")
        tasks: list[dict[str, Any]] = []
        for part in parts:
            segments = part.split("&")
            task_spec = segments[0].strip()
            if not task_spec:
                raise ValueError(f"任务链节点格式错误：{part}")
            payload: dict[str, Any]
            if task_spec.startswith("cv:"):
                payload = {"cvtask": task_spec[3:]}
            elif task_spec.startswith("ocr:"):
                payload = {"ocrtask": task_spec[4:]}
            else:
                payload = {"task": task_spec}
            payload["params"] = self._parse_params([segment for segment in segments[1:] if segment])
            tasks.append(self._validate_chain_task(payload))
        return tasks

    @staticmethod
    def _chain_task_name(payload: dict[str, Any]) -> str:
        for key in ("task", "cvtask", "ocrtask"):
            if key in payload:
                return f"{key}={payload[key]}"
        return "未知任务"

    def _format_chain_tasks(self, tasks: list[dict[str, Any]], run: ClientTaskChain | None = None) -> str:
        lines: list[str] = []
        for index, payload in enumerate(tasks):
            state = "待执行"
            task_id = ""
            if run is not None:
                state = run.task_states.get(index, "已跳过" if index in run.skipped else "待执行")
                if index in run.task_ids:
                    task_id = f"（{run.task_ids[index]}）"
            params = payload.get("params", {})
            suffix = f" {params}" if params else ""
            lines.append(f"{index + 1}. [{state}] {self._chain_task_name(payload)}{suffix}{task_id}")
        return "\n".join(lines)

    async def _send_chain_message(self, origin: Any, text: str) -> None:
        try:
            await self.context.send_message(origin, MessageChain([Comp.Plain(text=text)]))
        except Exception as exc:
            logger.warning("Failed to send WPR chain message: %s", exc)

    async def _start_chain(self, event: AstrMessageEvent, name: str) -> ClientTaskChain:
        await self._start_connection()
        tasks = self._load_chain(name)
        async with self._chain_lock:
            existing = self._chains.get(name)
            if existing is not None and existing.runner is not None and not existing.runner.done():
                raise ValueError(f"任务链正在执行：{name}")
            run = ClientTaskChain(name=name, tasks=tasks, origin=event.unified_msg_origin)
            self._chains[name] = run
            run.runner = asyncio.create_task(self._run_chain(run), name=f"wpr-chain-{name}")
            return run

    async def _run_chain(self, run: ClientTaskChain) -> None:
        try:
            for index, payload in enumerate(run.tasks):
                if run.stop_requested:
                    break
                if index in run.skipped:
                    run.task_states[index] = "已跳过"
                    continue
                run.current_index = index
                try:
                    accepted = await self._submit_for_origin(run.origin, payload)
                except Exception as exc:
                    run.task_states[index] = "创建失败"
                    run.status = "failed"
                    await self._send_chain_message(run.origin, f"WPR 任务链 {run.name} 创建第 {index + 1} 项失败：{exc}")
                    return
                task_id = str(accepted["task_id"])
                run.task_ids[index] = task_id
                cancel_after_create = run.stop_requested or index in run.skipped
                run.task_states[index] = "取消中" if cancel_after_create else "已创建"
                await self._send_chain_message(
                    run.origin,
                    f"WPR 任务链 {run.name} 已创建第 {index + 1}/{len(run.tasks)} 项\n"
                    f"任务：{self._chain_task_name(payload)}\n任务 ID：{task_id}",
                )
                if cancel_after_create:
                    await self._send({"action": "cancel", "task_id": task_id})
                terminal = await self._wait_task_terminal(task_id)
                terminal_state = str(terminal.get("status", "unknown"))
                run.task_states[index] = terminal_state
                if run.stop_requested:
                    break
                if terminal_state == "completed" or index in run.skipped:
                    continue
                run.status = "failed"
                await self._send_chain_message(
                    run.origin,
                    f"WPR 任务链 {run.name} 在第 {index + 1} 项结束，不再继续后续任务。\n"
                    f"状态：{terminal_state}",
                )
                return
            run.status = "cancelled" if run.stop_requested else "completed"
        except asyncio.CancelledError:
            run.stop_requested = True
            run.status = "cancelled"
            raise
        finally:
            run.current_index = None
            await self._send_chain_message(
                run.origin,
                f"WPR 任务链 {run.name} 已结束：{run.status}\n{self._format_chain_tasks(run.tasks, run)}",
            )

    async def _cancel_chain(self, name: str, index: int | None = None) -> str:
        run = self._chains.get(name)
        if run is None or run.runner is None or run.runner.done():
            raise ValueError(f"没有正在执行的任务链：{name}")
        if index is None:
            run.stop_requested = True
            run.status = "cancelling"
            if run.current_index is not None and run.current_index in run.task_ids:
                await self._send({"action": "cancel", "task_id": run.task_ids[run.current_index]})
            return f"已取消任务链 {name}；当前任务结束后不会创建下一项。"

        task_index = index - 1
        if not 0 <= task_index < len(run.tasks):
            raise ValueError(f"任务序号超出范围：{index}")
        if task_index < (run.current_index if run.current_index is not None else 0):
            raise ValueError(f"第 {index} 项已经结束，不能取消。")
        run.skipped.add(task_index)
        if task_index == run.current_index and task_index in run.task_ids:
            run.task_states[task_index] = "取消中"
            await self._send({"action": "cancel", "task_id": run.task_ids[task_index]})
            return f"已取消任务链 {name} 的第 {index} 项；收到终态后将继续下一项。"
        run.task_states[task_index] = "已跳过"
        return f"已跳过任务链 {name} 的第 {index} 项；执行到该项时会直接继续下一项。"

    async def _stop_all_chains(self) -> None:
        active = [run for run in self._chains.values() if run.runner is not None and not run.runner.done()]
        for run in active:
            run.stop_requested = True
            if run.current_index is not None and run.current_index in run.task_ids and self._connected.is_set():
                try:
                    await self._send({"action": "cancel", "task_id": run.task_ids[run.current_index]})
                except Exception:
                    pass
        for run in active:
            if run.runner is not None:
                run.runner.cancel()
        if active:
            await asyncio.gather(*(run.runner for run in active if run.runner is not None), return_exceptions=True)

    async def _handle_chain_command(self, event: AstrMessageEvent, args: list[str]) -> str | None:
        if not args:
            return None
        action = args[0].lower()
        if action in {"create", "update"}:
            if len(args) < 3:
                raise ValueError(f"用法：wpr {action} <任务链名> <任务> [任务 ...]")
            name = args[1]
            active = self._chains.get(name)
            if active is not None and active.runner is not None and not active.runner.done():
                raise ValueError(f"任务链正在执行，不能{action}：{name}")
            if action == "create" and self._chain_path(name).exists():
                raise ValueError(f"任务链已存在：{name}，请使用 wpr update {name} ...")
            tasks = self._parse_chain_steps(args[2:])
            await self._save_chain(name, tasks)
            verb = "已创建" if action == "create" else "已更新"
            return f"WPR 任务链{verb}：{name}\n{self._format_chain_tasks(tasks)}"

        if action == "chains":
            names = sorted(path.stem for path in self.chain_dir.glob("*.json"))
            if not names:
                return "尚未创建客户端任务链。"
            descriptions: list[str] = []
            for name in names:
                try:
                    descriptions.append(f"{name}（{len(self._load_chain(name))} 项）")
                except ValueError:
                    descriptions.append(f"{name}（文件无效）")
            return "WPR 客户端任务链：\n" + "\n".join(descriptions)

        if action == "show":
            if len(args) != 2:
                raise ValueError("用法：wpr show <任务链名>")
            name = args[1]
            run = self._chains.get(name)
            tasks = run.tasks if run is not None else self._load_chain(name)
            state = f"，状态：{run.status}" if run is not None else ""
            return f"WPR 任务链 {name}{state}\n{self._format_chain_tasks(tasks, run)}"

        if action == "delete":
            if len(args) != 2:
                raise ValueError("用法：wpr delete <任务链名>")
            name = args[1]
            run = self._chains.get(name)
            if run is not None and run.runner is not None and not run.runner.done():
                raise ValueError(f"任务链正在执行，不能删除：{name}")
            path = self._chain_path(name)
            if not path.is_file():
                raise ValueError(f"任务链不存在：{name}")
            await asyncio.to_thread(path.unlink)
            self._chains.pop(name, None)
            return f"已删除任务链：{name}"

        if action != "auto":
            return None
        if len(args) < 2:
            raise ValueError("用法：wpr auto <任务链名>，或 wpr auto cancel <任务链名> [任务序号]")
        sub_action = args[1].lower()
        if sub_action == "cancel":
            if len(args) not in {3, 4}:
                raise ValueError("用法：wpr auto cancel <任务链名> [任务序号]")
            index = int(args[3]) if len(args) == 4 else None
            return await self._cancel_chain(args[2], index)
        if sub_action == "status":
            if len(args) != 3:
                raise ValueError("用法：wpr auto status <任务链名>")
            run = self._chains.get(args[2])
            if run is None:
                tasks = self._load_chain(args[2])
                return f"WPR 任务链 {args[2]} 尚未运行\n{self._format_chain_tasks(tasks)}"
            return f"WPR 任务链 {run.name} 状态：{run.status}\n{self._format_chain_tasks(run.tasks, run)}"
        if len(args) != 2:
            raise ValueError("用法：wpr auto <任务链名>")
        run = await self._start_chain(event, args[1])
        return f"WPR 任务链已启动：{run.name}\n{self._format_chain_tasks(run.tasks, run)}"

    def _is_allowed(self, event: AstrMessageEvent) -> bool:
        return not self.allowed_users or str(event.get_sender_id()) in self.allowed_users

    def _llm_tool_error(self, event: AstrMessageEvent, task_name: str | None = None) -> str | None:
        """Return a user-safe error before an LLM tool controls WPR."""
        if not self.llm_tool_enabled:
            return "WPR LLM 工具已被插件配置禁用。"
        if not self._is_allowed(event):
            return "当前用户没有使用 WPR 控制插件的权限。"
        if task_name is not None and task_name not in self.llm_allowed_tasks:
            return f"任务不在 LLM 工具白名单中：{task_name}"
        return None

    @filter.llm_tool(name="wpr_get_status")
    async def llm_get_wpr_status(self, event: AstrMessageEvent) -> str:
        """查询 WPR 云游戏服务和视觉引擎的当前状态。"""
        error = self._llm_tool_error(event)
        if error:
            return error
        try:
            await self._start_connection()
            status = await self._query_server()
            return f"WPR 服务状态：{self._format_result(status.get('status'))}"
        except Exception as exc:
            logger.warning("LLM status tool failed: %s", exc)
            return f"无法查询 WPR 状态：{exc}"

    @filter.llm_tool(name="wpr_execute_task")
    async def llm_execute_wpr_task(self, event: AstrMessageEvent, task_name: str, params: dict[str, Any] | None = None) -> str:
        """提交一个已授权的 WPR 云游戏任务。

        Args:
            task_name(string): WPR 任务名称，例如 recruit_prepare、shop_buy 或 friend_visit_all。
            params(object): 任务参数对象；没有参数时传空对象。
        """
        if not isinstance(task_name, str):
            return "任务名称必须是字符串。"
        normalized_name = task_name.strip()
        error = self._llm_tool_error(event, normalized_name)
        if error:
            return error
        if params is None:
            params = {}
        if not isinstance(params, dict):
            return "任务参数必须是对象。"
        try:
            json.dumps(params, ensure_ascii=False)
        except (TypeError, ValueError) as exc:
            return f"任务参数无法编码为 JSON：{exc}"
        try:
            await self._start_connection()
            accepted = await self._submit(event, {"task": normalized_name, "params": params})
            return f"已提交 WPR 任务 {normalized_name}，任务 ID：{accepted['task_id']}。任务结束后插件会主动发送完成、失败或取消通知。"
        except Exception as exc:
            logger.warning("LLM task tool failed for %s: %s", normalized_name, exc)
            return f"提交 WPR 任务失败：{exc}"

    @filter.llm_tool(name="wpr_cancel_task")
    async def llm_cancel_wpr_task(self, event: AstrMessageEvent, task_id: str) -> str:
        """取消一个正在运行的 WPR 任务。

        Args:
            task_id(string): 要取消的 WPR 任务 ID。
        """
        error = self._llm_tool_error(event)
        if error:
            return error
        if not isinstance(task_id, str):
            return "任务 ID 必须是字符串。"
        normalized_task_id = task_id.strip()
        if not normalized_task_id:
            return "任务 ID 不能为空。"
        try:
            await self._start_connection()
            await self._send({"action": "cancel", "task_id": normalized_task_id})
            return f"已发送取消请求：{normalized_task_id}"
        except Exception as exc:
            logger.warning("LLM cancel tool failed for %s: %s", normalized_task_id, exc)
            return f"取消 WPR 任务失败：{exc}"

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
        for prefix in ("/wpr", "wpr"):
            if text.lower().startswith(prefix):
                text = text[len(prefix):].strip()
                break
        return shlex.split(text) if text else []

    @filter.command("wpr")
    async def wpr_command(self, event: AstrMessageEvent):
        """WPR 云游戏控制命令。"""
        if not self._is_allowed(event):
            yield event.plain_result("你没有使用 WPR 控制插件的权限。")
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
        websocket_session = event.unified_msg_origin in self.sessions
        try:
            chain_response = await self._handle_chain_command(event, args)
            if chain_response is not None:
                yield event.plain_result(chain_response)
            elif action == "start":
                await self._start_connection()
                self.sessions[event.unified_msg_origin] = str(event.get_sender_id())
                yield event.plain_result("WPR 控制会话已启动并连接 WebSocket。\n直接发送任务名或命令，例如：run、click x=0.5 y=0.5、start。\n发送 help 查看命令，发送 quit 退出会话并断开连接。")
            elif action == "task":
                if len(args) < 2:
                    raise ValueError("用法：/wpr task <任务名> [key=value ...]")
                params = self._parse_params(args[2:])
                if websocket_session:
                    accepted = await self._submit(event, {"task": args[1], "params": params})
                    yield event.plain_result(self._accepted_text(accepted))
                else:
                    yield await self._http_task_response(event, args[1], {"task": args[1], **params})
            elif action in {"cv", "ocr"}:
                if len(args) < 2:
                    label = "模板路径" if action == "cv" else "目标文字"
                    raise ValueError(f"用法：/wpr {action} <{label}> [key=value ...]")
                key = "cvtask" if action == "cv" else "ocrtask"
                params = self._parse_params(args[2:])
                if websocket_session:
                    accepted = await self._submit(event, {key: args[1], "params": params})
                    yield event.plain_result(self._accepted_text(accepted))
                else:
                    yield await self._http_task_response(event, f"{action} {args[1]}", {key: args[1], **params})
            elif action == "status":
                if len(args) == 1:
                    if websocket_session:
                        server = await self._query_server()
                        yield event.plain_result(f"WPR 服务状态\n{self._format_result(server.get('status'))}")
                    else:
                        server = await self._http_remote({"task": "status"})
                        if server.get("success") is False or server.get("error"):
                            raise RuntimeError(str(server.get("error", "WPR HTTP 状态查询失败")))
                        yield event.plain_result(f"WPR 服务状态\n{self._format_result(server)}")
                else:
                    task = await self._query_task(args[1])
                    yield await self._task_status_response(event, task)
            elif action == "cancel":
                if len(args) != 2:
                    raise ValueError("用法：/wpr cancel <任务ID>")
                await self._send({"action": "cancel", "task_id": args[1]})
                yield event.plain_result(f"已发送取消请求：{args[1]}")
            elif action == "quit":
                self.sessions.pop(event.unified_msg_origin, None)
                await self._stop_all_chains()
                await self._disconnect()
                yield event.plain_result("WPR 控制会话已退出，WebSocket 已断开。")
            else:
                yield event.plain_result(self._help_text())
        except Exception as exc:
            logger.exception("WPR command failed")
            yield event.plain_result(f"WPR 操作失败：{exc}")

    @filter.regex(r".*")
    async def session_message(self, event: AstrMessageEvent):
        """处理 /wpr start 后同一 QQ 会话内的控制文本。"""
        origin = event.unified_msg_origin
        owner_id = self.sessions.get(origin)
        if owner_id is None or owner_id != str(event.get_sender_id()):
            return
        message = event.message_str.strip()
        if not message or message.startswith("/"):
            return
        if message.lower() == "quit":
            self.sessions.pop(origin, None)
            await self._stop_all_chains()
            await self._disconnect()
            yield event.plain_result("WPR 控制会话已退出，WebSocket 已断开。")
            return
        try:
            parts = shlex.split(message)
            if not parts:
                return
            action = parts[0].lower()
            chain_response = await self._handle_chain_command(event, parts)
            if chain_response is not None:
                yield event.plain_result(chain_response)
            elif action == "help":
                yield event.plain_result(self._help_text())
            elif action == "status":
                if len(parts) == 1:
                    server = await self._query_server()
                    yield event.plain_result(f"WPR 服务状态\n{self._format_result(server.get('status'))}")
                else:
                    task = await self._query_task(parts[1])
                    yield await self._task_status_response(event, task)
            elif action == "cancel":
                if len(parts) != 2:
                    raise ValueError("用法：cancel <任务ID>")
                await self._send({"action": "cancel", "task_id": parts[1]})
                yield event.plain_result(f"已发送取消请求：{parts[1]}")
            elif action in {"cv", "ocr"}:
                if len(parts) < 2:
                    label = "模板路径" if action == "cv" else "目标文字"
                    raise ValueError(f"用法：{action} <{label}> [key=value ...]")
                accepted = await self._submit(event, {
                    "cvtask" if action == "cv" else "ocrtask": parts[1],
                    "params": self._parse_params(parts[2:]),
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
            logger.exception("WPR session command failed")
            yield event.plain_result(f"WPR 操作失败：{exc}")

    @staticmethod
    def _accepted_text(accepted: dict[str, Any]) -> str:
        return f"WPR 任务已提交\n任务 ID：{accepted['task_id']}"

    @staticmethod
    def _help_text() -> str:
        return """WPR 云游戏控制

 /wpr help
  显示本帮助。
 /wpr start
  建立 WebSocket 控制会话；会话中直接发送任务文本，quit 退出并断开。
/wpr task <任务名> [key=value ...]
  例：/wpr task run
  例：/wpr task click x=0.5 y=0.5
  例：/wpr task wait seconds=20
  例：/wpr task tab_open url=https://example.com
 /wpr cv <模板路径> [key=value ...]
 /wpr ocr <目标文字> [key=value ...]
/wpr status [任务ID]
/wpr cancel <任务ID>
/wpr create <链名> <任务> [任务 ...]
  例：/wpr create run click&x=1&y=1 pipeline&pipeline_name=startup2
/wpr update <链名> <任务> [任务 ...]
/wpr chains | /wpr show <链名> | /wpr delete <链名>
/wpr auto <链名>
/wpr auto status <链名>
/wpr auto cancel <链名> [任务序号]

未执行 /wpr start 时，task/cv/ocr/status 使用 HTTP；进入会话后使用 WebSocket。auto 始终使用 WebSocket。

会话内常用任务：
start                         启动默认明日方舟入口，不检查登录状态
shutdown                      强制关闭浏览器并重置状态
pipeline pipeline_name=mall  执行 OpenCV 流程
tab_list                      列出标签页
tab_new                       新建空白标签页
tab_open url=https://...      新建并打开网页
tab_switch tab_index=1        切换标签页
tab_close tab_index=1         关闭标签页

任务完成、失败或取消时会自动通知当前 QQ 会话，包含耗时和结束截图。"""
