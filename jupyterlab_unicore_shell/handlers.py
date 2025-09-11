import asyncio
import inspect
import json
import os
import threading

import pyunicore.client as uc_client
import pyunicore.credentials as uc_credentials
import pyunicore.forwarder as uc_forwarding
from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join
from tornado import web
from tornado.iostream import StreamClosedError
from traitlets import Any
from traitlets import Bool
from traitlets import List
from traitlets.config import Configurable

background_tasks = set()


class UNICOREReverseShell(Configurable):
    enabled = Bool(
        os.environ.get("JUPYTERLAB_UNICORE_SHELL_ENABLED", "false").lower()
        in ["1", "true"],
        config=True,
        help=("Enable extension backend"),
    )

    async def example_system_config(self):
        unity_userinfo_url = "https://login.jsc.fz-juelich.de/oauth2/userinfo"
        access_token = await self.get_access_token()
        if not access_token:
            return {}
        import re
        import requests

        try:
            r = requests.get(
                f"{unity_userinfo_url}",
                headers={
                    "Authorization": "Bearer {access_token}".format(
                        access_token=access_token
                    ),
                    "Accept": "application/json",
                },
            )
            r.raise_for_status()
        except:
            return {}
        resp = r.json()
        preferred_username = resp.get("preferred_username", False)
        entitlements = resp.get("entitlements", [])
        res_pattern = re.compile(
            r"^urn:"
            r"(?P<namespace>.+?(?=:res:)):"
            r"res:"
            r"(?P<systempartition>[^:]+):"
            r"(?P<project>[^:]+):"
            r"act:"
            r"(?P<account>[^:]+):"
            r"(?P<accounttype>[^:]+)$"
        )

        def getUrl(s):
            return f"https://unicore.fz-juelich.de/{s}/rest/core"

        ret = {}

        for entry in entitlements:
            match = res_pattern.match(entry)
            if match:
                account = match.group("account")
                system = match.group("systempartition")
                if account == preferred_username:
                    allowed_system = [x for x in self.systems if system.startswith(x)]
                    if len(allowed_system) > 0 and allowed_system[0] not in ret.keys():
                        ret[allowed_system[0]] = {"url": getUrl(allowed_system[0])}

        return ret

    system_config = Any(
        example_system_config,
        help="""
        Dict containing the UNICORE/X urls for supported systems.
        """,
    )

    systems = List(
        default_value=[
            x
            for x in os.environ.get("JUPYTERLAB_UNICORE_SHELL_SYSTEMS", "").split(",")
            if x
        ],
        help="""
        List containing the activated UNICORE/X systems.
        Optional parameter, can be used within c.UNICOREReverseShell.system_config .
        """,
    )

    async def get_system_config(self):
        _system_config = self.system_config
        if callable(_system_config):
            _system_config = _system_config(self)
            if inspect.isawaitable(_system_config):
                _system_config = await _system_config
        return _system_config

    access_token = Any(
        default_value=os.environ.get("ACCESS_TOKEN", None),
        config=True,
        help=(
            """
        String or function called to get current access token of user before sending
        request to the API.

        Example::

            def get_token(self):
                return "mytoken"
            c.UNICOREReverseShell.access_token = get_token
        """
        ),
    )

    async def get_access_token(self):
        _access_token = self.access_token
        if callable(_access_token):
            _access_token = _access_token(self)
            if inspect.isawaitable(_access_token):
                _access_token = await _access_token
        return _access_token

    unicore_forward_debug = Bool(
        os.environ.get("JUPYTERLAB_UNICORE_FORWARD_DEBUG", "false").lower()
        in ["1", "true"],
        config=True,
        help=("Enable debug output in unicore port forwarding"),
    )

    def default_unicore_shell_code(self):
        return """
module purge --force
module load Stages/2025
module load GCCcore/.13.3.0
module load jupyter-server
python3 terminal.py
"""

    unicore_shell_code = Any(
        default_value=default_unicore_shell_code,
        config=True,
        help="""
        Bash code executed on remote system.
        Must call `python terminal.py` in the end to start terminal.
        """,
    )

    async def get_unicore_shell_code(self):
        _unicore_shell_code = self.unicore_shell_code
        if callable(_unicore_shell_code):
            _unicore_shell_code = _unicore_shell_code(self)
            if inspect.isawaitable(_unicore_shell_code):
                _unicore_shell_code = await _unicore_shell_code
        return _unicore_shell_code

    def default_unicore_python_code(self):
        return """
import os
import socket
import sys
import terminado
import tornado.httpserver
import tornado.ioloop
import tornado.web

from datetime import datetime

class LaxTermSocket(terminado.TermSocket):
    active_clients = set()

    def check_origin(self, origin):
        return True

    def open(self, *args, **kwargs):
        super().open(*args, **kwargs)
        self.active_clients.add(self)
        print(f"{datetime.now()} - Client connected. Active: {len(self.active_clients)}", flush=True)

    def on_close(self):
        super().on_close()
        self.active_clients.discard(self)
        print(f"{datetime.now()} - Client disconnected. Active: {len(self.active_clients)}", flush=True)

class OneShotTermManager(terminado.UniqueTermManager):
    def client_disconnected(self, websocket) -> None:
        super().client_disconnected(websocket)
        sys.exit()


if __name__ == "__main__":
    term_manager = OneShotTermManager(shell_command=["bash"], term_settings={"cwd": os.path.expanduser("~")})
    app = tornado.web.Application([
        (r"/([^/]+)", LaxTermSocket, {"term_manager": term_manager}),
    ])
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind("sock")
    sock.listen(2)
    sock.setblocking(False)
    httpserver = tornado.httpserver.HTTPServer(app)
    httpserver.add_socket(sock)

    loop = tornado.ioloop.IOLoop.current()

    # Check every 10 seconds if no clients are connected
    def check_inactive():
        if not LaxTermSocket.active_clients:
            print(f"{datetime.now()} - No clients connected. Scheduling shutdown after 60s...", flush=True)
            loop.call_later(60, shutdown_if_still_inactive)

    def shutdown_if_still_inactive():
        if not LaxTermSocket.active_clients:
            print(f"{datetime.now()} - No clients connected for 60s. Shutting down.", flush=True)
            term_manager.shutdown()
            loop.stop()
        else:
            print(f"{datetime.now()} - A client connected again. Canceling shutdown.", flush=True)

    def shutdown():
        print(f"{datetime.now()} - Shutting down server...", flush=True)
        term_manager.shutdown()
        loop.stop()

    # Periodic check
    checker = tornado.ioloop.PeriodicCallback(check_inactive, 10000)
    checker.start()

    try:
        print(f"{datetime.now()} - Start listening.", flush=True)
        loop.start()
    finally:
        term_manager.shutdown()
"""

    unicore_python_code = Any(
        default_value=default_unicore_python_code,
        config=True,
        help="""
        Bash code executed on remote system.
        Must call `python terminal.py` in the end to start terminal.
        """,
    )

    async def get_unicore_python_code(self):
        _unicore_python_code = self.unicore_python_code
        if callable(_unicore_python_code):
            _unicore_python_code = _unicore_python_code(self)
            if inspect.isawaitable(_unicore_python_code):
                _unicore_python_code = await _unicore_python_code
        return _unicore_python_code


shells = {}


class ReverseShellJob:
    config = None
    status = None
    port = None
    _clients = None

    log = None

    system = None
    uc_job = None
    uc_forward = None
    uc_forward_thread = None

    background_forward_task = None

    def register_client(self) -> asyncio.Queue:
        q = asyncio.Queue()
        self._clients.append(q)
        return q

    def unregister_client(self, q: asyncio.Queue):
        self._clients.remove(q)

    async def broadcast_status(self, msg, ready=False, failed=False, newline=True):
        status = {"newline": newline}
        if ready:
            status["ready"] = True
        if failed:
            status["failed"] = True
        status["msg"] = msg
        if failed:
            self.status = None
        else:
            self.status = status
        for q in self._clients:
            await q.put(status)

    def __init__(self, system, config: UNICOREReverseShell, log):
        self.system = system
        self.config = config
        self.status = None
        self.log = log
        self._clients: list[asyncio.Queue] = []

    async def port_forward(self, credential) -> int:
        endpoint = self.uc_job.resource_url + "/forward-port?file=sock"
        self.uc_forward = uc_forwarding.Forwarder(
            uc_client.Transport(credential), endpoint
        )
        self.uc_forward.quiet = not self.config.unicore_forward_debug
        self.uc_forward_thread = threading.Thread(
            target=self.uc_forward.run, kwargs={"local_port": 0}, daemon=True
        )
        self.uc_forward_thread.start()
        while self.uc_forward.local_port == 0:
            await asyncio.sleep(1)
        return self.uc_forward.local_port

    def stop_job(self):
        if self.uc_job:
            try:
                if self.config.unicore_forward_debug:
                    self.uc_job.abort()
                else:
                    self.uc_job.delete()
            except:
                self.log.exception(f"Could not stop UNICORE job for {self.system}")
            self.uc_job = None

    async def run(self):
        try:
            await self._run()
        except Exception as e:
            self.log.exception(f"Terminal start on {self.system} failed.")
            await self.broadcast_status(
                f"Terminal start failed: {str(e)}",
                failed=True,
            )
            await self.broadcast_status(
                "You can close this terminal and try again. Check JupyterLab logs for more information."
            )
            self.stop_job()

    async def _run(self):
        access_token = await self.config.get_access_token()
        if not access_token:
            raise Exception(
                "No access token available. Check configuration or env variable ACCESS_TOKEN"
            )
        system_config = await self.config.get_system_config()
        if self.system not in system_config.keys():
            raise Exception(
                f"System {self.system} not configured in {system_config.keys()}"
            )

        await self.broadcast_status(
            f"Create UNICORE Job to start terminal on {self.system}:"
        )

        await self.broadcast_status("  Create UNICORE credentials ...")
        credential = uc_credentials.OIDCToken(access_token, None)
        await self.broadcast_status(" done", newline=False)

        await self.broadcast_status(f"  Create UNICORE client ...")
        client = uc_client.Client(
            credential, system_config[self.system].get("url", "NoUrlConfigured")
        )

        await self.broadcast_status(" done", newline=False)

        shell_code = await self.config.get_unicore_shell_code()

        python_code = await self.config.get_unicore_python_code()

        job_description = {
            "Job type": "ON_LOGIN_NODE",
            "Executable": "/bin/bash terminado.sh",
            "Imports": [
                {"From": "inline://dummy", "To": "terminado.sh", "Data": [shell_code]},
                {"From": "inline://dummy", "To": "terminal.py", "Data": [python_code]},
            ],
        }

        await self.broadcast_status("  Submit UNICORE Job ...")
        self.uc_job = client.new_job(job_description)

        await self.broadcast_status(" done", newline=False)
        status = None
        while self.uc_job.status not in [
            uc_client.JobStatus.RUNNING,
            uc_client.JobStatus.FAILED,
            uc_client.JobStatus.SUCCESSFUL,
            uc_client.JobStatus.UNDEFINED,
        ]:
            if status == self.uc_job.status:
                await self.broadcast_status(".", newline=False)
            else:
                await self.broadcast_status(
                    f"  Waiting for UNICORE Job to start. Current Status: {self.uc_job.status} ..."
                )
            status = self.uc_job.status
            await asyncio.sleep(uc_client._DEFAULT_CACHE_TIME)

        if self.uc_job.status in [
            uc_client.JobStatus.FAILED,
            uc_client.JobStatus.SUCCESSFUL,
            uc_client.JobStatus.UNDEFINED,
        ]:
            file_path = self.uc_job.working_dir.stat("stderr")
            file_size = file_path.properties["size"]
            await self.broadcast_status(f"Terminal could not be started.")
            if file_size == 0:
                uc_logs = "\n".join(self.uc_job.properties.get("log", []))
                self.log.error(f"UNICORE Logs: {uc_logs}")
            else:
                offset = max(0, file_size - 4096)
                s = file_path.raw(offset=offset)
                msg = s.data.decode()
                self.log.error(f"UNICORE Stdout: {msg}")
            raise Exception(f"UNICORE job unexpected status {self.uc_job.status}.")
        elif self.uc_job.status == uc_client.JobStatus.RUNNING:
            await self.broadcast_status("  Setting up port forwarding ...")
            self.port = await self.port_forward(credential)
            await asyncio.sleep(5)
            await self.broadcast_status(" done", newline=False)
            await self.broadcast_status(
                "  Connecting terminal ...",
                ready=True,
                newline=True,
            )
        else:
            raise Exception(f"Unexpected UNICORE Job Status: {self.uc_job.status}")


class ReverseShellAPIHandler(APIHandler):
    keepalive_interval = 8
    keepalive_task = None

    def get_content_type(self):
        return "text/event-stream"

    async def send_event(self, event):
        try:
            self.write(f"data: {json.dumps(event)}\n\n")
            await self.flush()
        except StreamClosedError:
            self.log.warning("Stream closed while handling %s", self.request.uri)
            # raise Finish to halt the handler
            raise web.Finish()

    def on_finish(self):
        if self.keepalive_task and not self.keepalive_task.done():
            try:
                self.keepalive_task.cancel()
            except:
                pass
        self.keepalive_task = None

    async def keepalive(self):
        """Write empty lines periodically

        to avoid being closed by intermediate proxies
        when there's a large gap between events.
        """
        try:
            while True:
                try:
                    self.write("\n\n")
                    await self.flush()
                except (StreamClosedError, RuntimeError):
                    return

                await asyncio.sleep(self.keepalive_interval)
        except asyncio.CancelledError:
            pass
        except:
            self.log.exception("Close keepalive")

    async def get(self, system):
        self.set_header("Content-Type", "text/event-stream")
        self.set_header("Cache-Control", "no-cache")
        self.set_header("Connection", "keep-alive")

        self.keepalive_task = asyncio.create_task(self.keepalive())
        if system not in shells.keys():
            shells[system] = ReverseShellJob(
                system, UNICOREReverseShell(config=self.config), log=self.log
            )
        status = shells[system].status
        if not status:
            task = asyncio.create_task(shells[system].run())
            background_tasks.add(task)
            task.add_done_callback(background_tasks.discard)
        else:
            await self.send_event(status)
            if status.get("ready", False):
                self.finish()
                return

        queue = shells[system].register_client()

        try:
            while True:
                get_task = asyncio.create_task(queue.get())

                # Wait for either a status update or keepalive timeout
                done, _ = await asyncio.wait(
                    [get_task, self.keepalive_task], return_when=asyncio.FIRST_COMPLETED
                )
                if self.keepalive_task in done:
                    break

                if get_task in done:
                    status = done.pop().result()
                    await self.send_event(status)
                    if status.get("ready", False):
                        break
        except asyncio.CancelledError:
            pass
        finally:
            shells[system].unregister_client(queue)
            self.finish()

    async def delete(self, system):
        if system not in shells.keys():
            self.set_status(404)
            return

        shell = shells[system]
        shell.stop_job()
        del shells[system]
        self.log.info(f"Stop {system} terminal UNICORE job")
        self.set_status(204)


class ReverseShellInitAPIHandler(APIHandler):
    async def get(self):
        config = UNICOREReverseShell(config=self.config)

        systems_config = await config.get_system_config()
        systems = list(systems_config.keys())
        self.set_status(200)
        self.finish(json.dumps(systems, sort_keys=True))


import json
import tornado.websocket
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join
from jupyter_server.auth.decorator import authorized


class RemoteTerminalRootHandler(APIHandler):
    auth_resource = "remoteshell"

    @web.authenticated
    @authorized
    async def post(self):
        data = self.get_json_body() or {}
        self.finish(json.dumps(data))

    @web.authenticated
    @authorized
    async def get(self, name=""):
        models = []
        for key in shells.keys():
            models.append({"name": key})
        self.finish(json.dumps(models))


class RemoteTerminalWSHandler(tornado.websocket.WebSocketHandler):
    async def open(self, name):
        if name not in shells.keys():
            raise Exception(f"WebSocket for {name} not available.")
        if not shells[name].port:
            raise Exception(f"WebSocket for {name} not ready yet.")
        port = shells[name].port

        """Proxy WS to remote terminado"""
        remote_url = f"ws://127.0.0.1:{port}/{name}"
        self.client = tornado.websocket.websocket_connect(
            remote_url, ping_interval=20, ping_timeout=30
        )
        self.remote = await self.client

        async def pump_remote():
            try:
                while True:
                    msg = await self.remote.read_message()
                    if msg is None:
                        break
                    await self.write_message(msg)
            except StreamClosedError:
                pass
            finally:
                self.close()

        IOLoop.current().spawn_callback(pump_remote)

    async def on_message(self, message):
        await self.remote.write_message(message)

    def on_close(self):
        if self.remote:
            self.remote.close()


def setup_handlers(web_app):
    host_pattern = ".*$"
    base_url = web_app.settings["base_url"]

    route_pattern_init = url_path_join(base_url, "jupyterlabunicoreshell")
    route_pattern = url_path_join(base_url, "jupyterlabunicoreshell", r"([^/]+)")
    remote_route_pattern = url_path_join(base_url, "remoteshell", "api", "terminals")
    remote_ws_pattern = url_path_join(
        base_url, "remoteshell", "terminals", "websocket", r"([^/]+)"
    )
    handlers = [
        (route_pattern, ReverseShellAPIHandler),
        (route_pattern_init, ReverseShellInitAPIHandler),
        (remote_route_pattern, RemoteTerminalRootHandler),
        (remote_ws_pattern, RemoteTerminalWSHandler),
    ]
    web_app.add_handlers(host_pattern, handlers)
