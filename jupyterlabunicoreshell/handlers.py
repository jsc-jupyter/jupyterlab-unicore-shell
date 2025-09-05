import asyncio
import inspect
import json
import threading
import os
import pyunicore.client as uc_client
import pyunicore.credentials as uc_credentials
import pyunicore.forwarder as uc_forwarding

from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join
from tornado import web

from traitlets.config import Configurable
from traitlets import Any, Bool
from tornado.iostream import StreamClosedError

import socket
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
            r = requests.get(f"{unity_userinfo_url}", headers={"Authorization": "Bearer {access_token}".format(access_token=access_token), "Accept": "application/json"})
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
            return f"https://zam2125.zam.kfa-juelich.de:9112/{s}/rest/core"

        allowed_systems = ["JUWELS", "JURECA", "JUPITER", "JUSUF", "DEEP"]
        ret = {}

        for entry in entitlements:
            match = res_pattern.match(entry)
            if match:
                account = match.group("account")
                system = match.group("systempartition")
                if account == preferred_username:
                    allowed_system = [x for x in allowed_systems if system.startswith(x)]
                    if len(allowed_system) > 0 and allowed_system[0] not in ret.keys():
                        ret[allowed_system[0]] ={"url": getUrl(allowed_system[0])}

        return ret

    system_config = Any(
        example_system_config,
        help="""
        Dict containing the UNICORE/X urls for supported systems.
        """
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

        Example:
        def get_token():
            return "mytoken"
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

shells = {}

class ReverseShellJob:
    config = None
    status = None
    _clients = None

    system = ""
    port = None

    background_forward_task = None

    def register_client(self) -> asyncio.Queue:
        q = asyncio.Queue()
        self._clients.append(q)
        return q

    def unregister_client(self, q: asyncio.Queue):
        self._clients.remove(q)
    
    async def broadcast_status(self, msg, ready=False, failed=False, host=None, port=None):
        status = {}
        if ready:
            status["ready"] = True
        if failed:
            status["failed"] = True
        if port:
            status["port"] = f"{port}"
        if host:
            status["host"] = host
        status["msg"] = msg
        self.status = status
        for q in self._clients:
            await q.put(status)

    def random_port(self):
        """Get a single random port."""
        sock = socket.socket()
        sock.bind(('', 0))
        port = sock.getsockname()[1]
        sock.close()
        return port


    def __init__(self, config: UNICOREReverseShell, system: str):
        self.config = config
        self.system = system
        self.status = None
        self._clients: list[asyncio.Queue] = []
    
    def port_forward(self, uc_job: uc_client.Job, credential, application_port: int, local_port: int):
        endpoint = uc_job.resource_url + f"/forward-port?port={application_port}"
        f = uc_forwarding.Forwarder(uc_client.Transport(credential), endpoint)
        f.quiet = False
        thread = threading.Thread(
            target=f.run,
            kwargs={"local_port": local_port},
            daemon=True
        )
        thread.start()

    async def run(self, system):
        access_token = await self.config.get_access_token()
        if not access_token:
            await self.broadcast_status("No access token available. Check configuration or env variable ACCESS_TOKEN", failed=True)
        system_config = await self.config.get_system_config()
        if system not in system_config.keys():
            await self.broadcast_status(f"System {system} not configured in {system_config.keys()}", failed=False)
            return

        await self.broadcast_status(f"Create UNICORE Job to start terminal on {system}")

        await self.broadcast_status("Create UNICORE credentials ...")
        credential = uc_credentials.OIDCToken(access_token, None)
        await self.broadcast_status("Create UNICORE client ...")
        client = uc_client.Client(credential, system_config[system].get("url", "NoUrlConfigured"))

        await self.broadcast_status("Create UNICORE client done.")

        shell_code = """
module purge --force
module load Stages/2025
module load GCCcore/.13.3.0
module load jupyter-server
python3 terminal.py
"""

        random_app_port = self.random_port()
        python_code = """
import terminado
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


if __name__ == "__main__":
    term_manager = terminado.UniqueTermManager(shell_command=["bash"])
    app = tornado.web.Application([
        (r"/terminals/websocket/([^/]+)", LaxTermSocket, {"term_manager": term_manager}),
    ])
    print(f"{datetime.now()} - Start listening on {app_port}", flush=True)
    httpserver = app.listen({app_port}, "0.0.0.0")

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
        loop.start()
    finally:
        term_manager.shutdown()
""".replace("{app_port}", f"{random_app_port}")

        job_description = {
            "Job type": "ON_LOGIN_NODE",
            "Executable": "/bin/bash terminado.sh",
            "Imports": [
                {
                    "From": "inline://dummy",
                    "To": "terminado.sh",
                    "Data": [
                        shell_code
                    ]
                },
                {
                    "From": "inline://dummy",
                    "To": "terminal.py",
                    "Data": [
                        python_code
                    ]
                }
            ]
        }

        await self.broadcast_status("Submit UNICORE Job ...")
        uc_job = client.new_job(job_description)

        await self.broadcast_status("Submit UNICORE Job successful.")
        while uc_job.status not in [uc_client.JobStatus.RUNNING, uc_client.JobStatus.FAILED, uc_client.JobStatus.SUCCESSFUL, uc_client.JobStatus.UNDEFINED]:
            await self.broadcast_status(f"Waiting for Terminal to start. Current Status: {uc_job.status}")
            await asyncio.sleep(2)

        if uc_job.status in ["FAILED", "SUCCESSFUL", "DONE"]:
            file_path = uc_job.working_dir.stat("stderr")
            file_size = file_path.properties["size"]
            await self.broadcast_status(f"Terminal could not be started.")
            if file_size == 0:
                uc_logs = '\n'.join(uc_job.properties.get("log", []))
                await self.broadcast_status(f"Unicore Logs: {uc_logs}", failed=True)
            else:
                offset = max(0, file_size - 4096)
                s = file_path.raw(offset=offset)
                msg = s.data.decode()
                await self.broadcast_status(f"Stdout: {msg}", failed=True)
        else:
            await self.broadcast_status("UNICORE Job is running. Connecting to terminal ...")
        
        await asyncio.sleep(2)
        
        local_port = self.random_port()
        self.port_forward(uc_job, credential, random_app_port, local_port)
        await self.broadcast_status("running", ready=True, port=local_port, host="localhost")




class ReverseShellAPIHandler(APIHandler):
    keepalive_interval = 8
    keepalive_task = None

    def get_content_type(self):
        return 'text/event-stream'

    async def send_event(self, event):
        try:
            self.write(f'data: {json.dumps(event)}\n\n')
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
            shells[system] = ReverseShellJob(UNICOREReverseShell(config=self.config), system)
        status = shells[system].status
        if not status:
            task = asyncio.create_task(shells[system].run(system))
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
                    [get_task, self.keepalive_task],
                    return_when=asyncio.FIRST_COMPLETED
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


class ReverseShellInitAPIHandler(APIHandler):
    async def get(self):
        config = UNICOREReverseShell(config=self.config)
        
        systems_config = await config.get_system_config()
        systems = list(systems_config.keys())
        self.set_status(200)
        self.finish(json.dumps(systems, sort_keys=True))


def setup_handlers(web_app):
    host_pattern = ".*$"
    base_url = web_app.settings["base_url"]

    route_pattern_init = url_path_join(base_url, "jupyterlabunicoreshell")
    route_pattern = url_path_join(base_url, "jupyterlabunicoreshell", r"([^/]+)")
    handlers = [(route_pattern, ReverseShellAPIHandler), (route_pattern_init, ReverseShellInitAPIHandler)]
    web_app.add_handlers(host_pattern, handlers)
