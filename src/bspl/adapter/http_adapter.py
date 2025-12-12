"""
HTTP transport support for BSPL adapter.

This module provides ``HTTPEmitter`` and ``HTTPReceiver`` classes which
implement the same interface as the existing ``Emitter`` and ``Receiver``
classes in the BSPL library, but send and receive messages over HTTP
instead of UDP/TCP sockets. The design aims to be a drop‐in replacement
for networking components used by ``bspl.adapter.core.Adapter``.  Users
can pass instances of these classes into the ``Adapter`` constructor via
the ``emitter`` and ``receiver`` keyword arguments.

The HTTP emitter uses an asynchronous HTTP client (``httpx``) to POST
messages to the destination agent’s URL. The destination must be a
string or two‐tuple specifying ``(host, port)``; a default path of
``/`` is appended when constructing the URL. The emitter encodes
messages by calling the same ``encode`` function used by the UDP
emitter, ensuring messages are JSON serialisable and consistent with
existing transport formats.

The HTTP receiver runs a simple asynchronous HTTP server using
``aiohttp``. When a POST request arrives on the configured host and
port, the receiver reads the JSON payload and forwards it to the
adapter’s ``receive`` method. The receiver can be stopped gracefully
via its ``stop`` method.

Example usage::

    from bspl.adapter.core import Adapter
    from bspl.adapter.emitter import encode  # reuse encode from emitter
    from http_adapter import HTTPEmitter, HTTPReceiver

    agent_urls = {
        "Merchant": "http://localhost:8000",
        "Supplier": "http://localhost:8001",
    }

    emitter = HTTPEmitter(base_urls=agent_urls)
    receiver = HTTPReceiver(host="0.0.0.0", port=8000)
    adapter = Adapter(name="Merchant", systems=systems, agents=agents,
                      emitter=emitter, receiver=receiver)

This will cause the adapter to send BSPL messages via HTTP POST
requests and listen for incoming messages on port 8000.

The implementation relies on optional dependencies: ``httpx`` for the
client and ``aiohttp`` for the server.  If these packages are not
installed, an ``ImportError`` will be raised during initialisation.  To
install them, run::

    pip install httpx aiohttp

"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, Tuple
from urllib.parse import urlparse

try:
    import httpx
except ImportError as exc:
    raise ImportError(
        "HTTPEmitter requires the 'httpx' package. Install it via pip: 'pip install httpx'"
    ) from exc

try:
    from aiohttp import web
except ImportError as exc:
    raise ImportError(
        "HTTPReceiver requires the 'aiohttp' package. Install it via pip: 'pip install aiohttp'"
    ) from exc

from bspl.adapter.emitter import encode


class HTTPEmitter:
    """Asynchronous BSPL message emitter using HTTP POST requests.

    The emitter takes a mapping of agent names to base URLs. When a
    message is sent, the emitter determines the target agent and
    constructs a POST request to that agent’s URL.  Messages are
    encoded to JSON bytes using the same ``encode`` function as the
    UDP emitter.  Basic statistics (bytes and packets sent) are
    maintained for monitoring purposes.

    Parameters
    ----------
    base_urls : Dict[str, str]
        Mapping of agent names to their corresponding base URL, e.g.
        ``{"Merchant": "http://localhost:8000"}``.  The base URL
        should include the scheme and host (and optionally the port),
        but not the path.
    encoder : callable, optional
        Function used to encode messages to bytes. Defaults to
        ``bspl.adapter.emitter.encode`` which JSON‐serialises
        message objects.
    path : str, optional
        HTTP path appended to the base URL when sending a POST
        request. Defaults to ``"/"``.
    timeout : float, optional
        Timeout for HTTP requests, in seconds. Defaults to 5.0.
    """

    def __init__(
        self,
        base_urls: Dict[str, str],
        encoder=encode,
        path: str = "/",
        timeout: float = 5.0,
    ) -> None:
        self.base_urls = base_urls
        self.encode = encoder
        self.path = path
        # build endpoint -> agent map so tuple destinations resolve reliably
        self._endpoint_to_agent: Dict[Tuple[str, int], str] = {}
        for agent, url in base_urls.items():
            parsed = urlparse(url)
            if parsed.hostname and parsed.port:
                self._endpoint_to_agent[(parsed.hostname, parsed.port)] = agent
        # create one shared client; reuse connections for efficiency
        self.client = httpx.AsyncClient(timeout=timeout)
        self.stats: Dict[str, int] = {"bytes": 0, "packets": 0}
        self.logger = logging.getLogger("bspl.http.emitter")

    async def send(self, message) -> None:
        """Encode and send a single message via HTTP POST.

        Parameters
        ----------
        message : bspl.adapter.message.Message
            Message instance to be sent.  The message must have a
            ``dest`` attribute representing the destination agent or
            endpoint.  If the destination is a tuple, it is used to
            look up the agent name via the adapter’s agent mapping
            (passed separately).
        """
        # Determine target agent name
        dest_agent = None
        dest = message.dest
        # dest may be a tuple (host, port) or a string agent name
        if isinstance(dest, tuple):
            dest_agent = self._endpoint_to_agent.get(dest)
        elif isinstance(dest, str):
            dest_agent = dest

        if dest_agent is None or dest_agent not in self.base_urls:
            raise RuntimeError(f"No base URL configured for destination {dest}")

        url = self.base_urls[dest_agent].rstrip("/") + self.path
        payload: bytes = self.encode(message)
        # JSON decode payload to dict for HTTP POST
        data: Dict[str, Any] = json.loads(payload)
        try:
            resp = await self.client.post(url, json=data)
            self.stats["bytes"] += len(payload)
            self.stats["packets"] += 1
            resp.raise_for_status()
        except httpx.RequestError as exc:
            self.logger.error(f"HTTPEmitter failed to reach {url}: {exc}")
            raise
        except httpx.HTTPStatusError as exc:
            self.logger.error(
                f"HTTPEmitter got bad status from {url}: {exc.response.status_code}"
            )
            raise

    async def bulk_send(self, messages) -> None:
        """Send a collection of messages concurrently via HTTP POST.

        Parameters
        ----------
        messages : Iterable[bspl.adapter.message.Message]
            A sequence of message instances to send.
        """
        await asyncio.gather(*(self.send(m) for m in messages))

    async def stop(self) -> None:
        """Close the underlying HTTP client."""
        await self.client.aclose()


class HTTPReceiver:
    """Asynchronous BSPL message receiver using an HTTP server.

    The receiver runs an ``aiohttp`` web application bound to the
    specified host and port. Incoming POST requests are expected to
    contain a JSON body representing a BSPL message (as produced by
    ``HTTPEmitter``).  Upon receiving a request, the receiver
    constructs a dictionary from the JSON and forwards it to the
    adapter’s ``receive`` method.  The receiver stores a reference to
    the running site so that it can be stopped cleanly.

    Parameters
    ----------
    host : str, optional
        Host interface on which to bind the server. Defaults to
        ``"0.0.0.0"``.
    port : int, optional
        Port to listen on. Defaults to ``8000``.
    path : str, optional
        HTTP path to receive messages. Defaults to ``"/"``.  If set
        to e.g. ``"/agent/Merchant"`` then the receiver will only
        accept requests on that path.
    decoder : callable, optional
        Function used to decode the received JSON. Defaults to
        ``json.loads``.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8000,
        path: str = "/",
        decoder=json.loads,
    ) -> None:
        self.host = host
        self.port = port
        self.path = path
        self.decode = decoder
        self.runner: web.AppRunner | None = None
        self.site: web.TCPSite | None = None
        self.adapter = None
        self.logger = logging.getLogger("bspl.http.receiver")

    async def task(self, adapter) -> None:
        """Start the HTTP server and begin accepting requests."""
        self.adapter = adapter
        app = web.Application()

        async def handle(request: web.Request) -> web.Response:
            try:
                data = await request.json()
            except Exception:
                return web.json_response({"error": "Invalid JSON"}, status=400)
            # Forward raw dict to adapter; adapter.receive expects dict
            if self.adapter is not None:
                try:
                    await self.adapter.receive(data)
                except Exception as exc:  # pragma: no cover - defensive
                    self.logger.error(f"HTTPReceiver failed to deliver message: {exc}")
                    return web.json_response(
                        {"error": "Adapter receive failed"}, status=500
                    )
            return web.json_response({"status": "ok"})

        # register route
        app.router.add_post(self.path, handle)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, host=self.host, port=self.port)
        await self.site.start()
        adapter.info(
            f"HTTPReceiver listening on http://{self.host}:{self.port}{self.path}"
        )

    async def stop(self) -> None:
        """Stop the HTTP server."""
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
