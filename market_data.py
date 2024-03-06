from abc import ABC, abstractmethod
import asyncio
import json
from logging import Logger
from multiprocessing import Queue
import os
from queue import Empty
from typing import Callable
import time

from websockets import client
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError


class MarketDataProvider(ABC):
    """
    Base class for all providers of market data. Wrap provider-specific business logic insider a common API.

    Args:
    logger (`logging.Logger`):
        Object for logging output
    receivers (`dict`):
        Mapping from consumer id to consumer queue
    """

    def __init__(
        self, logger: Logger, directives_queue: Queue, receivers: dict
    ) -> None:
        super().__init__()
        self.logger = logger
        self.directives = directives_queue
        self.receivers = receivers

        # State of subscriptions
        self.subs = dict()

    @abstractmethod
    def run(self, stream: str, symbol: str, id_consumer: str) -> None:
        """
        Subscribes consumer to a stream of data

        Args:
        stream (`str`):
            The specific stream to subscribe to, e.g., Q for quotes
        symbol (`str`):
            The asset symbol to subscribe to, e.g., AAPL
        id_consumer (`str`):
            The id associated with the consumer of the data, e.g., 42
        """
        raise NotImplementedError


class PolygonWebsocketProvider(MarketDataProvider):
    """
    Polygon
    """

    def __init__(
        self, logger: Logger, directives_queue: Queue, receivers: dict, uri_cluster: str
    ) -> None:
        super().__init__(logger, directives_queue, receivers)

        self.uri = uri_cluster
        self.api_key = os.environ.get("PG_API_KEY")

    async def _connect(self, processor: Callable) -> None:
        # while True:
        async for socket in client.connect(self.uri):
            self.socket = socket
            try:
                msg = await socket.recv()
                self.logger.info(msg)
                await socket.send(
                    json.dumps({"action": "auth", "params": self.api_key})
                )
                msg_auth = await socket.recv()
                msg_auth_parsed = json.loads(msg_auth)

                if msg_auth_parsed[0]["status"] == "auth_failed":
                    raise AssertionError(
                        f"Authentication failed: {msg_auth_parsed[0]['message']}"
                    )

                # Successfully authenticated, process messages
                while True:
                    try:
                        directives = self.directives.get_nowait()
                        self.logger.info("Recevied directives")
                    except Empty:
                        directives = []

                    for directive in directives:
                        action = directive["action"]
                        stream = directive["stream"]
                        id_recv = directive["id"]
                        self.logger.info(
                            f"Received {action} directive from {id_recv} for {stream}"
                        )

                        if action == "subscribe":
                            if stream in self.subs:
                                self.subs[stream].update([id_recv])
                            else:
                                # New stream, subscribe to it
                                self.subs[stream] = set([id_recv])
                                await socket.send(
                                    json.dumps(
                                        {"action": "subscribe", "params": stream}
                                    )
                                )
                        elif action == "unsubscribe":
                            if stream in self.subs:
                                self.subs[stream].remove(id_recv)
                            else:
                                self.logger.warn(
                                    f"Received directive from {id_recv} to unsubscribe from {stream} which is not subscribed to"
                                )
                        else:
                            self.logger.error(
                                f"Received unsupported directive {action} from {id_recv}"
                            )

                        for stream, receivers in self.subs.items():
                            if len(receivers) == 0:
                                await socket.send(
                                    json.dumps(
                                        {"action": "unsubscribe", "params": stream}
                                    )
                                )

                    try:
                        msg = await asyncio.wait_for(socket.recv(), 0.01)
                        await processor(msg)
                    except asyncio.TimeoutError:
                        _ = 1

            except ConnectionClosedOK:
                self.logger.info("Connection closed")
            except ConnectionClosedError:
                self.logger.error("Connection closed with error")

    async def _handle_message(self, msg):
        ts_received = time.time()
        messages = json.loads(msg)
        message = messages[-1]

        if message["ev"] == "status":
            print(message)
        else:
            message["ts_recv"] = ts_received
            stream = f"{message['ev']}.{message['sym']}"

            try:
                stream_receivers = self.subs[stream]

                for id_recv in stream_receivers:
                    self.receivers[id_recv].put(message)
            except Exception as e:
                self.logger.error(f"Unexpected {type(e)} when processing message")

    def run(self):
        print("Starting asyncio event loop")
        asyncio.run(self._connect(self._handle_message))
        print("After event loop")

    async def close(self):
        """
        Close the websocket connection.
        """
        self.logger.info("closing:")

        if self.socket:
            await self.socket.close()
            self.socket = None
        else:
            self.logger.info("no websocket open to close")
