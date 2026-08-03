from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import string
import warnings
from typing import Iterable, Optional

import carehare
from channels.exceptions import ChannelFull, StopConsumer
from channels.layers import BaseChannelLayer

from .multiqueue import MultiQueue
from .reader import consume_into_multi_queue_until_connection_close
from .util import (
    ChannelRecipient,
    GroupRecipient,
    gather_without_leaking,
    serialize_message,
)

logger = logging.getLogger(__name__)

ReconnectDelay = 1.0  # seconds -- delay before the first reconnect attempt
MaxReconnectDelay = 30.0  # seconds -- ceiling for the exponential backoff


async def _setup_connection(
    *,
    connection: carehare.Connection,
    queue_name: str,
    remote_capacity: Optional[int],
    expiry: Optional[int],
    groups_exchange: str,
) -> None:
    # Declare "groups" exchange. It may persist; spurious declarations
    # (such as on reconnect) are harmless.
    await connection.exchange_declare(groups_exchange, exchange_type="direct")

    # If we're reconnecting to RabbitMQ Node B after Node A disconnected us, our
    # previously-declared exclusive queue may still exist but _won't_ raise a
    # "resource-locked" error.
    #
    # [adamhooper, 2021-06-07] I'm not 100% sure this can actually happen. We
    # saw a problem that _looks_ like this on production, twice, when we
    # upgraded our node pools. The `queue_declare()` succeeded, but subsequent
    # `group_add()` operations failed because the queue did not exist. My theory
    # is: halfway along the way to deleting the exclusive queue, queue_declare()
    # succeeds, because maybe there's a race when deleting an exclusive queue in
    # which it appears non-exclusive to other nodes before it's deleted?
    #
    # ... this guess doesn't _literally_ violate the spec: "The client MAY NOT
    # attempt to use a queue that was declared as exclusive by another
    # still-open connection." Doesn't say anything about queues declared as
    # exclusive by another _closed_ connection on another node.
    #
    # If our woes persist, or if we go 1yr without seeing this message in
    # our logs, delete this block because we'll know it serves no purpose.
    # If, 1yr from now, we've solved our problem, let's build a reproduction
    # and report this upstream.
    try:
        await connection.queue_declare(queue_name, passive=True)
        # If we got here, the queue exists. The `queue_declare()` _below_ ought
        # to raise a 405 error if the queue still exists by the time we call it;
        # but we're guessing it might not, and there's no harm in raising the
        # same error ourselves.
        #
        # This exception will lead to a log message and a reconnect.
        #
        # TODO test using a whole RabbitMQ cluster, to expose this bug and
        # test this solution.
        raise carehare.ChannelClosedByServer(
            405, "channels_rabbitmq refusing to redeclare queue %s" % queue_name
        )
    except carehare.ChannelClosedByServer as err:
        if err.reply_code != 404:
            raise

    arguments = {}
    if remote_capacity is not None:
        arguments["x-max-length"] = remote_capacity
        arguments["x-overflow"] = "reject-publish"
    if expiry is not None:
        arguments["x-message-ttl"] = int(expiry * 1000)
    # This is an exclusive queue, because we want it to disappear when we
    # shut down. (Otherwise we'd leak queues every deploy.) And since an
    # exclusive queue disappears when we aren't connected, there's no way
    # to preserve every message across connects. That's okay -- disconnect
    # means one or two lost messages and nothing more.
    await connection.queue_declare(queue_name, exclusive=True, arguments=arguments)


async def _setup_subscriptions(
    *,
    connection: carehare.Connection,
    queue_name: str,
    groups_exchange: str,
    groups: Iterable[str],
) -> None:
    await gather_without_leaking(
        connection.queue_bind(
            queue_name=queue_name, exchange_name=groups_exchange, routing_key=group
        )
        for group in groups
    )


def _assert_channel_is_reply_channel(channel_name: str) -> None:
    if "!" not in channel_name:
        raise RuntimeError(
            """
***
You are trying to use "Normal Channels", as per
https://channels.readthedocs.io/en/1.x/concepts.html#channel-types.

channels_rabbitmq does not support Normal Channels. See
https://github.com/CJWorkbench/channels_rabbitmq/pull/11#issuecomment-499185070
for an explanation. You can build a pull request if you really want
this; but we heartily recommend you implement your work queue using
carehare directly. Your service will be simpler and it will scale better.
***"""
        )


def _random_letters(n: int) -> str:
    return "".join(random.choice(string.ascii_letters) for i in range(n))


class RabbitmqChannelLayer(BaseChannelLayer):
    """RabbitMQ channel layer.

    It routes all messages into a remote RabbitMQ server.

    Queue names look like "channels_asldkjfg"; Channel names look like
    "channels_asldkjfg!asdaSDGdlkgj". RabbitMQ only sees the queue name; the
    channel names are embedded in `message["__asgi_channel__"]`. Each
    Connection is responsible for a single queue and multiple channels on that
    queue. We call that queue a "channel_key".

    There are two "capacity" parameters: `remote_capacity` determines how many
    messages RabbitMQ will queue before a send raises `ChannelFull`.
    `local_capacity` determines the maximum number of messages to hold in
    memory in our ever-running pull-messages-from-RabbitMQ loop. If consumers
    are too slow then `local_capacity` messages will be buffered in memory; at
    that point Connection won't pull any more from RabbitMQ; so senders can
    send another `remote_capacity` messages to RabbitMQ and then they'll start
    raising `ChannelFull`.

    There is also an "expiry" parameter: this determines the minimum number of
    seconds a message must remain in RabbitMQ before being culled. A similar
    `local_expiry` determines the minimum amount of time a message must remain
    in the channels_rabbitmq buffers before it is acked (and potentially not
    delivered to all consumers).

    This layer does not implement "flush". To flush all state, simply
    disconnect all clients.
    """

    extensions = ["groups"]

    def __init__(
        self,
        host="amqp://guest:guest@127.0.0.1/asgi",
        local_capacity=100,
        remote_capacity=100,
        expiry=60,
        local_expiry: Optional[float] = None,
        group_expiry=None,
        ssl_context=None,
        groups_exchange="groups",
    ):
        self.host = host
        self.connect_timeout = 10.0  # seconds
        self.local_capacity = local_capacity
        self.remote_capacity = remote_capacity
        self.expiry = expiry
        self.local_expiry = local_expiry
        self.ssl_context = ssl_context
        self.groups_exchange = groups_exchange

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(
                """Refusing to initialize channel layer without a running event loop.

If you're writing a django-channels Worker, channels_rabbitmq does not support
this. Read the way to write workers in the carehare documentation.

If you're calling `async_to_sync()`, the call must be within code run by
`sync_to_async()`. Django Channels guarantees this for Django views. Elsewhere,
beware: `asgiref` *does* let you to call `async_to_sync()` without
`sync_to_async()`, but `channels_rabbitmq` *doesn't* -- hence this error."""
            )

        if group_expiry is not None:
            warnings.warn(
                (
                    "channels_rabbitmq does not support group_expiry. Please do not configure it. "
                    "For rationale, see "
                    "https://github.com/CJWorkbench/channels_rabbitmq/issues/18#issuecomment-547052373"
                ),
                category=DeprecationWarning,
            )

        # Choose queue name here: that way we can declare it on RabbitMQ with
        # exclusive=True and have it survive reconnections.
        self._queue_name = "channels_{rand}".format(rand=_random_letters(12))

        self._multi_queue = MultiQueue(capacity=local_capacity)
        self._carehare_connection: asyncio.Future[
            carehare.Connection
        ] = asyncio.Future()
        self._want_close: bool = False
        self._reconnect_delay: float = ReconnectDelay

    @property
    def carehare_connection(self) -> asyncio.Future[carehare.Connection]:
        if self._want_close:
            raise StopConsumer
        if self._carehare_connection.get_loop() != asyncio.get_running_loop():
            raise RuntimeError(
                "The caller tried using channels_rabbitmq on a different event loop than the one it was initialized with."
            )
        if not self._want_close and not hasattr(self, "_reconnect_forever_task"):
            self._reconnect_forever_task = asyncio.create_task(
                self._reconnect_forever()
            )
            if self.local_expiry is not None:
                self._expire_task = asyncio.create_task(
                    self._multi_queue.expire_locally_until_closed(
                        local_expiry=self.local_expiry
                    )
                )
        return self._carehare_connection

    async def _reconnect_forever(self):
        """Maintain self._carehare_connection.

        Cancel this task on event-loop shutdown (and at no other time). This
        will make `self.carehare_connection` raise `asyncio.CancelledError`.

        This task must outlive the failure of any single connection. If it ever
        returns or raises, the layer is dead for good: `carehare_connection`
        spawns it exactly once and nothing recreates it, so the process goes on
        accepting messages while delivering none of them.

        That is why everything but `CancelledError` is caught here, instead of an
        enumerated list of "expected" exceptions. `carehare`'s exceptions all
        derive straight from `Exception` with no shared base -- so such a list
        silently drifts out of sync with the library -- and
        `setup_connection()`, `setup_subscriptions()` and `start_consume()` are
        subclass extension points that may raise anything at all. Whatever comes
        out, the meaning is the same: this connection is gone. Log it, reset
        state, reconnect.
        """
        while not self._want_close:
            connection = None
            try:
                connection = await self._connect_and_setup()
                if connection is None:
                    break  # close() was called while we were retrying

                if self._want_close:
                    # close() landed while we were connecting. Do not start
                    # consuming: nothing would ever interrupt it, and close() is
                    # already awaiting this task. `finally` closes `connection`.
                    break

                self._carehare_connection.set_result(connection)
                self._reconnect_delay = ReconnectDelay  # a good connection resets it
                logger.info("Connected to RabbitMQ")
                await self.start_consume(connection=connection)
            except asyncio.CancelledError:
                raise  # event-loop shutdown: the one way out of this loop
            except Exception:
                if self._want_close:
                    logger.debug("RabbitMQ connection ended during close()")
                else:
                    logger.exception("Disconnected from RabbitMQ. Will reconnect.")
            finally:
                # Unconditional, and in `finally` rather than after the `except`:
                # a Future left resolved with a dead connection makes `send()`
                # publish into the void and makes health checks report a healthy
                # layer. A pending Future stalls senders until we reconnect,
                # which is what they already expect.
                self._carehare_connection = asyncio.Future()
                if connection is not None:
                    with contextlib.suppress(Exception):
                        # `close()` awaits `connection.closed`, so it re-raises
                        # whatever killed the connection. Never let cleanup end
                        # the loop we just hardened.
                        await connection.close()

            await self._sleep_before_retry()

    async def _connect_and_setup(self) -> Optional[carehare.Connection]:
        """Connect to RabbitMQ and declare our objects, retrying until it works.

        Returns None if `close()` was called while we were still retrying.
        """
        while not self._want_close:
            connection = None
            try:
                connection = carehare.Connection(
                    url=self.host,
                    connect_timeout=self.connect_timeout,
                    ssl=self.ssl_context,
                )
                # carehare's own connect_timeout only bounds the TCP/TLS connect,
                # not the AMQP handshake after it: a broker that accepts the
                # socket and then stalls would hang us forever.
                await asyncio.wait_for(
                    connection.connect(), timeout=self.connect_timeout
                )
                await self.setup_connection(connection=connection)
                await self.setup_subscriptions(connection=connection)
                return connection
            except asyncio.CancelledError:
                raise
            except Exception as err:
                logger.exception(
                    "Failure connecting to RabbitMQ: %s. Retrying", str(err)
                )
                if connection is not None:
                    with contextlib.suppress(Exception):
                        await connection.close()
                await self._sleep_before_retry()

        return None

    async def _sleep_before_retry(self) -> None:
        """Back off before the next connection attempt.

        The delay grows exponentially up to `MaxReconnectDelay` and resets once a
        connection succeeds. Without this, every instance in the fleet retries in
        lockstep once a second, hammering a broker that is typically mid-recovery
        (electing quorum-queue leaders) and least able to take it -- while
        emitting a stack trace per attempt per process.

        Jitter only ever *adds* to the delay, so the first retry still waits at
        least `ReconnectDelay`, as callers of `send()` rely on.

        Sleeps in short slices so `close()` never waits a full backoff period.
        """
        if self._want_close:
            return

        delay = self._reconnect_delay * (1.0 + random.random() * 0.5)
        self._reconnect_delay = min(self._reconnect_delay * 2, MaxReconnectDelay)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + delay
        while not self._want_close:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return
            await asyncio.sleep(min(remaining, 1.0))

    async def setup_connection(self, connection: carehare.Connection) -> None:
        await _setup_connection(
            connection=connection,
            queue_name=self._queue_name,
            remote_capacity=self.remote_capacity,
            expiry=self.expiry,
            groups_exchange=self.groups_exchange,
        )

    async def setup_subscriptions(self, connection: carehare.Connection) -> None:
        await _setup_subscriptions(
            connection=connection,
            queue_name=self._queue_name,
            groups_exchange=self.groups_exchange,
            groups=self._multi_queue._local_groups.keys(),
        )

    async def start_consume(self, connection: carehare.Connection) -> None:
        await consume_into_multi_queue_until_connection_close(
            connection=connection,
            channel=self._queue_name,
            multi_queue=self._multi_queue,
            prefetch_count=self.local_capacity,
        )

    async def close(self):
        """Close our RabbitMQ connection and all tasks.

        This is not part of the Channel Layer Specification; but it should be a
        part of every server's lifecycle.

        After calling this, no more messages will be delivered. All pending and
        future calls to `receive()` will raise `StopConsumer` (causing all
        WebSockets clients to disconnect). Undelivered messages will be lost.

        After this method returns, the RabbitMQ connection will be closed and
        all internal tasks will be completed.
        """

        if not self._want_close:
            self._want_close = True
            self._multi_queue.close()

            if hasattr(self, "_expire_task"):
                self._expire_task.cancel()
            if (
                hasattr(self, "_reconnect_forever_task")
                and self._carehare_connection.done()
            ):
                await self._carehare_connection.result().close()

        if hasattr(self, "_expire_task"):
            try:
                await self._expire_task
            except asyncio.CancelledError:
                pass
            if hasattr(self, "_expire_task"):
                delattr(self, "_expire_task")

        if hasattr(self, "_reconnect_forever_task"):
            await self._reconnect_forever_task
            if hasattr(self, "_reconnect_forever_task"):
                delattr(self, "_reconnect_forever_task")

    async def send(self, channel, message):
        """Send a message onto a (general or specific) channel."""
        assert self.valid_channel_name(channel), "Channel name not valid"
        _assert_channel_is_reply_channel(channel)

        logger.debug("send %r to %s", message, channel)

        body = serialize_message(ChannelRecipient(channel), message)

        connection = await self.carehare_connection

        try:
            await connection.publish(
                body, exchange_name="", routing_key=channel.split("!")[0]
            )
        except carehare.ServerSentNack:
            raise ChannelFull from None
        except carehare.ConnectionClosed:
            logger.warning(
                "Failed to send message %r to %s because we disconnected",
                message,
                channel,
            )

    async def receive(self, channel):
        """Receive the first message that arrives on the channel.

        If more than one coroutine waits on the same channel, only one waiter
        will receive the message when it arrives.
        """
        # Make sure the channel name is valid then get the non-local part
        # and thus its index
        assert self.valid_channel_name(channel)
        _assert_channel_is_reply_channel(channel)
        assert channel.startswith(self._queue_name + "!")
        return await self._multi_queue.get(channel)

    async def new_channel(self, prefix=""):
        """Create a new channel name that can be used by something in our
        process as a specific channel.
        """
        return "%s!%s" % (self._queue_name, _random_letters(12))

    async def group_add(self, group, channel):
        """Add the channel name to a group.

        Spec deviation: `channel` must have been created on the same event loop
        as the RabbitMQ connection. In other words: you can't subscribe someone
        else's channel to a group.
        """
        assert self.valid_group_name(group), "Group name not valid"
        assert self.valid_channel_name(channel), "Channel name not valid"

        # Connect, to make sure the next line doesn't happen during _setup_subscriptions()
        connection = await self.carehare_connection

        # Now, assume _setup_subscriptions() isn't running, so there's no race
        # writing+reading self._multi_queue.
        n_bindings = self._multi_queue.group_add(group, channel)
        if n_bindings == 1:
            logger.debug("Binding queue %s to group %s", self._queue_name, group)
            # This group is new to our connection-level queue. Make a
            # connection-level binding.

            try:
                await connection.queue_bind(
                    queue_name=self._queue_name,
                    exchange_name=self.groups_exchange,
                    routing_key=group,
                )
            except carehare.ChannelClosedByServer as err:
                logger.exception(
                    "RabbitMQ refused to bind: %s. Messages to group %s will not reach clients on this server.",
                    str(err),
                    group,
                )
            except carehare.ConnectionClosed:
                pass  # we'll queue_bind() during reconnect

    async def group_discard(self, group, channel):
        """Remove the channel from the named group if it is in the group;
        does nothing otherwise (does not error)

        Spec deviation: `channel` must have been created on the same event loop
        as the RabbitMQ connection. In other words: you can't subscribe someone
        else's channel to a group.
        """
        assert self.valid_group_name(group), "Group name not valid"
        assert self.valid_channel_name(channel), "Channel name not valid"

        # Connect, to make sure the next line doesn't happen during _setup_subscriptions()
        connection = await self.carehare_connection

        # Now, assume _setup_subscriptions() isn't running, so there's no race
        # writing+reading self._multi_queue.
        n_bindings = self._multi_queue.group_discard(group, channel)
        if n_bindings == 0:
            logger.debug("Unbinding queue %s from group %s", self._queue_name, group)
            try:
                await connection.queue_unbind(
                    queue_name=self._queue_name,
                    exchange_name=self.groups_exchange,
                    routing_key=group,
                )
            except carehare.ChannelClosedByServer as err:
                logger.exception(
                    "RabbitMQ refused to unbind from group %s: %s. This suggests a bug somewhere.",
                    str(err),
                    group,
                )
            except carehare.ConnectionClosed:
                pass  # after reconnect, we're unbound anyway

    async def group_send(self, group, message):
        """Send a message to the entire group."""
        assert self.valid_group_name(group), "Group name not valid"

        logger.debug("group_send %r to %s", message, group)

        body = serialize_message(GroupRecipient(group), message)

        connection = await self.carehare_connection

        try:
            await connection.publish(
                body, exchange_name=self.groups_exchange, routing_key=group
            )
        except carehare.ServerSentNack:
            # "Sending to a group never raises ChannelFull; instead, it must
            # silently drop the message if it is over capacity, as per ASGI’s
            # at-most-once delivery policy."
            # https://channels.readthedocs.io/en/stable/channel_layer_spec.html#capacity
            #
            # ... let's at least _warn_....
            logger.warning("Aborting send to group %s: a queue is at capacity", group)
        except carehare.ConnectionClosed:
            logger.warning(
                "Failed to send message %r to group %s because we disconnected",
                message,
                group,
            )
