# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""Minimal live OPC UA subscription check for the aggregation server."""

from __future__ import annotations

import asyncio
from datetime import datetime
import sys

from asyncua import Client


URL = sys.argv[1] if len(sys.argv) > 1 else "opc.tcp://127.0.0.1:48400/"
NODE_ID = sys.argv[2] if len(sys.argv) > 2 else "ns=15;i=6085"


def stamp() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


class Handler:
    def __init__(self) -> None:
        self.data_changes = 0
        self.status_changes = 0

    def datachange_notification(self, node, value, data) -> None:
        self.data_changes += 1
        print(stamp(), "DATA_CHANGE", node.nodeid.to_string(), repr(value), flush=True)

    def status_change_notification(self, status) -> None:
        self.status_changes += 1
        print(stamp(), "STATUS_CHANGE", status, flush=True)


async def main() -> int:
    print(stamp(), "TEST_START", URL, NODE_ID, flush=True)
    client = Client(URL, timeout=15)
    client.session_timeout = 60_000
    handler = Handler()
    subscription = None
    handle = None

    try:
        await client.connect()
        print(stamp(), "CONNECTED", flush=True)
        node = client.get_node(NODE_ID)
        value = await node.read_value()
        print(stamp(), "INITIAL_VALUE", repr(value), flush=True)

        subscription = await client.create_subscription(500, handler)
        print(
            stamp(),
            "SUBSCRIPTION_CREATED",
            subscription.subscription_id,
            flush=True,
        )
        handle = await subscription.subscribe_data_change(node)
        print(stamp(), "MONITORED_ITEM_CREATED", handle, flush=True)

        for second in range(1, 9):
            await asyncio.sleep(1)
            print(
                stamp(),
                "WAIT",
                second,
                "data_changes=",
                handler.data_changes,
                "status_changes=",
                handler.status_changes,
                flush=True,
            )

        passed = handler.data_changes > 0 and handler.status_changes == 0
        print(stamp(), "RESULT", "PASS" if passed else "FAIL", flush=True)
        return 0 if passed else 1
    except Exception as exc:
        print(stamp(), "EXCEPTION", type(exc).__name__, repr(exc), flush=True)
        return 2
    finally:
        if subscription is not None and handle is not None:
            try:
                await subscription.unsubscribe(handle)
                print(stamp(), "UNSUBSCRIBED", flush=True)
            except Exception as exc:
                print(stamp(), "UNSUBSCRIBE_FAILED", repr(exc), flush=True)
        if subscription is not None:
            try:
                await subscription.delete()
                print(stamp(), "SUBSCRIPTION_DELETED", flush=True)
            except Exception as exc:
                print(stamp(), "DELETE_FAILED", repr(exc), flush=True)
        try:
            await client.disconnect()
            print(stamp(), "DISCONNECTED", flush=True)
        except Exception as exc:
            print(stamp(), "DISCONNECT_FAILED", repr(exc), flush=True)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
