#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""Exercise aggregated subscriptions using an isolated, writable loopback source.

Build: cargo build --manifest-path opcua_agg_server/Cargo.toml --example subscription_fixture
Run:   python scripts/test-aggregation-subscriptions.py --aggregator-exe opcua_agg_server/target/debug/examples/subscription_fixture.exe

Requires asyncua (the project's requirements.txt includes it).
--extended-services opts into a synthetic adapter for missing asyncua source
services; verify it first with --source-only --extended-services. --source-only
checks the Python harness directly against its own source. A manually started
fixture can be tested with --source-port PORT --aggregator-url URL; optional
--node-a/--node-b override browsing. Only this run's unique namespace is writable.
The harness never discovers or registers with a plant discovery server.
"""

from __future__ import annotations

import argparse
import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
import ipaddress
import logging
from pathlib import Path
import socket
import subprocess
import tempfile
import time
from urllib.parse import urlsplit
import uuid

from asyncua import Client, Server, ua


def install_synthetic_source_services(source):
    """Fill three explicitly unimplemented asyncua server operations for tests.

    asyncua 1.1.8 and 2.0.1 ignore ClientHandle modifications and return dummy
    Good statuses for SetMonitoringMode/SetPublishingMode. This adapter changes
    only our synthetic in-process source, never installed files or the Rust
    server. It is opt-in with --extended-services, and --source-only exercises
    its behavior directly before using it to test aggregation delegation.
    """
    from asyncua.server.internal_subscription import InternalSubscription
    from asyncua.server.monitored_item_service import MonitoredItemService
    from asyncua.server.uaprocessor import UaProcessor
    from asyncua.ua.ua_binary import struct_from_binary

    source.iserver._ojies_fixture = True
    source.iserver.aspace._ojies_fixture = True
    if getattr(UaProcessor, "_ojies_fixture_adapter_installed", False):
        return
    UaProcessor._ojies_fixture_adapter_installed = True
    original_modify = MonitoredItemService._modify_monitored_item
    original_datachange = MonitoredItemService.datachange_callback
    original_process = UaProcessor._process_message
    original_pop_data = InternalSubscription._pop_triggered_datachanges
    original_pop_events = InternalSubscription._pop_triggered_events

    def modify(service, parameters):
        result = original_modify(service, parameters)
        if getattr(service.aspace, "_ojies_fixture", False) and result.StatusCode.is_good():
            service._monitored_items[parameters.MonitoredItemId].client_handle = parameters.RequestedParameters.ClientHandle
        return result

    async def datachange(service, handle, value, error=None):
        if getattr(service.aspace, "_ojies_fixture", False) and not error:
            item = service._monitored_items[service._monitored_datachange[handle]]
            if item.mode == ua.MonitoringMode.Disabled:
                return
        await original_datachange(service, handle, value, error)

    async def process(processor, typeid, requesthdr, seqhdr, body):
        if not getattr(processor.iserver, "_ojies_fixture", False):
            return await original_process(processor, typeid, requesthdr, seqhdr, body)
        subscriptions = processor.iserver.subscription_service.subscriptions
        if typeid == ua.NodeId(ua.ObjectIds.SetMonitoringModeRequest_Encoding_DefaultBinary):
            parameters = struct_from_binary(ua.SetMonitoringModeParameters, body)
            subscription = subscriptions.get(parameters.SubscriptionId)
            statuses = []
            for item_id in parameters.MonitoredItemIds:
                item = subscription.monitored_item_srv._monitored_items.get(item_id) if subscription else None
                if item is None:
                    statuses.append(ua.StatusCode(ua.StatusCodes.BadMonitoredItemIdInvalid))
                    continue
                item.mode = parameters.MonitoringMode
                if item.mode == ua.MonitoringMode.Disabled:
                    subscription._triggered_datachanges.pop(item_id, None)
                    item.mvalue = type(item.mvalue)()
                statuses.append(ua.StatusCode())
            response = ua.SetMonitoringModeResponse()
            response.Parameters.Results = statuses
        elif typeid == ua.NodeId(ua.ObjectIds.SetPublishingModeRequest_Encoding_DefaultBinary):
            parameters = struct_from_binary(ua.SetPublishingModeParameters, body)
            statuses = []
            for subscription_id in parameters.SubscriptionIds:
                subscription = subscriptions.get(subscription_id)
                if subscription is None:
                    statuses.append(ua.StatusCode(ua.StatusCodes.BadSubscriptionIdInvalid))
                    continue
                subscription._ojies_publishing_enabled = parameters.PublishingEnabled
                statuses.append(ua.StatusCode())
            response = ua.SetPublishingModeResponse()
            response.Parameters.Results = statuses
        else:
            return await original_process(processor, typeid, requesthdr, seqhdr, body)
        processor.send_response(requesthdr.RequestHandle, seqhdr, response)
        return True

    def pop_data(subscription, result):
        if getattr(subscription, "_ojies_publishing_enabled", True):
            original_pop_data(subscription, result)

    def pop_events(subscription, result):
        if getattr(subscription, "_ojies_publishing_enabled", True):
            original_pop_events(subscription, result)

    MonitoredItemService._modify_monitored_item = modify
    MonitoredItemService.datachange_callback = datachange
    UaProcessor._process_message = process
    InternalSubscription._pop_triggered_datachanges = pop_data
    InternalSubscription._pop_triggered_events = pop_events


async def disconnect_client(client):
    cleanup = asyncio.create_task(client.disconnect())
    done, _ = await asyncio.wait([cleanup], timeout=3)
    if not done:
        client.disconnect_socket()
        cleanup.cancel()
        return
    try:
        cleanup.result()
    except Exception:
        client.disconnect_socket()


@asynccontextmanager
async def connected_client(url, timeout):
    client = Client(url, timeout=timeout)
    await client.connect()
    try:
        yield client
    finally:
        await disconnect_client(client)


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def loopback_url(value: str) -> str:
    parsed = urlsplit(value)
    try:
        is_loopback = ipaddress.ip_address(parsed.hostname or "").is_loopback
    except ValueError:
        is_loopback = parsed.hostname == "localhost"
    if parsed.scheme != "opc.tcp" or not is_loopback or parsed.port is None:
        raise argparse.ArgumentTypeError("an explicit loopback opc.tcp:// URL is required")
    return value


class Notifications:
    def __init__(self, name: str, handles: dict[int, str]):
        self.name = name
        self.handles = handles
        self.seen: set[tuple[int, object]] = set()
        self.errors: list[str] = []
        self.changed = asyncio.Event()
        self.count = 0

    def datachange_notification(self, node, value, data):
        item = data.monitored_item
        handle = item.ClientHandle
        self.count += 1
        if self.handles.get(handle) != node.nodeid.to_string():
            self.errors.append(f"unexpected handle/node pair {handle}: {node.nodeid}")
        if not item.Value.StatusCode.is_good():
            self.errors.append(f"bad notification status: {item.Value.StatusCode}")
        self.seen.add((handle, value))
        self.changed.set()

    def status_change_notification(self, status):
        self.errors.append(f"subscription status changed: {status}")
        self.changed.set()

    async def expect(self, expected: dict[int, object], timeout: float):
        deadline = time.monotonic() + timeout
        while True:
            self.changed.clear()
            if self.errors:
                raise AssertionError(f"{self.name}: {self.errors}")
            missing = {key: value for key, value in expected.items() if (key, value) not in self.seen}
            if not missing:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"{self.name}: missing notifications {missing}; received {self.count}")
            try:
                await asyncio.wait_for(self.changed.wait(), remaining)
            except asyncio.TimeoutError as error:
                raise AssertionError(
                    f"{self.name}: missing notifications {missing}; received {self.count}"
                ) from error


    async def expect_handle(self, handle: int, timeout: float):
        async def wait_for_handle():
            while not any(received == handle for received, _ in self.seen):
                self.changed.clear()
                await self.changed.wait()
        try:
            await asyncio.wait_for(wait_for_handle(), timeout)
        except asyncio.TimeoutError as error:
            raise AssertionError(f"{self.name}: no notification for client handle {handle}") from error


async def subscribe(client, name: str, nodes, handles, interval: float):
    observer = Notifications(name, {handle: node.nodeid.to_string() for handle, node in zip(handles, nodes)})
    subscription = await client.create_subscription(interval, observer)
    requests = []
    for node, handle in zip(nodes, handles):
        request = ua.MonitoredItemCreateRequest()
        request.ItemToMonitor.NodeId = node.nodeid
        request.ItemToMonitor.AttributeId = ua.AttributeIds.Value
        request.MonitoringMode = ua.MonitoringMode.Reporting
        request.RequestedParameters.ClientHandle = handle
        request.RequestedParameters.SamplingInterval = interval
        request.RequestedParameters.QueueSize = 100
        request.RequestedParameters.DiscardOldest = True
        requests.append(request)
    results = await subscription.create_monitored_items(requests)
    assert len(results) == len(nodes), f"{name}: CreateMonitoredItems result count changed"
    for result in results:
        if isinstance(result, ua.StatusCode):
            result.check()
    assert len(set(results)) == len(results), f"{name}: duplicate MonitoredItemIds {results}"
    return subscription, observer, results


async def modify_mixed_subscription(client, subscription, observer, server_ids, old_handles, interval):
    # asyncua's public convenience API retains ClientHandle. Use the protocol
    # request here to verify that delegated modifications can replace it too.
    new_handles = [72003, 72009, 72021]
    parameters = ua.ModifyMonitoredItemsParameters()
    parameters.SubscriptionId = subscription.subscription_id
    parameters.TimestampsToReturn = ua.TimestampsToReturn.Both
    for server_id, old_handle, new_handle in zip(server_ids, old_handles, new_handles):
        request = ua.MonitoredItemModifyRequest()
        request.MonitoredItemId = server_id
        request.RequestedParameters.ClientHandle = new_handle
        request.RequestedParameters.SamplingInterval = interval * 2
        request.RequestedParameters.QueueSize = 100
        request.RequestedParameters.DiscardOldest = True
        parameters.ItemsToModify.append(request)
        # Register new handles before sending: an initial callback can arrive
        # before ModifyMonitoredItemsResponse, just as during item creation.
        subscription._monitored_items[new_handle] = subscription._monitored_items[old_handle]
        observer.handles[new_handle] = observer.handles[old_handle]
    results = await client.uaclient.modify_monitored_items(parameters)
    assert len(results) == len(server_ids), "mixed ModifyMonitoredItems result count changed"
    for result in results:
        result.StatusCode.check()
    for old_handle, new_handle in zip(old_handles, new_handles):
        item = subscription._monitored_items.pop(old_handle)
        item.client_handle = new_handle
        observer.handles.pop(old_handle)
    return new_handles


async def find_counters(client, namespace: str, explicit_ids):
    namespaces = await client.get_namespace_array()

    def belongs(node_id):
        return node_id.NamespaceIndex < len(namespaces) and namespaces[node_id.NamespaceIndex].startswith(namespace)

    if all(explicit_ids):
        counters = [client.get_node(value) for value in explicit_ids]
        assert all(belongs(node.nodeid) for node in counters), "explicit nodes must belong to this run's source namespace"
        return counters
    found = {}
    pending = [(client.nodes.objects, 0)]
    seen = set()
    while pending:
        node, depth = pending.pop(0)
        node_id = node.nodeid.to_string()
        if node_id in seen or depth > 8:
            continue
        seen.add(node_id)
        assert len(seen) <= 2000, "fixture browse exceeded 2000 nodes"
        for ref in await node.get_children_descriptions():
            child = client.get_node(ref.NodeId)
            name = ref.BrowseName.Name
            if name in ("TestCounterA", "TestCounterB") and belongs(child.nodeid):
                assert name not in found or found[name].nodeid == child.nodeid, f"ambiguous fixture counter {name}"
                found[name] = child
            if ref.NodeClass == ua.NodeClass.Object and not (ref.BrowseName.NamespaceIndex == 0 and name == "Server"):
                pending.append((child, depth + 1))
        if len(found) == 2:
            return [found["TestCounterA"], found["TestCounterB"]]
    raise AssertionError(f"this run's fixture counters were not aggregated; found {list(found)}")


async def run_checks(args, source_url, aggregation_url, namespace, source_nodes, source):
    async with AsyncExitStack() as stack:
        direct = await stack.enter_async_context(connected_client(source_url, args.timeout))
        upper = await stack.enter_async_context(connected_client(aggregation_url, args.timeout))
        counters = await find_counters(upper, namespace, (args.node_a, args.node_b))
        local = upper.get_node(ua.ObjectIds.Server_ServerStatus_CurrentTime)
        direct_nodes = [direct.get_node(node.nodeid) for node in source_nodes]
        subscriptions = []
        try:
            direct_sub, direct_observer, _ = await subscribe(direct, "direct source", direct_nodes, [91001, 91002], args.interval)
            subscriptions.append(direct_sub)
            remote_sub, remote_observer, _ = await subscribe(upper, "remote-only", counters, [41001, 41007], args.interval)
            subscriptions.append(remote_sub)
            mixed_sub, mixed_observer, mixed_ids = await subscribe(
                upper, "mixed remote/local/remote", [counters[0], local, counters[1]], [71003, 71009, 71021], args.interval
            )
            subscriptions.append(mixed_sub)

            mixed_remote_handles = [71003, 71021]

            async def expect_values(a, b, extra=None):
                observers = [direct_observer.expect({91001: a, 91002: b}, args.timeout),
                             remote_observer.expect({41001: a, 41007: b}, args.timeout),
                             mixed_observer.expect({mixed_remote_handles[0]: a, mixed_remote_handles[1]: b}, args.timeout)]
                if extra:
                    observers.append(extra.expect({81001: a, 81013: b}, args.timeout))
                await asyncio.gather(*observers)

            await expect_values(100, 200)
            await mixed_observer.expect_handle(71009, args.timeout)
            print("PASS initial notifications and explicit client handles", flush=True)

            for iteration in range(args.iterations):
                a, b = 1000 + iteration, 2000 + iteration
                churn = None
                if iteration % 3 == 0:
                    # Issue a mixed CreateMonitoredItems while lower notifications
                    # are active; this previously exposed lock-order deadlocks.
                    writes = asyncio.gather(source_nodes[0].write_value(a, ua.VariantType.Int64),
                                            source_nodes[1].write_value(b, ua.VariantType.Int64))
                    churn = await subscribe(upper, f"churn {iteration}", [local, counters[0], counters[1]],
                                            [81007, 81001, 81013], args.interval)
                    subscriptions.append(churn[0])
                    await writes
                else:
                    await source_nodes[0].write_value(a, ua.VariantType.Int64)
                    await source_nodes[1].write_value(b, ua.VariantType.Int64)
                await expect_values(a, b, churn[1] if churn else None)
                values = await upper.read_values(counters)
                assert values == [a, b], f"read routing/order mismatch: {values}"
                if churn:
                    await churn[0].delete()
                    subscriptions.remove(churn[0])
                if (iteration + 1) % 5 == 0:
                    print(f"PASS {iteration + 1}/{args.iterations} updates with concurrent subscription churn", flush=True)

            if args.extended_services:
                new_handles = await modify_mixed_subscription(
                    upper, mixed_sub, mixed_observer, mixed_ids, [71003, 71009, 71021], args.interval
                )
                mixed_remote_handles[:] = [new_handles[0], new_handles[2]]
                await source_nodes[0].write_value(30001, ua.VariantType.Int64)
                await source_nodes[1].write_value(30002, ua.VariantType.Int64)
                await expect_values(30001, 30002)
                print("PASS mixed remote/local modifications and changed ClientHandle routing", flush=True)

                for status in await mixed_sub.set_monitoring_mode(ua.MonitoringMode.Disabled):
                    status.check()
                await source_nodes[0].write_value(31001, ua.VariantType.Int64)
                await source_nodes[1].write_value(31002, ua.VariantType.Int64)
                await direct_observer.expect({91001: 31001, 91002: 31002}, args.timeout)
                await remote_observer.expect({41001: 31001, 41007: 31002}, args.timeout)
                await asyncio.sleep(max(0.5, args.interval * 6 / 1000))
                assert (new_handles[0], 31001) not in mixed_observer.seen, "disabled remote item still reports changes"
                assert (new_handles[2], 31002) not in mixed_observer.seen, "disabled remote item still reports changes"
                for status in await mixed_sub.set_monitoring_mode(ua.MonitoringMode.Reporting):
                    status.check()
                await source_nodes[0].write_value(32001, ua.VariantType.Int64)
                await source_nodes[1].write_value(32002, ua.VariantType.Int64)
                await expect_values(32001, 32002)
                print("PASS mixed SetMonitoringMode disable/resume", flush=True)

                for status in await mixed_sub.set_publishing_mode(False):
                    status.check()
                await source_nodes[0].write_value(33001, ua.VariantType.Int64)
                await source_nodes[1].write_value(33002, ua.VariantType.Int64)
                await direct_observer.expect({91001: 33001, 91002: 33002}, args.timeout)
                await remote_observer.expect({41001: 33001, 41007: 33002}, args.timeout)
                await asyncio.sleep(max(0.5, args.interval * 6 / 1000))
                assert (new_handles[0], 33001) not in mixed_observer.seen, "publishing-disabled subscription still delivers values"
                assert (new_handles[2], 33002) not in mixed_observer.seen, "publishing-disabled subscription still delivers values"
                for status in await mixed_sub.set_publishing_mode(True):
                    status.check()
                await expect_values(33001, 33002)
                print("PASS SetPublishingMode retains updates across pause/resume", flush=True)

            # Modifying the local item must preserve correct monitored-item IDs
            # for adjacent delegated items in the original mixed create batch.
            results = await mixed_sub.modify_monitored_item(mixed_ids[1], args.interval * 2, 100)
            for result in results:
                result.StatusCode.check()
            await mixed_sub.unsubscribe(mixed_ids[1])
            await source_nodes[0].write_value(50001, ua.VariantType.Int64)
            await source_nodes[1].write_value(50002, ua.VariantType.Int64)
            await expect_values(50001, 50002)
            print("PASS modify/delete local item preserves remote subscriptions", flush=True)

            await counters[0].write_value(60001, ua.VariantType.Int64)
            await counters[1].write_value(60002, ua.VariantType.Int64)
            assert await direct.read_values(direct_nodes) == [60001, 60002], "write-through did not reach source"
            await expect_values(60001, 60002)
            print("PASS write-through and notification fan-out", flush=True)

            if args.restart_source:
                # Keep the upper session and its subscription/item handles alive.
                # asyncua.stop closes TCP transports AND their sessions/subscriptions;
                # start reuses our address space, endpoint and namespace identities.
                await direct_sub.delete()
                subscriptions.remove(direct_sub)
                await disconnect_client(direct)
                print("RESTART stopping source and dropping lower sessions", flush=True)
                before_source_ids = set(source.iserver.subscription_service.subscriptions)
                assert before_source_ids, "source had no subscriptions before restart"
                # Close the listener first: asyncua's stop() normally leaves it
                # accepting reconnects while it waits for old transports to drain.
                source.bserver._server.close()
                # Python 3.13 wait_closed() also waits for active transports;
                # stop() must close those before we await the listener.
                await asyncio.wait_for(source.stop(), 8)
                # asyncua 2.x preserves external sessions across TCP shutdown,
                # even when Server.stop() is called. A real process restart
                # loses them, so explicitly close that registry before restart.
                retained_sessions = list(getattr(source.iserver, "_external_sessions", {}).values())
                for retained_session in retained_sessions:
                    await asyncio.wait_for(retained_session.close_session(delete_subs=True), 3)
                assert not source.iserver.subscription_service.subscriptions, "source restart did not remove old sessions/subscriptions"
                await asyncio.sleep(0.2)
                await source.start()
                print("RESTART source listening; waiting for existing upper subscriptions", flush=True)
                await source_nodes[0].write_value(70001, ua.VariantType.Int64)
                await source_nodes[1].write_value(70002, ua.VariantType.Int64)
                await asyncio.gather(
                    remote_observer.expect({41001: 70001, 41007: 70002}, args.reconnect_timeout),
                    mixed_observer.expect({mixed_remote_handles[0]: 70001, mixed_remote_handles[1]: 70002}, args.reconnect_timeout),
                )
                after_source_ids = set(source.iserver.subscription_service.subscriptions)
                assert after_source_ids and not before_source_ids.intersection(after_source_ids), "source subscription IDs did not change"

                # Exercise the original upper IDs against newly assigned lower IDs.
                for result in await mixed_sub.modify_monitored_item(mixed_ids[0], args.interval * 3, 100):
                    result.StatusCode.check()
                request = ua.MonitoredItemCreateRequest()
                request.ItemToMonitor.NodeId = counters[0].nodeid
                request.ItemToMonitor.AttributeId = ua.AttributeIds.Value
                request.MonitoringMode = ua.MonitoringMode.Reporting
                request.RequestedParameters.ClientHandle = 73001
                request.RequestedParameters.SamplingInterval = args.interval
                request.RequestedParameters.QueueSize = 100
                request.RequestedParameters.DiscardOldest = True
                mixed_observer.handles[73001] = counters[0].nodeid.to_string()
                extra_ids = await mixed_sub.create_monitored_items([request])
                if isinstance(extra_ids[0], ua.StatusCode):
                    extra_ids[0].check()
                await source_nodes[0].write_value(71001, ua.VariantType.Int64)
                await source_nodes[1].write_value(71002, ua.VariantType.Int64)
                await mixed_observer.expect({mixed_remote_handles[0]: 71001, mixed_remote_handles[1]: 71002, 73001: 71001}, args.timeout)
                await mixed_sub.unsubscribe([extra_ids[0], mixed_ids[2]])
                await counters[0].write_value(72001, ua.VariantType.Int64)
                await counters[1].write_value(72002, ua.VariantType.Int64)
                await remote_observer.expect({41001: 72001, 41007: 72002}, args.timeout)
                await mixed_observer.expect({mixed_remote_handles[0]: 72001}, args.timeout)
                await asyncio.sleep(max(0.5, args.interval * 6 / 1000))
                assert (mixed_remote_handles[1], 72002) not in mixed_observer.seen, "old upper item ID deletion did not reach recreated source item"
                assert await source_nodes[0].read_value() == 72001
                assert await source_nodes[1].read_value() == 72002
                print("PASS source session loss/restart, unchanged upper IDs, post-reconnect create/modify/delete/write", flush=True)
        except BaseException as error:
            print(f"FAIL regression: {type(error).__name__}: {error}", flush=True)
            raise
        finally:
            for subscription in reversed(subscriptions):
                try:
                    await asyncio.wait_for(subscription.delete(), 2)
                except Exception:
                    logging.warning("subscription cleanup failed", exc_info=True)


async def main(args):
    source_port = args.source_port or free_port()
    aggregation_port = free_port()
    while aggregation_port == source_port:
        aggregation_port = free_port()
    source_url = f"opc.tcp://127.0.0.1:{source_port}/"
    namespace = f"urn:ojies:subscription-regression:{uuid.uuid4().hex}"
    source = Server()
    await source.init()
    source.set_endpoint(source_url)
    source.set_server_name("Isolated Subscription Regression Source")
    await source.set_application_uri(namespace)
    source.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    if args.extended_services:
        install_synthetic_source_services(source)
        print("SYNTHETIC_SOURCE_ADAPTER ClientHandle, monitoring mode, publishing mode", flush=True)
    ns = await source.register_namespace(namespace)
    folder = await source.nodes.objects.add_object(ua.NodeId("RegressionSource", ns), "RegressionSource")
    counters = []
    for name, value in (("TestCounterA", 100), ("TestCounterB", 200)):
        node = await folder.add_variable(ua.NodeId(name, ns), ua.QualifiedName(name, ns), value, ua.VariantType.Int64)
        await node.set_writable()
        counters.append(node)

    process = None
    with tempfile.TemporaryDirectory(prefix="ojies-subscriptions-") as working_dir:
        log_path = Path(working_dir) / "fixture.log"
        async with source:
            print(f"SOURCE_READY {source_url} namespace={namespace}", flush=True)
            try:
                if args.aggregator_exe:
                    executable = args.aggregator_exe.resolve(strict=True)
                    with log_path.open("w", encoding="utf-8") as log:
                        process = subprocess.Popen([str(executable), str(source_port), str(aggregation_port)],
                                                   cwd=working_dir, stdout=log, stderr=subprocess.STDOUT,
                                                   creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
                    deadline = time.monotonic() + args.startup_timeout
                    while "FIXTURE_READY" not in log_path.read_text(encoding="utf-8", errors="replace"):
                        if process.poll() is not None:
                            raise AssertionError(f"aggregation fixture exited with {process.returncode}")
                        if time.monotonic() > deadline:
                            raise AssertionError("aggregation fixture startup timed out")
                        await asyncio.sleep(0.1)
                    aggregation_url = f"opc.tcp://127.0.0.1:{aggregation_port}/"
                elif args.source_only:
                    aggregation_url = source_url
                else:
                    aggregation_url = args.aggregator_url
                    await asyncio.sleep(args.startup_wait)
                print(f"AGGREGATOR {aggregation_url}", flush=True)
                await run_checks(args, source_url, aggregation_url, namespace, counters, source)
            except BaseException:
                if log_path.exists():
                    print("Fixture log (tail):\n" + log_path.read_text(encoding="utf-8", errors="replace")[-12000:], flush=True)
                raise
            finally:
                if process and process.poll() is None:
                    process.terminate()
                    try:
                        await asyncio.to_thread(process.wait, 5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        await asyncio.to_thread(process.wait)
    print("PASS all isolated subscription regression checks", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--aggregator-exe", type=Path)
    target.add_argument("--aggregator-url", type=loopback_url)
    target.add_argument("--source-only", action="store_true")
    parser.add_argument("--source-port", type=int, default=0)
    parser.add_argument("--node-a")
    parser.add_argument("--node-b")
    parser.add_argument("--iterations", type=int, default=30)
    parser.add_argument("--extended-services", action="store_true", help="test extra delegation services using the documented synthetic-source adapter")
    parser.add_argument("--restart-source", action="store_true", help="stop/restart the source while retaining upper subscriptions, then verify recovery and old item IDs")
    parser.add_argument("--reconnect-timeout", type=float, default=20)
    parser.add_argument("--interval", type=float, default=50, help="publishing and sampling interval in milliseconds")
    parser.add_argument("--timeout", type=float, default=8, help="request/notification timeout in seconds")
    parser.add_argument("--startup-timeout", type=float, default=90)
    parser.add_argument("--startup-wait", type=float, default=10, help="wait for a manually launched fixture")
    arguments = parser.parse_args()
    if arguments.restart_source and arguments.source_only:
        parser.error("--restart-source requires an aggregation fixture")
    if bool(arguments.node_a) != bool(arguments.node_b):
        parser.error("--node-a and --node-b must be supplied together")
    if arguments.iterations < 1 or arguments.interval <= 0 or arguments.timeout <= 0:
        parser.error("iterations, interval, and timeout must be positive")
    if not 0 <= arguments.source_port <= 65535:
        parser.error("source-port must be between 0 and 65535")
    logging.basicConfig(level=logging.ERROR)
    asyncio.run(main(arguments))
