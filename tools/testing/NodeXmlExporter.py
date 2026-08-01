#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

import asyncio
import argparse
import json
import logging
from pathlib import Path
import progressbar

from asyncua import Client

from XmlExporter import XmlExporter


class NodeXMLExporter:
    def __init__(self):
        self.nodes = []
        self.namespaces = {}
        self.visited = set()
        self.browse_skipped_nodes = []
        self.client = None
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    async def start_node_browse(self, rootnode):
        bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength)
        pending = [rootnode]
        while pending:
            node = pending.pop()
            node_key = str(node.nodeid)
            if node_key in self.visited:
                continue
            self.visited.add(node_key)
            try:
                children = await self._get_children_with_retry(node)
            except Exception as exc:
                if (
                    node.nodeid.NamespaceIndex == 0
                    and exc.__class__.__name__ == "BadNodeIdUnknown"
                ):
                    self.browse_skipped_nodes.append({
                        "node_id": node_key,
                        "namespace_index": 0,
                        "phase": "browse",
                        "error_type": exc.__class__.__name__,
                        "error": str(exc),
                    })
                    self.logger.warning(
                        "Excluded nonexistent namespace-zero browse result %s: %s",
                        node_key, exc,
                    )
                    continue
                raise
            self.nodes.append(node)
            bar.update(len(self.nodes))
            pending.extend(reversed(children))

    async def _get_children_with_retry(
            self, node, retry_delays=(0.25, 0.5, 1.0, 2.0, 4.0)
    ):
        """Retry only the server's transient BadUnexpectedError browse failure."""
        for attempt in range(len(retry_delays) + 1):
            try:
                # ReferenceType 33 = HierarchicalReferences.
                return await node.get_children(refs=33)
            except Exception as exc:
                if (
                    exc.__class__.__name__ != "BadUnexpectedError"
                    or attempt == len(retry_delays)
                ):
                    raise
                delay = retry_delays[attempt]
                self.logger.warning(
                    "Transient browse failure for %s (attempt %d/%d); "
                    "retrying in %.2fs: %s",
                    node.nodeid, attempt + 1, len(retry_delays) + 1, delay, exc,
                )
                await asyncio.sleep(delay)

    async def export_xml(
            self, namespaces=None, output_file="export.xml", export_values=False,
            skipped_nodes_report=None,
    ):
        if namespaces:
            self.logger.info("Export only NS %s" % namespaces)
            nodes = [node for node in self.nodes if node.nodeid.NamespaceIndex in namespaces]
        else:
            nodes = self.nodes

        self.logger.info("Export nodes to %s" % output_file)
        self.logger.info("Export node values: %s" % export_values)
        bar = progressbar.ProgressBar(max_value=len(nodes))
        exp = XmlExporter(self.client, export_values, bar.update)
        try:
            await exp.build_etree(nodes)
        finally:
            if skipped_nodes_report:
                skipped_nodes = self.browse_skipped_nodes + exp.skipped_nodes
                report = {
                    "schema": "ojies.xml-export-skipped-nodes/v1",
                    "policy": (
                        "Only namespace-zero browse results rejected by the server "
                        "with BadNodeIdUnknown are excluded. Every other failure is fatal."
                    ),
                    "skipped_count": len(skipped_nodes),
                    "skipped_nodes": skipped_nodes,
                    "fatal_failure_count": len(exp.failed_nodes),
                    "fatal_failures": exp.failed_nodes,
                }
                Path(skipped_nodes_report).write_text(
                    json.dumps(report, indent=2, ensure_ascii=False) + "\n",
                    encoding="utf-8",
                )
        await exp.write_xml(output_file)
        self.logger.info("Export finished")

    async def import_nodes(self, server_url="opc.tcp://localhost:16664", username="", password=""):
        from asyncua.crypto import security_policies
        import types

        self.client = Client(server_url)
        if username is not None:
            self.client.set_user(username)

        if password is not None:
            self.client.set_password(password)

        # Fix symmetric_key_size (not 0) of securityPolicy
        # sec_policy = security_policies.SecurityPolicy()
        # sec_policy.symmetric_key_size = 8
        # self.client.security_policy = sec_policy

        # Fix signature method of CryptographyNone
        # def signature(self, data):
        #    return None
        # fixed_signature = types.MethodType(signature, CryptographyNone)
        # self.client.security_policy.asymmetric_cryptography.signature = fixed_signature

        try:
            await self.client.connect()
        except Exception as e:
            raise RuntimeError(
                f"No connection established to {server_url}: {e}"
            ) from e

        self.logger.info("Client connected to %s" % server_url)

        for ns in await self.client.get_namespace_array():
            self.namespaces[await self.client.get_namespace_index(ns)] = ns

        root = self.client.get_root_node()
        self.logger.info("Starting to collect nodes. This may take some time ...")
        await self.start_node_browse(root)
        self.logger.info("All nodes collected")

    async def statistics(self):
        self.logger.info("Calculating statistics")
        typecounts_per_namespace = {}
        nodecount = len(self.nodes)
        bar = progressbar.ProgressBar()
        for idx in bar(range(nodecount)):
            node = self.nodes[idx]
            try:
                node_class = str(await node.read_node_class())
                ns = node.nodeid.NamespaceIndex
                if ns not in typecounts_per_namespace:
                    typecounts_per_namespace[ns] = {}
                if node_class not in typecounts_per_namespace[ns]:
                    typecounts_per_namespace[ns][node_class] = 1
                else:
                    typecounts_per_namespace[ns][node_class] += 1
            except Exception as e:
                self.logger.error("some error with %s: %s" % (node, e))

        for ns in typecounts_per_namespace:
            self.logger.info("NS%d (%s)" % (ns, self.namespaces[ns]))
            for type_info in typecounts_per_namespace[ns]:
                self.logger.info("\t%s:\t%d" % (type_info, typecounts_per_namespace[ns][type_info]))
        self.logger.info("\tTOTAL in namespace: %d" % len(self.nodes))


async def main():
    parser = argparse.ArgumentParser(
        description="Export Node XML from OPC UA server")
    parser.add_argument('serverUrl', help='Complete URL of the OPC UA server', default="opc.tcp://localhost:16664")
    parser.add_argument('-n', '--namespace',
                        metavar='<namespace>',
                        dest="namespaces",
                        action="append",
                        type=int,
                        help='Export only the given namespace indexes. Multiple NS indexes can be specified. If not specified, export all nodes.')
    parser.add_argument('outputFile',  default="nodes_output.xml",
                        help='Save exported nodes in specified XML file')
    parser.add_argument('-u', '--username', default="", metavar='<username>', dest="username", help="Username to login on server")
    parser.add_argument('-p', '--password', default="", metavar='<password>', dest="password", help="Password to login on server")
    parser.add_argument('-v', '--values', default=False, metavar='<values>', dest="export_values", help="Export node values to nodeset")
    parser.add_argument(
        '--statistics', action='store_true',
        help='Run the optional per-node statistics pass before exporting.',
    )
    parser.add_argument(
        '--skipped-nodes-report',
        help='Write a JSON audit report for excluded and fatal node exports.',
    )
    args = parser.parse_args()

    exporter = NodeXMLExporter()
    try:
        await exporter.import_nodes(
            server_url=args.serverUrl,
            username=args.username,
            password=args.password,
        )
        if args.statistics:
            await exporter.statistics()
        await exporter.export_xml(
            args.namespaces, args.outputFile, args.export_values,
            args.skipped_nodes_report,
        )
    finally:
        if exporter.client is not None:
            try:
                await exporter.client.disconnect()
            except Exception as exc:
                exporter.logger.warning("Disconnect failed: %s", exc)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    asyncio.run(main())
