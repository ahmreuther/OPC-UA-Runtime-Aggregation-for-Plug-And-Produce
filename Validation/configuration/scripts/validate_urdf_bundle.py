#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""Validate one extracted URDF bundle and emit a machine-readable report."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_urdf(urdf_path: Path) -> dict[str, object]:
    urdf_path = urdf_path.resolve()
    try:
        reported_urdf_path = urdf_path.relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        reported_urdf_path = urdf_path.as_posix()
    bundle_dir = urdf_path.parent
    root = ET.parse(urdf_path).getroot()

    links = [element.get("name", "") for element in root.findall("link")]
    joints = list(root.findall("joint"))
    link_names = set(links)
    dangling_joint_links: list[dict[str, str]] = []
    for joint in joints:
        joint_name = joint.get("name", "")
        for role in ("parent", "child"):
            element = joint.find(role)
            link_name = "" if element is None else element.get("link", "")
            if link_name not in link_names:
                dangling_joint_links.append(
                    {"joint": joint_name, "role": role, "link": link_name}
                )

    nonstandard_case_elements = [
        element.tag
        for element in root.iter()
        if element.tag in {"Visual", "Collision"}
    ]

    mesh_references: list[dict[str, object]] = []
    missing_meshes: list[str] = []
    escaped_mesh_paths: list[str] = []
    invalid_mesh_xml: list[str] = []
    for mesh in root.findall(".//mesh"):
        reference = mesh.get("filename", "")
        resolved = (bundle_dir / reference).resolve()
        try:
            resolved.relative_to(bundle_dir)
        except ValueError:
            escaped_mesh_paths.append(reference)
            continue

        if not resolved.is_file():
            missing_meshes.append(reference)
            mesh_references.append({"reference": reference, "exists": False})
            continue

        entry: dict[str, object] = {
            "reference": reference,
            "exists": True,
            "size": resolved.stat().st_size,
            "sha256": _sha256(resolved),
        }
        if resolved.suffix.lower() == ".dae":
            try:
                ET.parse(resolved)
                entry["xml_parseable"] = True
            except ET.ParseError:
                entry["xml_parseable"] = False
                invalid_mesh_xml.append(reference)
        mesh_references.append(entry)

    unique_meshes = {entry["reference"] for entry in mesh_references}
    passed = all(
        (
            root.tag == "robot",
            bool(root.get("name")),
            bool(links),
            not dangling_joint_links,
            not nonstandard_case_elements,
            bool(mesh_references),
            not missing_meshes,
            not escaped_mesh_paths,
            not invalid_mesh_xml,
        )
    )

    return {
        "schema": "ojies.urdf-bundle-validation/v1",
        "status": "passed" if passed else "failed",
        "urdf_path": reported_urdf_path,
        "urdf_sha256": _sha256(urdf_path),
        "robot_name": root.get("name", ""),
        "link_count": len(links),
        "joint_count": len(joints),
        "mesh_reference_count": len(mesh_references),
        "unique_mesh_reference_count": len(unique_meshes),
        "missing_meshes": missing_meshes,
        "escaped_mesh_paths": escaped_mesh_paths,
        "invalid_mesh_xml": invalid_mesh_xml,
        "dangling_joint_links": dangling_joint_links,
        "nonstandard_case_elements": nonstandard_case_elements,
        "mesh_references": mesh_references,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("urdf", type=Path)
    args = parser.parse_args()
    try:
        result = validate_urdf(args.urdf)
    except (OSError, ET.ParseError) as exc:
        result = {
            "schema": "ojies.urdf-bundle-validation/v1",
            "status": "failed",
            "urdf_path": str(args.urdf),
            "error": str(exc),
        }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
