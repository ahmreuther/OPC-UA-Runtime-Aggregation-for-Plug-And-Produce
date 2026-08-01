# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""Shared project paths for the OJIES plot scripts."""

from __future__ import annotations

import os
from pathlib import Path


PLOT_DIR = Path(__file__).resolve().parent
VALIDATION_ROOT = PLOT_DIR.parent
PROJECT_ROOT = VALIDATION_ROOT.parent
JOURNAL_DIR = PROJECT_ROOT / (
    "Preparation of Papers for IEEE Open Journal of the Industrial "
    "Electronics Society (January 2020)"
)
JOURNAL_IMAGE_DIR = JOURNAL_DIR / "Images"

PERFORMANCE_RAW_DATA_DIR = VALIDATION_ROOT / "performance" / "raw_data"
RESOURCE_USAGE_DIR = PERFORMANCE_RAW_DATA_DIR / "resource_usage"
SOURCE_SERVER_DATA_DIR = PERFORMANCE_RAW_DATA_DIR / "source_servers"
AGGREGATION_SERVER_DATA_DIR = PERFORMANCE_RAW_DATA_DIR / "aggregation_server"
BACKEND_DATA_DIR = PERFORMANCE_RAW_DATA_DIR / "configuration"


def output_dir() -> Path:
    """Return the figure output directory used by LaTeX."""
    configured = os.environ.get("OJIES_FIGURE_DIR")
    target = Path(configured).expanduser() if configured else JOURNAL_IMAGE_DIR
    target.mkdir(parents=True, exist_ok=True)
    return target
