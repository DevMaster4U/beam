"""Re-export shared gateway protocol for standalone worker-gateway runs."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from neurons.shared.gateway_protocol import (  # noqa: E402
    GatewayMode,
    build_task_accept_ack,
    build_task_offer_for_worker,
    build_task_result_summary_ack,
    build_worker_response_from_accept,
    build_worker_response_from_reject,
    parse_gateway_mode,
)

__all__ = [
    "GatewayMode",
    "build_task_accept_ack",
    "build_task_offer_for_worker",
    "build_task_result_summary_ack",
    "build_worker_response_from_accept",
    "build_worker_response_from_reject",
    "parse_gateway_mode",
]
