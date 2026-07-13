"""Thin CLI entrypoint that delegates to ``typesense_lite.run_node.main``.

Run from a checked-out project tree::

    .venv/bin/python scripts/run_node.py \\
        --role node --node-id node-1 \\
        --host 0.0.0.0 --port 9101 \\
        --config /var/lib/typesense_lite/cluster_config.json \\
        --data-dir /var/lib/typesense_lite/node-1
"""

from __future__ import annotations

from typesense_lite.run_node import main


if __name__ == "__main__":
    raise SystemExit(main())