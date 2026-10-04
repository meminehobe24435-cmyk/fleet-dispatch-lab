"""fleetlab — an event-driven fleet task dispatch and real-time state platform.

The package is deliberately built on the Python standard library only.  The
optional extras (``matplotlib`` for report figures, ``redis``/``fakeredis`` for
the Redis adapter and its tests) are never required to run the platform itself.

Simulation notice: **all data produced by this package is synthetic.**  It is
generated from a seeded random number generator and does not describe any real
port, terminal or vehicle fleet.
"""

from __future__ import annotations

__version__ = "1.0.0"

__all__ = ["__version__"]
