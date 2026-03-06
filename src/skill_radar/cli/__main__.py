"""Module entrypoint for ``python -m skill_radar.cli``.

This simply delegates to the Click group defined in ``skill_radar.cli``.
"""

from __future__ import annotations

from . import main

if __name__ == "__main__":
    # Click command group entry point
    main()
