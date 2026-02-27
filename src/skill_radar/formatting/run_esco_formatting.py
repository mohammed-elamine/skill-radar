from __future__ import annotations

from skill_radar.formatting.build_esco_terms import build_esco_terms
from skill_radar.formatting.format_esco import format_esco


def main() -> None:
    format_esco()
    build_esco_terms()


if __name__ == "__main__":
    main()
