
from __future__ import annotations

import argparse
import json
from pathlib import Path

from coordwatch.config import load_model_specs, load_source_manifest, load_variables
from coordwatch.io import load_json_if_exists
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import REPO_ROOT, ensure_repo_dirs


def doctor() -> None:
    ensure_repo_dirs()
    logger = get_logger(__name__)
    source_manifest = load_source_manifest()
    variables = load_variables()
    model_specs = load_model_specs()

    checks = {
        "repo_root": str(REPO_ROOT),
        "configs_present": all(
            [
                (REPO_ROOT / "configs/source_manifest.yml").exists(),
                (REPO_ROOT / "configs/variables.yml").exists(),
                (REPO_ROOT / "configs/model_specs.yml").exists(),
            ]
        ),
        "manual_episode_seed": (REPO_ROOT / "data/manual/episode_registry_seed.csv").exists(),
        "duration_weights": (REPO_ROOT / "data/reference/duration_weights.csv").exists(),
        "source_manifest_keys": sorted(source_manifest.keys()),
        "fred_series_count": len(variables.get("fred_core_series", {})),
        "reaction_spec": model_specs.get("reaction_function", {}),
    }

    demo_summary = load_json_if_exists(REPO_ROOT / "data/interim/demo_seed_summary.json")
    if demo_summary:
        checks["demo_seed_summary"] = demo_summary

    logger.info("CoordWatch doctor report")
    print(json.dumps(checks, indent=2, default=str))


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description="CoordWatch command line interface")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("doctor", help="Print repo/config health checks")

    args = parser.parse_args()
    if args.command == "doctor":
        doctor()


if __name__ == "__main__":
    main()
