#!/usr/bin/env python3
"""
Generate baseline OpenAPI schema for contract testing.

Usage:
    pixi run python -m scripts.generate_baseline_schema
"""

import json
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def generate_baseline_schema() -> None:
    """Generate baseline schema from current API."""
    try:
        from src.app import app

        # Get OpenAPI schema
        logger.info("Generating OpenAPI schema from application...")
        schema = app.openapi()

        # Save to baseline file
        baseline_path = (
            Path(__file__).parent.parent
            / "tests"
            / "contract"
            / "baseline_schema.json"
        )
        baseline_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Writing baseline schema to {baseline_path}")
        with open(baseline_path, "w") as f:
            json.dump(schema, f, indent=2, sort_keys=True)

        # Print summary
        print(f"Baseline schema saved to {baseline_path}")
        print(f"  Schema version: {schema['info']['version']}")
        print(f"  Endpoints: {len(schema['paths'])}")
        print(f"  Models: {len(schema.get('components', {}).get('schemas', {}))}")

        logger.info("Baseline schema generation completed successfully")

    except Exception as e:
        logger.error(f"Failed to generate baseline schema: {e}")
        raise


if __name__ == "__main__":
    generate_baseline_schema()
