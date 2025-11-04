"""Schema stability and breaking change detection tests."""

from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from tests.contract.helpers import SchemaComparator


class TestSchemaStability:
    """Test suite for detecting schema changes and breaking changes."""

    def test_baseline_schema_exists(self, baseline_schema_path: Path) -> None:
        """Ensure baseline schema exists for comparison."""
        if not baseline_schema_path.exists():
            pytest.skip(
                "No baseline schema found. Run 'pixi run python scripts/generate_baseline_schema.py' "
                "to create initial baseline."
            )

    def test_no_endpoints_removed(
        self,
        openapi_schema: Dict[str, Any],
        baseline_schema: Optional[Dict[str, Any]],
        baseline_schema_path: Path,
    ) -> None:
        """Detect removed endpoints (BREAKING CHANGE)."""
        if baseline_schema is None:
            pytest.skip("No baseline for comparison")

        baseline_paths = set(baseline_schema.get("paths", {}).keys())
        current_paths = set(openapi_schema.get("paths", {}).keys())

        removed_paths = baseline_paths - current_paths

        if removed_paths:
            pytest.fail(
                f"BREAKING CHANGE DETECTED: Endpoints removed!\n"
                f"Removed endpoints: {sorted(removed_paths)}\n\n"
                f"If this change is intentional:\n"
                f"1. Update frontend to handle removed endpoints\n"
                f"2. Regenerate baseline: pixi run python scripts/generate_baseline_schema.py\n"
                f"3. Commit updated baseline_schema.json"
            )

    def test_document_new_endpoints(
        self,
        openapi_schema: Dict[str, Any],
        baseline_schema: Optional[Dict[str, Any]],
    ) -> None:
        """Document new endpoint additions (NON-BREAKING)."""
        if baseline_schema is None:
            pytest.skip("No baseline for comparison")

        baseline_paths = set(baseline_schema.get("paths", {}).keys())
        current_paths = set(openapi_schema.get("paths", {}).keys())

        added_paths = current_paths - baseline_paths

        if added_paths:
            # This is non-breaking, just informational
            print(f"\nNew endpoints added: {sorted(added_paths)}")
            print("   Consider updating baseline if this is intentional.")

    def test_no_required_fields_removed_from_responses(
        self,
        openapi_schema: Dict[str, Any],
        baseline_schema: Optional[Dict[str, Any]],
    ) -> None:
        """Detect removed required response fields (BREAKING CHANGE)."""
        if baseline_schema is None:
            pytest.skip("No baseline for comparison")

        # Compare component schemas (Pydantic models)
        baseline_components = baseline_schema.get("components", {}).get("schemas", {})
        current_components = openapi_schema.get("components", {}).get("schemas", {})

        breaking_changes = []

        for schema_name in baseline_components:
            if schema_name not in current_components:
                breaking_changes.append(f"Schema '{schema_name}' completely removed")
                continue

            baseline_props = baseline_components[schema_name].get("properties", {})
            current_props = current_components[schema_name].get("properties", {})

            baseline_required = set(
                baseline_components[schema_name].get("required", [])
            )
            current_required = set(current_components[schema_name].get("required", []))

            # Check for removed required fields
            removed_required = baseline_required - current_required
            if removed_required:
                breaking_changes.append(
                    f"Schema '{schema_name}': Required fields removed: {removed_required}"
                )

            # Check for removed properties entirely
            removed_props = set(baseline_props.keys()) - set(current_props.keys())
            if removed_props:
                breaking_changes.append(
                    f"Schema '{schema_name}': Properties removed: {removed_props}"
                )

        if breaking_changes:
            pytest.fail(
                "BREAKING CHANGE DETECTED: Response schema changes!\n"
                + "\n".join(f"  - {change}" for change in breaking_changes)
                + "\n\nIf intentional, update baseline and coordinate with frontend."
            )

    def test_no_field_type_changes(
        self,
        openapi_schema: Dict[str, Any],
        baseline_schema: Optional[Dict[str, Any]],
    ) -> None:
        """Detect field type changes (BREAKING CHANGE)."""
        if baseline_schema is None:
            pytest.skip("No baseline for comparison")

        baseline_components = baseline_schema.get("components", {}).get("schemas", {})
        current_components = openapi_schema.get("components", {}).get("schemas", {})

        type_changes = []

        for schema_name in baseline_components:
            if schema_name not in current_components:
                continue

            baseline_props = baseline_components[schema_name].get("properties", {})
            current_props = current_components[schema_name].get("properties", {})

            for prop_name in baseline_props:
                if prop_name not in current_props:
                    continue

                baseline_type = baseline_props[prop_name].get("type")
                current_type = current_props[prop_name].get("type")

                if baseline_type and current_type and baseline_type != current_type:
                    type_changes.append(
                        f"{schema_name}.{prop_name}: Type changed from "
                        f"{baseline_type} to {current_type}"
                    )

        if type_changes:
            pytest.fail(
                "BREAKING CHANGE DETECTED: Field type changes!\n"
                + "\n".join(f"  - {change}" for change in type_changes)
                + "\n\nIf intentional, update baseline and coordinate with frontend."
            )

    def test_document_new_fields(
        self,
        openapi_schema: Dict[str, Any],
        baseline_schema: Optional[Dict[str, Any]],
    ) -> None:
        """Document new field additions (NON-BREAKING if optional)."""
        if baseline_schema is None:
            pytest.skip("No baseline for comparison")

        baseline_components = baseline_schema.get("components", {}).get("schemas", {})
        current_components = openapi_schema.get("components", {}).get("schemas", {})

        new_fields = []

        for schema_name in current_components:
            if schema_name not in baseline_components:
                continue

            baseline_props = baseline_components[schema_name].get("properties", {})
            current_props = current_components[schema_name].get("properties", {})

            added_props = set(current_props.keys()) - set(baseline_props.keys())
            if added_props:
                new_fields.append(f"{schema_name}: Added fields: {sorted(added_props)}")

        if new_fields:
            print("\nNew fields added:")
            for field in new_fields:
                print(f"  + {field}")

    def test_comprehensive_schema_comparison(
        self,
        openapi_schema: Dict[str, Any],
        baseline_schema: Optional[Dict[str, Any]],
        schema_comparator: SchemaComparator,
    ) -> None:
        """Run comprehensive schema comparison and generate report."""
        if baseline_schema is None:
            pytest.skip("No baseline for comparison")

        comparison = schema_comparator.compare_schemas(baseline_schema, openapi_schema)

        # Print detailed report
        report = schema_comparator.format_diff_report(comparison)
        print(f"\n{report}")

        # Save report to file for CI artifacts
        report_path = Path(__file__).parent / "schemas" / "comparison_report.txt"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w") as f:
            f.write(report)

        # Fail if breaking changes detected
        if comparison.is_breaking:
            pytest.fail(
                "BREAKING CHANGES DETECTED! See comparison report above.\n\n"
                "To update baseline after reviewing changes:\n"
                "  pixi run python scripts/generate_baseline_schema.py\n"
                "  git add tests/contract/baseline_schema.json\n"
                "  git commit -m 'Update API contract baseline'"
            )
