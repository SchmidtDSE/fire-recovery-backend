"""Helper utilities for contract testing."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Set


@dataclass
class SchemaComparison:
    """Results of comparing two OpenAPI schemas."""

    # Endpoint changes
    added_endpoints: Set[str] = field(default_factory=set)
    removed_endpoints: Set[str] = field(default_factory=set)
    modified_endpoints: Dict[str, List[str]] = field(default_factory=dict)

    # Schema changes
    added_schemas: Set[str] = field(default_factory=set)
    removed_schemas: Set[str] = field(default_factory=set)
    modified_schemas: Dict[str, List[str]] = field(default_factory=dict)

    # Breaking change summary
    is_breaking: bool = False
    breaking_reasons: List[str] = field(default_factory=list)
    non_breaking_changes: List[str] = field(default_factory=list)


class SchemaComparator:
    """Compare two OpenAPI schemas and detect changes."""

    def compare_schemas(
        self, baseline: Dict[str, Any], current: Dict[str, Any]
    ) -> SchemaComparison:
        """
        Compare two OpenAPI schemas and return detailed comparison.

        Args:
            baseline: Previous schema version
            current: Current schema version

        Returns:
            SchemaComparison object with detailed differences
        """
        comparison = SchemaComparison()

        # Compare endpoints
        self._compare_endpoints(baseline, current, comparison)

        # Compare component schemas
        self._compare_component_schemas(baseline, current, comparison)

        # Determine if breaking
        comparison.is_breaking = len(comparison.breaking_reasons) > 0

        return comparison

    def _compare_endpoints(
        self,
        baseline: Dict[str, Any],
        current: Dict[str, Any],
        comparison: SchemaComparison,
    ) -> None:
        """Compare endpoint paths."""
        baseline_paths = set(baseline.get("paths", {}).keys())
        current_paths = set(current.get("paths", {}).keys())

        # Detect additions (non-breaking)
        comparison.added_endpoints = current_paths - baseline_paths
        if comparison.added_endpoints:
            comparison.non_breaking_changes.append(
                f"Added {len(comparison.added_endpoints)} new endpoints"
            )

        # Detect removals (BREAKING)
        comparison.removed_endpoints = baseline_paths - current_paths
        if comparison.removed_endpoints:
            comparison.breaking_reasons.append(
                f"Removed endpoints: {sorted(comparison.removed_endpoints)}"
            )

    def _compare_component_schemas(
        self,
        baseline: Dict[str, Any],
        current: Dict[str, Any],
        comparison: SchemaComparison,
    ) -> None:
        """Compare component schemas (Pydantic models)."""
        baseline_schemas = baseline.get("components", {}).get("schemas", {})
        current_schemas = current.get("components", {}).get("schemas", {})

        baseline_names = set(baseline_schemas.keys())
        current_names = set(current_schemas.keys())

        # Schema additions (non-breaking)
        comparison.added_schemas = current_names - baseline_names
        if comparison.added_schemas:
            comparison.non_breaking_changes.append(
                f"Added {len(comparison.added_schemas)} new schemas"
            )

        # Schema removals (BREAKING)
        comparison.removed_schemas = baseline_names - current_names
        if comparison.removed_schemas:
            comparison.breaking_reasons.append(
                f"Removed schemas: {sorted(comparison.removed_schemas)}"
            )

        # Compare common schemas for modifications
        common_schemas = baseline_names & current_names
        for schema_name in common_schemas:
            changes = self._compare_single_schema(
                baseline_schemas[schema_name],
                current_schemas[schema_name],
                schema_name,
            )

            if changes["breaking"]:
                comparison.modified_schemas[schema_name] = changes["breaking"]
                comparison.breaking_reasons.extend(changes["breaking"])

            if changes["non_breaking"]:
                comparison.non_breaking_changes.extend(changes["non_breaking"])

    def _compare_single_schema(
        self,
        baseline_schema: Dict[str, Any],
        current_schema: Dict[str, Any],
        schema_name: str,
    ) -> Dict[str, List[str]]:
        """Compare a single schema definition."""
        changes: Dict[str, List[str]] = {"breaking": [], "non_breaking": []}

        # Compare required fields
        baseline_required = set(baseline_schema.get("required", []))
        current_required = set(current_schema.get("required", []))

        # Added required fields to request = BREAKING (frontend must provide)
        # Removed required fields from response = BREAKING (frontend expects them)
        removed_required = baseline_required - current_required
        if removed_required:
            changes["breaking"].append(
                f"{schema_name}: Removed required fields: {sorted(removed_required)}"
            )

        added_required = current_required - baseline_required
        if added_required:
            # Context matters: if this is a request model, adding required fields is breaking
            # We'll mark it as potentially breaking for manual review
            changes["breaking"].append(
                f"{schema_name}: Added required fields: {sorted(added_required)} "
                "(BREAKING if this is a request model)"
            )

        # Compare properties
        baseline_props = baseline_schema.get("properties", {})
        current_props = current_schema.get("properties", {})

        # Removed properties (BREAKING)
        removed_props = set(baseline_props.keys()) - set(current_props.keys())
        if removed_props:
            changes["breaking"].append(
                f"{schema_name}: Removed properties: {sorted(removed_props)}"
            )

        # Added properties (non-breaking if optional)
        added_props = set(current_props.keys()) - set(baseline_props.keys())
        if added_props:
            # Check if added as required
            added_required_props = added_props & current_required
            if added_required_props:
                changes["breaking"].append(
                    f"{schema_name}: Added required properties: {sorted(added_required_props)}"
                )
            else:
                changes["non_breaking"].append(
                    f"{schema_name}: Added optional properties: {sorted(added_props)}"
                )

        # Type changes (BREAKING)
        for prop_name in set(baseline_props.keys()) & set(current_props.keys()):
            baseline_type = baseline_props[prop_name].get("type")
            current_type = current_props[prop_name].get("type")

            if baseline_type and current_type and baseline_type != current_type:
                changes["breaking"].append(
                    f"{schema_name}.{prop_name}: Type changed from "
                    f"{baseline_type} to {current_type}"
                )

        return changes

    def format_diff_report(self, comparison: SchemaComparison) -> str:
        """Format a human-readable diff report."""
        lines = []

        lines.append("=" * 70)
        lines.append("OpenAPI Schema Comparison Report")
        lines.append("=" * 70)
        lines.append("")

        if comparison.is_breaking:
            lines.append("BREAKING CHANGES DETECTED")
            lines.append("")
            lines.append("The following changes will break existing API clients:")
            for reason in comparison.breaking_reasons:
                lines.append(f"  - {reason}")
            lines.append("")
            lines.append("Action required:")
            lines.append("  1. Coordinate with frontend team")
            lines.append("  2. Plan migration strategy")
            lines.append(
                "  3. Update baseline: pixi run python scripts/generate_baseline_schema.py"
            )
            lines.append("  4. Commit updated baseline_schema.json")
        else:
            lines.append("No breaking changes detected")

        if comparison.non_breaking_changes:
            lines.append("")
            lines.append("Non-breaking changes:")
            for change in comparison.non_breaking_changes:
                lines.append(f"  + {change}")

        if comparison.added_endpoints:
            lines.append("")
            lines.append("New endpoints:")
            for endpoint in sorted(comparison.added_endpoints):
                lines.append(f"  + {endpoint}")

        if comparison.removed_endpoints:
            lines.append("")
            lines.append("Removed endpoints (BREAKING):")
            for endpoint in sorted(comparison.removed_endpoints):
                lines.append(f"  - {endpoint}")

        lines.append("")
        lines.append("=" * 70)

        return "\n".join(lines)


class SchemaValidator:
    """Validate OpenAPI schema structure."""

    def validate_openapi_spec(self, schema: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Validate schema conforms to OpenAPI 3.x specification.

        Args:
            schema: OpenAPI schema dictionary

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        # Check required top-level fields
        if "openapi" not in schema:
            errors.append("Missing 'openapi' version field")
        if "info" not in schema:
            errors.append("Missing 'info' section")
        if "paths" not in schema:
            errors.append("Missing 'paths' section")

        return len(errors) == 0, errors
