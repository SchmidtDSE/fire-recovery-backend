#!/usr/bin/env python3
"""
Validate frontend API example payloads against OpenAPI schema.

This script:
1. Loads the OpenAPI schema from the backend
2. Reads frontend example payload files
3. Validates each example against the schema
4. Fails if any examples don't match the schema

This catches frontend/backend drift before deployment.
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any, List

try:
    from jsonschema import validate, ValidationError, RefResolver
except ImportError:
    print("ERROR: jsonschema not installed. Run: pip install jsonschema")
    sys.exit(1)


class Color:
    """Terminal colors"""

    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    END = "\033[0m"


def load_schema(schema_path: Path) -> Dict[str, Any]:
    """Load OpenAPI schema from file"""
    print(f"{Color.BLUE}Loading OpenAPI schema from {schema_path}...{Color.END}")
    with open(schema_path) as f:
        return json.load(f)


def load_examples(examples_dir: Path) -> List[Dict[str, Any]]:
    """Load all example payload files"""
    example_files = list(examples_dir.glob("*.json"))

    # Filter out README and other non-example files
    example_files = [
        f
        for f in example_files
        if not f.name.startswith("_") and f.name != "README.json"
    ]

    print(f"{Color.BLUE}Found {len(example_files)} example files{Color.END}")

    examples = []
    for example_file in example_files:
        with open(example_file) as f:
            try:
                example = json.load(f)
                example["_source_file"] = example_file.name
                examples.append(example)
            except json.JSONDecodeError as e:
                print(
                    f"{Color.RED}ERROR: Invalid JSON in {example_file.name}: {e}{Color.END}"
                )
                sys.exit(1)

    return examples


def get_request_schema(
    openapi_schema: Dict[str, Any],
    endpoint: str,
    method: str,
) -> Dict[str, Any]:
    """Extract request schema for a specific endpoint and method"""
    if endpoint not in openapi_schema.get("paths", {}):
        raise ValueError(f"Endpoint {endpoint} not found in OpenAPI schema")

    path_def = openapi_schema["paths"][endpoint]
    method_lower = method.lower()

    if method_lower not in path_def:
        raise ValueError(f"Method {method} not found for endpoint {endpoint}")

    operation = path_def[method_lower]

    if "requestBody" not in operation:
        raise ValueError(f"No request body defined for {method} {endpoint}")

    request_schema = operation["requestBody"]["content"]["application/json"]["schema"]

    # Resolve $ref if present
    if "$ref" in request_schema:
        ref_path = request_schema["$ref"]
        # ref_path looks like: "#/components/schemas/FireSeverityRequest"
        parts = ref_path.split("/")[1:]  # Skip the '#'

        schema_obj = openapi_schema
        for part in parts:
            schema_obj = schema_obj[part]

        return schema_obj

    return request_schema


def validate_example(
    example: Dict[str, Any],
    openapi_schema: Dict[str, Any],
) -> List[str]:
    """
    Validate a single example against the OpenAPI schema.

    Returns list of error messages (empty if valid).
    """
    errors = []
    source_file = example.get("_source_file", "unknown")

    # Extract example components
    endpoint = example.get("endpoint")
    method = example.get("method")
    request_body = example.get("request")

    # Validate example structure
    if not endpoint:
        errors.append(f"{source_file}: Missing 'endpoint' field")
        return errors

    if not method:
        errors.append(f"{source_file}: Missing 'method' field")
        return errors

    if not request_body:
        errors.append(f"{source_file}: Missing 'request' field")
        return errors

    # Get schema for this endpoint
    try:
        request_schema = get_request_schema(openapi_schema, endpoint, method)
    except ValueError as e:
        errors.append(f"{source_file}: {e}")
        return errors

    # Create resolver for handling $refs in nested schemas
    resolver = RefResolver.from_schema(openapi_schema)

    # Validate request body against schema
    try:
        validate(
            instance=request_body,
            schema=request_schema,
            resolver=resolver,
        )
    except ValidationError as e:
        # Format error message nicely
        error_path = " -> ".join(str(p) for p in e.path) if e.path else "root"
        errors.append(f"{source_file}: Validation error at '{error_path}': {e.message}")

    return errors


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Validate frontend API examples against OpenAPI schema"
    )
    parser.add_argument(
        "--schema",
        required=True,
        type=Path,
        help="Path to openapi.json file",
    )
    parser.add_argument(
        "--examples-dir",
        required=True,
        type=Path,
        help="Path to directory containing example JSON files",
    )

    args = parser.parse_args()

    # Validate inputs
    if not args.schema.exists():
        print(f"{Color.RED}ERROR: Schema file not found: {args.schema}{Color.END}")
        sys.exit(1)

    if not args.examples_dir.exists():
        print(
            f"{Color.RED}ERROR: Examples directory not found: {args.examples_dir}{Color.END}"
        )
        sys.exit(1)

    # Load schema and examples
    try:
        openapi_schema = load_schema(args.schema)
        examples = load_examples(args.examples_dir)
    except Exception as e:
        print(f"{Color.RED}ERROR: Failed to load files: {e}{Color.END}")
        sys.exit(1)

    if not examples:
        print(
            f"{Color.YELLOW}WARNING: No example files found in {args.examples_dir}{Color.END}"
        )
        print(f"{Color.GREEN}✓ Validation passed (no examples to validate){Color.END}")
        sys.exit(0)

    # Validate each example
    print(f"\n{Color.BLUE}Validating {len(examples)} examples...{Color.END}\n")

    all_errors = []

    for example in examples:
        source_file = example.get("_source_file", "unknown")
        endpoint = example.get("endpoint", "unknown")
        method = example.get("method", "unknown")

        print(f"  {source_file}: {method} {endpoint}...", end=" ")

        errors = validate_example(example, openapi_schema)

        if errors:
            print(f"{Color.RED}✗{Color.END}")
            all_errors.extend(errors)
        else:
            print(f"{Color.GREEN}✓{Color.END}")

    # Report results
    print()

    if all_errors:
        print(
            f"{Color.RED}✗ Validation failed with {len(all_errors)} error(s):{Color.END}\n"
        )
        for error in all_errors:
            print(f"  {Color.RED}• {error}{Color.END}")
        print()
        print(
            f"{Color.YELLOW}These errors indicate frontend/backend API drift.{Color.END}"
        )
        print(
            f"{Color.YELLOW}Either update the frontend examples or fix the backend API.{Color.END}"
        )
        sys.exit(1)
    else:
        print(f"{Color.GREEN}✓ All examples validated successfully!{Color.END}")
        print(
            f"{Color.GREEN}✓ Frontend examples match backend OpenAPI schema{Color.END}"
        )
        sys.exit(0)


if __name__ == "__main__":
    main()
