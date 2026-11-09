# Scripts

Utility and maintenance scripts for the fire-recovery-backend project.

## generate_baseline_schema.py

Writes the app's OpenAPI schema to `tests/contract/baseline_schema.json`, which the contract
tests compare against. Regenerate it after an intentional API change:

```bash
pixi run python -m scripts.generate_baseline_schema
```

CI also runs it and publishes the result as the `openapi-schema` artifact.
