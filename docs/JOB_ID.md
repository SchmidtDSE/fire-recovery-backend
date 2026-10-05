# Deterministic `job_id`

This is the one tracked home of the `job_id` decision log and contract for the
`deterministic-job-id-2026-09` plan. The frontend and validation repos link here instead of
copying this content — see `smrt-design/deterministic-job-id-2026-09.md` in this repo for the
full work-item plan and status.

This repo (`fire-recovery-backend`) is the only one that computes a `job_id`. No client
computes one; a client recovers from a lost response by resubmitting the identical request and
receiving the same id back.

## Decisions

| # | Question | Decision |
| --- | --- | --- |
| D1 | Does the browser derive `job_id`? | **No, and no client does.** A client recovers by resubmitting the identical request. Ids arrive in responses and are sent back on retry. |
| D2 | Vectors fetched live, or pinned? | **Pinned and committed.** Live fetch only in a scheduled drift alarm. |
| D3 | Who owns `pipeline_version` transport? | **Backend constant, served from `GET /version`.** Clients fetch it once per run and record it beside results for provenance. No client computes with it. |
| D4 | Idempotency key or content hash? | **Content-derived.** A client that lost all local state still recovers. |
| D5 | Frontend Phase 4's backend prerequisite | Replaced by D12. Phase 4 reads the published OpenAPI schema, which must already carry the D8 constraints. |
| D6 | Does a stored failure count as a cache hit? | **No, success only.** A stored failure is treated as unknown and re-runs. |
| D7 | How is the canonicalizer shared? | **It isn't.** One implementation, in the backend (`src/util/job_hash.py`). Promote it to a package the day a client needs ids before its first response; the vectors make that safe. |
| D8 | How is `fire_event_name` handled? | **The server validates only, never transforms.** A `Field(pattern=..., max_length=...)` on a shared `FireEventName` type, applied at every entry point. Senders that build names from external data sanitize them when constructing requests. **Amended 2026-10-05:** the frontend no longer rejects. It derives the name deterministically from what the user types and shows the derived name live before submit. The rule, in order: (1) drop every character that is neither in `[A-Za-z0-9_-]` nor whitespace; (2) trim leading and trailing whitespace; (3) replace each run of whitespace with a single `_`. So `" Dome  Fire "`, `"Dome Fire"` and `"Dome & Fire"` all derive to `Dome_Fire`. Any client that builds names from free text uses this same rule. The server never sees the raw input, so "server never transforms" still holds. |
| D9 | The 328 job ids on the old layout? | **Clean slate.** No migration, dual-read, or backfill. |
| D10 | Geometry edge cases: restrict input, or normalize? | **Normalize once, at the API boundary**, in the request model. |
| D11 | `refined_geojson` vs `refine_geojson`? | **`refined_geojson`** (`requests.py:81`). The drift is in the frontend's stale generated types. No backend rename. |
| D12 | When may frontend Phase 4 start? | **Once `be-name-validation` is deployed to the dev backend** and the schema is published at the D19 path. The frontend's constants script hard-fails on a missing expected constraint. |
| D13 | Is `sensor` required in the frontend type? | **No.** It's optional, and the default is emitted as a runtime constant. The server applies `DEFAULT_SENSOR` before hashing, so an omitted `sensor` hashes as the default. Changing `DEFAULT_SENSOR` requires a `PIPELINE_VERSION` bump. |
| D14 | Is `/upload/geojson` part of the normalization contract? | **No.** It stores a preview artifact that is neither hashed nor used by the pipeline. The authoritative boundary is the normalized copy `ProcessingRequest` produces. |
| D15 | Who owns this log and the contract spec? | **The backend, in `docs/JOB_ID.md`.** Other repos link to it. *(2026-09-29)* |
| D16 | When `backend-r3` and frontend r2 disagree? | **r2 wins on anything it revised or added.** `backend-r3` wins on details r2 doesn't address: the persisted canonical string and the near-zero vector. *(2026-09-29)* |
| D17 | How are work items referenced across repos? | **By stable item id (`be-*`, `fe-*`, `val-*`), never by PR number or list position.** *(2026-09-29)* |
| D18 | What does a sweep do if `GET /version` fails at start? | **Abort before submitting anything.** `/version` does no storage I/O; if it can't answer, the backend is unhealthy, and a sweep without a version has no provenance. Nothing is lost by stopping at startup. *(2026-09-29)* |
| D19 | Where is the OpenAPI schema published? | **At `openapi-schemas/{branch}.json` and `openapi-schemas/{sha}.json`** in `fire-recovery-store`, as the consumers already expect. Backend CI is fixed to match (`be-schema-publish-path`); the consumers don't adopt the accidental `…/dev.json/baseline_schema.json` name. Frontend and validation work that reads the schema waits on this. *(2026-10-05)* |
| D20 | What is the `fire_event_name` pattern? | **`^[A-Za-z0-9_-]+$`, strict: no spaces.** This is the safe set from the storage-path analysis. It excludes `/`, `.`-only, empty, `%`, `#`, `?`, whitespace and `\`. Loosening it later is backward-compatible, because every name valid today stays valid. Tightening it is not. Start strict. *(2026-10-05)* |

## Contract

Built from the **validated** `ProcessingRequest`, never from the raw body or `model_dump()`.
The model differs from the body — numeric strings are coerced, unknown keys are dropped,
`sensor` is defaulted, and `model_dump()` adds `"bbox": None` — so the canonical dict is built
from named model fields, never from `model_dump()`.

### Canonical form

```python
canonical = {
    "operation": "analyze_fire_severity",
    "pipeline_version": <PIPELINE_VERSION>,
    "fire_event_name": <str, matches ^[A-Za-z0-9_-]+$, validated not transformed>,
    "sensor": <"sentinel-2" | "landsat">,        # DEFAULT_SENSOR applied before hashing
    "prefire_date_range": [<str>, <str>],        # hashed verbatim, never parsed
    "postfire_date_range": [<str>, <str>],
    "coarse_geojson": {"type": "MultiPolygon", "coordinates": <nested ints>},
}
job_id = uuid.uuid5(NAMESPACE, json.dumps(canonical, sort_keys=True, separators=(",", ":")))
```

`NAMESPACE` is a hardcoded literal, identical everywhere, never changed. **Not yet chosen** —
filled in by `be-canonicalizer` when `src/util/job_hash.py` is implemented.

### Normalization (D10), in order

1. Unwrap `Feature` into its geometry, and drop `properties` / `id` / `bbox`.
2. Promote `Polygon` to `MultiPolygon`.
3. Truncate positions to `(lon, lat)`, dropping Z (`shpjs` emits Z for PolygonZ shapefiles).
4. Orient rings per RFC 7946 (`shapely.geometry.polygon.orient`).
5. Quantize to integer micro-degrees.

The starting vertex of a ring is deliberately **not** normalized.

### Micro-degrees

```
micro = floor(v * 1_000_000 + 0.5)
```

Write `floor(x + 0.5)` literally. Don't swap in `round` (banker's rounding) or `f"{v:.6f}"`. The
formula can't cross an integer anywhere in [-180, 180], so any "fix" is a regression. The tie
vectors are there to stop someone "fixing" it.

### `fire_event_name`

`FireEventName = Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]+$", max_length=...)]` (D20),
applied at every entry point: `ProcessingRequest`, `RefineRequest`, `VegMapResolveRequest`,
`GeoJSONUploadRequest`, the `upload_shapefile` Form field, and the three result path params.
Validate only, never transform — D8's derivation rule is a client-side concern.

`max_length` is **not yet chosen** — filled in by `be-name-validation`. It must stay well under
~900 (GCS's 1024-byte object-name cap minus a ~70-byte suffix) and comfortably above the
~30-character names validation sends.

### Vectors

Pinned and committed at `tests/fixtures/job_hash_vectors.json` (`be-vectors`), carrying
`pipeline_version`, run in this repo's CI against `ProcessingRequest.model_validate(request_body)`
→ `job_id_from_request`, so validation and normalization are pinned too.

**Must collapse to the same id:** a bare `Polygon` vs the same geometry wrapped in a `Feature`; a
`Feature` with `properties`/`id`/`bbox` populated; a `Polygon` vs a `MultiPolygon` of the same
ring; CW vs CCW winding; 2-element vs 3-element (Z) positions; `sensor` omitted vs an explicit
`"sentinel-2"`; coordinates differing below 1e-6; a coordinate sent as a numeric string vs as a
number (model coercion); an unknown extra key vs none.

**Must produce different ids:** `sentinel-2` vs `landsat`; two `fire_event_name` values;
coordinates differing at 1e-6; pre/post date ranges swapped; a ring rotated to a different
starting vertex (deliberately pinned).

**Numeric and structural pins:** `0.0078125` → `7813` and `-0.0078125` → `-7812`; `180.0` →
`180000000` and `-90.0` → `-90000000`; `-116.0` → `-116000000`, serialized as an int; a coordinate
near zero; `"2023-06-01"` vs `"2023-06-01T00:00:00"` → different ids (dates hashed verbatim, a
deliberate over-distinction); a polygon with a hole (ring order significant); a two-polygon
`MultiPolygon` (nesting depth 3).
