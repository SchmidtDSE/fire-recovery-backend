"""Constants that define the severity v2 pipeline's identity and deployment."""

# Part of every job id and recorded in every published item. Bump it whenever
# the output for an identical request would change: an index formula, the
# compositing, the mask, the grid rules, or DEFAULT_SENSOR.
PIPELINE_VERSION = 1

# Environment variables the deploy sets, reported by GET /version.
ENV_GIT_SHA = "GIT_SHA"
ENV_IMAGE = "SEVERITY_IMAGE"
