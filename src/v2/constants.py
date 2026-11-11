"""Constants that define the severity v2 pipeline's identity, limits and deployment."""

# Part of every job id and recorded in every published item. Bump it whenever
# the output for an identical request would change: an index formula, the
# compositing, the mask, the grid rules, or DEFAULT_SENSOR.
PIPELINE_VERSION = 1

# Environment variables the deploy sets, reported by GET /version.
ENV_GIT_SHA = "GIT_SHA"
ENV_IMAGE = "SEVERITY_IMAGE"

# Request limits. Placeholders until there is usage to size them (design §6).
FIRE_EVENT_NAME_MAX_LENGTH = 128
MAX_WINDOW_DAYS = 90
MAX_BUFFERED_MEGAPIXELS = 100.0

# The snapped tiles are about this fraction of the buffered box's longer side.
# A coarser snap keeps more keys stable but adds more area (design §6). Part of
# the key's definition: changing it re-keys every analysis.
SNAP_TILES_PER_SIDE = 8

# Concurrency and access (design §6). Placeholders, like the limits above.
# fire-recovery-iac's severity-v2-quota on josh's cluster admits exactly this many
# Jobs at the gke-josh profile's requests, so change the quota if either changes.
MAX_ACTIVE_JOBS = 4
ANONYMOUS_STARTS_PER_WINDOW = 10
RATE_WINDOW_SECONDS = 3600
BUSY_RETRY_AFTER_SECONDS = 60

# API configuration.
ENV_TARGET = "SEVERITY_TARGET"  # a target profile (src/v2/k8s.py), or "local"
LOCAL_TARGET = "local"
ENV_API_URL = (
    "SEVERITY_API_URL"  # public base URL for status links, if not the request's
)
