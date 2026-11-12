"""Constants that define the severity v2 pipeline's identity, limits and deployment."""

# Part of every job id and recorded in every published item. Bump it whenever
# the output for an identical request would change: an index formula, the
# compositing, the mask, the grid rules, the scene selection, or DEFAULT_SENSOR.
# 2: one processing per Sentinel-2 acquisition, each scene scaled with its own
# offset (2026-11). 3: an observation is clear only if both reflectances are
# above 0, as designed; DN 1-1000 under the -0.1 offset no longer pass (2026-11).
PIPELINE_VERSION = 3

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
# Without an API key, an address may start this many analyses per window. The
# count lives in the API's memory: exact with its one instance, reset by a deploy
# or by the instance stopping, which is why the dev service keeps one warm.
ANONYMOUS_STARTS_PER_WINDOW = 10
RATE_WINDOW_SECONDS = 72 * 3600
BUSY_RETRY_AFTER_SECONDS = 60

# API configuration.
ENV_TARGET = "SEVERITY_TARGET"  # a target profile (src/v2/service/k8s.py), or "local"
LOCAL_TARGET = "local"
ENV_API_URL = (
    "SEVERITY_API_URL"  # public base URL for status links, if not the request's
)
