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
