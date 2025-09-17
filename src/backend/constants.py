DEFAULT_MAX_COMMITMENT_LOOKBACK = 360
DEFAULT_CHAIN_SYNC_INTERVAL = 30
MAX_WORKERS = 4
HEARTBEAT_INTERVAL = 30

#  Yield control every N blocks to prevent blocking WebSocket connections
CHAIN_SCAN_YIELD_INTERVAL = 2

# API Pagination constants
DEFAULT_PAGE_LIMIT = 100
MAX_PAGE_LIMIT = 1000
MIN_PAGE_LIMIT = 1

BACKEND_PORT = 8080

# Competition scoring thresholds
DEFAULT_MIN_AVG_REWARD = 0.0
DEFAULT_WIN_MARGIN_PCT = 0.05  # 5% margin required to win

# Scoring and weight setting intervals (in seconds)
SCORE_EVALUATION_INTERVAL = 300  # 5 minutes
WEIGHT_BROADCAST_INTERVAL = 600  # 10 minutes

# Task startup delays (in seconds)
SCORE_EVALUATION_STARTUP_DELAY = 10
WEIGHT_BROADCAST_STARTUP_DELAY = 10
