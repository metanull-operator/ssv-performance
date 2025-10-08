# Maximum length of a Discord message. Messages
# will be bundled into chunks less than this length.
MAX_DISCORD_MESSAGE_LENGTH = 2000

# Number of days of operator performance history
# to display for the /operator slash command
OPERATOR_24H_HISTORY_COUNT = 7

# Performance alert thresholds for 24h and 30d performance
# If an operator's performance drops below any of these
# thresholds, an alert will be sent to subscribers
ALERTS_THRESHOLDS_24H = {0.95, 0.75}
ALERTS_THRESHOLDS_30D = {0.98}

# Default number of characters wide a chart should be
# when rendering charts in messages
DEFAULT_NUMBER_OF_SEGMENTS = 20

# Field names in the operator data
FIELD_OPERATOR_ID = 'OperatorID'
FIELD_OPERATOR_NAME = 'Name'
FIELD_IS_VO = 'isVO'
FIELD_IS_PRIVATE = 'isPrivate'
FIELD_VALIDATOR_COUNT = 'ValidatorCount'
FIELD_ADDRESS = 'Address'
FIELD_OPERATOR_UPDATED_AT = 'OperatorUpdatedAt'
FIELD_OPERATOR_REMOVED = 'OperatorRemoved'
FIELD_PERF_DATA_24H = 'Performance24h'
FIELD_PERF_DATA_30D = 'Performance30d'
FIELD_PERFORMANCE = 'Performance'
FIELD_PERFORMANCE_DATE = 'PerformanceDate'
FIELD_OPERATOR_FEE = 'OperatorFee'
FIELD_OPERATOR_FEE_DATE = 'OperatorFeeDate'
FIELD_NETWORK = 'Network'