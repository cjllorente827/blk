

TASK_ACTION_OPTIONS = ["auto", "manual"]
AUTO, MANUAL = TASK_ACTION_OPTIONS

TASK_STATUS_OPTIONS = ["complete", "skipped", "failed"]
COMPLETE, SKIP, FAIL = TASK_STATUS_OPTIONS

DEPENDENCY_STRATEGIES = [
    "one-to-one", 
    "all-to-all", 
    "one-to-all", 
    "manual", 
    "none"
]

ONE_TO_ONE, ALL_TO_ALL, ONE_TO_ALL, MANUAL, NONE = DEPENDENCY_STRATEGIES

MAX_SEGMENTS = 10000
ITERATION_LIMIT = 10000