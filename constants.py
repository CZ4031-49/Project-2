class CONSTANTS:
    LOG_FILE_FULL_PLAN_PREFIX = "Plans output:"

    OUTER_PLAN_NAME = "Plan"
    INTERMEDIATE_PLAN_NAME = "Plans"
    NODE_TYPE_NAME = "Node Type"
    PLAN_ROW_SIZE_NAME = "Plan Rows"
    PLAN_WIDTH_SIZE_NAME = "Plan Width"
    PARALLEL_AWARE_NAME = "Parallel Aware"
    ASYNC_CAPABILITY_NAME = "Async Capable"

    RELATION_NAME = "Relation Name"
    START_COST_NAME = "Startup Cost"
    TOTAL_COST_NAME = "Total Cost"

    ALIAS_NAME = "Alias"

    node_type_mapping_to_sql_keyword = {"Limit": "LIMIT"}

    # join constants
    JOIN_TYPE_NAME = "Join Type"

    # scan constants
    INDEX_COND_NAME = "Index Cond"
    INDEX_NAME = "Index Name"
    PARENT_RELATIONSHIP_NAME = "Parent Relationship"
    SCAN_DIRECTION_NAME = "Scan Direction"