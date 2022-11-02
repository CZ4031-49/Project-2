import sqlparse
from sqlparse.sql import *
import re


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
    JOIN_FILTER_NAME = "Join Filter"

    # scan constants
    FILTER_NAME = "Filter"
    INDEX_COND_NAME = "Index Cond"
    INDEX_NAME = "Index Name"
    PARENT_RELATIONSHIP_NAME = "Parent Relationship"
    SCAN_DIRECTION_NAME = "Scan Direction"

    BITMAPHEAPSCAN_CONDITION_NAME = "Recheck Cond"
    INDEX_CONDITION_NAME = "Index Cond"

    # operation type
    SCAN = "SCAN"
    JOIN = "JOIN"
    EMIT = "EMIT"
    AUX = "AUXILIARY"

    # sub operation types
    # SCAN
    INDEXSCAN = "Index Scan"
    SEQSCAN = "Sequential Scan"
    INDEXONLYSCAN = "Index Only Scan"
    BITMAPSCAN = "Bitmap Scan"
    BITMAPINDEXSCAN = "Bitmap Index Scan"
    BITMAPHEAPSCAN = "Bitmap Heap Scan"
    TIDSCAN = "TID Scan"

    SCANTYPES = (INDEXSCAN, SEQSCAN, INDEXONLYSCAN, BITMAPSCAN, TIDSCAN, BITMAPHEAPSCAN, BITMAPINDEXSCAN)

    # JOIN
    HASHJOIN = "Hash Join"
    INDEXJOIN = "Index Join"
    NL = "Nested Loop"
    SORTMERGEJOIN = "Sort Merge Join"

    JOINTYPES = (HASHJOIN, INDEXJOIN, NL, SORTMERGEJOIN)

    # EMIT
    MATERIALIZE = "Materialize"

    EMITTYPES = (MATERIALIZE)

    # AUX
    LIMIT = "Limit"
    SORT = "Sort"
    AGGREGATE = "Aggregate"
    HASHAGGREGATE = "Hash Aggregate"
    GROUPBYAGGREGATE = "Group By Aggregate"
    UNIQUE = "Unique"
    LOCKROWS = "LockRows"
    SETOP = "SetOp"
    MEMOIZE = "Memoize"

    APPEND = "Append"
    SUBQUERYSCAN = "Subquery Scan"

    AUXTYPES = (LIMIT, SORT, AGGREGATE, HASHAGGREGATE, GROUPBYAGGREGATE, UNIQUE, LOCKROWS, SETOP, MEMOIZE)

    # attribute constant for explanation insertion
    EXPLANATION = "Explanation"

    # status used by annotation to denote few node status
    HASCHILDWITHINDEX = "Has Child With Index"
    CONTAINRELATION = "Contain Relation"

