from __future__ import annotations

import json
import re

import ast
import sqlparse
from sqlparse.sql import *
import re


from typing import Any

# from utilities_and_parsing import *


# from utilities_and_parsing import *

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


def read_log_file(file_name):
    parsed_log_lines = []
    with open(file_name, "r") as log_file:
        for line in log_file.readlines():
            full_pattern_regex = f"DEBUG:root:{CONSTANTS.LOG_FILE_FULL_PLAN_PREFIX} "
            if re.search(full_pattern_regex, line):
                line = line[len(full_pattern_regex):]
                parsed_log_lines.append(line)
    return parsed_log_lines


def chop_plan_dict(plan_in: dict,
                   what_to_remain: list | tuple = (CONSTANTS.NODE_TYPE_NAME,
                                                   CONSTANTS.INTERMEDIATE_PLAN_NAME,
                                                   CONSTANTS.OUTER_PLAN_NAME,
                                                   CONSTANTS.EXPLANATION
                                                 #  CONSTANTS.RELATION_NAME,
                                                 #  CONSTANTS.FILTER_NAME,
                                                 #  CONSTANTS.JOIN_FILTER_NAME,
                                                   )):
    """
    view a summary based on selected attributes, and this function does an easy job inplace. remember
    to copy your dict and since this is inplace to visualize should still dump as json or sth else
    :param plan_in: plan dict to summarize
    :param what_to_remain: what attribute to summarize
    :return: None
    """

    def chop_keys(x):
        list_of_keys = tuple(cur_plan.keys())
        for key in list_of_keys:
            if key not in what_to_remain:
                x.pop(key)

    frontier = [plan_in]
    while len(frontier) > 0:
        cur_plan = frontier.pop()
        try:
            plans = cur_plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]
         #   print(cur_plan['Node Type'])
            chop_keys(cur_plan)
            if isinstance(plans, (list, tuple)):
                for next_plan in plans:
                    # should be a _iter of dicts
                    frontier.append(next_plan)
              #      print(next_plan['Node Type'])
            else:
                frontier.append(plans)
            #    print(plans['Node Type'])
        except KeyError:
            # first try is this a root relation
            try:
                root_plan = cur_plan[CONSTANTS.OUTER_PLAN_NAME]
                # if sucessful here, should be a root
                chop_keys(cur_plan)
                frontier.append(root_plan)
               # print(root_plan['Node Type'])
            except KeyError:
                chop_keys(cur_plan)
              #  print(cur_plan)
                print(f"there's neither {CONSTANTS.OUTER_PLAN_NAME} nor {CONSTANTS.INTERMEDIATE_PLAN_NAME} "
                      f"attribute in this plan, it's either a leaf or error.")


def parse_sql_query_with_categories(query: str) -> dict[str, list]:
    parsed = sqlparse.parse(query)[0]

    tokens = {"all": [], "identifiers": [], }
    for idx, elem in enumerate(parsed.tokens):
        elem: sqlparse.sql.Token | sqlparse.sql.Identifier | sqlparse.sql.Comparison
        if isinstance(elem, sqlparse.sql.Identifier):
            tokens["identifiers"].append(elem)
        tokens["all"].append(elem)

    return tokens


def map_node_type_to_operation(node_type: str) -> str:
    if node_type in CONSTANTS.SCANTYPES:
        return CONSTANTS.SCAN
    elif node_type in CONSTANTS.JOINTYPES:
        return CONSTANTS.JOIN
    elif node_type in CONSTANTS.EMITTYPES:
        return CONSTANTS.EMIT
    elif node_type in CONSTANTS.AUXTYPES:
        return CONSTANTS.AUX
    else:
        raise ValueError(f"unidentified node type: {node_type}")


def annotate_query_plan(plan: dict) -> None:
    assert isinstance(plan, dict), f"plan is {type(plan)} instead of dict"
    if CONSTANTS.OUTER_PLAN_NAME in plan:
        raw_plan = plan[CONSTANTS.OUTER_PLAN_NAME]
    else:
        raw_plan = plan
    if CONSTANTS.INTERMEDIATE_PLAN_NAME not in raw_plan:
        operation = CONSTANTS.SCAN
        _explain_node(raw_plan, operation)
    else:
        for next_plan in raw_plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]:
            annotate_query_plan(next_plan)
        try:
            operation = map_node_type_to_operation(raw_plan[CONSTANTS.NODE_TYPE_NAME])
        except ValueError as e:
            print(f"current plan: \n"
                  f"{json.dumps(raw_plan, indent=4, sort_keys=True)}\n"
                  f"throws error: {e}")
            """
            unsupported nodes:  (remember to add here if you see new ones)
            """
            raw_plan[CONSTANTS.EXPLANATION] = f"Node type: {raw_plan[CONSTANTS.NODE_TYPE_NAME]} " \
                                              f"is not supported upon this project scope."
            cur_contain_relation = []
            for child in raw_plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]:
                cur_contain_relation += child[CONSTANTS.CONTAINRELATION]
            raw_plan[CONSTANTS.CONTAINRELATION] = cur_contain_relation
            return
        _explain_node(raw_plan, operation)


def _explain_node(plan: dict, node_type: str) -> None:
    explanation = f"Node type: {plan[CONSTANTS.NODE_TYPE_NAME]}: \n" 
    # if plan[CONSTANTS.NODE_TYPE_NAME] == "Limit":
    #     print("debug")
    #     print(node_type)
    if node_type == CONSTANTS.EMIT:
        for child in plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]:
            if CONSTANTS.HASCHILDWITHINDEX in child:
                plan[CONSTANTS.HASCHILDWITHINDEX] = True
        if plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.MATERIALIZE:
            # heuristic explanation, not so sure is there other case to choose materialize model,
            # as if self defined cost
            explanation += f"Operation {CONSTANTS.MATERIALIZE} emits intermediate result from previous node(s) " \
                           f"{[x[CONSTANTS.NODE_TYPE_NAME] for x in plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]]}.\n" \
                           f"It has expected size {plan[CONSTANTS.PLAN_ROW_SIZE_NAME]}, " \
                           f"as it is a small batch considering database capacity, " \
                           f"materialize emit is the best to handle this intermediate result " \
                           f"(as this batch size can be emitted at once)."
        this_contain_relation = []
        for child in plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]:
            this_contain_relation += child[CONSTANTS.CONTAINRELATION]
        plan[CONSTANTS.CONTAINRELATION] = this_contain_relation

    elif node_type == CONSTANTS.JOIN:
        if plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.HASHJOIN:
            assert len(plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]) == 2, f"a hash join node has operand number " \
                                                                     f"{len(plan[CONSTANTS.INTERMEDIATE_PLAN_NAME])} " \
                                                                     f"instead of 2"
            l_size = plan[CONSTANTS.INTERMEDIATE_PLAN_NAME][0][CONSTANTS.PLAN_ROW_SIZE_NAME]
            r_size = plan[CONSTANTS.INTERMEDIATE_PLAN_NAME][1][CONSTANTS.PLAN_ROW_SIZE_NAME]
            explanation += f"A hash join is selected here since two operands can be considered 'big' " \
                           f"(with size {l_size} and {r_size}. Furthermore, both operands do not have well-formulated" \
                           f"indices on join attributes (eg. indices are non-clustered and thus for join with two " \
                           f"similar size relations index join has high cost).\n" \
                           f"A hash join can also be selected as this is the most efficient way if both relations " \
                           f"do not have efficient index and buffer size for this join is small."
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.NL:
            # if it's chained with index scan at right side it's as fetching right side index,
            # as index based join
            has_index_scan_child = False
            both_has_index = True
            child_without_index_size = []
            for child in plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]:
                if CONSTANTS.HASCHILDWITHINDEX in child:
                    child[CONSTANTS.EXPLANATION] += f"\nThis node that has index scan in it child nodes is used by " \
                                                    f"its direct parent node as nes" \
                                                    f"ted loop join"
                    if CONSTANTS.JOIN_FILTER_NAME in plan:
                        child[CONSTANTS.EXPLANATION] += f", with a condition {plan[CONSTANTS.JOIN_FILTER_NAME]}."
                    else:
                        child[CONSTANTS.EXPLANATION] += f"."
                    has_index_scan_child = True
                else:
                    both_has_index = False
                    child_without_index_size.append(child[CONSTANTS.PLAN_ROW_SIZE_NAME])
            if both_has_index:
                explanation += f"This nested loop join has two operands that does index scan on original relation, " \
                               f"hence this nested loop is a zig-zag join on both indices"
                if CONSTANTS.JOIN_FILTER_NAME in plan:
                    explanation += f", with" \
                                   f" this filter: {plan[CONSTANTS.JOIN_FILTER_NAME]}. "
                else:
                    explanation += f"."

            elif has_index_scan_child:
                assert len(child_without_index_size) == 1, f"child without index size has length neq to 1 " \
                                                           f"({len(child_without_index_size)}), in node " \
                                                           f"{plan[CONSTANTS.NODE_TYPE_NAME]}."
                explanation += f"Only one operand of this index join has index scan on original relation, " \
                               f"and the one " \
                               f"without using index has size {child_without_index_size[0]}.Without knowing distinct " \
                               f"values on join condition for the operand that uses index, since this size is small, " \
                               f"heuristically index join performs better as only few index fetches are expected on " \
                               f"relation with index. "
            else:
                child_sizes = []
                for child in plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]:
                    child_sizes.append(child[CONSTANTS.PLAN_ROW_SIZE_NAME])
                assert len(child_sizes) == 2, f"child of a join has number neq to 2 " \
                                              f"({len(child_sizes)}), in node " \
                                              f"{plan[CONSTANTS.NODE_TYPE_NAME]}."
                explanation += f"Both operands don't use index scans on their original relations, this is possible " \
                               f"especially for deep join trees with lots of intermediate results. In this case a " \
                               f"nested loop join is preferred possibly because size of two operands are both small. " \
                               f"\nSize of two operands: {child_sizes}\n"
                if CONSTANTS.JOIN_FILTER_NAME in plan:
                    explanation += f"A block based nested loop is assumed with join " \
                                   f"filter {plan[CONSTANTS.JOIN_FILTER_NAME]} "
                else:
                    pass
            if CONSTANTS.JOIN_FILTER_NAME in plan:
                if not re.search("=", plan[CONSTANTS.JOIN_FILTER_NAME]):
                    explanation += f"\nThis join has an inequality condition {plan[CONSTANTS.JOIN_FILTER_NAME]} so " \
                                   f"only nested loop join can handle this condition well."
            else:
                explanation += f"\n This nested loop join has no filter, it can because either conditions " \
                               f"are pushed down to scans or relations are small so a NL join is preferred."
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.INDEXJOIN:
            explanation += f"A join based on two well-formulated index.\n" \
                           f"Operation can thus directly manipulate indices instead of probing tuples, " \
                           f"and only retrieve tuples based on indices of relations " \
                           f"({plan[CONSTANTS.CONTAINRELATION]})."
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.SORTMERGEJOIN:
            explanation += f"Sort merge join is only selected when both operands are sorted and " \
                           f"join filter ({plan[CONSTANTS.JOIN_FILTER_NAME]}) is an equality."
            for child in plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]:
                if child[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.SORT:
                    child[CONSTANTS.EXPLANATION] += f"\nThis sort is used by its intermediate sort merge join " \
                                                    f"parent node with join filter " \
                                                    f"{plan[CONSTANTS.JOIN_FILTER_NAME]}."
                else:
                    child[CONSTANTS.EXPLANATION] += f"\nThis node has a direct sort merge parent with " \
                                                    f"join filter {plan[CONSTANTS.JOIN_FILTER_NAME]}, hence this " \
                                                    f"relation is already sorted as additional information."
                    if len(plan[CONSTANTS.CONTAINRELATION]) == 1:
                        child[CONSTANTS.EXPLANATION] += f"\nThis implies that original relation " \
                                                        f"({child[CONSTANTS.CONTAINRELATION]}) " \
                                                        f"is already retrieved in order."
                    else:
                        child[CONSTANTS.EXPLANATION] += f"\nThis node comes from at least two relations " \
                                                        f"({child[CONSTANTS.CONTAINRELATION]}), hence this order " \
                                                        f"is from a previous sort node on intermediate result."
        this_contain_relation = []
        for child in plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]:
            try:
                this_contain_relation += child[CONSTANTS.CONTAINRELATION]
            except KeyError as e:
                print(f"plan: \n"
                      f"{json.dumps(plan, sort_keys=True, indent=4)}\n"
                      f"throws key error {e}")
                raise
        plan[CONSTANTS.CONTAINRELATION] = this_contain_relation
    elif node_type == CONSTANTS.SCAN:
        if plan[CONSTANTS.NODE_TYPE_NAME] in (CONSTANTS.INDEXSCAN, CONSTANTS.INDEXONLYSCAN):
            explanation += f"This is an {plan[CONSTANTS.NODE_TYPE_NAME]} node.\n"
            if CONSTANTS.FILTER_NAME in plan:
                # there's an index filter
                explanation += f"There is a condition {plan[CONSTANTS.FILTER_NAME]} that can be pushed down the plan " \
                               f"tree specifically for this relation {plan[CONSTANTS.RELATION_NAME]}.\nWith this " \
                               f"condition, index scan is selected since either there's an efficient eg. clustered " \
                               f"index on condition attribute(s), or expected output from scan is small enough (in " \
                               f"this case expected output tuples = {plan[CONSTANTS.PLAN_ROW_SIZE_NAME]}).\nSince " \
                               f"database capability can varies a lot, it is difficult for this annotatorto " \
                               f"specifically decide which one is the most likely reason, or both. "
            else:
                # there's no filter, it is likely that this is for a following join
                explanation += f"There is no condition on {plan[CONSTANTS.RELATION_NAME]} index scan. Without knowing " \
                               f"predecessors of this node, this index scan is likely " \
                               f"to be selectedas there is likely " \
                               f"to be a join that has condition on this relation, " \
                               f"or has build relation very small so " \
                               f"a join fetching probe relation index is prefered. "
            plan[CONSTANTS.HASCHILDWITHINDEX] = True
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.BITMAPSCAN:
            explanation += f"Bitmap scan is a hybrid mode of index scan and sequential scan, here it is selected " \
                           f"because {plan[CONSTANTS.PLAN_ROW_SIZE_NAME]} is too big for index scan (which incurs " \
                           f"random I/O cost so size should be small) and too small for sequential scan."
            plan[CONSTANTS.HASCHILDWITHINDEX] = True
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.BITMAPHEAPSCAN:
            assert len(plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]) == 1, f"a bitmap scan node has child with length " \
                                                                     f"{len(plan[CONSTANTS.INTERMEDIATE_PLAN_NAME])} " \
                                                                     f"instead of 1."
            assert plan[CONSTANTS.INTERMEDIATE_PLAN_NAME][0][CONSTANTS.NODE_TYPE_NAME] == \
                   CONSTANTS.BITMAPINDEXSCAN, f"a bitmap scan node has child of " \
                                              f"{plan[CONSTANTS.INTERMEDIATE_PLAN_NAME][CONSTANTS.NODE_TYPE_NAME]} " \
                                              f"instead of a bitmap index scan. "
            # contain relation attribute, always followed by bitmap heap scan
            explanation += f"A bitmap heap scan is always a direct parent of bitmap index scan.\n" \
                           f"It uses index bitmap generated by condition " \
                           f"{plan[CONSTANTS.BITMAPHEAPSCAN_CONDITION_NAME]} and fetch corresponding pages " \
                           f"stored in the heap."
            plan[CONSTANTS.HASCHILDWITHINDEX] = True
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.BITMAPINDEXSCAN:
            # doesn't contain relation attribute
            explanation += f"A bitmap index scan is used to generate a bitmap based on indices of selected " \
                           f"attribute(s).\n" \
                           f"It has condition {plan[CONSTANTS.INDEX_CONDITION_NAME]}.\n" \
                           f"Bit map index scan always has a bitmap heap scan node as its immediate parent node."
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.SEQSCAN:
            explanation += f"As sequential scan uses no auxiliary utilities of this relation: " \
                           f"{plan[CONSTANTS.RELATION_NAME]}, without looking parent nodes, " \
                           f"it is either because this relation is small that using index is costly, " \
                           f"or there's a following hash join that utilise this sequential scan, " \
                           f"or later join operation selects most parts of this relation as join candidate tuples."
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.TIDSCAN:
            """TID scan is a very specific kind of scan in PostgreSQL and gets selected only if there is TID in the 
            query predicate. """
            explanation += f"TID scan is specific to PostgreSQL and only gets selected if query specifies " \
                           f"a TID predicate."
        try:
            if plan[CONSTANTS.NODE_TYPE_NAME] not in (CONSTANTS.BITMAPINDEXSCAN):
                plan[CONSTANTS.CONTAINRELATION] = [plan[CONSTANTS.RELATION_NAME]]
            else:
                plan[CONSTANTS.CONTAINRELATION] = []
        except KeyError as e:
            print(f"current plan: \n"
                  f"{json.dumps(plan, indent=4, sort_keys=True)}\n"
                  f"throws key error: \n"
                  f"{e}")
            raise
    elif node_type == CONSTANTS.AUX:
        if plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.LIMIT:
            """
            Limit nodes get added to the plan tree if the “limit/offset” clause is used in the SELECT query. 
            """
            explanation += f"This node is added in one-to-one correspondence of a LIMIT or OFFSET clause " \
                           f"in the SELECT query."
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.SORT:
            # be careful about sort merge join, remember to incl check for sort
            # node with a sort merge join
            explanation += f"A node that sorts this intermediate result.\n" \
                           f"This is because either there is ORDER BY clause in query, or " \
                           f"there is a following sort-merge join."
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.UNIQUE:
            """Note that depending on the query, selectivity and other resource info, the distinct value can be 
            retrieved using HashAggregate/GroupAggregate also without using Unique node. """
            explanation += f"A node that generate a set of rows from its input. This is equivalent to " \
                           f"a UNIQUE filter.\n" \
                           f"Notice that AGGREGATION can also produce unique results by their nature, so " \
                           f"it is unlikely that this node has direct child that is an aggregation."
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.AGGREGATE:
            """Aggregate node gets added as part of a plan tree if there is an aggregate function used to compute 
            single results from multiple input rows. Some of the aggregate functions used are COUNT, SUM, 
            AVG (AVERAGE), MAX (MAXIMUM) and MIN (MINIMUM). """
            explanation = f"A general aggregation node without specifying aggregation method."
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.HASHAGGREGATE or node_type == CONSTANTS.GROUPBYAGGREGATE:
            explanation += f"An aggregation node, instantiated because there exists aggregation specified by " \
                           f"GROUP BY keyword in original query.\n" \
                           f"It additionally specifies aggregate method as {node_type}."
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.LOCKROWS:
            explanation += f"Lock all rows selected in this node."
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.SETOP:
            explanation += f"A node that combines result from multiple queries, usually 2. " \
                           f"It does not specify method to combine such as append and subquery scan."
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.APPEND:
            explanation += f"A node that append subquery intermediate result to its parent. " \
                           f"Used as a sub type of SETOP."
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.SUBQUERYSCAN:
            explanation += f"A node that scans subquery intermediate result, as a " \
                           f"sub kind of SETOP. Used by outer query."
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.MEMOIZE:
            explanation += f"A memoize node caches intermediate result for speed up. It is" \
                           f"an auxiliary node specific to machine."
        this_contain_relation = []
        for child in plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]:
            this_contain_relation += child[CONSTANTS.CONTAINRELATION]
        plan[CONSTANTS.CONTAINRELATION] = this_contain_relation

    else:
        raise NotImplementedError(f"unrecognized node type: {plan[CONSTANTS.NODE_TYPE_NAME]}")
    plan[CONSTANTS.EXPLANATION] = explanation
    return


sample_query = """ SELECT s_address, p_partkey
                FROM supplier s, part p, partsupp ps
                WHERE  
                    ps.ps_partkey=p.p_partkey AND
                    ps.ps_suppkey=s.s_suppkey AND
                    p.p_retailprice<500 AND
                    s.s_phone='800-807-9579'
                """


def annotate_plan(plan_dict: dict[str, Any], use_plan_log=False, log_file_name=None):
    if use_plan_log and log_file_name:
        raw_plans = read_log_file("test_prep.log")
        first_complete_plan_str = raw_plans[0]
        print(first_complete_plan_str)
        first_complete_plan = ast.literal_eval(first_complete_plan_str)
    else:
        first_complete_plan = plan_dict

    json_dumped_str = json.dumps(first_complete_plan, sort_keys=True, indent=4)
    print(json_dumped_str)

    p = first_complete_plan.copy()
    print(p)

    """
    sample query:
    "SELECT * FROM customer c JOIN orders o ON o.o_custkey=c.c_custkey WHERE c.c_custkey < 100 LIMIT 10"
    SELECT *
    FROM customer c, orders o
    WHERE o.o_custkey=c.c_custkey AND
        c.c_custkey < 100 AND
        o.o_custkey > 50 OR
        o.0_custkey = 1
    """

    sample_tokens = parse_sql_query_with_categories(sample_query)

    annotate_query_plan(p)
    json_dumped_str = json.dumps(p, sort_keys=True, indent=4)
    print(json_dumped_str)
            