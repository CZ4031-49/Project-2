from __future__ import annotations

import constants

from utilities_and_parsing import *

sample_query = """ SELECT s_address, p_partkey
                FROM supplier s, part p, partsupp ps
                WHERE  
                    ps.ps_partkey=p.p_partkey AND
                    ps.ps_suppkey=s.s_suppkey AND
                    p.p_retailprice<500 AND
                    s.s_phone='800-807-9579'
                
                
                """

use_plan_log = True

if use_plan_log:
    raw_plans = read_log_file("test_prep.log")
else:
    raise NotImplementedError()

first_complete_plan_str = raw_plans[0]
first_complete_plan = ast.literal_eval(first_complete_plan_str)

for plan in first_complete_plan:
    json_dumped_str = json.dumps(plan, sort_keys=True, indent=4)
    print(json_dumped_str)

try:
    plan0 = first_complete_plan[0]
except Exception as e:
    print(e)
    plan0 = first_complete_plan
plan0: dict
p = plan0.copy()
print(plan0)

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


def annotate_query_plan(plan: dict) -> None:
    if CONSTANTS.OUTER_PLAN_NAME in plan:
        raw_plan = plan[CONSTANTS.OUTER_PLAN_NAME]
    else:
        raw_plan = plan
    if CONSTANTS.INTERMEDIATE_PLAN_NAME not in raw_plan:
        # base file scan case
        relation_name = raw_plan[CONSTANTS.RELATION_NAME]
        scan_type = raw_plan[CONSTANTS.NODE_TYPE_NAME]
        expected_size = raw_plan[CONSTANTS.PLAN_ROW_SIZE_NAME]
        operation = CONSTANTS.SCAN
        explain_node(raw_plan, operation)
    else:
        for next_plan in raw_plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]:
            annotate_query_plan(next_plan)
        operation = map_node_type_to_operation(raw_plan[CONSTANTS.NODE_TYPE_NAME])
        explain_node(raw_plan, operation)


def explain_node(plan: dict, node_type: str) -> None:
    explanation = ''
    if node_type == CONSTANTS.EMIT:
        for child in plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]:
            if CONSTANTS.HASCHILDWITHINDEX in child:
                plan[CONSTANTS.HASCHILDWITHINDEX] = True
        if plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.MATERIALIZE:
            # heuristic explanation, not so sure is there other case to choose materialize model,
            # as if self defined cost
            explanation = f"Operation {CONSTANTS.MATERIALIZE} emits intermediate result from previous node(s) " \
                          f"{[x[CONSTANTS.NODE_TYPE_NAME] for x in plan[CONSTANTS.INTERMEDIATE_PLAN_NAME]]}.\n" \
                          f"It has expected size {plan[CONSTANTS.PLAN_ROW_SIZE_NAME]}, " \
                          f"as it is a small batch considering database capacity, " \
                          f"materialize emit is the best to handle this intermediate result " \
                          f"(as this batch size can be emitted at once)."

    elif node_type == CONSTANTS.JOIN:
        if plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.HASHJOIN:
            pass
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
                                                    f"ted loop join, with a condition " \
                                                    f"{plan[CONSTANTS.JOIN_FILTER_NAME]}. \n "
                    has_index_scan_child = True
                else:
                    both_has_index = False
                    child_without_index_size.append(child[CONSTANTS.PLAN_ROW_SIZE_NAME])
            if both_has_index:
                explanation = f"This nested loop join has two operands that does index scan on original relation, " \
                              f"hence this nested loop is a zig-zag join on both indices with" \
                              f" this filter: {plan[CONSTANTS.JOIN_FILTER_NAME]}. "
            elif has_index_scan_child:
                assert len(child_without_index_size) == 1, f"child without index size has length neq to 1 " \
                                                           f"({len(child_without_index_size)}), in node " \
                                                           f"{plan[CONSTANTS.NODE_TYPE_NAME]}."
                explanation = f"Only one operand of this index join has index scan on original relation, and the one " \
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
                explanation = f"Both operands don't use index scans on their original relations, this is possible " \
                              f"especially for deep join trees with lots of intermediate results. In this case a " \
                              f"nested loop join is preferred possibly because size of two operands are both small. " \
                              f"\nSize of two operands: {child_sizes}\n" \
                              f"A block based nested loop is assumed with join " \
                              f"filter {plan[CONSTANTS.JOIN_FILTER_NAME]} "
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.INDEXJOIN:
            pass
    elif node_type == CONSTANTS.SCAN:
        if plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.INDEXSCAN:
            if CONSTANTS.FILTER_NAME in plan:
                # there's an index filter
                explanation = f"There is a condition {plan[CONSTANTS.FILTER_NAME]} that can be pushed down the plan " \
                              f"tree specifically for this relation {plan[CONSTANTS.RELATION_NAME]}.\nWith this " \
                              f"condition, index scan is selected since either there's an efficient eg. clustered " \
                              f"index on condition attribute(s), or expected output from scan is small enough (in " \
                              f"this case expected output tuples = {plan[CONSTANTS.PLAN_ROW_SIZE_NAME]}).\nSince " \
                              f"database capability can varies a lot, it is difficult for this annotatorto " \
                              f"specifically decide which one is the most likely reason, or both. "
            else:
                # there's no filter, it is likely that this is for a following join
                explanation = f"There is no condition on {plan[CONSTANTS.RELATION_NAME]} index scan. Without knowing " \
                              f"predecessors of this node, this index scan is likely " \
                              f"to be selectedas there is likely " \
                              f"to be a join that has condition on this relation, " \
                              f"or has build relation very small so " \
                              f"a join fetching probe relation index is prefered. "
            plan[CONSTANTS.HASCHILDWITHINDEX] = True
        elif plan[CONSTANTS.NODE_TYPE_NAME] == CONSTANTS.SEQSCAN:
            explanation = f"As sequential scan uses no auxiliary utilities of this relation: " \
                          f"{plan[CONSTANTS.RELATION_NAME]}, without looking parent nodes, " \
                          f"it is either because this relation is small that using index is costly, " \
                          f"or there'sa following hash join that utilise this sequential scan, " \
                          f"or later join operation joins an attribute without cluster index on this table."
    plan[CONSTANTS.EXPLANATION] = explanation
    return


annotate_query_plan(plan0)
json_dumped_str = json.dumps(plan0, sort_keys=True, indent=4)
print(json_dumped_str)
