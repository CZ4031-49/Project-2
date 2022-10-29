from __future__ import annotations

import json
import re
import ast
from constants import CONSTANTS


def read_log_file(file_name):
    parsed_log_lines = []
    with open(file_name, "r") as log_file:
        for line in log_file.readlines():
            full_pattern_regex = f"DEBUG:root:{CONSTANTS.LOG_FILE_FULL_PLAN_PREFIX} "
            if re.search(full_pattern_regex, line):
                line = line[len(full_pattern_regex):]
                parsed_log_lines.append(line)
    return parsed_log_lines


log_to_test = read_log_file("preprocessing.log")
first_complete_plan_str = log_to_test[0]
first_complete_plan = ast.literal_eval(first_complete_plan_str)

print(len(first_complete_plan))
for plan in first_complete_plan:
    json_dumped_str = json.dumps(plan, sort_keys=True, indent=4)
    print(json_dumped_str)


def chop_plan_dict(plan_in: dict,
                   what_to_remain: list | tuple = (CONSTANTS.NODE_TYPE_NAME,
                                                   CONSTANTS.INTERMEDIATE_PLAN_NAME,
                                                   CONSTANTS.OUTER_PLAN_NAME,
                                                   CONSTANTS.RELATION_NAME)):
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
            chop_keys(cur_plan)
            if isinstance(plans, (list, tuple)):
                for next_plan in plans:
                    # should be a _iter of dicts
                    frontier.append(next_plan)
            else:
                frontier.append(plans)
        except KeyError:
            # first try is this a root relation
            try:
                root_plan = cur_plan[CONSTANTS.OUTER_PLAN_NAME]
                # if sucessful here, should be a root
                chop_keys(cur_plan)
                frontier.append(root_plan)
            except KeyError:
                chop_keys(cur_plan)
                print(f"there's neither {CONSTANTS.OUTER_PLAN_NAME} nor {CONSTANTS.INTERMEDIATE_PLAN_NAME} "
                      f"attribute in this plan, it's either a leaf or error.")


try:
    plan0 = first_complete_plan[0]
except Exception as e:
    print(e)
    plan0 = first_complete_plan
plan0: dict
p = plan0.copy()
chop_plan_dict(p)
json_dumped_str = json.dumps(p, sort_keys=True, indent=4)
print(json_dumped_str)


