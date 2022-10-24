import psycopg
import json
import logging


class PlannerConfig:
    def __init__(self) -> None:
        self.settings = {
            "enable_bitmapscan": "off",
            "enable_indexscan": "off",
            "enable_indexonlyscan": "off",
            "enable_seqscan": "off",
            "enable_tidscan": "off",
            "enable_mergejoin": "off",
            "enable_nestloop": "off",
            "enable_hashjoin": "off",
            "enable_incremental_sort": "off",
            "enable_material": "off",
            "enable_sort": "off",
        }

    def get_config_statements(self):
        statements = []
        for k, v in self.settings.items():
            if v == "on":
                statements.append(self.enable_statement(k))
            else:
                statements.append(self.disable_statement(k))

        return statements

    def get_best_plan_statements(self):
        statements = []
        for k, _ in self.settings.items():
            statements.append(self.enable_statement(k))

        return statements

    def enable_statement(self, setting):
        return f"SELECT set_config('{setting}', 'on', true)"

    def disable_statement(self, setting):
        return f"SELECT set_config('{setting}', 'off', true)"

    def toggle_setting(self, setting: str, val: str):
        self.settings[setting] = val


class Connector:
    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string

    def connect(self):
        return psycopg.connect(self.connection_string)


class Executor:
    def __init__(self, connection_string: str) -> None:
        self.connector = Connector(connection_string)
        self.pc = PlannerConfig()

    def configure_connector(self, connection_string: str):
        self.connector = Connector(connection_string)

    def disable_setting(self, setting: str):
        self.pc.toggle_setting(setting, "off")

    def enable_setting(self, setting: str):
        self.pc.toggle_setting(setting, "on")

    def execute_with_options(self, query: str):
        options = self.pc.get_config_statements()
        with self.connector.connect() as conn:
            with conn.cursor() as cur:
                for option in options:
                    cur.execute(option)

                cur.execute(query)
                res = cur.fetchone()
                # print(json.dumps(res, indent=4))
                if res is not None:
                    return res[0][0]
                return res

    def execute_best_plan(self, query: str):
        options = self.pc.get_best_plan_statements()
        with self.connector.connect() as conn:
            with conn.cursor() as cur:
                for option in options:
                    cur.execute(option)

                cur.execute(query)
                res = cur.fetchone()
                # print(json.dumps(res, indent=4))
                if res is not None:
                    return res[0][0]
                return res


class Preprocessor:
    def __init__(self, connection_string) -> None:
        self.e = Executor(connection_string)
        self.selection_order = [
            "enable_bitmapscan",
            "enable_indexscan",
            "enable_indexonlyscan",
            "enable_seqscan",
            "enable_tidscan",
        ]
        self.join_order = ["enable_nestloop", "enable_hashjoin", "enable_mergejoin"]

    def runner(self, query):
        query = "EXPLAIN (FORMAT JSON) " + query
        best = self.get_best_plan(query)
        second = self.get_second_best_plan(query)
        logging.debug(json.dumps(best, indent=4))
        logging.debug(json.dumps(second, indent=4))
        return [best, second]

    def get_best_plan(self, query):
        res = self.e.execute_best_plan(query)
        return res

    def get_second_best_plan(self, query: str):
        plans = self.selection_planner(query)
        cost_and_index = []
        for i in range(len(plans)):
            cost = self.get_node_cost(plans[i], ["Scan"])
            cost_and_index.append((cost, i))

        sorted_by_cost = sorted(cost_and_index)
        second_best_scan = self.selection_order[sorted_by_cost[1][1]]

        self.e.enable_setting(second_best_scan)
        plans = self.join_planner(query)
        cost_and_index = []
        for i in range(len(plans)):
            cost = self.get_node_cost(plans[i], ["Nested", "Join"])
            cost_and_index.append((cost, i))
        sorted_by_cost = sorted(cost_and_index)
        second_best_join = self.join_order[sorted_by_cost[1][1]]

        self.e.enable_setting(second_best_join)
        second_best_plan = self.e.execute_with_options(query)
        return second_best_plan

    def get_node_cost(self, plan, nodes):
        cost = 0
        current_plan = plan["Plan"]
        current_level = [current_plan]

        while len(current_level):
            current_plan = current_level.pop()
            for node in nodes:
                if node in current_plan["Node Type"]:
                    cost += current_plan["Total Cost"]
                    break

            if "Plans" in current_plan:
                for plan in current_plan["Plans"]:
                    current_level.append(plan)

        return cost

    def selection_planner(self, query):
        plans = []

        for setting in self.selection_order:
            self.e.enable_setting(setting)
            plans.append(self.e.execute_with_options(query))
            self.e.disable_setting(setting)

        return plans

    def join_planner(self, query):
        plans = []
        for setting in self.join_order:
            self.e.enable_setting(setting)
            plans.append(self.e.execute_with_options(query))
            self.e.disable_setting(setting)

        return plans
