import psycopg
import json


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
                print(json.dumps(res, indent=4))
                # do sth with the json -> get the best and 2nd best plan?

    def execute_best_plan(self, query: str):
        options = self.pc.get_best_plan_statements()
        with self.connector.connect() as conn:
            with conn.cursor() as cur:
                for option in options:
                    cur.execute(option)

                cur.execute(query)
                res = cur.fetchone()
                print(json.dumps(res, indent=4))


class Preprocessor:
    def __init__(self, connection_string) -> None:
        self.e = Executor(connection_string)

    def selection_planner(self, query):
        self.e.execute_best_plan(query)

        self.e.enable_setting("enable_bitmapscan")
        self.e.execute_with_options(query)
        self.e.disable_setting("enable_bitmapscan")

        self.e.enable_setting("enable_indexscan")
        self.e.execute_with_options(query)
        self.e.disable_setting("enable_indexscan")

        self.e.enable_setting("enable_indexonlyscan")
        self.e.execute_with_options(query)
        self.e.disable_setting("enable_indexonlyscan")

        self.e.enable_setting("enable_seqscan")
        self.e.execute_with_options(query)
        self.e.disable_setting("enable_seqscan")

        self.e.enable_setting("enable_tidscan")
        self.e.execute_with_options(query)
        self.e.disable_setting("enable_tidscan")

    def join_planner(self, query):
        self.e.enable_setting("enable_hashjoin")
        self.e.execute_with_options(query)
        self.e.disable_setting("enable_hashjoin")

        self.e.enable_setting("enable_mergejoin")
        self.e.execute_with_options(query)
        self.e.disable_setting("enable_mergejoin")

        self.e.enable_setting("enable_nestloop")
        self.e.execute_with_options(query)
        self.e.disable_setting("enable_nestloop")
