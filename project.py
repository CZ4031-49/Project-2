import psycopg
import json


class PlannerConfig:
    settings = {
        "enable_bitmapscan": "off",
        "enable_hashjoin": "off",
        "enable_incremental_sort": "off",
        "enable_indexscan": "off",
        "enable_indexonlyscan": "off",
        "enable_material": "off",
        "enable_mergejoin": "off",
        "enable_nestloop": "off",
        "enable_seqscan": "off",
        "enable_sort": "off",
    }

    @classmethod
    def get_config_statements(cls):
        statements = []
        for k, v in cls.settings.items():
            if v == "on":
                statements.append(cls.enable_statement(k))
            else:
                statements.append(cls.disable_statement(k))

        return statements

    @classmethod
    def enable_statement(cls, setting):
        return f"SELECT set_config('{setting}', 'on', true)"

    @classmethod
    def disable_statement(cls, setting):
        return f"SELECT set_config('{setting}', 'off', true)"

    @classmethod
    def toggle_setting(cls, setting: str, val: str):
        cls.settings[setting] = val


class Connector:
    def __init__(self, dbname, user, host, port, password) -> None:
        self.dbname = dbname
        self.user = user
        self.host = host
        self.port = port
        self.password = password

    def connect(self):
        return psycopg.connect(
            f"dbname={self.dbname} user={self.user} host={self.host} port={self.port} password={self.password}"
        )


class Executor:
    connector = Connector("tpc", "postgres", "host", "5432", "pasword")
    pc = PlannerConfig()

    @classmethod
    def disable_setting(cls, setting: str):
        cls.pc.toggle_setting(setting, "off")

    @classmethod
    def enable_setting(cls, setting: str):
        cls.pc.toggle_setting(setting, "on")

    @classmethod
    def execute_with_options(cls, query: str):
        options = cls.pc.get_config_statements()
        with Executor.connector.connect() as conn:
            with conn.cursor() as cur:
                for option in options:
                    cur.execute(option)

                cur.execute(query)
                res = cur.fetchone()
                print(json.dumps(res, indent=4))
                # do sth with the json -> get the best and 2nd best plan?


def selection_planner(query):
    e = Executor()

    e.enable_setting("enable_bitmapscan")
    e.execute_with_options(query)
    e.disable_setting("enable_bitmapscan")

    e.enable_setting("enable_indexscan")
    e.execute_with_options(query)
    e.disable_setting("enable_indexscan")

    e.enable_setting("enable_indexonlyscan")
    e.execute_with_options(query)
    e.disable_setting("enable_indexonlyscan")

    e.enable_setting("enable_seqscan")
    e.execute_with_options(query)
    e.disable_setting("enable_seqscan")


def main():
    query = (
        "EXPLAIN (FORMAT JSON) SELECT * FROM customer WHERE c_custkey < 100 LIMIT 10"
    )
    selection_planner(query)


if __name__ == "__main__":
    main()
