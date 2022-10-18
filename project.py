import psycopg
import json


class PlannerConfig:
    enable_bitmapscan = "off"
    enable_hashjoin = "off"
    enable_incremental_sort = "off"
    enable_indexscan = "off"
    enable_indexonlyscan = "off"
    enable_material = "off"
    enable_mergejoin = "off"
    enable_nestloop = "off"
    enable_seqscan = "off"
    enable_sort = "off"

    @classmethod
    def init_default_config(cls):
        statements = [
            cls.disable_statement("enable_bitmapscan"),
            cls.disable_statement("enable_hashjoin"),
            cls.disable_statement("enable_incremental_sort"),
            cls.disable_statement("enable_indexscan"),
            cls.disable_statement("enable_indexonlyscan"),
            cls.disable_statement("enable_material"),
            cls.disable_statement("enable_mergejoin"),
            cls.disable_statement("enable_nestloop"),
            cls.disable_statement("enable_seqscan"),
            cls.disable_statement("enable_sort"),
        ]
        return statements

    @classmethod
    def enable_statement(cls, setting):
        return f"SELECT set_config('{setting}', 'on', true)"

    @classmethod
    def disable_statement(cls, setting):
        return f"SELECT set_config('{setting}', 'off', true)"


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


def selection_planner(query):
    connector = Connector("dbname", "user", "host", "port", "password")
    pc = PlannerConfig()
    with connector.connect() as conn:
        with conn.cursor() as cur:
            config = pc.init_default_config()
            for statement in config:
                cur.execute(statement)

            cur.execute(pc.enable_statement("enable_bitmapscan"))
            cur.execute(query)
            print(json.dumps(cur.fetchall(), indent=4))
            cur.execute(pc.disable_statement("enable_bitmapscan"))

            cur.execute(pc.enable_statement("enable_indexonlyscan"))
            cur.execute(query)
            print(json.dumps(cur.fetchall(), indent=4))
            cur.execute(pc.disable_statement("enable_indexonlyscan"))

            cur.execute(pc.enable_statement("enable_indexscan"))
            cur.execute(query)
            print(json.dumps(cur.fetchall(), indent=4))
            cur.execute(pc.disable_statement("enable_indexscan"))

            cur.execute(pc.enable_statement("enable_seqscan"))
            cur.execute(query)
            print(json.dumps(cur.fetchall(), indent=4))
            cur.execute(pc.disable_statement("enable_seqscan"))


def main():
    query = "EXPLAIN (FORMAT JSON) SELECT * FROM customer WHERE c_custkey = 100"
    selection_planner(query)


if __name__ == "__main__":
    main()
