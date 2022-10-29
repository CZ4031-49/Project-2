import json
import preprocessing
import logging


def main():
    logging.basicConfig(
        filename="preprocessing.log", encoding="utf-8", level=logging.DEBUG
    )
    # query = "SELECT * FROM customer c JOIN orders o ON o.o_custkey=c.c_custkey WHERE c.c_custkey < 100 LIMIT 10"
    query = "SELECT * FROM customer c, orders o WHERE o.o_custkey=c.c_custkey AND c.c_custkey < 100 AND o.o_custkey > 50 OR o.o_custkey = 1"
    conn = "connection string"
    preprocessor = preprocessing.Preprocessor(conn)
    plans = preprocessor.runner(query)
    logging.debug(f"{json.dumps(plans, indent=2)}")


if __name__ == "__main__":
    main()
