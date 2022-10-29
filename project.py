import preprocessing
import logging
import annotator


def main():
    full_plan_log_print_prefix = "Plans output:"

    logging.basicConfig(
        filename="preprocessing.log", encoding="utf-8", level=logging.DEBUG
    )
    query = "SELECT * FROM customer c JOIN orders o ON o.o_custkey=c.c_custkey WHERE c.c_custkey < 100 LIMIT 10"
    conn = "connection string"
    preprocessor = preprocessing.Preprocessor(conn)
    plans = preprocessor.runner(query)
    logging.debug(f"{full_plan_log_print_prefix} {plans}")


if __name__ == "__main__":
    main()
