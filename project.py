import json
import preprocessing
import logging
import annotator


def main():
    full_plan_log_print_prefix = "Plans output:"
    try:
        with open("test_prep.log", 'w') as t:
            pass
    except Exception as e:
        print(e)
    logging.basicConfig(
        filename="test_prep.log", encoding="utf-8", level=logging.DEBUG
    )

    # query = "SELECT * FROM customer c JOIN orders o ON o.o_custkey=c.c_custkey WHERE c.c_custkey < 100 LIMIT 10"
    # query = "SELECT * FROM customer c, orders o " \
    #         "WHERE o.o_custkey=c.c_custkey AND " \
    #         "c.c_custkey < 100 AND " \
    #         "o.o_custkey > 50 OR " \
    #         "o.o_custkey = 1"
    queries = [
        """
        SELECT s_suppkey, SUM(p_retailprice)
        FROM supplier s, part p, partsupp ps
        WHERE ps.ps_partkey = p.p_partkey AND
              ps.ps_suppkey = s.s_suppkey
        GROUP BY s.s_suppkey 
        """,

        # """
        # SELECT s_address, p_partkey
        # FROM supplier s, part p, partsupp ps
        # WHERE
        #     ps.ps_partkey = p.p_partkey AND
        #     ps.ps_suppkey = s.s_suppkey AND
        #     p.p_retailprice < 500 AND
        #     s.s_phone != '800-807-9579'
        # ORDER BY p_partkey
        #
        # """,
        # """
        # SELECT *
        # FROM customer c JOIN orders o
        # ON o.o_custkey=c.c_custkey
        # WHERE c.c_custkey < 100
        # LIMIT 10
        # """,

    ]

    conn = """  host=localhost
                dbname=postgres
                port=5432
                user=postgres
                password=zpz12345"""  # insert password here to connect to your database in PostgreSQL
    preprocessor = preprocessing.Preprocessor(conn)
    for query in queries:
        plans = preprocessor.runner(query)
        logging.debug(f"Current query: {query}")
        logging.debug(f"length of plans for current query: {len(plans)}")
        logging.debug(f"debug cur plan:  {json.dumps(plans[-1], sort_keys=True, indent=4)}")
        best_plan = plans[-1]
        annotator.annotate_query_plan(best_plan)
        logging.debug(f"Annotated best plan:  {json.dumps(best_plan, sort_keys=True, indent=4)}")
        print(json.dumps(best_plan, sort_keys=True, indent=4))

        logging.debug(f"debug alternate plan:  {json.dumps(plans[-2], sort_keys=True, indent=4)}")
        second_best_plan = plans[-2]
        annotator.annotate_query_plan(second_best_plan)
        logging.debug(f"Annotated alternate plan:  {json.dumps(second_best_plan, sort_keys=True, indent=4)}")
        print(json.dumps(second_best_plan, sort_keys=True, indent=4))


if __name__ == "__main__":
    main()
