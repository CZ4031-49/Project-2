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
    query = """ SELECT s_address, p_partkey
                FROM supplier s, part p, partsupp ps
                WHERE  
                    ps.ps_partkey = p.p_partkey AND
                    ps.ps_suppkey = s.s_suppkey AND
                    p.p_retailprice < 500 AND
                    s.s_phone != '800-807-9579'
                
                
                """

    conn = """  host=localhost
                dbname=postgres
                port=5432
                user=postgres
                password=zpz12345"""
    preprocessor = preprocessing.Preprocessor(conn)
    plans = preprocessor.runner(query)
    logging.debug(query)
    logging.debug(f"{full_plan_log_print_prefix} {plans}")


if __name__ == "__main__":
    main()
