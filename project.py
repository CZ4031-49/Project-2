import preprocessing


def main():
    query = "EXPLAIN (FORMAT JSON) SELECT * FROM customer c JOIN orders o ON o.o_custkey=c.c_custkey WHERE c.c_custkey < 100 LIMIT 10"
    preprocessor = preprocessing.Preprocessor()
    preprocessor.selection_planner(query)
    preprocessor.join_planner(query)


if __name__ == "__main__":
    main()
