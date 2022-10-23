import preprocessing


def main():
    query = "SELECT * FROM customer c JOIN orders o ON o.o_custkey=c.c_custkey WHERE c.c_custkey < 100 LIMIT 10"
    conn = "connection string"
    preprocessor = preprocessing.Preprocessor(conn)
    plans = preprocessor.runner(query)


if __name__ == "__main__":
    main()
