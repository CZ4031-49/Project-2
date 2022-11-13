import json
from preprocessing import Preprocessor
import logging
import annotator
from interface import *
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--conn', help="connection string for database")
    args = parser.parse_args()
    conn = args.conn
    p = Preprocessor(conn)

    root = tk.Tk()
    app = App(root, conn, p)
    root.geometry('1500x1000+0+0')
    root.mainloop()


if __name__ == "__main__":
    main()
