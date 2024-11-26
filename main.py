# main.py

from src.query_executor import QueryExecutor

def main():
    executor = QueryExecutor()
    print("Simple SQL Interpreter. Type 'exit' to quit.")
    while True:
        try:
            sql = input("SQL> ")
            if sql.lower() == 'exit':
                break
            executor.execute(sql)
        except KeyboardInterrupt:
            print("\nExiting.")
            break

if __name__ == "__main__":
    main()
