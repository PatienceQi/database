# main.py

from src.query_executor import QueryExecutor

def main():
    executor = QueryExecutor()
    print("Welcome to the DBMS Prototype System. Type 'exit' or 'quit' to exit.")
    while True:
        try:
            sql = input("dbms> ")
            if sql.strip().lower() in ['exit', 'quit']:
                print("Exiting the system.")
                break
            if not sql.strip():
                continue  # 忽略空输入
            executor.execute(sql)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
