# src/query_executor.py

from src.database import Database, Table
from src.sql_parser import SQLParser

class QueryExecutor:
    def __init__(self, database=None):
        if database is None:
            self.database = Database()
        else:
            self.database = database
        self.parser = SQLParser()

    def execute(self, sql):
        # 去除首尾空白并忽略大小写
        sql = sql.strip()

        # 解析 SQL 语句
        parsed = self.parser.parse(sql)
        if parsed is None:
            return  # 解析失败

        action = parsed.get('action')

        if action == 'CREATE TABLE':
            self._execute_create_table(parsed)
        elif action == 'INSERT INTO':
            self._execute_insert_into(parsed)
        elif action == 'SELECT':
            self._execute_select(parsed)
        elif action == 'ALTER TABLE':
            self._execute_alter_table(parsed)
        elif action == 'DELETE FROM':
            self._execute_delete_from(parsed)
        elif action == 'UPDATE':
            self._execute_update(parsed)
        elif action == 'DROP TABLE':
            self._execute_drop_table(parsed)
        elif action == 'BEGIN TRANSACTION':
            self._execute_begin_transaction()
        elif action == 'COMMIT':
            self._execute_commit()
        elif action == 'ROLLBACK':
            self._execute_rollback()
        else:
            raise ValueError(f"Unsupported SQL statement: {sql}")

    def _execute_create_table(self, parsed):
        table_name = parsed['table_name']
        columns = parsed['columns']
        try:
            self.database.create_table(table_name, columns)
        except ValueError as e:
            print(e)

    def _execute_insert_into(self, parsed):
        table_name = parsed['table_name']
        columns = parsed.get('columns')
        values = parsed['values']
        try:
            if columns and columns != self.database.get_table(table_name).column_names:
                # 如果指定了列名，需按列名顺序插入
                table = self.database.get_table(table_name)
                ordered_values = [None] * len(table.column_names)
                for col, val in zip(columns, values):
                    if col not in table.column_names:
                        raise ValueError(f"Column '{col}' does not exist in table '{table_name}'.")
                    idx = table.column_names.index(col)
                    ordered_values[idx] = val
                # 填充未指定的列为 None
                for i in range(len(ordered_values)):
                    if ordered_values[i] is None:
                        ordered_values[i] = "None"
                self.database.insert_into(table_name, ordered_values)
            else:
                self.database.insert_into(table_name, values)
        except ValueError as e:
            print(e)

    def _execute_select(self, parsed):
        table_name = parsed['table_name']
        columns = parsed['columns']
        where = parsed.get('where')
        try:
            results = self.database.select_from(table_name, columns, where)
            # 打印结果
            if columns == ["*"]:
                print("\t".join(self.database.get_table(table_name).column_names))
            else:
                print("\t".join(columns))
            for row in results:
                print("\t".join(map(str, row)))
        except ValueError as e:
            raise e

    def _execute_alter_table(self, parsed):
        table_name = parsed['table_name']
        operation = parsed['operation']
        table = self.database.get_table(table_name)
        if not table:
            print(f"Table '{table_name}' does not exist.")
            return

        if operation == 'ADD COLUMN':
            column_name = parsed['column_name']
            column_type = parsed['column_type']
            table.add_column(column_name, column_type)
        elif operation == 'DROP COLUMN':
            column_name = parsed['column_name']
            table.drop_column(column_name)
        elif operation == 'MODIFY COLUMN':
            column_name = parsed['column_name']
            new_column_type = parsed['column_type']
            table.modify_column(column_name, new_column_type)
        else:
            print(f"Unsupported ALTER TABLE operation: {operation}")

    def _execute_delete_from(self, parsed):
        table_name = parsed['table_name']
        where = parsed.get('where')
        try:
            self.database.delete_from(table_name, where)
        except ValueError as e:
            print(e)

    def _execute_update(self, parsed):
        table_name = parsed['table_name']
        set_values = parsed['set_values']
        where = parsed.get('where')
        try:
            self.database.update(table_name, set_values, where)
        except ValueError as e:
            print(e)

    def _execute_drop_table(self, parsed):
        table_name = parsed['table_name']
        try:
            self.database.drop_table(table_name)
        except ValueError as e:
            print(e)

    def _execute_begin_transaction(self):
        try:
            self.database.begin_transaction()
        except ValueError as e:
            print(e)

    def _execute_commit(self):
        try:
            self.database.commit()
        except ValueError as e:
            print(e)

    def _execute_rollback(self):
        try:
            self.database.rollback()
        except ValueError as e:
            print(e)
