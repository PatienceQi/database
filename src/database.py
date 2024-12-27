# src/database.py

import copy

class Database:
    def __init__(self):
        self.tables = {}
        self.in_transaction = False
        self.transaction_backup = None

    def begin_transaction(self):
        if self.in_transaction:
            raise ValueError("A transaction is already in progress.")
        self.transaction_backup = copy.deepcopy(self.tables)
        self.in_transaction = True
        print("Transaction started.")

    def commit(self):
        if not self.in_transaction:
            raise ValueError("No transaction in progress.")
        self.transaction_backup = None
        self.in_transaction = False
        print("Transaction committed.")

    def rollback(self):
        if not self.in_transaction:
            raise ValueError("No transaction in progress.")
        self.tables = self.transaction_backup
        self.transaction_backup = None
        self.in_transaction = False
        print("Transaction rolled back.")

    def create_table(self, table_name, columns):
        if table_name in self.tables:
            raise ValueError(f"Table '{table_name}' already exists.")
        self.tables[table_name] = Table(table_name, columns)
        print(f"Table '{table_name}' created with columns: {columns}")

    def get_table(self, table_name):
        return self.tables.get(table_name)

    def insert_into(self, table_name, values):
        table = self.get_table(table_name)
        if not table:
            raise ValueError(f"Table '{table_name}' does not exist.")
        table.insert_row(values)

    def select_from(self, table_name, columns=None, where=None):
        table = self.get_table(table_name)
        if not table:
            raise ValueError(f"Table '{table_name}' does not exist.")
        return table.select(columns, where)

    def delete_from(self, table_name, where=None):
        table = self.get_table(table_name)
        if not table:
            raise ValueError(f"Table '{table_name}' does not exist.")
        table.delete_rows(where)

    def update(self, table_name, set_values, where=None):
        table = self.get_table(table_name)
        if not table:
            raise ValueError(f"Table '{table_name}' does not exist.")
        table.update_rows(set_values, where)

    def drop_table(self, table_name):
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")
        del self.tables[table_name]
        print(f"Dropped table '{table_name}'.")

class Table:
    def __init__(self, name, columns):
        self.name = name
        self.columns = {col_name: col_type.upper() for col_name, col_type in columns.items()}
        self.column_names = list(columns.keys())
        self.rows = []
        self.indexes = {}  # 未来可扩展索引功能

    def insert_row(self, values):
        if len(values) != len(self.column_names):
            raise ValueError("Column count doesn't match value count.")
        # 类型转换
        converted_values = []
        for value, col_name in zip(values, self.column_names):
            col_type = self.columns[col_name]
            try:
                if value is None:
                    converted_values.append(None)
                elif col_type == 'INT':
                    converted_values.append(int(value))
                else:
                    converted_values.append(str(value))
            except ValueError:
                raise ValueError(f"Invalid value for column '{col_name}': {value}")
        self.rows.append(converted_values)
        print(f"Inserted into '{self.name}': {converted_values}")

    def select(self, columns, where=None):
        # 确认列是否存在
        if columns == ["*"]:
            selected_columns = self.column_names
        else:
            for col in columns:
                if col not in self.column_names:
                    raise ValueError(f"Column '{col}' does not exist in table '{self.name}'.")
            selected_columns = columns

        # 获取列索引
        col_indices = [self.column_names.index(col) for col in selected_columns]

        result = []
        for row in self.rows:
            if where:
                where_col, operator, where_val = where
                if where_col not in self.column_names:
                    raise ValueError(f"Column '{where_col}' does not exist in table '{self.name}'.")
                where_idx = self.column_names.index(where_col)
                cell_value = row[where_idx]
                if not self._evaluate_condition(cell_value, operator, where_val):
                    continue
            selected_row = [row[idx] for idx in col_indices]
            result.append(selected_row)
        return result

    def delete_rows(self, where=None):
        initial_count = len(self.rows)
        if where:
            where_col, operator, where_val = where
            if where_col not in self.column_names:
                raise ValueError(f"Column '{where_col}' does not exist in table '{self.name}'.")
            where_idx = self.column_names.index(where_col)
            new_rows = []
            for row in self.rows:
                cell_value = row[where_idx]
                if not self._evaluate_condition(cell_value, operator, where_val):
                    new_rows.append(row)
            self.rows = new_rows
        else:
            # 删除所有行
            self.rows = []
        deleted_count = initial_count - len(self.rows)
        print(f"Deleted {deleted_count} row(s) from '{self.name}'.")

    def update_rows(self, set_values, where=None):
        update_count = 0
        for row in self.rows:
            if where:
                where_col, operator, where_val = where
                if where_col not in self.column_names:
                    raise ValueError(f"Column '{where_col}' does not exist in table '{self.name}'.")
                where_idx = self.column_names.index(where_col)
                cell_value = row[where_idx]
                if not self._evaluate_condition(cell_value, operator, where_val):
                    continue
            for col, val in set_values.items():
                if col not in self.column_names:
                    raise ValueError(f"Column '{col}' does not exist in table '{self.name}'.")
                idx = self.column_names.index(col)
                col_type = self.columns[col]
                try:
                    if col_type == 'INT':
                        row[idx] = int(val)
                    else:
                        row[idx] = str(val)
                except ValueError:
                    raise ValueError(f"Invalid value for column '{col}': {val}")
            update_count += 1
        print(f"Updated {update_count} row(s) in '{self.name}'.")

    def _evaluate_condition(self, left, operator, right):
        if isinstance(left, str) and isinstance(right, str):
            pass  # 字符串比较
        elif isinstance(left, int) and isinstance(right, int):
            pass  # 整数比较
        else:
            # 尝试类型转换
            try:
                if isinstance(left, str):
                    left = int(left)
                if isinstance(right, str):
                    right = int(right)
            except ValueError:
                pass  # 保持原有类型

        if operator == '=':
            return left == right
        elif operator == '<':
            return left < right
        elif operator == '>':
            return left > right
        else:
            raise ValueError(f"Unsupported operator '{operator}'")

    def add_column(self, column_name, column_type):
        if column_name in self.columns:
            print(f"Column '{column_name}' already exists in table '{self.name}'.")
            return
        self.columns[column_name] = column_type.upper()
        self.column_names.append(column_name)
        # 为现有行添加默认值 None
        for row in self.rows:
            row.append(None)
        print(f"Added column '{column_name}' of type '{column_type}' to table '{self.name}'.")

    def drop_column(self, column_name):
        if column_name not in self.columns:
            print(f"Column '{column_name}' does not exist in table '{self.name}'.")
            return
        idx = self.column_names.index(column_name)
        del self.columns[column_name]
        self.column_names.remove(column_name)
        for row in self.rows:
            del row[idx]
        print(f"Dropped column '{column_name}' from table '{self.name}'.")

    def modify_column(self, column_name, new_column_type):
        if column_name not in self.columns:
            print(f"Column '{column_name}' does not exist in table '{self.name}'.")
            return
        self.columns[column_name] = new_column_type.upper()
        print(f"Modified column '{column_name}' to type '{new_column_type}' in table '{self.name}'.")
