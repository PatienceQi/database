import json

class Table:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns  # dict of column_name: data_type
        self.rows = []

    def insert(self, row):
        if len(row) != len(self.columns):
            raise ValueError("Row has incorrect number of columns.")
        self.rows.append(row)

    def to_dict(self):
        return {
            'name': self.name,
            'columns': self.columns,
            'rows': self.rows
        }

    @staticmethod
    def from_dict(data):
        table = Table(data['name'], data['columns'])
        table.rows = data['rows']
        return table
#Table类用于表示数据库中的一张表，包含表名、列定义和行数据。
#insert方法用于向表中插入新行。
#to_dict和from_dict方法用于将表数据序列化和反序列化，以支持数据持久化。


class Database:
    def __init__(self):
        self.tables = {}

    def create_table(self, name, columns):
        if name in self.tables:
            raise ValueError(f"Table {name} already exists.")
        self.tables[name] = Table(name, columns)

    def drop_table(self, name):
        if name not in self.tables:
            raise ValueError(f"Table {name} does not exist.")
        del self.tables[name]

    def insert_into(self, table_name, row):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist.")
        self.tables[table_name].insert(row)

    def select_from(self, table_name, columns=None, where=None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist.")
        table = self.tables[table_name]
        if columns is None:
            selected_columns = list(table.columns.keys())
        else:
            selected_columns = columns

        # Get column indices
        column_indices = [list(table.columns.keys()).index(col) for col in selected_columns]

        # Filter rows based on 'where' condition
        if where:
            filtered_rows = []
            for row in table.rows:
                if self._evaluate_where(row, table, where):
                    filtered_rows.append(row)
        else:
            filtered_rows = table.rows

        # Select specified columns
        result = []
        for row in filtered_rows:
            selected_row = [row[idx] for idx in column_indices]
            result.append(selected_row)

        return result

    def _evaluate_where(self, row, table, where):
        # 简单的WHERE条件解析，支持单个条件，例如 "age > 20"
        column, operator, value = where
        col_idx = list(table.columns.keys()).index(column)
        cell = row[col_idx]
        try:
            # 尝试将值转换为数字类型
            value = float(value)
            cell = float(cell)
        except ValueError:
            # 如果无法转换，保持字符串类型
            pass

        if operator == '=':
            return cell == value
        elif operator == '<':
            return cell < value
        elif operator == '>':
            return cell > value
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    def save_to_disk(self, filepath):
        data = {table_name: table.to_dict() for table_name, table in self.tables.items()}
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)

    def load_from_disk(self, filepath):
        with open(filepath, 'r') as f:
            data = json.load(f)
        self.tables = {name: Table.from_dict(tbl) for name, tbl in data.items()}

#Database类管理多个Table实例，提供创建、删除表和数据操作的方法。
#insert_into方法用于向指定表插入数据。
#select_from方法用于从指定表查询数据，支持选择特定列和简单的WHERE条件。
#_evaluate_where方法用于评估单个WHERE条件。
#save_to_disk和load_from_disk方法用于将数据库状态保存到磁盘或从磁盘加载。