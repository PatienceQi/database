# src/sql_parser.py

import re

class SQLParser:
    def parse(self, sql):
        sql = sql.strip().rstrip(';')
        
        # 使用正则表达式时忽略大小写
        if re.match(r'^CREATE\s+TABLE', sql, re.IGNORECASE):
            return self._parse_create_table(sql)
        elif re.match(r'^INSERT\s+INTO', sql, re.IGNORECASE):
            return self._parse_insert_into(sql)
        elif re.match(r'^SELECT', sql, re.IGNORECASE):
            return self._parse_select(sql)
        elif re.match(r'^ALTER\s+TABLE', sql, re.IGNORECASE):
            return self._parse_alter_table(sql)
        elif re.match(r'^DELETE\s+FROM', sql, re.IGNORECASE):
            return self._parse_delete_from(sql)
        elif re.match(r'^UPDATE', sql, re.IGNORECASE):
            return self._parse_update(sql)
        elif re.match(r'^DROP\s+TABLE', sql, re.IGNORECASE):
            return self._parse_drop_table(sql)
        elif re.match(r'^BEGIN\s+TRANSACTION$', sql, re.IGNORECASE):
            return {"action": "BEGIN TRANSACTION"}
        elif re.match(r'^COMMIT$', sql, re.IGNORECASE):
            return {"action": "COMMIT"}
        elif re.match(r'^ROLLBACK$', sql, re.IGNORECASE):
            return {"action": "ROLLBACK"}
        else:
            raise ValueError(f"Unable to parse SQL statement: {sql}")

    def _parse_create_table(self, sql):
        pattern = r"CREATE\s+TABLE\s+(\w+)\s*\((.+)\)"
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("CREATE TABLE syntax error.")
        table_name = match.group(1)
        columns_str = match.group(2)
        columns = {}
        for col_def in columns_str.split(','):
            parts = col_def.strip().split()
            if len(parts) != 2:
                raise ValueError(f"Invalid column definition: {col_def}")
            col_name, col_type = parts
            columns[col_name] = col_type
        return {"action": "CREATE TABLE", "table_name": table_name, "columns": columns}

    def _parse_insert_into(self, sql):
        pattern = r"INSERT\s+INTO\s+(\w+)\s*\((.+)\)\s+VALUES\s*\((.+)\)"
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("INSERT INTO syntax error.")
        table_name = match.group(1)
        columns = [col.strip() for col in match.group(2).split(',')]
        values = [val.strip().strip("'") for val in match.group(3).split(',')]
        return {"action": "INSERT INTO", "table_name": table_name, "columns": columns, "values": values}

    def _parse_select(self, sql):
        pattern = r"SELECT\s+(.+)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?"
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("SELECT syntax error.")
        columns = [col.strip() for col in match.group(1).split(',')]
        table_name = match.group(2)
        where_clause = match.group(3)
        where = None
        if where_clause:
            where = self._parse_where(where_clause)
        return {"action": "SELECT", "columns": columns, "table_name": table_name, "where": where}

    def _parse_alter_table(self, sql):
        pattern_add = r"ALTER\s+TABLE\s+(\w+)\s+ADD\s+COLUMN\s+(\w+)\s+(\w+)"
        pattern_drop = r"ALTER\s+TABLE\s+(\w+)\s+DROP\s+COLUMN\s+(\w+)"
        pattern_modify = r"ALTER\s+TABLE\s+(\w+)\s+MODIFY\s+COLUMN\s+(\w+)\s+(\w+)"
        if re.search(r'ADD\s+COLUMN', sql, re.IGNORECASE):
            match = re.match(pattern_add, sql, re.IGNORECASE)
            if not match:
                raise ValueError("ALTER TABLE ADD COLUMN syntax error.")
            table_name, column_name, column_type = match.groups()
            return {"action": "ALTER TABLE", "operation": "ADD COLUMN", "table_name": table_name, "column_name": column_name, "column_type": column_type}
        elif re.search(r'DROP\s+COLUMN', sql, re.IGNORECASE):
            match = re.match(pattern_drop, sql, re.IGNORECASE)
            if not match:
                raise ValueError("ALTER TABLE DROP COLUMN syntax error.")
            table_name, column_name = match.groups()
            return {"action": "ALTER TABLE", "operation": "DROP COLUMN", "table_name": table_name, "column_name": column_name}
        elif re.search(r'MODIFY\s+COLUMN', sql, re.IGNORECASE):
            match = re.match(pattern_modify, sql, re.IGNORECASE)
            if not match:
                raise ValueError("ALTER TABLE MODIFY COLUMN syntax error.")
            table_name, column_name, column_type = match.groups()
            return {"action": "ALTER TABLE", "operation": "MODIFY COLUMN", "table_name": table_name, "column_name": column_name, "column_type": column_type}
        else:
            raise ValueError("Unsupported ALTER TABLE operation.")

    def _parse_delete_from(self, sql):
        pattern = r"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?"
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("DELETE FROM syntax error.")
        table_name = match.group(1)
        where_clause = match.group(2)
        where = None
        if where_clause:
            where = self._parse_where(where_clause)
        return {"action": "DELETE FROM", "table_name": table_name, "where": where}

    def _parse_update(self, sql):
        pattern = r"UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$"
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("UPDATE syntax error.")
        table_name = match.group(1)
        set_clause = match.group(2)
        where_clause = match.group(3)
        set_values = {}
        for assignment in set_clause.split(','):
            parts = assignment.strip().split('=')
            if len(parts) != 2:
                raise ValueError(f"Invalid SET assignment: {assignment}")
            col, val = parts
            set_values[col.strip()] = val.strip().strip("'")
        where = None
        if where_clause:
            where = self._parse_where(where_clause)
        return {"action": "UPDATE", "table_name": table_name, "set_values": set_values, "where": where}

    def _parse_drop_table(self, sql):
        pattern = r"DROP\s+TABLE\s+(\w+)"
        match = re.match(pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError("DROP TABLE syntax error.")
        table_name = match.group(1)
        return {"action": "DROP TABLE", "table_name": table_name}

    def _parse_where(self, clause):
        # 简单的WHERE子句解析器，支持 "column operator value"
        pattern = r"(\w+)\s*(=|<|>)\s*('?[\w\s]+'?)"
        match = re.match(pattern, clause, re.IGNORECASE)
        if not match:
            raise ValueError("WHERE clause syntax error.")
        column, operator, value = match.groups()
        value = value.strip("'")  # 去除引号
        # 尝试转换为整数
        if value.isdigit():
            value = int(value)
        return (column, operator, value)
