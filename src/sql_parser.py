# src/sql_parser.py
import pyparsing as pp

class SQLParser:
    def __init__(self):
        self._build_grammar()

    def _build_grammar(self):
        # 基本元素
        identifier = pp.Word(pp.alphas, pp.alphanums + "_").setName("identifier")
        integer = pp.Word(pp.nums).setName("integer")
        string = pp.QuotedString("'").setName("string")
        value = integer | string

        # 数据类型
        datatype = pp.Keyword("INT", caseless=True) | pp.Keyword("STRING", caseless=True)

        # 列定义
        column_def = pp.Group(identifier("name") + datatype("type"))
        column_def_list = pp.delimitedList(column_def)

        # CREATE TABLE 语句
        create_table = (
            pp.Keyword("CREATE TABLE", caseless=True)("action")
            + identifier("table_name")
            + pp.Suppress("(")
            + column_def_list("columns")
            + pp.Suppress(")")
        )

        # INSERT INTO 语句
        insert_into = (
            pp.Keyword("INSERT INTO", caseless=True)("action")
            + identifier("table_name")
            + pp.Keyword("VALUES", caseless=True)
            + pp.Suppress("(")
            + pp.Group(pp.delimitedList(value))("values")
            + pp.Suppress(")")
        )

        # SELECT 语句（包括 SELECT *）
        select = (
            pp.Keyword("SELECT", caseless=True)("action")
            + (pp.Keyword("*")("columns") | pp.Group(pp.delimitedList(identifier))("columns"))
            + pp.Keyword("FROM", caseless=True)
            + identifier("table_name")
            + pp.Optional(
                pp.Keyword("WHERE", caseless=True)
                + identifier("where_column")
                + pp.oneOf("= < >")("operator")
                + value("where_value")
            )
        )

        # ALTER TABLE 语句
        alter_table = (
            pp.Keyword("ALTER TABLE", caseless=True)("action")
            + identifier("table_name")
            + (
                pp.Keyword("ADD COLUMN", caseless=True)("operation")
                + identifier("column_name")
                + datatype("column_type")
                | pp.Keyword("DROP COLUMN", caseless=True)("operation")
                + identifier("column_name")
                | pp.Keyword("MODIFY COLUMN", caseless=True)("operation")
                + identifier("column_name")
                + datatype("column_type")
            )
        )

        # DELETE FROM 语句
        delete_from = (
            pp.Keyword("DELETE FROM", caseless=True)("action")
            + identifier("table_name")
            + pp.Optional(
                pp.Keyword("WHERE", caseless=True)
                + identifier("where_column")
                + pp.oneOf("= < >")("operator")
                + value("where_value")
            )
        )

        # UPDATE 语句
        update = (
            pp.Keyword("UPDATE", caseless=True)("action")
            + identifier("table_name")
            + pp.Keyword("SET", caseless=True)
            + pp.delimitedList(
                pp.Group(identifier("column") + pp.Suppress("=") + value("value"))
            )("set_values")
            + pp.Optional(
                pp.Keyword("WHERE", caseless=True)
                + identifier("where_column")
                + pp.oneOf("= < >")("operator")
                + value("where_value")
            )
        )

        # DROP TABLE 语句
        drop_table = (
            pp.Keyword("DROP TABLE", caseless=True)("action")
            + identifier("table_name")
        )

        # 组合语法
        self.grammar = create_table | insert_into | select | alter_table | delete_from | update | drop_table

    def parse(self, sql):
        try:
            result = self.grammar.parseString(sql, parseAll=True)
            action = result.get("action").upper()

            if action == "CREATE TABLE":
                columns = {col["name"]: col["type"].upper() for col in result["columns"]}
                return {"action": "CREATE TABLE", "table_name": result["table_name"], "columns": columns}

            elif action == "INSERT INTO":
                values = result["values"].asList()
                return {"action": "INSERT INTO", "table_name": result["table_name"], "values": values}

            elif action == "SELECT":
                if result["columns"] == "*":
                    columns = "*"
                else:
                    columns = result["columns"].asList()
                where = None
                if "where_column" in result:
                    where = (result["where_column"], result["operator"], result["where_value"])
                return {"action": "SELECT", "table_name": result["table_name"], "columns": columns, "where": where}

            elif action == "ALTER TABLE":
                operation = result["operation"].upper()
                parsed = {"action": "ALTER TABLE", "table_name": result["table_name"], "operation": operation}
                if operation == "ADD COLUMN":
                    parsed["column_name"] = result["column_name"]
                    parsed["column_type"] = result["column_type"].upper()
                elif operation == "DROP COLUMN":
                    parsed["column_name"] = result["column_name"]
                elif operation == "MODIFY COLUMN":
                    parsed["column_name"] = result["column_name"]
                    parsed["column_type"] = result["column_type"].upper()
                return parsed

            elif action == "DELETE FROM":
                where = None
                if "where_column" in result:
                    where = (result["where_column"], result["operator"], result["where_value"])
                return {"action": "DELETE FROM", "table_name": result["table_name"], "where": where}

            elif action == "UPDATE":
                set_values = {s["column"]: self._convert_value(s["value"]) for s in result["set_values"]}
                where = None
                if "where_column" in result:
                    where = (result["where_column"], result["operator"], self._convert_value(result["where_value"]))
                return {"action": "UPDATE", "table_name": result["table_name"], "set_values": set_values, "where": where}

            elif action == "DROP TABLE":
                return {"action": "DROP TABLE", "table_name": result["table_name"]}

            else:
                print(f"Unsupported SQL action: {action}")
                return None

        except pp.ParseException as pe:
            print(f"SQL Parsing Error: {pe}")
            return None

    def _convert_value(self, value):
        try:
            return int(value)
        except ValueError:
            return value.strip("'")
