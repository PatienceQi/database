import pyparsing as pp

class SQLParser:
    def __init__(self):
        # 定义数据类型，支持INT和STRING
        self.integer = pp.Regex(r'\d+').setName("integer")
        self.identifier = pp.Word(pp.alphas + "_", pp.alphas + "_0123456789").setName("identifier")
        self.column_name = self.identifier
        self.value = pp.QuotedString("'") | self.integer  # 支持字符串和整数类型的值

        # SQL语句的各个部分
        self.create_table_stmt = self._create_table_stmt()
        self.insert_stmt = self._insert_stmt()
        self.select_stmt = self._select_stmt()

    def _create_table_stmt(self):
        # CREATE TABLE语句
        create_keyword = pp.Suppress(pp.Keyword("CREATE TABLE", caseless=True))
        table_name = self.identifier
        lparen = pp.Suppress("(")
        rparen = pp.Suppress(")")
        comma = pp.Suppress(",")

        # 定义列名和数据类型，并将每个列定义分组
        column_def = pp.Group(self.column_name + (pp.Keyword("INT", caseless=True) | pp.Keyword("STRING", caseless=True)))
        column_defs = pp.Group(column_def + pp.ZeroOrMore(comma + column_def))  # 支持多个列

        return create_keyword + table_name + lparen + column_defs + rparen

    def _insert_stmt(self):
        # INSERT INTO语句
        insert_keyword = pp.Suppress(pp.Keyword("INSERT INTO", caseless=True))
        table_name = self.identifier
        values_keyword = pp.Suppress(pp.Keyword("VALUES", caseless=True))
        lparen = pp.Suppress("(")
        rparen = pp.Suppress(")")
        value_list = pp.Group(pp.delimitedList(self.value))  # 将值列表分组

        return insert_keyword + table_name + values_keyword + lparen + value_list + rparen

    def _select_stmt(self):
        # SELECT语句
        select_keyword = pp.Suppress(pp.Keyword("SELECT", caseless=True))
        from_keyword = pp.Suppress(pp.Keyword("FROM", caseless=True))
        where_keyword = pp.Suppress(pp.Keyword("WHERE", caseless=True))
        comma = pp.Suppress(",")
        
        # 定义列列表，并将其分组
        column_list = pp.Group(self.column_name + pp.ZeroOrMore(comma + self.column_name))  # 支持多个列
        
        # 定义WHERE条件，并将其分组
        condition = pp.Group(self.column_name + pp.oneOf("= < >") + self.value)
        
        return select_keyword + column_list + from_keyword + self.identifier + pp.Optional(where_keyword + condition)

    def parse_create(self, sql):
        try:
            result = self.create_table_stmt.parseString(sql, parseAll=True)
            return result
        except pp.ParseException as e:
            print(f"CREATE TABLE语法错误: {e}")
            return None

    def parse_insert(self, sql):
        try:
            result = self.insert_stmt.parseString(sql, parseAll=True)
            return result
        except pp.ParseException as e:
            print(f"INSERT语法错误: {e}")
            return None

    def parse_select(self, sql):
        try:
            result = self.select_stmt.parseString(sql, parseAll=True)
            return result
        except pp.ParseException as e:
            print(f"SELECT语法错误: {e}")
            return None
