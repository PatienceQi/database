import unittest
from src.sql_parser import SQLParser

class TestSQLParser(unittest.TestCase):
    def setUp(self):
        self.parser = SQLParser()

    def test_create_table(self):
        sql = "CREATE TABLE students (id INT, name STRING)"
        result = self.parser.parse_create(sql)
        print("CREATE TABLE解析结果:", result)  # 添加打印语句用于调试
        self.assertIsNotNone(result)
        self.assertEqual(result[0], "students")  # 表名
        self.assertEqual(result[1][0][0], "id")  # 第一列名
        self.assertEqual(result[1][0][1].upper(), "INT")  # 第一列数据类型
        self.assertEqual(result[1][1][0], "name")  # 第二列名
        self.assertEqual(result[1][1][1].upper(), "STRING")  # 第二列数据类型

    def test_insert_into(self):
        sql = "INSERT INTO students VALUES (1, 'Alice')"
        result = self.parser.parse_insert(sql)
        print("INSERT INTO解析结果:", result)  # 添加打印语句用于调试
        self.assertIsNotNone(result)
        self.assertEqual(result[0], "students")  # 表名
        self.assertEqual(result[1][0], "1")  # 第一列值
        self.assertEqual(result[1][1], "Alice")  # 第二列值

    def test_select(self):
        sql = "SELECT id, name FROM students WHERE age > 20"
        result = self.parser.parse_select(sql)
        print("SELECT解析结果:", result)  # 添加打印语句用于调试
        self.assertIsNotNone(result)
        self.assertEqual(result[0][0], "id")  # 第一列名
        self.assertEqual(result[0][1], "name")  # 第二列名
        self.assertEqual(result[1], "students")  # 表名
        self.assertEqual(result[2][0], "age")  # WHERE 条件列名
        self.assertEqual(result[2][1], ">")  # WHERE 条件运算符
        self.assertEqual(result[2][2], "20")  # WHERE 条件值

if __name__ == '__main__':
    unittest.main()
