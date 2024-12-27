# tests/test_sql.py

import unittest
from io import StringIO
import sys

# 确保可以导入 src 包
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.query_executor import QueryExecutor

class TestSQLExecutor(unittest.TestCase):
    def setUp(self):
        """初始化 QueryExecutor 实例，每个测试用例前都会执行"""
        self.executor = QueryExecutor()

    def test_create_table(self):
        """测试创建表功能"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)
        
        # 检查表是否存在
        self.assertIn('students', self.executor.database.tables)
        
        # 检查表的列定义
        table = self.executor.database.get_table('students')
        self.assertEqual(table.columns, {'id': 'INT', 'name': 'TEXT'})
        self.assertEqual(table.column_names, ['id', 'name'])
        self.assertEqual(table.rows, [])

    def test_create_existing_table(self):
        """测试创建已存在的表应抛出错误"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)
        
        with self.assertRaises(ValueError) as context:
            self.executor.execute(create_sql)
        
        self.assertIn("Table 'students' already exists.", str(context.exception))

    def test_insert_into_table(self):
        """测试向表中插入数据"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)
        
        insert_sql = "INSERT INTO students (id, name) VALUES (1, 'Alice')"
        self.executor.execute(insert_sql)
        
        table = self.executor.database.get_table('students')
        self.assertEqual(len(table.rows), 1)
        self.assertEqual(table.rows[0], [1, 'Alice'])

    def test_insert_into_nonexistent_table(self):
        """测试向不存在的表中插入数据应抛出错误"""
        insert_sql = "INSERT INTO students (id, name) VALUES (1, 'Alice')"
        
        with self.assertRaises(ValueError) as context:
            self.executor.execute(insert_sql)
        
        self.assertIn("Table 'students' does not exist.", str(context.exception))

    def test_select_all(self):
        """测试选择所有列的数据"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)
        self.executor.execute("INSERT INTO students (id, name) VALUES (1, 'Alice')")
        self.executor.execute("INSERT INTO students (id, name) VALUES (2, 'Bob')")

        # 捕获标准输出
        captured_output = StringIO()
        sys.stdout = captured_output

        select_sql = "SELECT * FROM students"
        self.executor.execute(select_sql)

        # 恢复标准输出
        sys.stdout = sys.__stdout__
        output = captured_output.getvalue()

        # 检查输出内容
        self.assertIn("id\tname", output)
        self.assertIn("1\tAlice", output)
        self.assertIn("2\tBob", output)

    def test_select_with_where(self):
        """测试带有 WHERE 子句的选择操作"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)
        self.executor.execute("INSERT INTO students (id, name) VALUES (1, 'Alice')")
        self.executor.execute("INSERT INTO students (id, name) VALUES (2, 'Bob')")

        # 捕获标准输出
        captured_output = StringIO()
        sys.stdout = captured_output

        select_sql = "SELECT name FROM students WHERE id = 1"
        self.executor.execute(select_sql)

        # 恢复标准输出
        sys.stdout = sys.__stdout__
        output = captured_output.getvalue()

        # 检查输出内容
        self.assertIn("name", output)
        self.assertIn("Alice", output)
        self.assertNotIn("Bob", output)

    def test_update_rows(self):
        """测试更新表中的数据"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)
        self.executor.execute("INSERT INTO students (id, name) VALUES (1, 'Alice')")
        self.executor.execute("INSERT INTO students (id, name) VALUES (2, 'Bob')")

        update_sql = "UPDATE students SET name = 'Charlie' WHERE id = 1"
        self.executor.execute(update_sql)

        table = self.executor.database.get_table('students')
        self.assertEqual(table.rows[0], [1, 'Charlie'])
        self.assertEqual(table.rows[1], [2, 'Bob'])

    def test_delete_rows(self):
        """测试删除表中的数据"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)
        self.executor.execute("INSERT INTO students (id, name) VALUES (1, 'Alice')")
        self.executor.execute("INSERT INTO students (id, name) VALUES (2, 'Bob')")

        delete_sql = "DELETE FROM students WHERE id = 1"
        self.executor.execute(delete_sql)

        table = self.executor.database.get_table('students')
        self.assertEqual(len(table.rows), 1)
        self.assertEqual(table.rows[0], [2, 'Bob'])

    def test_alter_add_column(self):
        """测试添加列到表中"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)

        alter_sql = "ALTER TABLE students ADD COLUMN age INT"
        self.executor.execute(alter_sql)

        table = self.executor.database.get_table('students')
        self.assertIn('age', table.columns)
        self.assertEqual(table.column_names, ['id', 'name', 'age'])

        # 插入包含新列的数据
        self.executor.execute("INSERT INTO students (id, name, age) VALUES (1, 'Alice', 20)")
        self.executor.execute("INSERT INTO students (id, name, age) VALUES (2, 'Bob', 22)")

        table = self.executor.database.get_table('students')
        self.assertEqual(table.rows[0], [1, 'Alice', 20])
        self.assertEqual(table.rows[1], [2, 'Bob', 22])

    def test_alter_drop_column(self):
        """测试从表中删除列"""
        create_sql = "CREATE TABLE students (id INT, name TEXT, age INT)"
        self.executor.execute(create_sql)

        alter_sql = "ALTER TABLE students DROP COLUMN age"
        self.executor.execute(alter_sql)

        table = self.executor.database.get_table('students')
        self.assertNotIn('age', table.columns)
        self.assertEqual(table.column_names, ['id', 'name'])

    def test_alter_modify_column(self):
        """测试修改列的数据类型"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)

        alter_sql = "ALTER TABLE students MODIFY COLUMN name INT"
        self.executor.execute(alter_sql)

        table = self.executor.database.get_table('students')
        self.assertEqual(table.columns['name'], 'INT')

        # 尝试插入无法转换为 INT 的值应抛出错误
        insert_sql = "INSERT INTO students (id, name) VALUES (1, 'Alice')"
        with self.assertRaises(ValueError) as context:
            self.executor.execute(insert_sql)
        
        self.assertIn("Invalid value for column 'name': Alice", str(context.exception))

    def test_drop_table(self):
        """测试删除表"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)

        drop_sql = "DROP TABLE students"
        self.executor.execute(drop_sql)

        self.assertNotIn('students', self.executor.database.tables)

    def test_begin_commit_transaction(self):
        """测试事务的开始与提交"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)

        self.executor.execute("BEGIN TRANSACTION")
        self.executor.execute("INSERT INTO students (id, name) VALUES (1, 'Alice')")
        self.executor.execute("COMMIT")

        table = self.executor.database.get_table('students')
        self.assertEqual(len(table.rows), 1)
        self.assertEqual(table.rows[0], [1, 'Alice'])

    def test_begin_rollback_transaction(self):
        """测试事务的开始与回滚"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)

        self.executor.execute("BEGIN TRANSACTION")
        self.executor.execute("INSERT INTO students (id, name) VALUES (1, 'Alice')")
        self.executor.execute("ROLLBACK")

        table = self.executor.database.get_table('students')
        self.assertEqual(len(table.rows), 0)

    def test_nested_transaction(self):
        """测试嵌套事务应抛出错误"""
        self.executor.execute("BEGIN TRANSACTION")
        
        with self.assertRaises(ValueError) as context:
            self.executor.execute("BEGIN TRANSACTION")
        
        self.assertIn("A transaction is already in progress.", str(context.exception))

    def test_transaction_commit_without_begin(self):
        """测试在未开始事务的情况下提交应抛出错误"""
        with self.assertRaises(ValueError) as context:
            self.executor.execute("COMMIT")
        
        self.assertIn("No transaction in progress.", str(context.exception))

    def test_transaction_rollback_without_begin(self):
        """测试在未开始事务的情况下回滚应抛出错误"""
        with self.assertRaises(ValueError) as context:
            self.executor.execute("ROLLBACK")
        
        self.assertIn("No transaction in progress.", str(context.exception))

    def test_invalid_sql(self):
        """测试无效的 SQL 语句应抛出错误"""
        invalid_sql = "INVALID SQL"
        
        with self.assertRaises(ValueError) as context:
            self.executor.execute(invalid_sql)
        
        self.assertIn("Unable to parse SQL statement", str(context.exception))

    def test_insert_missing_columns(self):
        """测试插入时缺少部分列，系统应为缺失的列赋予默认值 'None'"""
        create_sql = "CREATE TABLE students (id INT, name TEXT, age INT)"
        self.executor.execute(create_sql)

        insert_sql = "INSERT INTO students (id, name) VALUES (1, 'Alice')"
        self.executor.execute(insert_sql)

        table = self.executor.database.get_table('students')
        self.assertEqual(len(table.rows), 1)
        self.assertEqual(table.rows[0], [1, 'Alice', 'None'])

    def test_insert_extra_columns(self):
        """测试插入时包含额外的列应抛出错误"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)

        insert_sql = "INSERT INTO students (id, name, age) VALUES (1, 'Alice', 20)"
        
        with self.assertRaises(ValueError) as context:
            self.executor.execute(insert_sql)
        
        self.assertIn("Column 'age' does not exist in table 'students'.", str(context.exception))

    def test_update_nonexistent_column(self):
        """测试更新不存在的列应抛出错误"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)
        self.executor.execute("INSERT INTO students (id, name) VALUES (1, 'Alice')")

        update_sql = "UPDATE students SET age = 20 WHERE id = 1"
        
        with self.assertRaises(ValueError) as context:
            self.executor.execute(update_sql)
        
        self.assertIn("Column 'age' does not exist in table 'students'.", str(context.exception))

    def test_delete_all_rows(self):
        """测试删除表中的所有行"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)
        self.executor.execute("INSERT INTO students (id, name) VALUES (1, 'Alice')")
        self.executor.execute("INSERT INTO students (id, name) VALUES (2, 'Bob')")

        delete_sql = "DELETE FROM students"
        self.executor.execute(delete_sql)

        table = self.executor.database.get_table('students')
        self.assertEqual(len(table.rows), 0)

    def test_drop_nonexistent_table(self):
        """测试删除不存在的表应抛出错误"""
        drop_sql = "DROP TABLE students"
        
        with self.assertRaises(ValueError) as context:
            self.executor.execute(drop_sql)
        
        self.assertIn("Table 'students' does not exist.", str(context.exception))

    def test_add_existing_column(self):
        """测试添加已存在的列应忽略或抛出错误"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)

        alter_sql = "ALTER TABLE students ADD COLUMN name TEXT"
        # 根据 Table.add_column 的实现，添加已存在的列会打印消息但不抛出错误
        captured_output = StringIO()
        sys.stdout = captured_output

        self.executor.execute(alter_sql)

        sys.stdout = sys.__stdout__
        output = captured_output.getvalue()
        self.assertIn("Column 'name' already exists in table 'students'.", output)

    def test_drop_nonexistent_column(self):
        """测试删除不存在的列应打印消息但不抛出错误"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)

        alter_sql = "ALTER TABLE students DROP COLUMN age"
        captured_output = StringIO()
        sys.stdout = captured_output

        self.executor.execute(alter_sql)

        sys.stdout = sys.__stdout__
        output = captured_output.getvalue()
        self.assertIn("Column 'age' does not exist in table 'students'.", output)

    def test_modify_nonexistent_column(self):
        """测试修改不存在的列应打印消息但不抛出错误"""
        create_sql = "CREATE TABLE students (id INT, name TEXT)"
        self.executor.execute(create_sql)

        alter_sql = "ALTER TABLE students MODIFY COLUMN age INT"
        captured_output = StringIO()
        sys.stdout = captured_output

        self.executor.execute(alter_sql)

        sys.stdout = sys.__stdout__
        output = captured_output.getvalue()
        self.assertIn("Column 'age' does not exist in table 'students'.", output)

if __name__ == '__main__':
    unittest.main()
