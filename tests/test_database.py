import unittest
from src.database import Database

class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db = Database()

    def test_create_and_drop_table(self):
        self.db.create_table("students", {"id": "INT", "name": "STRING"})
        self.assertIn("students", self.db.tables)
        self.db.drop_table("students")
        self.assertNotIn("students", self.db.tables)

    def test_insert_and_select(self):
        self.db.create_table("students", {"id": "INT", "name": "STRING"})
        self.db.insert_into("students", [1, "Alice"])
        self.db.insert_into("students", [2, "Bob"])
        result = self.db.select_from("students")
        expected = [
            [1, "Alice"],
            [2, "Bob"]
        ]
        self.assertEqual(result, expected)

    def test_select_with_where(self):
        self.db.create_table("students", {"id": "INT", "name": "STRING", "age": "INT"})
        self.db.insert_into("students", [1, "Alice", 22])
        self.db.insert_into("students", [2, "Bob", 19])
        self.db.insert_into("students", [3, "Charlie", 25])
        result = self.db.select_from("students", where=("age", ">", "20"))
        expected = [
            [1, "Alice", 22],
            [3, "Charlie", 25]
        ]
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()

#TestDatabase类包含三个测试用例：
#test_create_and_drop_table：测试表的创建和删除功能。
#test_insert_and_select：测试数据插入和查询功能。
#test_select_with_where：测试带有WHERE条件的查询功能。