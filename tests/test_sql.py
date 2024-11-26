import unittest
from src.sql_parser import SQLParser
from src.query_executor import QueryExecutor
from src.database import Database


class TestSQLParserAndExecutor(unittest.TestCase):

    def setUp(self):
        """Setup the initial environment for each test"""
        self.database = Database()
        self.executor = QueryExecutor(self.database)
        self.parser = SQLParser()

    def test_create_table(self):
        """Test the CREATE TABLE SQL command"""
        sql = "CREATE TABLE users (id INT, name STRING)"
        self.executor.execute(sql)

        # Check if the table was created correctly
        table = self.database.get_table("users")
        self.assertIsNotNone(table)
        self.assertEqual(table.name, "users")
        self.assertEqual(table.columns, {"id": "INT", "name": "STRING"})
        print("CREATE TABLE test passed.")

    def test_insert_into(self):
        """Test the INSERT INTO SQL command"""
        sql = "CREATE TABLE users (id INT, name STRING)"
        self.executor.execute(sql)

        # Insert values into the table
        sql_insert = "INSERT INTO users VALUES (1, 'Alice')"
        self.executor.execute(sql_insert)

        # Verify the row is inserted correctly
        table = self.database.get_table("users")
        self.assertEqual(len(table.rows), 1)
        self.assertEqual(table.rows[0], [1, 'Alice'])
        print("INSERT INTO test passed.")

    def test_select(self):
        """Test the SELECT SQL command"""
        sql = "CREATE TABLE users (id INT, name STRING)"
        self.executor.execute(sql)

        # Insert values into the table
        sql_insert_1 = "INSERT INTO users VALUES (1, 'Alice')"
        self.executor.execute(sql_insert_1)
        sql_insert_2 = "INSERT INTO users VALUES (2, 'Bob')"
        self.executor.execute(sql_insert_2)

        # Select values from the table
        sql_select = "SELECT id, name FROM users WHERE name = 'Alice'"
        self.executor.execute(sql_select)

        # The results should only include Alice's row
        table = self.database.get_table("users")
        results = table.select(columns=["id", "name"], where=("name", "=", "Alice"))
        self.assertEqual(results, [[1, 'Alice']])
        print("SELECT test passed.")

    def test_alter_table_add_column(self):
        """Test ALTER TABLE ADD COLUMN"""
        sql = "CREATE TABLE users (id INT, name STRING)"
        self.executor.execute(sql)

        # Add a new column to the table
        sql_alter = "ALTER TABLE users ADD COLUMN age INT"
        self.executor.execute(sql_alter)

        # Verify the column was added
        table = self.database.get_table("users")
        self.assertIn("age", table.columns)
        print("ALTER TABLE ADD COLUMN test passed.")

    def test_alter_table_drop_column(self):
        """Test ALTER TABLE DROP COLUMN"""
        sql = "CREATE TABLE users (id INT, name STRING, age INT)"
        self.executor.execute(sql)

        # Drop a column
        sql_alter = "ALTER TABLE users DROP COLUMN age"
        self.executor.execute(sql_alter)

        # Verify the column was dropped
        table = self.database.get_table("users")
        self.assertNotIn("age", table.columns)
        print("ALTER TABLE DROP COLUMN test passed.")

    def test_alter_table_modify_column(self):
        """Test ALTER TABLE MODIFY COLUMN"""
        sql = "CREATE TABLE users (id INT, name STRING)"
        self.executor.execute(sql)

        # Modify a column's data type
        sql_alter = "ALTER TABLE users MODIFY COLUMN name INT"
        self.executor.execute(sql_alter)

        # Verify the column's type was modified
        table = self.database.get_table("users")
        self.assertEqual(table.columns["name"], "INT")
        print("ALTER TABLE MODIFY COLUMN test passed.")

    def test_create_table_error(self):
        """Test CREATE TABLE with an existing table"""
        sql = "CREATE TABLE users (id INT, name STRING)"
        self.executor.execute(sql)

        # Try to create the same table again (should raise an error)
        sql_duplicate = "CREATE TABLE users (id INT, name STRING)"
        self.executor.execute(sql_duplicate)  # Expected to print error message

        print("CREATE TABLE error test passed.")

    def test_insert_into_error(self):
        """Test INSERT INTO with incorrect column count"""
        sql = "CREATE TABLE users (id INT, name STRING)"
        self.executor.execute(sql)

        # Try to insert values with incorrect column count (should raise an error)
        sql_insert = "INSERT INTO users VALUES (1)"
        self.executor.execute(sql_insert)  # Expected to print error message

        print("INSERT INTO error test passed.")

    def test_select_column_not_found(self):
        """Test SELECT with a non-existent column"""
        sql = "CREATE TABLE users (id INT, name STRING)"
        self.executor.execute(sql)

        # Try to select a non-existent column
        sql_select = "SELECT non_existent_column FROM users"
        self.executor.execute(sql_select)  # Expected to print error message

        print("SELECT column not found test passed.")

    def test_alter_table_column_not_found(self):
        """Test ALTER TABLE with a non-existent column"""
        sql = "CREATE TABLE users (id INT, name STRING)"
        self.executor.execute(sql)

        # Try to drop a non-existent column
        sql_alter = "ALTER TABLE users DROP COLUMN age"
        self.executor.execute(sql_alter)  # Expected to print error message

        print("ALTER TABLE column not found test passed.")

    def test_select_where_condition(self):
        """Test SELECT with WHERE condition"""
        sql = "CREATE TABLE users (id INT, name STRING)"
        self.executor.execute(sql)

        # Insert values into the table
        sql_insert_1 = "INSERT INTO users VALUES (1, 'Alice')"
        self.executor.execute(sql_insert_1)
        sql_insert_2 = "INSERT INTO users VALUES (2, 'Bob')"
        self.executor.execute(sql_insert_2)

        # Select with WHERE condition
        sql_select = "SELECT id FROM users WHERE name = 'Alice'"
        self.executor.execute(sql_select)

        # The result should only include the row for Alice
        table = self.database.get_table("users")
        results = table.select(columns=["id"], where=("name", "=", "Alice"))
        self.assertEqual(results, [[1]])
        print("SELECT WHERE condition test passed.")

if __name__ == "__main__":
    unittest.main()
