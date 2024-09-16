# database.py

import pyodbc

class DatabaseManager:
    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.connection = self.get_connection()
        self.cursor = self.connection.cursor()

    def get_connection(self):
        conn_str = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={self.server};'
            f'DATABASE={self.database};'
            f'Trusted_Connection=yes;'
        )
        conn = pyodbc.connect(conn_str)
        return conn

    def table_exists(self, table_name):
        self.cursor.execute("""
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = ?
        """, table_name)
        return self.cursor.fetchone() is not None

    def create_table(self, table_name, fields_config):
        create_table_sql = f"CREATE TABLE {table_name} (\n"
        create_table_sql += "    file_name NVARCHAR(255) PRIMARY KEY,\n"
        for field_name in fields_config.keys():
            create_table_sql += f"    [{field_name}] NVARCHAR(MAX),\n"
        create_table_sql = create_table_sql.rstrip(',\n') + "\n);"
        self.cursor.execute(create_table_sql)
        self.connection.commit()

    def insert_data(self, table_name, data, fields_config):
        field_names = list(fields_config.keys())
        columns = ['file_name'] + field_names
        placeholders = ', '.join(['?'] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        values = [data.get('file_name')] + [data.get(field) for field in field_names]
        try:
            self.cursor.execute(insert_sql, values)
            self.connection.commit()
        except pyodbc.IntegrityError as e:
            print(f"Error inserting {data.get('file_name')}: {e}")
            pass

    def close(self):
        self.cursor.close()
        self.connection.close()
