# database.py

import pyodbc
import logging
import traceback

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
            error_message = f"Integrity error inserting {data.get('file_name')}: {e}"
            error_details = traceback.format_exc()
            logging.error(error_message)
            self.log_error(error_message, error_details, function_name='insert_data', file_name=data.get('file_name'))
            pass  # Handle duplicate primary key or other integrity errors
        except Exception as e:
            error_message = f"Error inserting data for {data.get('file_name')}: {e}"
            error_details = traceback.format_exc()
            logging.error(error_message)
            self.log_error(error_message, error_details, function_name='insert_data', file_name=data.get('file_name'))
            raise  # Re-raise exception to be handled at a higher level
    
    def ensure_error_log_table(self):
        table_name = 'ErrorLog'
        if not self.table_exists(table_name):
            create_table_sql = """
                CREATE TABLE ErrorLog (
                    log_id INT IDENTITY(1,1) PRIMARY KEY,
                    timestamp DATETIME DEFAULT GETDATE(),
                    error_message NVARCHAR(MAX),
                    error_details NVARCHAR(MAX),
                    file_name NVARCHAR(255) NULL,
                    function_name NVARCHAR(255)
                );
            """
            self.cursor.execute(create_table_sql)
            self.connection.commit()
            logging.info(f"Table '{table_name}' created.")

    def ensure_activity_log_table(self):
        table_name = 'ActivityLog'
        if not self.table_exists(table_name):
            create_table_sql = """
                CREATE TABLE ActivityLog (
                    log_id INT IDENTITY(1,1) PRIMARY KEY,
                    timestamp DATETIME DEFAULT GETDATE(),
                    file_name NVARCHAR(255),
                    status NVARCHAR(50),
                    message NVARCHAR(MAX),
                    details NVARCHAR(MAX) NULL,
                    function_name NVARCHAR(255)
                );
            """
            self.cursor.execute(create_table_sql)
            self.connection.commit()
            logging.info(f"Table '{table_name}' created.")

    def log_error(self, error_message, error_details, function_name, file_name=None):
            # Log to logger
            logging.error(f"{error_message} in {function_name} for file {file_name}")
            logging.error(f"Details: {error_details}")
            # Log to database
            insert_sql = """
                INSERT INTO ErrorLog (error_message, error_details, function_name, file_name)
                VALUES (?, ?, ?, ?)
            """
            values = (error_message, error_details, function_name, file_name)
            try:
                self.cursor.execute(insert_sql, values)
                self.connection.commit()
            except Exception as e:
                # If logging to the database fails, log to logger
                logging.error(f"Failed to log error to database: {e}")
                logging.error(f"Original Error: {error_message}, Details: {error_details}")

    def log_activity(self, file_name, status, message, function_name, details=None):
        insert_sql = """
            INSERT INTO ActivityLog (file_name, status, message, function_name, details)
            VALUES (?, ?, ?, ?, ?)
        """
        values = (file_name, status, message, function_name, details)
        try:
            self.cursor.execute(insert_sql, values)
            self.connection.commit()
        except Exception as e:
            # If logging to the database fails, log to console
            logging.error(f"Failed to log activity to database: {e}")
            logging.error(f"Activity: File={file_name}, Status={status}, Message={message}")

    def close(self):
        self.cursor.close()
        self.connection.close()
