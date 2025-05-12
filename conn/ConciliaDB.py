import mysql.connector
from mysql.connector import Error

class DBConnection:
    def __init__(self):

        # except configparser.Error as e:
        #    print("Error al leer el archivo de configuración:", e)
        #    self.conn = None
        self.host = "10.0.0.33"
        self.database = "db_conciliacion"
        self.user = "root"
        self.password = "root"
        self.port = "3306"
        self.conn = self.create_connection()


    def create_connection(self):
        try:
            conn = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port = self.port
            )
            if conn.is_connected():
                print("Connection to MySQL se ha establecido con éxito.")
                return conn
        except Error as e:
            print(f"Error: {e}")
            self.conn = None

    def execute(self, query):
        if self.conn is None:
            print("La Connection no se pudo establecer")
            return None
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            self.conn.commit()
           # print("Query ejecutado con éxito")
        except Error as e:
            print(f"Error: {e}")
            return None
        return cursor
    def executemany(self, query, rows):

        if self.conn is None:
            print("La Connection no se pudo establecer")
            return None
        cursor = self.conn.cursor()
        try:
            cursor.executemany(query, rows)
            self.conn.commit()
            print(f'{cursor.rowcount} registros insertados con éxito')
        except Error as e:
            print(f"Error: {e}")
            return None
        return cursor
    def close_connection(self):
        if self.conn is not None and self.conn.is_connected():
            self.conn.close()
            print("Connection cerrada")

