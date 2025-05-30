class ConectorManagerDB:
    def __init__(self, plataforma):
        self.plataforma = plataforma
        self.connection = None

    def get_connection(self):
        if self.plataforma == 1:
            print("Conectando a Mysql !!!")
            from conn.ConciliaDB import DBConnection
            return DBConnection()  # Conexión a MySQL

        else:
            raise ValueError("Plataforma no soportada")

