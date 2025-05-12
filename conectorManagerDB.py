class ConectorManagerDB:
    def __init__(self, plataforma):
        self.plataforma = plataforma
        self.connection = None

    def get_connection(self):
        """🔹 Devuelve la conexión adecuada según el valor de plataforma."""
        if self.plataforma == 1:
            from conn.ConciliaDB import DBConnection
            self.connection = DBConnection()  # Conexión a MySQL
            """
                    elif self.plataforma == 2:
                from conn.FacturacionConnectionSybase import DBConnectionSybase
                self.connection = DBConnectionSybase()
            """
        else:
            raise ValueError("Plataforma no soportada")
        return self.connection

