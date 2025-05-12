import json
import os

import pandas as pd
from dotenv import load_dotenv
import logging
from conectorManagerDB import ConectorManagerDB
load_dotenv()
class Conciliador:
    def __init__(self, ruta_bancos="C:/temp/bancos/bancos.xls", ruta_mayor="C:/temp/bancos/mayor.xls", salida="C:temp//bancos/"):
        self.ruta_bancos = ruta_bancos
        self.ruta_mayor = ruta_mayor
        self.salida = salida
        self.plataforma = int(os.getenv("PLATAFORMA", 1))
    def cargar_datos(self):
        """Carga los datos de los archivos Excel en DataFrames."""
        self.df_bancos = pd.read_excel(self.ruta_bancos, dtype={'comprobante': str})
        self.df_mayor = pd.read_excel(self.ruta_mayor, dtype={'comprobante': str})

    def procesar_datos(self):
        """Procesa los datos para realizar la conciliación."""
        # Crear columna c4 con los últimos 4 dígitos del comprobante
        self.df_bancos['c4'] = self.df_bancos['comprobante'].astype(str).str.zfill(4).str[-4:]
        self.df_mayor['c4'] = self.df_mayor['comprobante'].astype(str).str.zfill(4).str[-4:]

        # Ordenar por c4 e importe
        self.df_bancos = self.df_bancos.sort_values(by=['c4', 'importe'])
        self.df_mayor = self.df_mayor.sort_values(by=['c4', 'importe'])

        # Conciliar movimientos que coinciden por importe y comprobante
        self.resultado_concilia = pd.merge(
            self.df_mayor, self.df_bancos, on=['c4', 'importe'], how='inner', indicator=True
        )

        # Movimientos únicos en bancos y empresa
        self.unicos_empresa = self.df_mayor[~((self.df_mayor['importe'].isin(self.df_bancos['importe'])) &
                                              (self.df_mayor['c4'].isin(self.df_bancos['c4'])))]
        self.unicos_banco = self.df_bancos[~((self.df_bancos['importe'].isin(self.df_mayor['importe'])) &
                                             (self.df_bancos['c4'].isin(self.df_mayor['c4'])))].sort_values(by='concepto')

        # Totales agrupados por concepto
        self.totales_banco = self.df_bancos.groupby('concepto')['importe'].sum().sort_index()

    def guardar_resultados(self):
        manager = ConectorManagerDB(self.plataforma)
        """
         aca grabar los resultados e la base de dato db_concilia de la web

        :return:
        """
        """Guarda los resultados en archivos CSV."""
        self.unicos_banco.to_csv(f"{self.salida}resultados_bancos.csv", sep=",", decimal=".", index=False)
        self.unicos_empresa.to_csv(f"{self.salida}resultados_empresa.csv", sep=",", decimal=".", index=False)
        self.totales_banco.to_csv(f"{self.salida}totales_banco.csv", sep=",", decimal=".", index=True)
        self.resultado_concilia.to_csv(f"{self.salida}resultados_concilia.csv", sep=",", decimal=".", index=False)




    def validarConexion(self):
        """Valida la conexión con el servicio web"""
        try:
            logging.info("Validando conexión con el servicio web de conciliacion base...")
            manager = ConectorManagerDB(self.plataforma)
            db = manager.get_connection()


            if self.plataforma == 1:

                with db.conn.cursor() as cursor:
                    query = """SELECT 1"""  # 🔹
                    cursor.execute(query)
                    rows = cursor.fetchone()
                    if rows is None:
                        logging.error("Error en la conexión a la base de datos.")
                        resp_json = {
                            "control": "ERROR",
                            "codigo": "500",
                            "mensaje": "Error en la conexión a la base de datos.",
                            "servidores": {
                                "DbServer": "ERROR",
                            }
                        }
                        logging.info(json.dumps(resp_json, ensure_ascii=False, indent=4))
                        return resp_json
                    else:
                        logging.info("Conexión exitosa a la base de datos.")
                        resp_json = {
                            "control": "OK",
                            "codigo": "200",
                            "mensaje": "Conexión exitosa",
                            "servidores": {
                                "DbServer": "OK",
                            }
                        }
                    logging.info(json.dumps(resp_json, ensure_ascii=False, indent=4))
                    return resp_json
            else:
                # Implementar la lógica para otras plataformas
                return {
                    "control": "ERROR",
                    "codigo": 500,
                    "mensaje": f"La validación de conexión para esta plataforma no está implementada.: {self.plataforma}"
                }
                raise NotImplementedError("La validación de conexión para esta plataforma no está implementada.")
        except Exception as e:
            logging.error(f"Error al validar la conexión: {str(e)}")
            return {
                "control": "ERROR",
                "codigo": 500,
                "mensaje": f"Error interno al validar la conexión: {str(e)}"
            }



    def ejecutar(self):
        """Ejecuta todo el flujo de conciliación."""
        self.cargar_datos()
        self.procesar_datos()
        self.guardar_resultados()