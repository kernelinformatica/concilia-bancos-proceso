import json
import os
from datetime import datetime

import pandas as pd
import logging
from io import BytesIO
from dotenv import load_dotenv
from conectorManagerDB import ConectorManagerDB

load_dotenv()

class Conciliador:
    def __init__(self, bancos_stream: BytesIO, mayor_stream: BytesIO, salida = "/var/www/clients/client4/web28/web/conciliaciones-bancarias/upload/", id_empresa=0, id_usuario=0, id_tipo_conicliacion=1):

        self.bancos_stream = bancos_stream
        self.mayor_stream = mayor_stream
        self.salida = salida
        self.id_empresa = id_empresa
        self.id_usuario = id_usuario
        self.id_tipo_concilia = id_tipo_conicliacion
        if not self.salida.endswith('/'):
            self.salida += '/'

        self.plataforma = int(os.getenv("PLATAFORMA", 1))

    def cargar_datos(self):
        """Carga los datos desde BytesIO en DataFrames."""
        self.df_bancos = pd.read_excel(self.bancos_stream, dtype={'comprobante': str})
        self.df_mayor = pd.read_excel(self.mayor_stream, dtype={'comprobante': str})

    def procesar_datos(self):
        """Procesa los datos para realizar la conciliación."""
        self.df_bancos['c4'] = self.df_bancos['comprobante'].astype(str).str.zfill(4).str[-4:]
        self.df_mayor['c4'] = self.df_mayor['comprobante'].astype(str).str.zfill(4).str[-4:]

        self.df_bancos = self.df_bancos.sort_values(by=['c4', 'importe'])
        self.df_mayor = self.df_mayor.sort_values(by=['c4', 'importe'])

        self.resultado_concilia = pd.merge(
            self.df_mayor, self.df_bancos, on=['c4', 'importe'], how='inner', indicator=True
        )

        self.unicos_empresa = self.df_mayor[~((self.df_mayor['importe'].isin(self.df_bancos['importe'])) &
                                              (self.df_mayor['c4'].isin(self.df_bancos['c4'])))]
        self.unicos_banco = self.df_bancos[~((self.df_bancos['importe'].isin(self.df_mayor['importe'])) &
                                             (self.df_bancos['c4'].isin(self.df_mayor['c4'])))].sort_values(by='concepto')


        self.totales_banco = self.df_bancos.groupby('concepto')['importe'].sum().sort_index()


        # Guardo los datos generados por pandas
        self.guardaResultadosConciliacion(self.resultado_concilia)
        self.guardarTotalesBanco(self.totales_banco)
        #self.guardarUnicosEmpresa()
        #self.guardarUnicosBanco()



    def guardarTotalesBanco(self, resultado_totales_banco):
        print("------------------------ guardarTotalesBanco()  ------------------------")

        numerador = self.traerNumeradorActual()
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()
        sql_select = "SELECT plan_cuentas FROM SisMaster WHERE idEmpresa = %s and m_asiento_concilia = %s AND procesado_sn = 'N' AND estado = 1  group by plan_cuentas, idEmpresa, idUsuario"
        cursor.execute(sql_select, (self.id_empresa, numerador))
        resultado = cursor.fetchone()
        if resultado is None:
           print(f"Plan de cuentas encontrado:")
           plan_cuentas = 0
        else:
           plan_cuentas = resultado[0]



        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        fecha = datetime.now().strftime('%Y-%m-%d')
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()
        try:
            # Borro la tabla antes de volvar la conciliacion
            delete_sql = "DELETE FROM SisMasterTotales WHERE idEmpresa = %s AND procesado_sn = 'N' AND estado = 1"
            cursor.execute(delete_sql, (self.id_empresa,))
            db_connection.commit()
            print("Registros eliminados correctamente antes del INSERT.")

            try:
                # Crear lista de tuplas con los valores a insertar
                df = resultado_totales_banco.to_frame()
                valores = []  # Definir la lista vacía fuera del bucle
                for row in df.itertuples():
                    #print(f'Concepto: {row.Index}, Importe: {row.importe} INgreso: '+ {row.m_ingreso})
                    valores.append((
                        self.id_tipo_concilia,
                        self.id_empresa,
                        numerador,
                        fecha,
                        row.Index,
                        row.importe,
                        str(fecha_actual),
                        'N',
                        0,
                        plan_cuentas,
                        self.id_usuario,
                        1
                    ))

                # Nueva estructura de inserción
                sql = """
                    INSERT INTO SisMasterTotales (
                        idConcilia, idEmpresa, m_asiento_concilia, m_ingreso, concepto, importe, fechayhora, procesado_sn, plan_cuentas, plan_cuentas_concilia, idUsuario, estado
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                # Ejecutar la inserción de múltiples filas
                cursor.executemany(sql, valores)

                # Confirmar la transacción
                db_connection.commit()

                print(f"Se insertaron {cursor.rowcount} registros correctamente.")

            except Exception as e:
                print(f"Error al insertar en la base de datos: {e}")
        except Exception as e:
            print(f"Error al insertar en la base de datos: {e}")

        finally:
            cursor.close()
            db_connection.close()




    def guardaResultadosConciliacion(self, resultado_concilia):
        print("------------------------ guardaResultadosConciliacion() ------------------------")
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()

        try:
            # Borro la tabla antes de volvar la conciliacion
            delete_sql = "DELETE FROM SisMaster WHERE idEmpresa = %s AND procesado_sn = 'N' AND estado = 1"
            cursor.execute(delete_sql, (self.id_empresa,))
            db_connection.commit()
            print("Registros eliminados correctamente antes del INSERT.")

            numerador = self.proximoNumeroAsientoConcilia()
            # Crear lista de tuplas con los valores a insertar
            valores = [
                (
                    self.id_tipo_concilia,
                    self.id_empresa,
                    row['m_asiento'],
                    numerador,
                    row['m_pase'],
                    row['m_ingreso'],
                    row['plan_cuentas'],
                    row['concepto'],
                    row['detalle'],
                    row['nro_comp'],
                    0,  # Debito
                    0,  # Credito
                    row['codigo'],
                    row['Saldo'],
                    row['importe'],
                    "N",
                    self.id_usuario,  # idUsuario
                    1  # estado
                )
                for index, row in resultado_concilia.iterrows()
            ]

            # Nueva estructura de inserción sin placeholders dinámicos
            sql = """
                  INSERT INTO SisMaster (
                      idConcilia, idEmpresa, m_asiento, m_asiento_concilia, m_pase, m_ingreso, plan_cuentas, concepto, detalle, nro_comp, debito, 
                      credito, codigo, saldo,  importe, procesado_sn, idUsuario, estado
                  ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,  %s, %s, %s, %s)
                  """

            # Ejecutar la inserción de múltiples filas
            print(f"Ejecutando consulta: {sql} con valores {valores}")
            cursor.executemany(sql, valores)

            # Confirmar la transacción
            db_connection.commit()

            print(f"Se insertaron {cursor.rowcount} registros correctamente.")

        except Exception as e:
            print(f"Error al insertar en la base de datos: {e}")

        finally:
            cursor.close()
            db_connection.close()

    def guardar_resultados(self):
        manager = ConectorManagerDB(self.plataforma)
        if not os.path.exists(self.salida):
            os.makedirs(self.salida, exist_ok=True)
        """Guarda los resultados en la base de datos"""
        self.unicos_banco.to_csv(f"{self.salida}resultados_bancos.csv", sep=",", decimal=".", index=False)
        self.unicos_empresa.to_csv(f"{self.salida}resultados_empresa.csv", sep=",", decimal=".", index=False)
        self.totales_banco.to_csv(f"{self.salida}totales_banco.csv", sep=",", decimal=".", index=True)
        self.resultado_concilia.to_csv(f"{self.salida}resultados_concilia.csv", sep=",", decimal=".", index=False)
        print("--------->  CONCILIADOR() guardar_resultados() --> "+self.salida)
    def traerNumeradorActual(self):
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()
        try:
          # Obtener el numerador actual
          sql_select = "SELECT numerador FROM Numerador WHERE idEmpresa = %s"
          cursor.execute(sql_select, (self.id_empresa,))
          resultado = cursor.fetchone()
          return resultado[0] if resultado else 0
        except Exception as e:
            return 0
            print("NO SE ENCONTRO UN NUMERADOR VALIDO")



    def proximoNumeroAsientoConcilia(self):
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()

        try:
            # Obtener el numerador actual
            sql_select = "SELECT numerador FROM Numerador WHERE idEmpresa = %s"
            cursor.execute(sql_select, (self.id_empresa,))
            resultado = cursor.fetchone()

            if resultado:
                numerador_actual = resultado[0]
                numerador_nuevo = numerador_actual + 1  # Incrementar el numerador

                # Actualizar el valor en la base de datos
                sql_update = "UPDATE Numerador SET numerador = %s WHERE idEmpresa = %s"
                cursor.execute(sql_update, (numerador_nuevo, self.id_empresa))
                db_connection.commit()  # Confirmar la transacción
                return numerador_nuevo
                print(f"Nuevo numerador actualizado a: {numerador_nuevo}")

            else:
                return 0
                print(f"No se encontró un numerador para idEmpresa {self.id_empresa}")

        except Exception as e:
            print(f"Error al actualizar el numerador: {e}")
            db_connection.rollback()  # Revertir cambios en caso de error

        finally:
            cursor.close()
            db_connection.close()

    def ejecutar(self):
        """Ejecuta todo el flujo de conciliación."""
        self.cargar_datos()
        self.procesar_datos()
        self.guardar_resultados()




