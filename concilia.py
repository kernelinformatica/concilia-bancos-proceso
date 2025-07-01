import json
import os
from datetime import datetime

import numpy as np
import pandas as pd
import logging
from io import BytesIO

from dateutil.parser import parser, parse
from dotenv import load_dotenv
from conectorManagerDB import ConectorManagerDB



load_dotenv()

class Conciliador:
    # 192.168.254.15 = /home/administrador/conciliaciones/concilia-procesa/archivos/conciliaciones/upload
    # 192.168.254.47 = /var/www/clients/client4/web28/web/conciliaciones-bancarias/upload/
    def __init__(self, bancos_stream: BytesIO, mayor_stream: BytesIO, salida = "/var/www/clients/client4/web28/web/conciliaciones-bancarias/upload/", id_empresa=0, id_usuario=0, id_tipo_conicliacion=1, cuenta_concilia=0):

        self.bancos_stream = bancos_stream
        self.mayor_stream = mayor_stream
        self.salida = salida
        self.id_empresa = id_empresa
        self.id_usuario = id_usuario
        self.id_tipo_concilia = id_tipo_conicliacion
        self.cuenta_concilia = cuenta_concilia
        if not self.salida.endswith('/'):
            self.salida += '/'

        self.plataforma = int(os.getenv("PLATAFORMA", 1))






    def cargar_datos(self):
        self.df_bancos = pd.read_excel(self.bancos_stream, dtype={'comprobante': str})
        self.df_mayor = pd.read_excel(self.mayor_stream, dtype={'comprobante': str})



    def cargar_datos_2(self):
        """Carga los datos desde BytesIO en DataFrames y normaliza nombres de columnas clave."""

        # Mapa de nombres alternativos
        columnas_equivalentes = {
            "importe": ["importe", "m_importe"],
            "comprobante": ["comprobante", "nro_comp_asoc", "nro_comp", "nro_comp_preimp"],
            "detalle": ["detalle", "m_detalle"],


        }

        # Cargar bancos sin modificar columnas
        self.df_bancos = pd.read_excel(self.bancos_stream, dtype={'comprobante': str})

        # Cargar mayor y aplicar renombrado de columnas
        self.df_mayor = pd.read_excel(self.mayor_stream, dtype={'comprobante': str})
        self.df_mayor = self.unificar_columnas(self.df_mayor, columnas_equivalentes)


    def procesar_datos(self):
        """Procesa los datos para realizar la conciliación con tolerancia en importes."""
        # Normalizar comprobante
        self.df_bancos['c4'] = self.df_bancos['comprobante'].astype(str).str.zfill(4).str[-4:]
        self.df_mayor['c4'] = self.df_mayor['comprobante'].astype(str).str.zfill(4).str[-4:]

        # Redondear importes a 2 decimales para tolerancia
        self.df_bancos['importe_r'] = self.df_bancos['importe'].round(2)
        self.df_mayor['importe_r'] = self.df_mayor['importe'].round(2)

        # Merge flexible por c4 e importe redondeado
        self.resultado_concilia = pd.merge(
            self.df_mayor, self.df_bancos, on=['c4', 'importe_r'], how='inner', indicator=True,
            suffixes=('_mayor', '_banco')
        )



        # Después del merge, agrega la columna 'importe' original del mayor o banco
        if 'importe_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['importe'] = self.resultado_concilia['importe_mayor']
        elif 'importe_banco' in self.resultado_concilia.columns:
            self.resultado_concilia['importe'] = self.resultado_concilia['importe_banco']

        # Registros únicos en mayor (no conciliados)
        self.unicos_empresa = self.df_mayor[
            ~self.df_mayor.set_index(['c4', 'importe_r']).index.isin(
                self.resultado_concilia.set_index(['c4', 'importe_r']).index)
        ].copy()
        # Asegura que 'importe' esté presente
        if 'importe' not in self.unicos_empresa.columns and 'importe_r' in self.unicos_empresa.columns:
            self.unicos_empresa['importe'] = self.unicos_empresa['importe_r']

        # Registros únicos en banco (no conciliados)
        self.unicos_banco = self.df_bancos[
            ~self.df_bancos.set_index(['c4', 'importe_r']).index.isin(
                self.resultado_concilia.set_index(['c4', 'importe_r']).index)
        ].copy().sort_values(by='concepto')
        if 'importe' not in self.unicos_banco.columns and 'importe_r' in self.unicos_banco.columns:
            self.unicos_banco['importe'] = self.unicos_banco['importe_r']

        # Totales por concepto
        self.totales_banco = self.df_bancos.groupby('concepto')['importe'].sum().sort_index()










    def unificar_columnas(self, df, alias_dict):
        """Renombra las columnas de un DataFrame según un mapa de nombres posibles."""
        renombrar = {}
        for nombre_final, posibles_alias in alias_dict.items():
            for alias in posibles_alias:
                if alias in df.columns:
                    renombrar[alias] = nombre_final
                    break  # Solo toma el primero que encuentre
        return df.rename(columns=renombrar)



    def guardarUnicosEntidad(self, unicos_entidad, cuenta_concilia):
        print("------------------------ guardarUnicosEntidad()  ------------------------")
        logging.info(unicos_entidad)

        # Filtrar filas con todos los valores NaN
        df = unicos_entidad.dropna(how='all')
        print(f"DataFrame después de eliminar filas con todos los valores NaN: {len(df)} filas")

        # Reemplazar valores NaN en columnas específicas
        df = df.fillna({
            'Fecha': '1970-01-01',
            'comprobante': '',
            'debito': 0,
            'credito': 0,
            'Saldo': 0,
            'codigo': '',
            'importe': 0,
            'concepto': '',
            'c4': 0,
            'nro_comp': 0  # Valor predeterminado para 'nro_comp'
        })
        df = df[~((df['Fecha'] == '1970-01-01') & (df['concepto'] == '') & (df['importe'] == 0))]
        # Asegurarse de que 'nro_comp' sea numérico
        df['nro_comp'] = "0"#pd.to_numeric(df['nro_comp'], errors='coerce').fillna(0).astype(float)
        # Reemplazar valores NaN o vacíos en la columna 'c4' con un valor predeterminado
        df = df.fillna({'c4': 0})
        # Asegurarse de que 'c4' sea numérico y compatible con DECIMAL
        df['c4'] = pd.to_numeric(df['c4'], errors='coerce').fillna(0).astype(float)

        numerador = self.traerNumeradorActual()
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()

        try:
            # Borrar registros existentes
            delete_sql = "DELETE FROM SisMasterEntidad WHERE idEmpresa = %s AND procesado_sn = 'N' AND estado = 1"
            cursor.execute(delete_sql, (self.id_empresa,))
            db_connection.commit()
            print("Registros eliminados correctamente antes del INSERT.")

            # Crear lista de tuplas con los valores a insertar
            valores = []
            for row in df.itertuples(index=False):
                if row is not None:
                    valores.append((
                        self.id_tipo_concilia,
                        self.id_empresa,
                        row.Fecha,
                        row.comprobante,
                        numerador,
                        0,
                        0,
                        row.importe,
                        row.debito,
                        row.credito,
                        row.Saldo,
                        row.codigo,
                        0,
                        row.concepto,
                        "",
                        fecha_actual,
                        'N',
                        "0",
                        cuenta_concilia,
                        self.id_usuario,
                        row.c4,
                        1,
                        0
                    ))

            # Verificar el tamaño de la lista de valores
            print(f"Número de valores a insertar: {len(valores)}")

            # Ejecutar la inserción de múltiples filas
            sql = """INSERT INTO SisMasterEntidad (idConcilia, idEmpresa,  m_ingreso, nro_comp, m_asiento_concilia, m_asiento, m_pase,  importe, debito, credito, saldo, codigo, m_minuta, concepto, detalle, fechayhora, procesado_sn, plan_cuentas, plan_cuentas_concilia, idUsuario, c4, estado, padron_codigo
                   ) VALUES (%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                  """
            cursor.executemany(sql, valores)
            db_connection.commit()

            print(f"Se insertaron {cursor.rowcount} registros correctamente.")

        except Exception as e:
            print(f"Error al insertar en la base de datos: {e}")

        finally:
            cursor.close()
            db_connection.close()


    def guardarUnicosEntidadOriginal(self, unicos_entidad, cuenta_concilia):

        print("------------------------ guardarUnicosEntidadOriginal()  ------------------------")
        logging.info(unicos_entidad)
        df = unicos_entidad
        print(df)

        numerador = self.traerNumeradorActual()
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        fecha = datetime.now().strftime('%Y-%m-%d')
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()
        try:
            # Borro la tabla antes de volvar la conciliacion
            delete_sql = "DELETE FROM SisMasterEntidad WHERE idEmpresa = %s AND procesado_sn = 'N' AND estado = 1 AND idUsuario = %s"
            cursor.execute(delete_sql, (self.id_empresa,self.id_usuario))
            db_connection.commit()
            print("Registros eliminados correctamente antes del INSERT.")

            try:
                # Crear lista de tuplas con los valores a insertar

                valores = []  # Definir la lista vacía fuera del bucle

                for row in df.itertuples(index=False):

                    if row is not None:
                        #print(f'Concepto: {row.concepto}, Importe: {row.importe} INgreso: '+ {row.m_ingreso})


                        valores.append((
                            self.id_tipo_concilia,
                            self.id_empresa,
                            row.Fecha,
                            row.comprobante,
                            numerador,
                            0,
                            0,
                            row.importe,
                            row.debito,
                            row.credito,
                            row.Saldo,
                            row.codigo,
                            0,
                            row.concepto,
                            "",
                            fecha_actual,
                            'N',
                            "0",
                            cuenta_concilia,
                            self.id_usuario,
                            row.c4,
                            1,
                            0
                        ))

                        # Nueva estructura de inserción
                        sql = """INSERT INTO SisMasterEntidad (idConcilia, idEmpresa,  m_ingreso, nro_comp, m_asiento_concilia, m_asiento, m_pase,  importe, debito, credito, saldo, codigo, m_minuta, concepto, detalle, fechayhora, procesado_sn, plan_cuentas, plan_cuentas_concilia, idUsuario, c4, estado, padron_codigo
                               ) VALUES (%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                              """

                        # Ejecutar la inserción de múltiples filas
                        cursor.executemany(sql, valores)

                        # Confirmar la transacción
                        db_connection.commit()

                        print(f"Se insertaron {cursor.rowcount} registros correctamente.")
                        # self.guardarUnicosEntidad(self.unicos_banco, self.cuenta_concilia)

                        # Verificar si la cuenta de conciliación es válida
                        if not cuenta_concilia:
                            print(f"Cuenta de conciliación no válida para el concepto {row.concepto}.")
                            continue

            except Exception as e:
                print(f"Error al insertar en la base de datos: {e}")
        except Exception as e:
            print(f"Error al insertar en la base de datos: {e}")

        finally:
            cursor.close()
            db_connection.close()




    def guardarUnicosEmpresa(self, unicos_empresa, cuenta_concilia):
        logging.info(unicos_empresa)
        print("------------------------ guardarUnicosEmpresa()  ------------------------")

        df = unicos_empresa
        print(df)

        numerador = self.traerNumeradorActual()
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        fecha = datetime.now().strftime('%Y-%m-%d')
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()
        try:
            # Borro la tabla antes de volvar la conciliacion
            delete_sql = "DELETE FROM SisMasterEmpresa WHERE idEmpresa = %s AND procesado_sn = 'N' AND estado = 1"
            cursor.execute(delete_sql, (self.id_empresa,))
            db_connection.commit()
            print("Registros eliminados correctamente antes del INSERT.")

            try:
                # Crear lista de tuplas con los valores a insertar

                valores = []  # Definir la lista vacía fuera del bucle
                for row in df.itertuples():
                    valores.append((
                        self.id_tipo_concilia,
                        self.id_empresa,
                        row.m_ingreso,
                        row.comprobante,
                        numerador,
                        row.m_asiento,
                        row.m_pase,
                        row.importe,
                        row.m_minuta,
                        row.concepto_codigo,
                        row.detalle,
                        fecha_actual,
                        'N',
                        row.plan_cuentas,
                        cuenta_concilia,
                        self.id_usuario,
                        row.c4,
                        1,
                        row.padron_codigo
                    ))

                # Nueva estructura de inserción
                sql = """
                      INSERT INTO SisMasterEmpresa (
                      idConcilia, idEmpresa,  m_ingreso, nro_comp, m_asiento_concilia, m_asiento, m_pase,  importe, m_minuta, concepto, detalle, fechayhora, procesado_sn, plan_cuentas, plan_cuentas_concilia, idUsuario, c4, estado, padron_codigo
                      ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                      """

                # Ejecutar la inserción de múltiples filas
                cursor.executemany(sql, valores)

                # Confirmar la transacción
                db_connection.commit()

                print(f"Se insertaron {cursor.rowcount} registros correctamente.")
                #self.guardarUnicosEntidad(self.unicos_banco, self.cuenta_concilia)
            except Exception as e:
                print(f"Error al insertar en la base de datos: {e}")
        except Exception as e:
            print(f"Error al insertar en la base de datos: {e}")

        finally:
            cursor.close()
            db_connection.close()



    def guardarTotalesBanco(self, resultado_totales_banco, cuenta_concilia):
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
                self.guardarUnicosEmpresa(self.unicos_empresa, self.cuenta_concilia)
                self.guardarUnicosEntidad(self.unicos_banco, self.cuenta_concilia)
            except Exception as e:
                print(f"Error al insertar en la base de datos: {e}")
        except Exception as e:
            print(f"Error al insertar en la base de datos: {e}")

        finally:
            cursor.close()
            db_connection.close()




    def guardaResultadosConciliacion(self, resultado_concilia, cuenta_concilia =0):
        print("------------------------ guardaResultadosConciliacion() ------------------------")
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()

        try:

            try:
                # Borro las tablas, FactConcilia y SisMaster antes de volvar la conciliacion
                delete_sql_cab = "DELETE FROM ConciliaCab WHERE idEmpresa = %s AND procesado_sn = 'N' AND estado = 1 and idUsuario = %s and idConcilia = %s"
                cursor.execute(delete_sql_cab, (self.id_empresa, self.id_usuario, self.id_tipo_concilia))
                db_connection.commit()
            except Exception as e:
                logging.error("No se pudo borrar la cabeceras "+str(e))


            delete_sql = "DELETE FROM SisMaster WHERE idEmpresa = %s AND procesado_sn = 'N' AND idUSuario = %s and estado = 1"
            cursor.execute(delete_sql, (self.id_empresa,self.id_usuario))
            db_connection.commit()
            resultado_concilia["plan_cuentas"] = resultado_concilia["plan_cuentas"].astype(str)  # Convertir a string
            if not resultado_concilia["plan_cuentas"].eq(cuenta_concilia).all():
                result = {
                    "codigo":400,
                    "control":"ERROR",
                    "mensaje": "Error: La cuenta a conciliar en su archivo no coincide con la seleccionada ("+str(cuenta_concilia)+"), verifique el plan de cuentas cargado.",

                }
                return result
            else:
                numerador = self.proximoNumeroAsientoConcilia()
                # Grabo primero ConciliaCab
                sql_insert_cab = """
                            INSERT INTO ConciliaCab (idEmpresa, idConcilia, nombre, descripcion, asiento_concilia, procesado_sn, idUsuario, estado)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                cursor.execute(sql_insert_cab, (self.id_empresa, self.id_tipo_concilia, numerador, numerador, numerador, "N", self.id_usuario, 1))
                db_connection.commit()



                valores = [
                    (
                        self.id_tipo_concilia,
                        self.id_empresa,
                        row['m_asiento'],
                        numerador,
                        row['m_pase'],
                        self.normalizarFechas(row['m_ingreso']),
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



                cursor.executemany(sql, valores)

                # Confirmar la transacción
                db_connection.commit()
                result = {
                    "codigo": 200,
                    "control": "OK",
                    "mensaje": "Proceso de conciliación se completo con éxito. ("+str(cursor.rowcount)+") registros insertados."

                }
                self.guardarTotalesBanco(self.totales_banco, self.cuenta_concilia)
                # guardar las diferencias
                return result


        except Exception as e:
            result = {
                "codigo": 400,
                "control": "ERROR",
                "mensaje": "Error al insertar en la base de datos "+str(e),

            }
            return result


        finally:
            cursor.close()
            db_connection.close()

    def normalizarFechas(self, fecha):

        if not fecha or str(fecha).strip().lower() in ["", "none", "nan", "null"]:
            print("⚠️ Fecha vacía o inválida, se usará 1900-01-01")
            return "1900-01-01"

        try:
            fech = parse(str(fecha), dayfirst=True)

            return fech.strftime('%Y-%m-%d')
        except Exception as e:
            print(f"⚠️ Error al parsear: {fecha} → {e}")
            return "1900-01-01"

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
        """Ejecuta todo el flujo de conciliación y devuelve el resultado."""
        try:
            self.cargar_datos_2()
            self.procesar_datos()
            return self.guardaResultadosConciliacion(self.resultado_concilia, self.cuenta_concilia)
        except Exception as e:
            # Retornar error en caso de excepción
            return {
                "codigo": 500,
                "control": "ERROR",
                "mensaje": f"Error durante la ejecución del proceso: {str(e)}"
            }




