import json
import os
from datetime import datetime

import numpy as np
import pandas as pd
import logging
from io import BytesIO
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
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
        self.columnas_equivalentes = {}
        if not self.salida.endswith('/'):
            self.salida += '/'

        self.plataforma = int(os.getenv("PLATAFORMA", 1))









    def cargar_datos(self):
        """Carga los datos desde BytesIO en DataFrames y normaliza nombres de columnas clave."""

        # Mapa de nombres alternativos
        self.columnas_equivalentes = {
            "importe": ["importe", "m_importe", "saldo"],
            "comprobante": ["comprobante", "nro_comp_asoc", "nro_comp", "nro_comp_preimp", "nro", "numero"],
            "detalle": ["detalle", "m_detalle", "descripcion", "m_descripcion", "m_glosa"],
            "cuit": ["cuit", "cuit_proveedor", "cuit_cliente", "cuit_beneficiario"],
            "concepto": ["concepto", "m_concepto", "concepto_codigo", "concepto_descripcion", "concepto_banco",
                         "concepto_bancos", "bancos_concepto", "conce", "concept"],
        }






        # Cargar bancos y aplicar renombrado de columnas
        self.df_bancos = pd.read_excel(self.bancos_stream, dtype={'comprobante': str})
        self.df_bancos = self.unificar_columnas(self.df_bancos, self.columnas_equivalentes)

        # Cargar mayor y aplicar renombrado de columnas
        self.df_mayor = pd.read_excel(self.mayor_stream, dtype={'comprobante': str})
        self.df_mayor = self.unificar_columnas(self.df_mayor, self.columnas_equivalentes)
        print(self.df_mayor)



    def get_col(self, df, key):
        equivalentes = [e.lower().strip() for e in self.columnas_equivalentes[key]]
        columnas_actuales = [col.lower().strip() for col in df.columns]

        for nombre_equivalente in equivalentes:
            nombre_equivalente = nombre_equivalente.lower().strip()
            for col in df.columns:
                if col.lower().strip() == nombre_equivalente:
                    return col
        # Fallback: coincidencia parcial
        for nombre_equivalente in equivalentes:
            for col in df.columns:
                if nombre_equivalente.lower() in col.lower():
                    return col
        return None

    def procesar_datos(self):
        print("Procesa los datos para realizar la conciliación con tolerancia en importes.")

        # Helper para obtener el nombre real de columna según equivalencias


        # Obtener nombres reales de columnas
        col_comp = self.get_col(self.df_bancos, 'comprobante')

        col_imp = self.get_col(self.df_bancos, 'importe')
        col_imp_mayor = self.get_col(self.df_bancos, 'importe')
        col_conc_banco = self.get_col(self.df_bancos, 'concepto')
        col_conc_mayor = self.get_col(self.df_mayor, 'concepto')
        col_detalle_banco = self.get_col(self.df_bancos, 'concepto')
        col_detalle_mayor = self.get_col(self.df_mayor, 'detalle')
        col_cuit_mayor = self.get_col(self.df_mayor, 'cuit')
        col_comp_banco = self.get_col(self.df_bancos, 'comprobante')
        col_comp_mayor = self.get_col(self.df_mayor, 'comprobante')

        # Normalizar comprobante
        self.df_bancos['c4'] = self.df_bancos[col_comp].astype(str).str.zfill(4).str[-4:]
        self.df_mayor['c4'] = self.df_mayor[col_comp].astype(str).str.zfill(4).str[-4:]

        # Redondear importes
        self.df_bancos['importe_r'] = self.df_bancos[col_imp].round(2)
        self.df_mayor['importe_r'] = self.df_mayor[col_imp].round(2)


        if not all([col_comp_banco, col_comp_mayor, col_imp, col_imp_mayor, col_detalle_banco, col_detalle_mayor]):
            missing = [
                "comprobante en banco" if not col_comp_banco else "",
                "comprobante en mayor" if not col_comp_mayor else "",
                "importe en banco" if not col_imp else "",
                "importe en mayor" if not col_imp else "",
                #"detalle en banco" if not col_detalle_banco else "",
                #"detalle en mayor" if not col_detalle_mayor else "",
            ]
            raise ValueError(f"Faltan columnas esenciales para la conciliación: {', '.join(filter(None, missing))}")
            self.df_bancos['comprobante_norm'] = self.df_bancos[col_comp_banco].apply(self.normalizar_comprobante)
            self.df_mayor['comprobante_norm'] = self.df_mayor[col_comp_mayor].apply(self.normalizar_comprobante)
            self.df_bancos['c4'] = self.df_bancos[col_comp_banco].astype(str).str.zfill(12).str[-4:]
            self.df_mayor['c4'] = self.df_mayor[col_comp_mayor].astype(str).str.zfill(12).str[-4:]
            self.df_bancos['c8'] = self.df_bancos[col_comp_banco].astype(str).str.zfill(12).str[-8:]
            self.df_mayor['c8'] = self.df_mayor[col_comp_mayor].astype(str).str.zfill(12).str[-8:]
            self.df_mayor['cuit'] = self.df_mayor[col_cuit_mayor].astype(str).str.zfill(12).str[-11:]
            self.df_bancos['importe_r'] = pd.to_numeric(self.df_bancos[col_imp_banco], errors='coerce').fillna(0).round(
                2)
            self.df_mayor['importe_r'] = pd.to_numeric(self.df_mayor[col_imp_mayor], errors='coerce').fillna(0).round(2)
            self.df_bancos['importe_abs'] = self.df_bancos['importe_r']
            self.df_mayor['importe_abs'] = self.df_mayor['importe_r']

            self.df_bancos['cuit'] = self.df_bancos[col_detalle_banco].apply(self.extraer_cuit).apply(
                self.normalizar_cuit)
            if col_cuit_mayor:
                self.df_mayor['cuit'] = self.df_mayor[col_cuit_mayor].apply(self.normalizar_cuit)
            else:
                self.df_mayor['cuit'] = self.df_mayor[col_detalle_mayor].apply(self.extraer_cuit).apply(
                    self.normalizar_cuit)

            logging.info(f"Inicial: bancos={len(self.df_bancos)}, mayor={len(self.df_mayor)}")

        # Merge flexible


        self.resultado_concilia = pd.merge(
            self.df_mayor, self.df_bancos,
            on=['c4', 'importe_r'], how='inner', indicator=True,
            suffixes=('_mayor', '_banco')
        )


        # Aplicación para concepto y detalle
        self.asignar_columna_equivalente(
            self.resultado_concilia, 'concepto',
            self.columnas_equivalentes['concepto'], ['banco', 'mayor']
        )

        self.asignar_columna_equivalente(
            self.resultado_concilia, 'detalle',
            self.columnas_equivalentes['detalle'], ['banco', 'mayor']
        )



        # Asignar importe original
        if f'{col_imp}_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['importe'] = self.resultado_concilia[f'{col_imp}_mayor']
        elif f'{col_imp}_banco' in self.resultado_concilia.columns:
            self.resultado_concilia['importe'] = self.resultado_concilia[f'{col_imp}_banco']

        # Registros únicos en mayor
        self.unicos_empresa = self.df_mayor[
            ~self.df_mayor.set_index(['c4', 'importe_r']).index.isin(
                self.resultado_concilia.set_index(['c4', 'importe_r']).index)
        ].copy()
        if 'importe' not in self.unicos_empresa.columns:
            self.unicos_empresa['importe'] = self.unicos_empresa['importe_r']

        # Registros únicos en banco
        self.unicos_banco = self.df_bancos[
            ~self.df_bancos.set_index(['c4', 'importe_r']).index.isin(
                self.resultado_concilia.set_index(['c4', 'importe_r']).index)
        ].copy()
        if 'importe' not in self.unicos_banco.columns:
            self.unicos_banco['importe'] = self.unicos_banco['importe_r']

        # Normalizar columna plan de cuentas para eliminar decimales innecesarios antes de guardar o insertar
        if 'plan_cuentas' in self.resultado_concilia.columns:
            self.resultado_concilia['plan_cuentas'] = self.resultado_concilia['plan_cuentas'].apply(lambda x: str(int(x)) if pd.notnull(x) and isinstance(x, float) and x.is_integer() else str(x))

        # Totales por concepto
        self.totales_banco = self.df_bancos.groupby(col_conc_banco)[col_imp].sum().sort_index()
        # Conciliación adicional por CUIT e importe
        # Conciliación por CUIT e importe
        resultado_cuit = self.conciliar_por_cuit_importe(self.unicos_empresa, self.unicos_banco)
        if not resultado_cuit.empty:
            self.resultado_concilia = pd.concat([self.resultado_concilia, resultado_cuit], ignore_index=True)
        print (self.resultado_concilia)
        # fin merge flexible antes de nuevo merge flexible








    def normalizar_datos(df, origen):
        df = df.copy()

        # Validar columnas esperadas
        columnas_requeridas = ['fecha', 'concepto', 'comprobante', 'importe']
        faltantes = [col for col in columnas_requeridas if col not in df.columns]
        if faltantes:
            raise ValueError(f"{origen}: faltan columnas requeridas: {faltantes}")

        # Normalizar comprobante
        df['c4'] = df['comprobante'].astype(str).str.zfill(4).str[-4:]

        # Validar y convertir importe
        df['importe'] = pd.to_numeric(df['importe'], errors='coerce')
        df['importe_r'] = df['importe'].round(2)

        # Validar concepto como string
        #df['concepto'] = df['concepto'].astype(str)

        return df

    """
    def normalizar_datos2(df, origen):
        df = df.copy()

        # Validar columnas esperadas
        columnas_requeridas = ['fecha', 'concepto', 'comprobante', 'importe']
        faltantes = [col for col in columnas_requeridas if col not in df.columns]
        if faltantes:
            raise ValueError(f"{origen}: faltan columnas requeridas: {faltantes}")

        # Normalizar comprobante
        df['comprobante'] = df['comprobante'].astype(str)
        df['c4'] = df['comprobante'].str.zfill(4).str[-4:]

        # Validar y convertir importe
        df['importe'] = pd.to_numeric(df['importe'], errors='coerce')
        df['importe_r'] = df['importe'].round(2)

        # Normalizar concepto como string, incluso si viene como número
        df['concepto'] = df['concepto'].astype(str).str.strip()

        return df
    """

    def asignar_columna_equivalente(self, df, destino, equivalencias, sufijos):
        for base in equivalencias:
            for suf in sufijos:
                col = f"{base}_{suf}"
                if col in df.columns:
                    df[destino] = df[col]
                    logging.info(f"Asignado '{destino}' desde columna: {col}")
                    return
        # Fallback por coincidencia parcial
        posibles = [col for col in df.columns if destino in col]
        if posibles:
            df[destino] = df[posibles[0]]
            logging.info(f"Asignado '{destino}' desde columna parcial: {posibles[0]}")



    def unificar_columnas(self, df, alias_dict, log_faltantes=False):
        """Renombra columnas según alias y crea vacías si no se encuentran. Opcionalmente loguea faltantes."""
        df = df.copy()
        renombrar = {}
        faltantes = []

        for nombre_final, posibles_alias in alias_dict.items():
            for alias in posibles_alias:
                if alias in df.columns:
                    renombrar[alias] = nombre_final
                    break
            else:
                # Si no se encuentra ningún alias, crear columna vacía
                df[nombre_final] = None
                faltantes.append(nombre_final)

        df = df.rename(columns=renombrar)

        if log_faltantes and faltantes:
            print(f"[Unificar] Columnas no encontradas y creadas vacías: {faltantes}")

        return df

    def guardarUnicosEntidad(self, unicos_entidad, cuenta_concilia):
        print("|------------------------> guardarUnicosEntidad("+str(cuenta_concilia)+")  <------------------------|")
        logging.info(unicos_entidad)

        # Filtrar filas con todos los valores NaN
        df = unicos_entidad.dropna(how='all')
        print(df)
        print(f"DataFrame después de eliminar filas con todos los valores NaN: {len(df)} filas")

        # Reemplazar valores NaN en columnas específicas
        df = df.fillna({
            'Fecha': '1970-01-01',
            'concepto': '',
            'comprobante': '',
            'importe': 0,

        })
        df = df[~((df['Fecha'] == '1970-01-01') & (df['concepto'] == '') & (df['importe'] == 0))]
        # Asegurarse de que 'nro_comp' sea numérico
        df['comprobante'] = "0"#pd.to_numeric(df['nro_comp'], errors='coerce').fillna(0).astype(float)


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
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        row.concepto,
                        fecha_actual,
                        'N',
                        "0",
                        cuenta_concilia,
                        self.id_usuario,
                        0,
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
            # Borro la tabla antes de volar la conciliacion
            delete_sql = "DELETE FROM SisMasterEntidad WHERE idEmpresa = %s AND procesado_sn = 'N' AND estado = 1 AND idUsuario = %s"
            cursor.execute(delete_sql, (self.id_empresa,self.id_usuario))
            db_connection.commit()
            print("Registros eliminados correctamente antes del INSERT.")

            try:
                # Crear lista de tuplas con los valores a insertar

                valores = []  # Definir la lista vacía fuera del bucle

                for row in df.itertuples(index=False):

                    if row is not None:
                        print(f'Concepto: {row.concepto}, Importe: {row.importe} INgreso: '+ {row.m_ingreso})


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
                            0,
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

        logging.info("------------------------ guardarUnicosEmpresa()  ------------------------")
        logging.info(unicos_empresa)
        logging.info("-------------------------------------------------------------------------")
        df = unicos_empresa
        print(df)

        numerador = self.traerNumeradorActual()
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        fecha = datetime.now().strftime('%Y-%m-%d')
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()
        try:
            # Borro la tabla antes de volar la conciliacion
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

        logging.info("------------------------ guardarTotalesBanco()  ------------------------")
        logging.info(resultado_totales_banco)
        logging.info("-------------------------------------------------------------------------")

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
            # Borro la tabla antes de volar la conciliacion
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
        logging.info("|------------------------> guardaResultadosConciliacion() <------------------------|")
        logging.info(resultado_concilia)
        logging.info("---------------------------------------------------------------------------------------")
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()

        try:

            try:
                # Borro las tablas, FactConcilia y SisMaster antes de volar la conciliacion
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
                        row['concepto_mayor'] if 'concepto_mayor' in row else row['concepto'],
                        row['detalle_mayor'],
                        row['nro_comp'],
                        row['c4'] if ('c4' in row and pd.notnull(row['c4'])) else row.get('comprobante_banco', None), # nro_comp_asoc
                        0,  # Debito
                        0,  # Credito
                        0,
                        row['importe'],
                        row['importe'],
                        "N",
                        self.id_usuario,  # idUsuario
                        1,  # estado
                        # CUIT: si existe cuit_mayor y no es nulo/NaN, usarlo; si no, usar cuit
                        row['cuit_mayor'] if ('cuit_mayor' in row and pd.notnull(row['cuit_mayor'])) else row.get('cuit', None)
                    )
                    for index, row in resultado_concilia.iterrows()

                ]
                logging.info(valores)
                # Nueva estructura de inserción sin placeholders dinámicos
                sql = """
                      INSERT INTO SisMaster (
                          idConcilia, idEmpresa, m_asiento, m_asiento_concilia, m_pase, m_ingreso, plan_cuentas, concepto, detalle, nro_comp, nro_comp_asoc,debito, 
                          credito, codigo, saldo,  importe, procesado_sn, idUsuario, estado, cuit
                      ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,  %s, %s, %s, %s, %s,%s)
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
                "mensaje": "GuardarResultadosConciliacion: Error al insertar en la base de datos "+str(e),
                "detalle" : str(valores)
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
            self.cargar_datos()
            self.procesar_datos()
            return self.guardaResultadosConciliacion(self.resultado_concilia, self.cuenta_concilia)
        except Exception as e:
            # Retornar error en caso de excepción
            return {
                "codigo": 500,
                "control": "ERROR",
                "mensaje": f"Error durante la ejecución del proceso: {str(e)}"
            }




    def limpiar_columnas_resultado(self, df):
        # Elimina columnas innecesarias y asegura el formato correcto
        columnas_validas = [
            'cuit', 'importe_r', 'comprobante_norm', 'c4', 'c8', 'concepto', 'detalle',
            # Agrega aquí todas las columnas que espera el INSERT final
        ]
        # Elimina columnas Unnamed y otras no deseadas
        df = df.loc[:, [col for col in df.columns if col in columnas_validas]]
        if 'UNNAMED: 0' in df.columns:
            df = df.drop('UNNAMED: 0', axis=1)
        return df


    def extraer_cuit(self, texto):
        """Extrae el primer CUIT válido de un texto usando regex."""
        import re
        if not isinstance(texto, str):
            return ''
        match = re.search(r'\b(\d{11})\b', texto)
        return match.group(1) if match else ''

    def normalizar_cuit(self, cuit):
        """Normaliza el CUIT a string de 11 dígitos, o vacío si no es válido."""
        if pd.isnull(cuit):
            return ''
        cuit_str = str(cuit).strip()
        return cuit_str if len(cuit_str) == 11 and cuit_str.isdigit() else ''

    def conciliar_por_cuit_importe(self, mayor_rest, banco_rest):
        """
        Realiza la conciliación por CUIT e importe sobre los no conciliados.
        """
        # Extraer y normalizar CUIT del banco (usando detalle/concepto)
        col_detalle_banco = self.get_col(banco_rest, 'concepto')
        if col_detalle_banco is None or banco_rest[col_detalle_banco].isnull().all():
            col_detalle_banco = self.get_col(banco_rest, 'concepto')
        banco_rest['cuit'] = banco_rest[col_detalle_banco].apply(self.extraer_cuit).apply(self.normalizar_cuit)
        mayor_rest['cuit'] = mayor_rest['cuit'].apply(self.normalizar_cuit)
        # Filtrar solo los que tienen CUIT e importe
        mayor_con_cuit = mayor_rest.dropna(subset=['cuit', 'importe_r'])
        banco_con_cuit = banco_rest.dropna(subset=['cuit', 'importe_r'])
        mayor_con_cuit['cuit'] = mayor_con_cuit['cuit'].astype(str)
        banco_con_cuit['cuit'] = banco_con_cuit['cuit'].astype(str)
        resultado_cuit = pd.DataFrame()
        if not mayor_con_cuit.empty and not banco_con_cuit.empty:
            try:
                resultado_cuit = pd.merge(
                    mayor_con_cuit,
                    banco_con_cuit,
                    on=['cuit', 'importe_r'],
                    how='inner',
                    indicator=True,
                    suffixes=('_mayor', '_banco')
                )
                # Asignar correctamente las columnas del banco al resultado
                resultado_cuit['saldo'] = resultado_cuit['saldo_banco'] if 'saldo_banco' in resultado_cuit.columns else resultado_cuit.get('saldo', 0)
                resultado_cuit['importe'] = resultado_cuit['importe_banco'] if 'importe_banco' in resultado_cuit.columns else resultado_cuit.get('importe', 0)
            except Exception as e:
                logging.warning(f"Conciliación por CUIT falló: {e}")
        return resultado_cuit
