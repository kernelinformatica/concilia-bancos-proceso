import json
import os
from datetime import datetime
import re
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
        # Detectar el motor según el formato del archivo
        bancos_engine = self.detectar_motor(self.bancos_stream)
        mayor_engine = self.detectar_motor(self.mayor_stream)
        self.df_bancos = pd.read_excel(self.bancos_stream, dtype={'comprobante': str}, engine=bancos_engine)
        self.df_mayor = pd.read_excel(self.mayor_stream, dtype={'comprobante': str}, engine=mayor_engine)



    def cargar_datos_2(self):
        """Carga los datos desde BytesIO en DataFrames y normaliza nombres de columnas clave."""

        # Mapa de nombres alternativos
        self.columnas_equivalentes = {
            "importe": ["importe", "m_importe","saldo"],
            "comprobante": ["comprobante", "nro_comp_asoc", "nro_comp", "nro_comp_preimp", "nro", "numero"],
            "detalle": ["detalle", "m_detalle", "descripcion", "m_descripcion",  "m_glosa"],
            "cuit": ["cuit", "cuit_proveedor", "cuit_cliente", "cuit_beneficiario"],
            "concepto": ["concepto", "m_concepto", "concepto_codigo", "concepto_descripcion", "concepto_banco", "concepto_bancos", "bancos_concepto",  "conce", "concept"],
        }
        # Detectar el motor según el formato del archivo
        bancos_engine = self.detectar_motor(self.bancos_stream)
        mayor_engine = self.detectar_motor(self.mayor_stream)

        # Cargar bancos y aplicar renombrado de columnas
        self.df_bancos = pd.read_excel(self.bancos_stream, dtype={'comprobante': str}, engine=bancos_engine)
        self.df_bancos = self.unificar_columnas(self.df_bancos, self.columnas_equivalentes)

        # Cargar mayor y aplicar renombrado de columnas
        self.df_mayor = pd.read_excel(self.mayor_stream, dtype={'comprobante': str}, engine=mayor_engine)
        self.df_mayor = self.unificar_columnas(self.df_mayor, self.columnas_equivalentes)


        # Limpieza inicial de datos
        self.df_bancos = self.limpiar_dataframe(self.df_bancos)
        self.df_mayor = self.limpiar_dataframe(self.df_mayor)


    def limpiar_dataframe(self, df):
        """Limpia un DataFrame: elimina filas totalmente vacías y convierte tipos."""
        df.dropna(how='all', inplace=True)

        # Obtener nombres de columna para importe y comprobante
        col_imp = self.get_col(df, 'importe')
        col_comp = self.get_col(df, 'comprobante')

        if col_imp:
            df[col_imp] = pd.to_numeric(df[col_imp], errors='coerce').fillna(0)

        if col_comp:
            # Rellena NaNs con string vacío antes de convertir a string
            df[col_comp] = df[col_comp].fillna('').astype(str)

        return df

    def filtrar_por_cuit_y_importe(self, df_bancos, df_mayor, columnas_equivalentes):
        """
        Filtra registros de conciliación basados en CUIT extraído de la descripción y el importe,
        considerando columnas equivalentes.
        """

        def obtener_columna(df, claves):
            """Obtiene la primera columna válida según las claves equivalentes."""
            for clave in claves:
                if clave in df.columns:
                    return clave
            return None

        # Obtener las columnas equivalentes para 'detalle' y 'importe'
        col_detalle_bancos = obtener_columna(df_bancos, columnas_equivalentes.get('concepto', []))
        col_detalle_mayor = obtener_columna(df_mayor, columnas_equivalentes.get('detalle', []))

        col_importe_bancos = obtener_columna(df_bancos, columnas_equivalentes.get('importe', []))
        col_importe_mayor = obtener_columna(df_mayor, columnas_equivalentes.get('importe', []))

        if not col_detalle_bancos or not col_detalle_mayor:
            raise ValueError("No se encontró una columna válida para 'detalle' en los DataFrames.")
        if not col_importe_bancos or not col_importe_mayor:
            raise ValueError("No se encontró una columna válida para 'importe' en los DataFrames.")

        # Extraer CUIT de las columnas de detalle
        df_bancos['cuit'] = df_bancos[col_detalle_bancos].apply(
            lambda x: re.search(r'\b\d{2}-\d{8}-\d\b', str(x)).group(0) if re.search(r'\b\d{2}-\d{8}-\d\b',
                                                                                     str(x)) else None
        )
        df_mayor['cuit'] = df_mayor[col_detalle_mayor].apply(
            lambda x: re.search(r'\b\d{2}-\d{8}-\d\b', str(x)).group(0) if re.search(r'\b\d{2}-\d{8}-\d\b',
                                                                                     str(x)) else None
        )

        # Filtrar registros donde el CUIT no sea nulo
        df_bancos_filtrado = df_bancos.dropna(subset=['cuit'])
        df_mayor_filtrado = df_mayor.dropna(subset=['cuit'])

        # Realizar el filtrado por CUIT e importe
        resultado_cuit_importe = pd.merge(
            df_bancos_filtrado, df_mayor_filtrado,
            left_on=['cuit', col_importe_bancos], right_on=['cuit', col_importe_mayor],
            how='inner', indicator=True
        )
        logging.info(resultado_cuit_importe)
        # Retornar los resultados del filtrado
        return resultado_cuit_importe


    """
    def _procesar_datos_origin_(self):
        print("Procesa los datos para realizar la conciliación con tolerancia en importes.")
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
        self.resultado_concilia['concepto'] = self.resultado_concilia['concepto_banco']
        if 'concepto' not in self.resultado_concilia.columns:
            posibles = [col for col in self.resultado_concilia.columns if 'concepto' in col]
            if posibles:
                self.resultado_concilia['concepto'] = self.resultado_concilia[posibles[0]]

        # Asegurar que 'detalle' esté presente

        self.resultado_concilia['detalle'] = self.resultado_concilia['Concepto']
        logging.info(self.resultado_concilia['detalle'])
        # Asignar 'concepto' desde concepto_codigo del mayor
        if 'concepto_codigo' in self.resultado_concilia.columns:
            self.resultado_concilia['concepto'] = self.resultado_concilia['concepto_codigo']
        elif 'concepto_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['concepto'] = self.resultado_concilia['concepto_mayor']
        else:
            posibles_codigo = [col for col in self.resultado_concilia.columns if 'concepto' in col and 'codigo' in col]
            if posibles_codigo:
                self.resultado_concilia['concepto'] = self.resultado_concilia[posibles_codigo[0]]

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

    """
    def get_col(self, df, key):
        if key not in self.columnas_equivalentes:
            return None
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
        col_comp_banco = self.get_col(self.df_bancos, 'comprobante')
        col_comp_mayor = self.get_col(self.df_mayor, 'comprobante')
        col_imp_banco = self.get_col(self.df_bancos, 'importe')
        col_imp_mayor = self.get_col(self.df_mayor, 'importe')
        col_cuit_mayor = self.get_col(self.df_mayor, 'cuit')
        col_conc_banco = self.get_col(self.df_bancos, 'concepto')
        col_conc_mayor = self.get_col(self.df_mayor, 'concepto')
        col_detalle_banco = self.get_col(self.df_bancos, 'detalle')
        col_detalle_mayor = self.get_col(self.df_mayor, 'detalle')

        # --- Verificación de columnas esenciales ---
        if not all([col_comp_banco, col_comp_mayor, col_imp_banco, col_imp_mayor, col_detalle_banco, col_detalle_mayor]):
            missing = [
                "comprobante en banco" if not col_comp_banco else "",
                "comprobante en mayor" if not col_comp_mayor else "",
                "importe en banco" if not col_imp_banco else "",
                "importe en mayor" if not col_imp_mayor else "",
                "detalle en banco" if not col_detalle_banco else "",
                "detalle en mayor" if not col_detalle_mayor else "",
            ]
            raise ValueError(f"Faltan columnas esenciales para la conciliación: {', '.join(filter(None, missing))}")


        # Normalizar comprobante
        self.df_bancos['c4'] = self.df_bancos[col_comp_banco].astype(str).str.zfill(8).str[-4:]
        self.df_mayor['c4'] = self.df_mayor[col_comp_mayor].astype(str).str.zfill(8).str[-4:]

        # Redondear importes
        self.df_bancos['importe_r'] = self.df_bancos[col_imp_banco].round(2)
        self.df_mayor['importe_r'] = self.df_mayor[col_imp_mayor].round(2)

        # --- Extracción y Normalización de CUIT ---
        self.df_bancos['cuit'] = self.df_bancos[col_detalle_banco].apply(self.extraer_cuit).apply(self.normalizar_cuit)

        if col_cuit_mayor:
            self.df_mayor['cuit'] = self.df_mayor[col_cuit_mayor].apply(self.normalizar_cuit)
        else:
            self.df_mayor['cuit'] = self.df_mayor[col_detalle_mayor].apply(self.extraer_cuit).apply(self.normalizar_cuit)


        # === ETAPA 1: Conciliación por Comprobante e Importe ===
        self.resultado_concilia = pd.merge(
            self.df_mayor.dropna(subset=['c4', 'importe_r']),
            self.df_bancos.dropna(subset=['c4', 'importe_r']),
            on=['c4', 'importe_r'], how='inner', indicator=True,
            suffixes=('_mayor', '_banco')
        )
        logging.info(f"Registros conciliados en Etapa 1 (comprobante+importe): {len(self.resultado_concilia)}")

        # Identificar los registros que NO se conciliaron en la etapa 1
        keys_conciliados_mayor = self.resultado_concilia[['c4', 'importe_r']].apply(tuple, axis=1)
        mayor_no_conciliado = self.df_mayor[~self.df_mayor[['c4', 'importe_r']].apply(tuple, axis=1).isin(keys_conciliados_mayor)].copy()

        keys_conciliados_banco = self.resultado_concilia[['c4', 'importe_r']].apply(tuple, axis=1)
        banco_no_conciliado = self.df_bancos[~self.df_bancos[['c4', 'importe_r']].apply(tuple, axis=1).isin(keys_conciliados_banco)].copy()


        # === ETAPA 2: Conciliación por CUIT e Importe (sobre los restantes) ===
        # Preparamos los dataframes restantes para el merge por CUIT
        mayor_con_cuit = mayor_no_conciliado.dropna(subset=['cuit', 'importe_r'])
        banco_con_cuit = banco_no_conciliado.dropna(subset=['cuit', 'importe_r'])

        if not mayor_con_cuit.empty and not banco_con_cuit.empty:
            resultado_cuit = pd.merge(
                mayor_con_cuit,
                banco_con_cuit,
                on=['cuit', 'importe_r'],
                how='inner',
                indicator=True,
                suffixes=('_mayor', '_banco')
            )
            logging.info(f"Registros conciliados en Etapa 2 (CUIT+importe): {len(resultado_cuit)}")

            # Unir los resultados de ambas etapas
            if not resultado_cuit.empty:
                # Asegurarse que ambos dataframes tienen las mismas columnas antes de concatenar
                common_cols = self.resultado_concilia.columns.intersection(resultado_cuit.columns)
                self.resultado_concilia = pd.concat(
                    [self.resultado_concilia[common_cols], resultado_cuit[common_cols]],
                    ignore_index=True
                )

        # --- Post-procesamiento del resultado final ---
        # Asegurar que el CUIT esté presente en el resultado
        if 'cuit_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['cuit'] = self.resultado_concilia['cuit_mayor']
        elif 'cuit_banco' in self.resultado_concilia.columns:
            self.resultado_concilia['cuit'] = self.resultado_concilia['cuit_banco']

        # Aplicación para concepto y detalle
        self.asignar_columna_equivalente(
            self.resultado_concilia, 'concepto',
            self.columnas_equivalentes.get('concepto', []), ['banco', 'mayor']
        )

        self.asignar_columna_equivalente(
            self.resultado_concilia, 'detalle',
            self.columnas_equivalentes.get('detalle', []), ['banco', 'mayor']
        )

        # Asignar importe original
        col_imp_final = self.get_col(self.resultado_concilia, 'importe')
        if f'{col_imp_final}_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['importe'] = self.resultado_concilia[f'{col_imp_final}_mayor']
        elif f'{col_imp_final}_banco' in self.resultado_concilia.columns:
            self.resultado_concilia['importe'] = self.resultado_concilia[f'{col_imp_final}_banco']


        # --- Cálculo de Únicos (No Conciliados) ---
        # Índices de todo lo que SÍ se concilió
        idx_conciliados_mayor = pd.MultiIndex.from_frame(self.resultado_concilia[['c4', 'importe_r', 'cuit']])
        idx_conciliados_banco = pd.MultiIndex.from_frame(self.resultado_concilia[['c4', 'importe_r', 'cuit']])

        # Crear índices en los dataframes originales
        idx_original_mayor = pd.MultiIndex.from_frame(self.df_mayor[['c4', 'importe_r', 'cuit']])
        idx_original_banco = pd.MultiIndex.from_frame(self.df_bancos[['c4', 'importe_r', 'cuit']])

        # Filtrar para obtener los únicos
        self.unicos_empresa = self.df_mayor[~idx_original_mayor.isin(idx_conciliados_mayor)].copy()
        self.unicos_banco = self.df_bancos[~idx_original_banco.isin(idx_conciliados_banco)].copy()


        if 'importe' not in self.unicos_empresa.columns:
            self.unicos_empresa['importe'] = self.unicos_empresa['importe_r']

        if 'importe' not in self.unicos_banco.columns:
            self.unicos_banco['importe'] = self.unicos_banco['importe_r']

        # Totales por concepto
        if col_conc_banco and col_imp_banco:
            self.totales_banco = self.df_bancos.groupby(col_conc_banco)[col_imp_banco].sum().sort_index()
        else:
            self.totales_banco = pd.Series()

    def get_col(self, df, key):
        if key not in self.columnas_equivalentes:
            return None
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
        col_comp_banco = self.get_col(self.df_bancos, 'comprobante')
        col_comp_mayor = self.get_col(self.df_mayor, 'comprobante')
        col_imp_banco = self.get_col(self.df_bancos, 'importe')
        col_imp_mayor = self.get_col(self.df_mayor, 'importe')
        col_cuit_mayor = self.get_col(self.df_mayor, 'cuit')
        col_conc_banco = self.get_col(self.df_bancos, 'concepto')
        col_conc_mayor = self.get_col(self.df_mayor, 'concepto')
        col_detalle_banco = self.get_col(self.df_bancos, 'detalle')
        col_detalle_mayor = self.get_col(self.df_mayor, 'detalle')

        # --- Verificación de columnas esenciales ---
        if not all([col_comp_banco, col_comp_mayor, col_imp_banco, col_imp_mayor, col_detalle_banco, col_detalle_mayor]):
            missing = [
                "comprobante en banco" if not col_comp_banco else "",
                "comprobante en mayor" if not col_comp_mayor else "",
                "importe en banco" if not col_imp_banco else "",
                "importe en mayor" if not col_imp_mayor else "",
                "detalle en banco" if not col_detalle_banco else "",
                "detalle en mayor" if not col_detalle_mayor else "",
            ]
            raise ValueError(f"Faltan columnas esenciales para la conciliación: {', '.join(filter(None, missing))}")


        # Normalizar comprobante
        self.df_bancos['c4'] = self.df_bancos[col_comp_banco].astype(str).str.zfill(8).str[-4:]
        self.df_mayor['c4'] = self.df_mayor[col_comp_mayor].astype(str).str.zfill(8).str[-4:]

        # Redondear importes
        self.df_bancos['importe_r'] = self.df_bancos[col_imp_banco].round(2)
        self.df_mayor['importe_r'] = self.df_mayor[col_imp_mayor].round(2)

        # --- Extracción y Normalización de CUIT ---
        self.df_bancos['cuit'] = self.df_bancos[col_detalle_banco].apply(self.extraer_cuit).apply(self.normalizar_cuit)

        if col_cuit_mayor:
            self.df_mayor['cuit'] = self.df_mayor[col_cuit_mayor].apply(self.normalizar_cuit)
        else:
            self.df_mayor['cuit'] = self.df_mayor[col_detalle_mayor].apply(self.extraer_cuit).apply(self.normalizar_cuit)


        # === ETAPA 1: Conciliación por Comprobante e Importe ===
        self.resultado_concilia = pd.merge(
            self.df_mayor.dropna(subset=['c4', 'importe_r']),
            self.df_bancos.dropna(subset=['c4', 'importe_r']),
            on=['c4', 'importe_r'], how='inner', indicator=True,
            suffixes=('_mayor', '_banco')
        )
        logging.info(f"Registros conciliados en Etapa 1 (comprobante+importe): {len(self.resultado_concilia)}")

        # Identificar los registros que NO se conciliaron en la etapa 1
        keys_conciliados_mayor = self.resultado_concilia[['c4', 'importe_r']].apply(tuple, axis=1)
        mayor_no_conciliado = self.df_mayor[~self.df_mayor[['c4', 'importe_r']].apply(tuple, axis=1).isin(keys_conciliados_mayor)].copy()

        keys_conciliados_banco = self.resultado_concilia[['c4', 'importe_r']].apply(tuple, axis=1)
        banco_no_conciliado = self.df_bancos[~self.df_bancos[['c4', 'importe_r']].apply(tuple, axis=1).isin(keys_conciliados_banco)].copy()


        # === ETAPA 2: Conciliación por CUIT e Importe (sobre los restantes) ===
        # Preparamos los dataframes restantes para el merge por CUIT
        mayor_con_cuit = mayor_no_conciliado.dropna(subset=['cuit', 'importe_r'])
        banco_con_cuit = banco_no_conciliado.dropna(subset=['cuit', 'importe_r'])

        if not mayor_con_cuit.empty and not banco_con_cuit.empty:
            resultado_cuit = pd.merge(
                mayor_con_cuit,
                banco_con_cuit,
                on=['cuit', 'importe_r'],
                how='inner',
                indicator=True,
                suffixes=('_mayor', '_banco')
            )
            logging.info(f"Registros conciliados en Etapa 2 (CUIT+importe): {len(resultado_cuit)}")

            # Unir los resultados de ambas etapas
            if not resultado_cuit.empty:
                # Asegurarse que ambos dataframes tienen las mismas columnas antes de concatenar
                common_cols = self.resultado_concilia.columns.intersection(resultado_cuit.columns)
                self.resultado_concilia = pd.concat(
                    [self.resultado_concilia[common_cols], resultado_cuit[common_cols]],
                    ignore_index=True
                )

        # --- Post-procesamiento del resultado final ---
        # Asegurar que el CUIT esté presente en el resultado
        if 'cuit_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['cuit'] = self.resultado_concilia['cuit_mayor']
        elif 'cuit_banco' in self.resultado_concilia.columns:
            self.resultado_concilia['cuit'] = self.resultado_concilia['cuit_banco']

        # Aplicación para concepto y detalle
        self.asignar_columna_equivalente(
            self.resultado_concilia, 'concepto',
            self.columnas_equivalentes.get('concepto', []), ['banco', 'mayor']
        )

        self.asignar_columna_equivalente(
            self.resultado_concilia, 'detalle',
            self.columnas_equivalentes.get('detalle', []), ['banco', 'mayor']
        )

        # Asignar importe original
        col_imp_final = self.get_col(self.resultado_concilia, 'importe')
        if f'{col_imp_final}_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['importe'] = self.resultado_concilia[f'{col_imp_final}_mayor']
        elif f'{col_imp_final}_banco' in self.resultado_concilia.columns:
            self.resultado_concilia['importe'] = self.resultado_concilia[f'{col_imp_final}_banco']


        # --- Cálculo de Únicos (No Conciliados) ---
        # Índices de todo lo que SÍ se concilió
        idx_conciliados_mayor = pd.MultiIndex.from_frame(self.resultado_concilia[['c4', 'importe_r', 'cuit']])
        idx_conciliados_banco = pd.MultiIndex.from_frame(self.resultado_concilia[['c4', 'importe_r', 'cuit']])

        # Crear índices en los dataframes originales
        idx_original_mayor = pd.MultiIndex.from_frame(self.df_mayor[['c4', 'importe_r', 'cuit']])
        idx_original_banco = pd.MultiIndex.from_frame(self.df_bancos[['c4', 'importe_r', 'cuit']])

        # Filtrar para obtener los únicos
        self.unicos_empresa = self.df_mayor[~idx_original_mayor.isin(idx_conciliados_mayor)].copy()
        self.unicos_banco = self.df_bancos[~idx_original_banco.isin(idx_conciliados_banco)].copy()


        if 'importe' not in self.unicos_empresa.columns:
            self.unicos_empresa['importe'] = self.unicos_empresa['importe_r']

        if 'importe' not in self.unicos_banco.columns:
            self.unicos_banco['importe'] = self.unicos_banco['importe_r']

        # Totales por concepto
        if col_conc_banco and col_imp_banco:
            self.totales_banco = self.df_bancos.groupby(col_conc_banco)[col_imp_banco].sum().sort_index()
        else:
            self.totales_banco = pd.Series()

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

    def detectar_motor(self, stream: BytesIO):
        try:
            # Verificar si el flujo contiene datos
            if stream.getbuffer().nbytes == 0:
                raise ValueError("El flujo está vacío. Asegúrate de que contiene datos válidos.")

            # Leer los primeros bytes para determinar el formato
            stream.seek(0)  # Asegurarse de que el puntero esté al inicio
            header = stream.read(8)
            stream.seek(0)  # Reiniciar el puntero del stream después de leer

            # Validar el encabezado para determinar el motor
            if header[:2] == b'\xD0\xCF':  # Archivos .xls (formato antiguo)
                return 'xlrd'
            elif header.startswith(b'PK'):  # Archivos .xlsx o .xlsm
                return 'openpyxl'
            elif header:  # Si hay un encabezado pero no coincide con los formatos conocidos
                logger.warning(f"Encabezado inesperado: {header}. Intentando con 'xlrd'.")
                return 'xlrd'
            else:
                raise ValueError("Formato de archivo no soportado. Especifique un motor válido.")
        except Exception as e:
            raise ValueError(f"Error al detectar el motor: {e}")

    def extraer_cuit(self, texto):
        """Extrae un CUIT de un texto, con o sin guiones."""
        if not isinstance(texto, str):
            return None
        # Primero busca con guiones
        match = re.search(r'\b(\d{2}-\d{8}-\d{1})\b', texto)
        if match:
            return match.group(1)
        # Si no encuentra, busca sin guiones (11 dígitos seguidos)
        match = re.search(r'\b(\d{11})\b', texto)
        if match:
            cuit = match.group(1)
            # Opcional: formatear a XX-XXXXXXXX-X
            return f"{cuit[:2]}-{cuit[2:10]}-{cuit[10:]}"
        return None

    def normalizar_cuit(self, cuit):
        """Normaliza un CUIT a formato XX-XXXXXXXX-X si es posible."""
        if not cuit or not isinstance(cuit, str):
            return None
        cuit_limpio = re.sub(r'[^0-9]', '', cuit)
        if len(cuit_limpio) == 11:
            return f"{cuit_limpio[:2]}-{cuit_limpio[2:10]}-{cuit_limpio[10:]}"
        return None # Devuelve None si no se puede normalizar

    def cargar_datos_2(self):
        """Carga los datos desde BytesIO en DataFrames y normaliza nombres de columnas clave."""

        # Mapa de nombres alternativos
        self.columnas_equivalentes = {
            "importe": ["importe", "m_importe","saldo"],
            "comprobante": ["comprobante", "nro_comp_asoc", "nro_comp", "nro_comp_preimp", "nro", "numero"],
            "detalle": ["detalle", "m_detalle", "descripcion", "m_descripcion",  "m_glosa"],
            "cuit": ["cuit", "cuit_proveedor", "cuit_cliente", "cuit_beneficiario"],
            "concepto": ["concepto", "m_concepto", "concepto_codigo", "concepto_descripcion", "concepto_banco", "concepto_bancos", "bancos_concepto",  "conce", "concept"],
        }
        # Detectar el motor según el formato del archivo
        bancos_engine = self.detectar_motor(self.bancos_stream)
        mayor_engine = self.detectar_motor(self.mayor_stream)

        # Cargar bancos y aplicar renombrado de columnas
        self.df_bancos = pd.read_excel(self.bancos_stream, dtype={'comprobante': str}, engine=bancos_engine)
        self.df_bancos = self.unificar_columnas(self.df_bancos, self.columnas_equivalentes)

        # Cargar mayor y aplicar renombrado de columnas
        self.df_mayor = pd.read_excel(self.mayor_stream, dtype={'comprobante': str}, engine=mayor_engine)
        self.df_mayor = self.unificar_columnas(self.df_mayor, self.columnas_equivalentes)


        # Limpieza inicial de datos
        self.df_bancos = self.limpiar_dataframe(self.df_bancos)
        self.df_mayor = self.limpiar_dataframe(self.df_mayor)


    def limpiar_dataframe(self, df):
        """Limpia un DataFrame: elimina filas totalmente vacías y convierte tipos."""
        df.dropna(how='all', inplace=True)

        # Obtener nombres de columna para importe y comprobante
        col_imp = self.get_col(df, 'importe')
        col_comp = self.get_col(df, 'comprobante')

        if col_imp:
            df[col_imp] = pd.to_numeric(df[col_imp], errors='coerce').fillna(0)

        if col_comp:
            # Rellena NaNs con string vacío antes de convertir a string
            df[col_comp] = df[col_comp].fillna('').astype(str)

        return df

    def filtrar_por_cuit_y_importe(self, df_bancos, df_mayor, columnas_equivalentes):
        """
        Filtra registros de conciliación basados en CUIT extraído de la descripción y el importe,
        considerando columnas equivalentes.
        """

        def obtener_columna(df, claves):
            """Obtiene la primera columna válida según las claves equivalentes."""
            for clave in claves:
                if clave in df.columns:
                    return clave
            return None

        # Obtener las columnas equivalentes para 'detalle' y 'importe'
        col_detalle_bancos = obtener_columna(df_bancos, columnas_equivalentes.get('concepto', []))
        col_detalle_mayor = obtener_columna(df_mayor, columnas_equivalentes.get('detalle', []))

        col_importe_bancos = obtener_columna(df_bancos, columnas_equivalentes.get('importe', []))
        col_importe_mayor = obtener_columna(df_mayor, columnas_equivalentes.get('importe', []))

        if not col_detalle_bancos or not col_detalle_mayor:
            raise ValueError("No se encontró una columna válida para 'detalle' en los DataFrames.")
        if not col_importe_bancos or not col_importe_mayor:
            raise ValueError("No se encontró una columna válida para 'importe' en los DataFrames.")

        # Extraer CUIT de las columnas de detalle
        df_bancos['cuit'] = df_bancos[col_detalle_bancos].apply(
            lambda x: re.search(r'\b\d{2}-\d{8}-\d\b', str(x)).group(0) if re.search(r'\b\d{2}-\d{8}-\d\b',
                                                                                     str(x)) else None
        )
        df_mayor['cuit'] = df_mayor[col_detalle_mayor].apply(
            lambda x: re.search(r'\b\d{2}-\d{8}-\d\b', str(x)).group(0) if re.search(r'\b\d{2}-\d{8}-\d\b',
                                                                                     str(x)) else None
        )

        # Filtrar registros donde el CUIT no sea nulo
        df_bancos_filtrado = df_bancos.dropna(subset=['cuit'])
        df_mayor_filtrado = df_mayor.dropna(subset=['cuit'])

        # Realizar el filtrado por CUIT e importe
        resultado_cuit_importe = pd.merge(
            df_bancos_filtrado, df_mayor_filtrado,
            left_on=['cuit', col_importe_bancos], right_on=['cuit', col_importe_mayor],
            how='inner', indicator=True
        )
        logging.info(resultado_cuit_importe)
        # Retornar los resultados del filtrado
        return resultado_cuit_importe


    """
    def _procesar_datos_origin_(self):
        print("Procesa los datos para realizar la conciliación con tolerancia en importes.")
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
        self.resultado_concilia['concepto'] = self.resultado_concilia['concepto_banco']
        if 'concepto' not in self.resultado_concilia.columns:
            posibles = [col for col in self.resultado_concilia.columns if 'concepto' in col]
            if posibles:
                self.resultado_concilia['concepto'] = self.resultado_concilia[posibles[0]]

        # Asegurar que 'detalle' esté presente

        self.resultado_concilia['detalle'] = self.resultado_concilia['Concepto']
        logging.info(self.resultado_concilia['detalle'])
        # Asignar 'concepto' desde concepto_codigo del mayor
        if 'concepto_codigo' in self.resultado_concilia.columns:
            self.resultado_concilia['concepto'] = self.resultado_concilia['concepto_codigo']
        elif 'concepto_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['concepto'] = self.resultado_concilia['concepto_mayor']
        else:
            posibles_codigo = [col for col in self.resultado_concilia.columns if 'concepto' in col and 'codigo' in col]
            if posibles_codigo:
                self.resultado_concilia['concepto'] = self.resultado_concilia[posibles_codigo[0]]

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

    """
    def get_col(self, df, key):
        if key not in self.columnas_equivalentes:
            return None
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
        col_comp_banco = self.get_col(self.df_bancos, 'comprobante')
        col_comp_mayor = self.get_col(self.df_mayor, 'comprobante')
        col_imp_banco = self.get_col(self.df_bancos, 'importe')
        col_imp_mayor = self.get_col(self.df_mayor, 'importe')
        col_cuit_mayor = self.get_col(self.df_mayor, 'cuit')
        col_conc_banco = self.get_col(self.df_bancos, 'concepto')
        col_conc_mayor = self.get_col(self.df_mayor, 'concepto')
        col_detalle_banco = self.get_col(self.df_bancos, 'detalle')
        col_detalle_mayor = self.get_col(self.df_mayor, 'detalle')

        # --- Verificación de columnas esenciales ---
        if not all([col_comp_banco, col_comp_mayor, col_imp_banco, col_imp_mayor, col_detalle_banco, col_detalle_mayor]):
            missing = [
                "comprobante en banco" if not col_comp_banco else "",
                "comprobante en mayor" if not col_comp_mayor else "",
                "importe en banco" if not col_imp_banco else "",
                "importe en mayor" if not col_imp_mayor else "",
                "detalle en banco" if not col_detalle_banco else "",
                "detalle en mayor" if not col_detalle_mayor else "",
            ]
            raise ValueError(f"Faltan columnas esenciales para la conciliación: {', '.join(filter(None, missing))}")


        # Normalizar comprobante
        self.df_bancos['c4'] = self.df_bancos[col_comp_banco].astype(str).str.zfill(8).str[-4:]
        self.df_mayor['c4'] = self.df_mayor[col_comp_mayor].astype(str).str.zfill(8).str[-4:]

        # Redondear importes
        self.df_bancos['importe_r'] = self.df_bancos[col_imp_banco].round(2)
        self.df_mayor['importe_r'] = self.df_mayor[col_imp_mayor].round(2)

        # --- Extracción y Normalización de CUIT ---
        self.df_bancos['cuit'] = self.df_bancos[col_detalle_banco].apply(self.extraer_cuit).apply(self.normalizar_cuit)

        if col_cuit_mayor:
            self.df_mayor['cuit'] = self.df_mayor[col_cuit_mayor].apply(self.normalizar_cuit)
        else:
            self.df_mayor['cuit'] = self.df_mayor[col_detalle_mayor].apply(self.extraer_cuit).apply(self.normalizar_cuit)


        # === ETAPA 1: Conciliación por Comprobante e Importe ===
        self.resultado_concilia = pd.merge(
            self.df_mayor.dropna(subset=['c4', 'importe_r']),
            self.df_bancos.dropna(subset=['c4', 'importe_r']),
            on=['c4', 'importe_r'], how='inner', indicator=True,
            suffixes=('_mayor', '_banco')
        )
        logging.info(f"Registros conciliados en Etapa 1 (comprobante+importe): {len(self.resultado_concilia)}")

        # Identificar los registros que NO se conciliaron en la etapa 1
        keys_conciliados_mayor = self.resultado_concilia[['c4', 'importe_r']].apply(tuple, axis=1)
        mayor_no_conciliado = self.df_mayor[~self.df_mayor[['c4', 'importe_r']].apply(tuple, axis=1).isin(keys_conciliados_mayor)].copy()

        keys_conciliados_banco = self.resultado_concilia[['c4', 'importe_r']].apply(tuple, axis=1)
        banco_no_conciliado = self.df_bancos[~self.df_bancos[['c4', 'importe_r']].apply(tuple, axis=1).isin(keys_conciliados_banco)].copy()


        # === ETAPA 2: Conciliación por CUIT e Importe (sobre los restantes) ===
        # Preparamos los dataframes restantes para el merge por CUIT
        mayor_con_cuit = mayor_no_conciliado.dropna(subset=['cuit', 'importe_r'])
        banco_con_cuit = banco_no_conciliado.dropna(subset=['cuit', 'importe_r'])

        if not mayor_con_cuit.empty and not banco_con_cuit.empty:
            resultado_cuit = pd.merge(
                mayor_con_cuit,
                banco_con_cuit,
                on=['cuit', 'importe_r'],
                how='inner',
                indicator=True,
                suffixes=('_mayor', '_banco')
            )
            logging.info(f"Registros conciliados en Etapa 2 (CUIT+importe): {len(resultado_cuit)}")

            # Unir los resultados de ambas etapas
            if not resultado_cuit.empty:
                # Asegurarse que ambos dataframes tienen las mismas columnas antes de concatenar
                common_cols = self.resultado_concilia.columns.intersection(resultado_cuit.columns)
                self.resultado_concilia = pd.concat(
                    [self.resultado_concilia[common_cols], resultado_cuit[common_cols]],
                    ignore_index=True
                )

        # --- Post-procesamiento del resultado final ---
        # Asegurar que el CUIT esté presente en el resultado
        if 'cuit_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['cuit'] = self.resultado_concilia['cuit_mayor']
        elif 'cuit_banco' in self.resultado_concilia.columns:
            self.resultado_concilia['cuit'] = self.resultado_concilia['cuit_banco']

        # Aplicación para concepto y detalle
        self.asignar_columna_equivalente(
            self.resultado_concilia, 'concepto',
            self.columnas_equivalentes.get('concepto', []), ['banco', 'mayor']
        )

        self.asignar_columna_equivalente(
            self.resultado_concilia, 'detalle',
            self.columnas_equivalentes.get('detalle', []), ['banco', 'mayor']
        )

        # Asignar importe original
        col_imp_final = self.get_col(self.resultado_concilia, 'importe')
        if f'{col_imp_final}_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['importe'] = self.resultado_concilia[f'{col_imp_final}_mayor']
        elif f'{col_imp_final}_banco' in self.resultado_concilia.columns:
            self.resultado_concilia['importe'] = self.resultado_concilia[f'{col_imp_final}_banco']


        # --- Cálculo de Únicos (No Conciliados) ---
        # Índices de todo lo que SÍ se concilió
        idx_conciliados_mayor = pd.MultiIndex.from_frame(self.resultado_concilia[['c4', 'importe_r', 'cuit']])
        idx_conciliados_banco = pd.MultiIndex.from_frame(self.resultado_concilia[['c4', 'importe_r', 'cuit']])

        # Crear índices en los dataframes originales
        idx_original_mayor = pd.MultiIndex.from_frame(self.df_mayor[['c4', 'importe_r', 'cuit']])
        idx_original_banco = pd.MultiIndex.from_frame(self.df_bancos[['c4', 'importe_r', 'cuit']])

        # Filtrar para obtener los únicos
        self.unicos_empresa = self.df_mayor[~idx_original_mayor.isin(idx_conciliados_mayor)].copy()
        self.unicos_banco = self.df_bancos[~idx_original_banco.isin(idx_conciliados_banco)].copy()


        if 'importe' not in self.unicos_empresa.columns:
            self.unicos_empresa['importe'] = self.unicos_empresa['importe_r']

        if 'importe' not in self.unicos_banco.columns:
            self.unicos_banco['importe'] = self.unicos_banco['importe_r']

        # Totales por concepto
        if col_conc_banco and col_imp_banco:
            self.totales_banco = self.df_bancos.groupby(col_conc_banco)[col_imp_banco].sum().sort_index()
        else:
            self.totales_banco = pd.Series()

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

    def detectar_motor(self, stream: BytesIO):
        try:
            # Verificar si el flujo contiene datos
            if stream.getbuffer().nbytes == 0:
                raise ValueError("El flujo está vacío. Asegúrate de que contiene datos válidos.")

            # Leer los primeros bytes para determinar el formato
            stream.seek(0)  # Asegurarse de que el puntero esté al inicio
            header = stream.read(8)
            stream.seek(0)  # Reiniciar el puntero del stream después de leer

            # Validar el encabezado para determinar el motor
            if header[:2] == b'\xD0\xCF':  # Archivos .xls (formato antiguo)
                return 'xlrd'
            elif header.startswith(b'PK'):  # Archivos .xlsx o .xlsm
                return 'openpyxl'
            elif header:  # Si hay un encabezado pero no coincide con los formatos conocidos
                logger.warning(f"Encabezado inesperado: {header}. Intentando con 'xlrd'.")
                return 'xlrd'
            else:
                raise ValueError("Formato de archivo no soportado. Especifique un motor válido.")
        except Exception as e:
            raise ValueError(f"Error al detectar el motor: {e}")

    def extraer_cuit(self, texto):
        """Extrae un CUIT de un texto, con o sin guiones."""
        if not isinstance(texto, str):
            return None
        # Primero busca con guiones
        match = re.search(r'\b(\d{2}-\d{8}-\d{1})\b', texto)
        if match:
            return match.group(1)
        # Si no encuentra, busca sin guiones (11 dígitos seguidos)
        match = re.search(r'\b(\d{11})\b', texto)
        if match:
            cuit = match.group(1)
            # Opcional: formatear a XX-XXXXXXXX-X
            return f"{cuit[:2]}-{cuit[2:10]}-{cuit[10:]}"
        return None

    def normalizar_cuit(self, cuit):
        """Normaliza un CUIT a formato XX-XXXXXXXX-X si es posible."""
        if not cuit or not isinstance(cuit, str):
            return None
        cuit_limpio = re.sub(r'[^0-9]', '', cuit)
        if len(cuit_limpio) == 11:
            return f"{cuit_limpio[:2]}-{cuit_limpio[2:10]}-{cuit_limpio[10:]}"
        return None # Devuelve None si no se puede normalizar

    def cargar_datos_2(self):
        """Carga los datos desde BytesIO en DataFrames y normaliza nombres de columnas clave."""

        # Mapa de nombres alternativos
        self.columnas_equivalentes = {
            "importe": ["importe", "m_importe","saldo"],
            "comprobante": ["comprobante", "nro_comp_asoc", "nro_comp", "nro_comp_preimp", "nro", "numero"],
            "detalle": ["detalle", "m_detalle", "descripcion", "m_descripcion",  "m_glosa"],
            "cuit": ["cuit", "cuit_proveedor", "cuit_cliente", "cuit_beneficiario"],
            "concepto": ["concepto", "m_concepto", "concepto_codigo", "concepto_descripcion", "concepto_banco", "concepto_bancos", "bancos_concepto",  "conce", "concept"],
        }
        # Detectar el motor según el formato del archivo
        bancos_engine = self.detectar_motor(self.bancos_stream)
        mayor_engine = self.detectar_motor(self.mayor_stream)

        # Cargar bancos y aplicar renombrado de columnas
        self.df_bancos = pd.read_excel(self.bancos_stream, dtype={'comprobante': str}, engine=bancos_engine)
        self.df_bancos = self.unificar_columnas(self.df_bancos, self.columnas_equivalentes)

        # Cargar mayor y aplicar renombrado de columnas
        self.df_mayor = pd.read_excel(self.mayor_stream, dtype={'comprobante': str}, engine=mayor_engine)
        self.df_mayor = self.unificar_columnas(self.df_mayor, self.columnas_equivalentes)


        # Limpieza inicial de datos
        self.df_bancos = self.limpiar_dataframe(self.df_bancos)
        self.df_mayor = self.limpiar_dataframe(self.df_mayor)


    def limpiar_dataframe(self, df):
        """Limpia un DataFrame: elimina filas totalmente vacías y convierte tipos."""
        df.dropna(how='all', inplace=True)

        # Obtener nombres de columna para importe y comprobante
        col_imp = self.get_col(df, 'importe')
        col_comp = self.get_col(df, 'comprobante')

        if col_imp:
            df[col_imp] = pd.to_numeric(df[col_imp], errors='coerce').fillna(0)

        if col_comp:
            # Rellena NaNs con string vacío antes de convertir a string
            df[col_comp] = df[col_comp].fillna('').astype(str)

        return df

    def filtrar_por_cuit_y_importe(self, df_bancos, df_mayor, columnas_equivalentes):
        """
        Filtra registros de conciliación basados en CUIT extraído de la descripción y el importe,
        considerando columnas equivalentes.
        """

        def obtener_columna(df, claves):
            """Obtiene la primera columna válida según las claves equivalentes."""
            for clave in claves:
                if clave in df.columns:
                    return clave
            return None

        # Obtener las columnas equivalentes para 'detalle' y 'importe'
        col_detalle_bancos = obtener_columna(df_bancos, columnas_equivalentes.get('concepto', []))
        col_detalle_mayor = obtener_columna(df_mayor, columnas_equivalentes.get('detalle', []))

        col_importe_bancos = obtener_columna(df_bancos, columnas_equivalentes.get('importe', []))
        col_importe_mayor = obtener_columna(df_mayor, columnas_equivalentes.get('importe', []))

        if not col_detalle_bancos or not col_detalle_mayor:
            raise ValueError("No se encontró una columna válida para 'detalle' en los DataFrames.")
        if not col_importe_bancos or not col_importe_mayor:
            raise ValueError("No se encontró una columna válida para 'importe' en los DataFrames.")

        # Extraer CUIT de las columnas de detalle
        df_bancos['cuit'] = df_bancos[col_detalle_bancos].apply(
            lambda x: re.search(r'\b\d{2}-\d{8}-\d\b', str(x)).group(0) if re.search(r'\b\d{2}-\d{8}-\d\b',
                                                                                     str(x)) else None
        )
        df_mayor['cuit'] = df_mayor[col_detalle_mayor].apply(
            lambda x: re.search(r'\b\d{2}-\d{8}-\d\b', str(x)).group(0) if re.search(r'\b\d{2}-\d{8}-\d\b',
                                                                                     str(x)) else None
        )

        # Filtrar registros donde el CUIT no sea nulo
        df_bancos_filtrado = df_bancos.dropna(subset=['cuit'])
        df_mayor_filtrado = df_mayor.dropna(subset=['cuit'])

        # Realizar el filtrado por CUIT e importe
        resultado_cuit_importe = pd.merge(
            df_bancos_filtrado, df_mayor_filtrado,
            left_on=['cuit', col_importe_bancos], right_on=['cuit', col_importe_mayor],
            how='inner', indicator=True
        )
        logging.info(resultado_cuit_importe)
        # Retornar los resultados del filtrado
        return resultado_cuit_importe


    """
    def _procesar_datos_origin_(self):
        print("Procesa los datos para realizar la conciliación con tolerancia en importes.")
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
        self.resultado_concilia['concepto'] = self.resultado_concilia['concepto_banco']
        if 'concepto' not in self.resultado_concilia.columns:
            posibles = [col for col in self.resultado_concilia.columns if 'concepto' in col]
            if posibles:
                self.resultado_concilia['concepto'] = self.resultado_concilia[posibles[0]]

        # Asegurar que 'detalle' esté presente

        self.resultado_concilia['detalle'] = self.resultado_concilia['Concepto']
        logging.info(self.resultado_concilia['detalle'])
        # Asignar 'concepto' desde concepto_codigo del mayor
        if 'concepto_codigo' in self.resultado_concilia.columns:
            self.resultado_concilia['concepto'] = self.resultado_concilia['concepto_codigo']
        elif 'concepto_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['concepto'] = self.resultado_concilia['concepto_mayor']
        else:
            posibles_codigo = [col for col in self.resultado_concilia.columns if 'concepto' in col and 'codigo' in col]
            if posibles_codigo:
                self.resultado_concilia['concepto'] = self.resultado_concilia[posibles_codigo[0]]

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

    """
    def get_col(self, df, key):
        if key not in self.columnas_equivalentes:
            return None
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
        col_comp_banco = self.get_col(self.df_bancos, 'comprobante')
        col_comp_mayor = self.get_col(self.df_mayor, 'comprobante')
        col_imp_banco = self.get_col(self.df_bancos, 'importe')
        col_imp_mayor = self.get_col(self.df_mayor, 'importe')
        col_cuit_mayor = self.get_col(self.df_mayor, 'cuit')
        col_conc_banco = self.get_col(self.df_bancos, 'concepto')
        col_conc_mayor = self.get_col(self.df_mayor, 'concepto')
        col_detalle_banco = self.get_col(self.df_bancos, 'detalle')
        col_detalle_mayor = self.get_col(self.df_mayor, 'detalle')

        # --- Verificación de columnas esenciales ---
        if not all([col_comp_banco, col_comp_mayor, col_imp_banco, col_imp_mayor, col_detalle_banco, col_detalle_mayor]):
            missing = [
                "comprobante en banco" if not col_comp_banco else "",
                "comprobante en mayor" if not col_comp_mayor else "",
                "importe en banco" if not col_imp_banco else "",
                "importe en mayor" if not col_imp_mayor else "",
                "detalle en banco" if not col_detalle_banco else "",
                "detalle en mayor" if not col_detalle_mayor else "",
            ]
            raise ValueError(f"Faltan columnas esenciales para la conciliación: {', '.join(filter(None, missing))}")


        # Normalizar comprobante
        self.df_bancos['c4'] = self.df_bancos[col_comp_banco].astype(str).str.zfill(8).str[-4:]
        self.df_mayor['c4'] = self.df_mayor[col_comp_mayor].astype(str).str.zfill(8).str[-4:]

        # Redondear importes
        self.df_bancos['importe_r'] = self.df_bancos[col_imp_banco].round(2)
        self.df_mayor['importe_r'] = self.df_mayor[col_imp_mayor].round(2)

        # --- Extracción y Normalización de CUIT ---
        self.df_bancos['cuit'] = self.df_bancos[col_detalle_banco].apply(self.extraer_cuit).apply(self.normalizar_cuit)

        if col_cuit_mayor:
            self.df_mayor['cuit'] = self.df_mayor[col_cuit_mayor].apply(self.normalizar_cuit)
        else:
            self.df_mayor['cuit'] = self.df_mayor[col_detalle_mayor].apply(self.extraer_cuit).apply(self.normalizar_cuit)


        # === ETAPA 1: Conciliación por Comprobante e Importe ===
        self.resultado_concilia = pd.merge(
            self.df_mayor.dropna(subset=['c4', 'importe_r']),
            self.df_bancos.dropna(subset=['c4', 'importe_r']),
            on=['c4', 'importe_r'], how='inner', indicator=True,
            suffixes=('_mayor', '_banco')
        )
        logging.info(f"Registros conciliados en Etapa 1 (comprobante+importe): {len(self.resultado_concilia)}")

        # Identificar los registros que NO se conciliaron en la etapa 1
        keys_conciliados_mayor = self.resultado_concilia[['c4', 'importe_r']].apply(tuple, axis=1)
        mayor_no_conciliado = self.df_mayor[~self.df_mayor[['c4', 'importe_r']].apply(tuple, axis=1).isin(keys_conciliados_mayor)].copy()

        keys_conciliados_banco = self.resultado_concilia[['c4', 'importe_r']].apply(tuple, axis=1)
        banco_no_conciliado = self.df_bancos[~self.df_bancos[['c4', 'importe_r']].apply(tuple, axis=1).isin(keys_conciliados_banco)].copy()


        # === ETAPA 2: Conciliación por CUIT e Importe (sobre los restantes) ===
        # Preparamos los dataframes restantes para el merge por CUIT
        mayor_con_cuit = mayor_no_conciliado.dropna(subset=['cuit', 'importe_r'])
        banco_con_cuit = banco_no_conciliado.dropna(subset=['cuit', 'importe_r'])

        if not mayor_con_cuit.empty and not banco_con_cuit.empty:
            resultado_cuit = pd.merge(
                mayor_con_cuit,
                banco_con_cuit,
                on=['cuit', 'importe_r'],
                how='inner',
                indicator=True,
                suffixes=('_mayor', '_banco')
            )
            logging.info(f"Registros conciliados en Etapa 2 (CUIT+importe): {len(resultado_cuit)}")

            # Unir los resultados de ambas etapas
            if not resultado_cuit.empty:
                # Asegurarse que ambos dataframes tienen las mismas columnas antes de concatenar
                common_cols = self.resultado_concilia.columns.intersection(resultado_cuit.columns)
                self.resultado_concilia = pd.concat(
                    [self.resultado_concilia[common_cols], resultado_cuit[common_cols]],
                    ignore_index=True
                )

        # --- Post-procesamiento del resultado final ---
        # Asegurar que el CUIT esté presente en el resultado
        if 'cuit_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['cuit'] = self.resultado_concilia['cuit_mayor']
        elif 'cuit_banco' in self.resultado_concilia.columns:
            self.resultado_concilia['cuit'] = self.resultado_concilia['cuit_banco']

        # Aplicación para concepto y detalle
        self.asignar_columna_equivalente(
            self.resultado_concilia, 'concepto',
            self.columnas_equivalentes.get('concepto', []), ['banco', 'mayor']
        )

        self.asignar_columna_equivalente(
            self.resultado_concilia, 'detalle',
            self.columnas_equivalentes.get('detalle', []), ['banco', 'mayor']
        )

        # Asignar importe original
        col_imp_final = self.get_col(self.resultado_concilia, 'importe')
        if f'{col_imp_final}_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['importe'] = self.resultado_concilia[f'{col_imp_final}_mayor']
        elif f'{col_imp_final}_banco' in self.resultado_concilia.columns:
            self.resultado_concilia['importe'] = self.resultado_concilia[f'{col_imp_final}_banco']


        # --- Cálculo de Únicos (No Conciliados) ---
        # Índices de todo lo que SÍ se concilió
        idx_conciliados_mayor = pd.MultiIndex.from_frame(self.resultado_concilia[['c4', 'importe_r', 'cuit']])
        idx_conciliados_banco = pd.MultiIndex.from_frame(self.resultado_concilia[['c4', 'importe_r', 'cuit']])

        # Crear índices en los dataframes originales
        idx_original_mayor = pd.MultiIndex.from_frame(self.df_mayor[['c4', 'importe_r', 'cuit']])
        idx_original_banco = pd.MultiIndex.from_frame(self.df_bancos[['c4', 'importe_r', 'cuit']])

        # Filtrar para obtener los únicos
        self.unicos_empresa = self.df_mayor[~idx_original_mayor.isin(idx_conciliados_mayor)].copy()
        self.unicos_banco = self.df_bancos[~idx_original_banco.isin(idx_conciliados_banco)].copy()


        if 'importe' not in self.unicos_empresa.columns:
            self.unicos_empresa['importe'] = self.unicos_empresa['importe_r']

        if 'importe' not in self.unicos_banco.columns:
            self.unicos_banco['importe'] = self.unicos_banco['importe_r']

        # Totales por concepto
        if col_conc_banco and col_imp_banco:
            self.totales_banco = self.df_bancos.groupby(col_conc_banco)[col_imp_banco].sum().sort_index()
        else:
            self.totales_banco = pd.Series()

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

    def detectar_motor(self, stream: BytesIO):
        try:
            # Verificar si el flujo contiene datos
            if stream.getbuffer().nbytes == 0:
                raise ValueError("El flujo está vacío. Asegúrate de que contiene datos válidos.")

            # Leer los primeros bytes para determinar el formato
            stream.seek(0)  # Asegurarse de que el puntero esté al inicio
            header = stream.read(8)
            stream.seek(0)  # Reiniciar el puntero del stream después de leer

            # Validar el encabezado para determinar el motor
            if header[:2] == b'\xD0\xCF':  # Archivos .xls (formato antiguo)
                return 'xlrd'
            elif header.startswith(b'PK'):  # Archivos .xlsx o .xlsm
                return 'openpyxl'
            elif header:  # Si hay un encabezado pero no coincide con los formatos conocidos
                logger.warning(f"Encabezado inesperado: {header}. Intentando con 'xlrd'.")
                return 'xlrd'
            else:
                raise ValueError("Formato de archivo no soportado. Especifique un motor válido.")
        except Exception as e:
            raise ValueError(f"Error al detectar el motor: {e}")

    def extraer_cuit(self, texto):
        """Extrae un CUIT de un texto, con o sin guiones."""
        if not isinstance(texto, str):
            return None
        # Primero busca con guiones
        match = re.search(r'\b(\d{2}-\d{8}-\d{1})\b', texto)
        if match:
            return match.group(1)
        # Si no encuentra, busca sin guiones (11 dígitos seguidos)
        match = re.search(r'\b(\d{11})\b', texto)
        if match:
            cuit = match.group(1)
            # Opcional: formatear a XX-XXXXXXXX-X
            return f"{cuit[:2]}-{cuit[2:10]}-{cuit[10:]}"
        return None

    def normalizar_cuit(self, cuit):
        """Normaliza un CUIT a formato XX-XXXXXXXX-X si es posible."""
        if not cuit or not isinstance(cuit, str):
            return None
        cuit_limpio = re.sub(r'[^0-9]', '', cuit)
        if len(cuit_limpio) == 11:
            return f"{cuit_limpio[:2]}-{cuit_limpio[2:10]}-{cuit_limpio[10:]}"
        return None # Devuelve None si no se puede normalizar

    def cargar_datos_2(self):
        """Carga los datos desde BytesIO en DataFrames y normaliza nombres de columnas clave."""

        # Mapa de nombres alternativos
        self.columnas_equivalentes = {
            "importe": ["importe", "m_importe","saldo"],
            "comprobante": ["comprobante", "nro_comp_asoc", "nro_comp", "nro_comp_preimp", "nro", "numero"],
            "detalle": ["detalle", "m_detalle", "descripcion", "m_descripcion",  "m_glosa"],
            "cuit": ["cuit", "cuit_proveedor", "cuit_cliente", "cuit_beneficiario"],
            "concepto": ["concepto", "m_concepto", "concepto_codigo", "concepto_descripcion", "concepto_banco", "concepto_bancos", "bancos_concepto",  "conce", "concept"],
        }
        # Detectar el motor según el formato del archivo
        bancos_engine = self.detectar_motor(self.bancos_stream)
        mayor_engine = self.detectar_motor(self.mayor_stream)

        # Cargar bancos y aplicar renombrado de columnas
        self.df_bancos = pd.read_excel(self.bancos_stream, dtype={'comprobante': str}, engine=bancos_engine)
        self.df_bancos = self.unificar_columnas(self.df_bancos, self.columnas_equivalentes)

        # Cargar mayor y aplicar renombrado de columnas
        self.df_mayor = pd.read_excel(self.mayor_stream, dtype={'comprobante': str}, engine=mayor_engine)
        self.df_mayor = self.unificar_columnas(self.df_mayor, self.columnas_equivalentes)


        # Limpieza inicial de datos
        self.df_bancos = self.limpiar_dataframe(self.df_bancos)
        self.df_mayor = self.limpiar_dataframe(self.df_mayor)


    def limpiar_dataframe(self, df):
        """Limpia un DataFrame: elimina filas totalmente vacías y convierte tipos."""
        df.dropna(how='all', inplace=True)

        # Obtener nombres de columna para importe y comprobante
        col_imp = self.get_col(df, 'importe')
        col_comp = self.get_col(df, 'comprobante')

        if col_imp:
            df[col_imp] = pd.to_numeric(df[col_imp], errors='coerce').fillna(0)

        if col_comp:
            # Rellena NaNs con string vacío antes de convertir a string
            df[col_comp] = df[col_comp].fillna('').astype(str)

        return df

    def filtrar_por_cuit_y_importe(self, df_bancos, df_mayor, columnas_equivalentes):
        """
        Filtra registros de conciliación basados en CUIT extraído de la descripción y el importe,
        considerando columnas equivalentes.
        """

        def obtener_columna(df, claves):
            """Obtiene la primera columna válida según las claves equivalentes."""
            for clave in claves:
                if clave in df.columns:
                    return clave
            return None

        # Obtener las columnas equivalentes para 'detalle' y 'importe'
        col_detalle_bancos = obtener_columna(df_bancos, columnas_equivalentes.get('concepto', []))
        col_detalle_mayor = obtener_columna(df_mayor, columnas_equivalentes.get('detalle', []))

        col_importe_bancos = obtener_columna(df_bancos, columnas_equivalentes.get('importe', []))
        col_importe_mayor = obtener_columna(df_mayor, columnas_equivalentes.get('importe', []))

        if not col_detalle_bancos or not col_detalle_mayor:
            raise ValueError("No se encontró una columna válida para 'detalle' en los DataFrames.")
        if not col_importe_bancos or not col_importe_mayor:
            raise ValueError("No se encontró una columna válida para 'importe' en los DataFrames.")

        # Extraer CUIT de las columnas de detalle
        df_bancos['cuit'] = df_bancos[col_detalle_bancos].apply(
            lambda x: re.search(r'\b\d{2}-\d{8}-\d\b', str(x)).group(0) if re.search(r'\b\d{2}-\d{8}-\d\b',
                                                                                     str(x)) else None
        )
        df_mayor['cuit'] = df_mayor[col_detalle_mayor].apply(
            lambda x: re.search(r'\b\d{2}-\d{8}-\d\b', str(x)).group(0) if re.search(r'\b\d{2}-\d{8}-\d\b',
                                                                                     str(x)) else None
        )

        # Filtrar registros donde el CUIT no sea nulo
        df_bancos_filtrado = df_bancos.dropna(subset=['cuit'])
        df_mayor_filtrado = df_mayor.dropna(subset=['cuit'])

        # Realizar el filtrado por CUIT e importe
        resultado_cuit_importe = pd.merge(
            df_bancos_filtrado, df_mayor_filtrado,
            left_on=['cuit', col_importe_bancos], right_on=['cuit', col_importe_mayor],
            how='inner', indicator=True
        )
        logging.info(resultado_cuit_importe)
        # Retornar los resultados del filtrado
        return resultado_cuit_importe


    """
    def _procesar_datos_origin_(self):
        print("Procesa los datos para realizar la conciliación con tolerancia en importes.")
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
        self.resultado_concilia['concepto'] = self.resultado_concilia['concepto_banco']
        if 'concepto' not in self.resultado_concilia.columns:
            posibles = [col for col in self.resultado_concilia.columns if 'concepto' in col]
            if posibles:
                self.resultado_concilia['concepto'] = self.resultado_concilia[posibles[0]]

        # Asegurar que 'detalle' esté presente

        self.resultado_concilia['detalle'] = self.resultado_concilia['Concepto']
        logging.info(self.resultado_concilia['detalle'])
        # Asignar 'concepto' desde concepto_codigo del mayor
        if 'concepto_codigo' in self.resultado_concilia.columns:
            self.resultado_concilia['concepto'] = self.resultado_concilia['concepto_codigo']
        elif 'concepto_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['concepto'] = self.resultado_concilia['concepto_mayor']
        else:
            posibles_codigo = [col for col in self.resultado_concilia.columns if 'concepto' in col and 'codigo' in col]
            if posibles_codigo:
                self.resultado_concilia['concepto'] = self.resultado_concilia[posibles_codigo[0]]

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

    """
    def get_col(self, df, key):
        if key not in self.columnas_equivalentes:
            return None
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
        col_comp_banco = self.get_col(self.df_bancos, 'comprobante')
        col_comp_mayor = self.get_col(self.df_mayor, 'comprobante')
        col_imp_banco = self.get_col(self.df_bancos, 'importe')
        col_imp_mayor = self.get_col(self.df_mayor, 'importe')
        col_cuit_mayor = self.get_col(self.df_mayor, 'cuit')
        col_conc_banco = self.get_col(self.df_bancos, 'concepto')
        col_conc_mayor = self.get_col(self.df_mayor, 'concepto')
        col_detalle_banco = self.get_col(self.df_bancos, 'detalle')
        col_detalle_mayor = self.get_col(self.df_mayor, 'detalle')

        # --- Verificación de columnas esenciales ---
        if not all([col_comp_banco, col_comp_mayor, col_imp_banco, col_imp_mayor, col_detalle_banco, col_detalle_mayor]):
            missing = [
                "comprobante en banco" if not col_comp_banco else "",
                "comprobante en mayor" if not col_comp_mayor else "",
                "importe en banco" if not col_imp_banco else "",
                "importe en mayor" if not col_imp_mayor else "",
                "detalle en banco" if not col_detalle_banco else "",
                "detalle en mayor" if not col_detalle_mayor else "",
            ]
            raise ValueError(f"Faltan columnas esenciales para la conciliación: {', '.join(filter(None, missing))}")


        # Normalizar comprobante
        self.df_bancos['c4'] = self.df_bancos[col_comp_banco].astype(str).str.zfill(8).str[-4:]
        self.df_mayor['c4'] = self.df_mayor[col_comp_mayor].astype(str).str.zfill(8).str[-4:]

        # Redondear importes
        self.df_bancos['importe_r'] = self.df_bancos[col_imp_banco].round(2)
        self.df_mayor['importe_r'] = self.df_mayor[col_imp_mayor].round(2)

        # --- Extracción y Normalización de CUIT ---
        self.df_bancos['cuit'] = self.df_bancos[col_detalle_banco].apply(self.extraer_cuit).apply(self.normalizar_cuit)

        if col_cuit_mayor:
            self.df_mayor['cuit'] = self.df_mayor[col_cuit_mayor].apply(self.normalizar_cuit)
        else:
            self.df_mayor['cuit'] = self.df_mayor[col_detalle_mayor].apply(self.extraer_cuit).apply(self.normalizar_cuit)


        # === ETAPA 1: Conciliación por Comprobante e Importe ===
        self.resultado_concilia = pd.merge(
            self.df_mayor.dropna(subset=['c4', 'importe_r']),
            self.df_bancos.dropna(subset=['c4', 'importe_r']),
            on=['c4', 'importe_r'], how='inner', indicator=True,
            suffixes=('_mayor', '_banco')
        )
        logging.info(f"Registros conciliados en Etapa 1 (comprobante+importe): {len(self.resultado_concilia)}")

        # Identificar los registros que NO se conciliaron en la etapa 1
        keys_conciliados_mayor = self.resultado_concilia[['c4', 'importe_r']].apply(tuple, axis=1)
        mayor_no_conciliado = self.df_mayor[~self.df_mayor[['c4', 'importe_r']].apply(tuple, axis=1).isin(keys_conciliados_mayor)].copy()

        keys_conciliados_banco = self.resultado_concilia[['c4', 'importe_r']].apply(tuple, axis=1)
        banco_no_conciliado = self.df_bancos[~self.df_bancos[['c4', 'importe_r']].apply(tuple, axis=1).isin(keys_conciliados_banco)].copy()


        # === ETAPA 2: Conciliación por CUIT e Importe (sobre los restantes) ===
        # Preparamos los dataframes restantes para el merge por CUIT
        mayor_con_cuit = mayor_no_conciliado.dropna(subset=['cuit', 'importe_r'])
        banco_con_cuit = banco_no_conciliado.dropna(subset=['cuit', 'importe_r'])

        if not mayor_con_cuit.empty and not banco_con_cuit.empty:
            resultado_cuit = pd.merge(
                mayor_con_cuit,
                banco_con_cuit,
                on=['cuit', 'importe_r'],
                how='inner',
                indicator=True,
                suffixes=('_mayor', '_banco')
            )
            logging.info(f"Registros conciliados en Etapa 2 (CUIT+importe): {len(resultado_cuit)}")

            # Unir los resultados de ambas etapas
            if not resultado_cuit.empty:
                # Asegurarse que ambos dataframes tienen las mismas columnas antes de concatenar
                common_cols = self.resultado_concilia.columns.intersection(resultado_cuit.columns)
                self.resultado_concilia = pd.concat(
                    [self.resultado_concilia[common_cols], resultado_cuit[common_cols]],
                    ignore_index=True
                )

        # --- Post-procesamiento del resultado final ---
        # Asegurar que el CUIT esté presente en el resultado
        if 'cuit_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['cuit'] = self.resultado_concilia['cuit_mayor']
        elif 'cuit_banco' in self.resultado_concilia.columns:
            self.resultado_concilia['cuit'] = self.resultado_concilia['cuit_banco']

        # Aplicación para concepto y detalle
        self.asignar_columna_equivalente(
            self.resultado_concilia, 'concepto',
            self.columnas_equivalentes.get('concepto', []), ['banco', 'mayor']
        )

        self.asignar_columna_equivalente(
            self.resultado_concilia, 'detalle',
            self.columnas_equivalentes.get('detalle', []), ['banco', 'mayor']
        )

        # Asignar importe original
        col_imp_final = self.get_col(self.resultado_concilia, 'importe')
        if f'{col_imp_final}_mayor' in self.resultado_concilia.columns:
            self.resultado_concilia['importe'] = self.resultado_concilia[f'{col_imp_final}_mayor']
        elif f'{col_imp_final}_banco' in self.resultado_concilia.columns:
            self.resultado_concilia['importe'] = self.resultado_concilia[f'{col_imp_final}_banco']


        # --- Cálculo de Únicos (No Conciliados) ---
        # Índices de todo lo que SÍ se concilió
        idx_conciliados_mayor = pd.MultiIndex.from_frame(self.resultado_concilia[['c4', 'importe_r', 'cuit']])
        idx_conciliados_banco = pd.MultiIndex.from_frame(self.resultado_concilia[['c4', 'importe_r', 'cuit']])

        # Crear índices en los dataframes originales
        idx_original_mayor = pd.MultiIndex.from_frame(self.df_mayor[['c4', 'importe_r', 'cuit']])
        idx_original_banco = pd.MultiIndex.from_frame(self.df_bancos[['c4', 'importe_r', 'cuit']])

        # Filtrar para obtener los únicos
        self.unicos_empresa = self.df_mayor[~idx_original_mayor.isin(idx_conciliados_mayor)].copy()
        self.unicos_banco = self.df_bancos[~idx_original_banco.isin(idx_conciliados_banco)].copy()


        if 'importe' not in self.unicos_empresa.columns:
            self.unicos_empresa['importe'] = self.unicos_empresa['importe_r']

        if 'importe' not in self.unicos_banco.columns:
            self.unicos_banco['importe'] = self.unicos_banco['importe_r']

        # Totales por concepto
        if col_conc_banco and col_imp_banco:
            self.totales_banco = self.df_bancos.groupby(col_conc_banco)[col_imp_banco].sum().sort_index()
        else:
            self.totales_banco = pd.Series()

    def asignar_columna_equivalente(self, df, destino, equivalencias, sufijos):
        if not equivalencias: return
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

        # Filtrar filas with todos los valores NaN
        df = unicos_entidad.dropna(how='all')
        print(f"DataFrame después de eliminar filas con todos los valores NaN: {len(df)} filas")

        # Reemplazar valores NaN en columnas específicas
        columnas_a_rellenar = {
            'Fecha': pd.to_datetime('1970-01-01'),
            'concepto': '',
            'comprobante': '0',
            'importe': 0,
        }
        # Obtener nombres reales de las columnas
        col_fecha = self.get_col(df, 'fecha') if self.get_col(df, 'fecha') else 'Fecha'
        col_concepto = self.get_col(df, 'concepto') if self.get_col(df, 'concepto') else 'concepto'
        col_comprobante = self.get_col(df, 'comprobante') if self.get_col(df, 'comprobante') else 'comprobante'
        col_importe = self.get_col(df, 'importe') if self.get_col(df, 'importe') else 'importe'

        df[col_fecha] = pd.to_datetime(df[col_fecha], errors='coerce').fillna(pd.to_datetime('1970-01-01'))
        df[col_concepto] = df[col_concepto].fillna('')
        df[col_comprobante] = df[col_comprobante].fillna('0')
        df[col_importe] = pd.to_numeric(df[col_importe], errors='coerce').fillna(0)


        df = df[~((df[col_fecha] == pd.to_datetime('1970-01-01')) & (df[col_concepto] == '') & (df[col_importe] == 0))]

        if df.empty:
            print("No hay registros únicos de entidad para guardar.")
            return

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
                # Acceder a los campos por su nombre de columna real
                fecha_val = getattr(row, col_fecha, pd.to_datetime('1970-01-01'))
                comprobante_val = getattr(row, col_comprobante, '0')
                importe_val = getattr(row, col_importe, 0)
                concepto_val = getattr(row, col_concepto, '')

                valores.append((
                    self.id_tipo_concilia,
                    self.id_empresa,
                    fecha_val,
                    comprobante_val,
                    numerador,
                    0,
                    0,
                    importe_val,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    concepto_val,
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
        df = unicos_empresa.copy()
        print(df)

        # Rellenar NaNs para evitar errores en la base de datos
        # Corrección: Usar un diccionario para especificar los valores de relleno por columna
        fill_values = {
            'm_ingreso': '1900-01-01',
            'comprobante': '',
            'm_asiento': 0,
            'm_pase': 0,
            'importe': 0.0,
            'm_minuta': '',
            'concepto': '',
            'detalle': '',
            'plan_cuentas': '',
            'c4': '',
            'padron_codigo': 0
        }
        df.fillna(value=fill_values, inplace=True)

        # Normalizar la columna de fecha al formato YYYY-MM-DD
        if 'm_ingreso' in df.columns:
            df['m_ingreso'] = df['m_ingreso'].apply(self.normalizarFechas)


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
                        row.concepto,
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

                print(f"Se insertaron en unicos empresa {cursor.rowcount} registros correctamente.")
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
                self.guardarUnicosEmpresa(self.unicos_empresa, self.cuenta_concilia)
                self.guardarUnicosEntidad(self.unicos_banco, self.cuenta_concilia)
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

    def extraer_cuit(self, texto):
        """Extrae un CUIT de un texto, con o sin guiones."""
        if not isinstance(texto, str):
            return None
        # Primero busca con guiones
        match = re.search(r'\b(\d{2}-\d{8}-\d{1})\b', texto)
        if match:
            return match.group(1)
        # Si no encuentra, busca sin guiones (11 dígitos seguidos)
        match = re.search(r'\b(\d{11})\b', texto)
        if match:
            cuit = match.group(1)
            # Opcional: formatear a XX-XXXXXXXX-X
            return f"{cuit[:2]}-{cuit[2:10]}-{cuit[10:]}"
        return None

    def normalizar_cuit(self, cuit):
        """Normaliza un CUIT a formato XX-XXXXXXXX-X si es posible."""
        if not cuit or not isinstance(cuit, str):
            return None
        cuit_limpio = re.sub(r'[^0-9]', '', cuit)
        if len(cuit_limpio) == 11:
            return f"{cuit_limpio[:2]}-{cuit_limpio[2:10]}-{cuit_limpio[10:]}"
        return None # Devuelve None si no se puede normalizar

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
                        row['concepto_mayor'] if 'concepto_mayor' in row and pd.notna(row['concepto_mayor']) else row.get('concepto'),
                        row['detalle_mayor'] if 'detalle_mayor' in row and pd.notna(row['detalle_mayor']) else row.get('detalle'),
                        row['nro_comp'],
                        0,  # Debito
                        0,  # Credito
                        0,
                        row['importe'],
                        row['importe'],
                        "N",
                        self.id_usuario,  # idUsuario
                        1  # estado
                    )

                    for index, row in resultado_concilia.iterrows()


                ]
                logging.info(valores)
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
                "mensaje": "GuardarResultadosConciliacion: Error al insertar en la base de datos "+str(e),

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

            # Segunda pasada de conciliación por CUIT e Importe
            if not self.unicos_banco.empty and not self.unicos_empresa.empty:
                # Obtener nombres de columnas de importe
                col_imp_banco = self.get_col(self.unicos_banco, 'importe')
                col_imp_mayor = self.get_col(self.unicos_empresa, 'importe')

                # Asegurarse que las columnas de CUIT e importe redondeado existan
                if 'cuit' in self.unicos_banco.columns and 'cuit' in self.unicos_empresa.columns and col_imp_banco and col_imp_mayor:
                    self.unicos_banco['importe_r'] = self.unicos_banco[col_imp_banco].round(2)
                    self.unicos_empresa['importe_r'] = self.unicos_empresa[col_imp_mayor].round(2)

                    # Realizar el merge por CUIT e importe redondeado
                    conciliados_cuit = pd.merge(
                        self.unicos_empresa.dropna(subset=['cuit', 'importe_r']),
                        self.unicos_banco.dropna(subset=['cuit', 'importe_r']),
                        on=['cuit', 'importe_r'],
                        how='inner',
                        suffixes=('_mayor', '_banco')
                    )

                    if not conciliados_cuit.empty:
                        # Combinar con los resultados de la primera pasada
                        self.resultado_concilia = pd.concat([self.resultado_concilia, conciliados_cuit], ignore_index=True)

                        # Actualizar los registros únicos
                        keys_cuit_mayor = conciliados_cuit[['cuit', 'importe_r']].apply(tuple, axis=1)
                        self.unicos_empresa = self.unicos_empresa[~self.unicos_empresa[['cuit', 'importe_r']].apply(tuple, axis=1).isin(keys_cuit_mayor)].copy()

                        keys_cuit_banco = conciliados_cuit[['cuit', 'importe_r']].apply(tuple, axis=1)
                        self.unicos_banco = self.unicos_banco[~self.unicos_banco[['cuit', 'importe_r']].apply(tuple, axis=1).isin(keys_cuit_banco)].copy()


            return self.guardaResultadosConciliacion(self.resultado_concilia, self.cuenta_concilia)
        except Exception as e:
            # Retornar error en caso de excepción
            return {
                "codigo": 500,
                "control": "ERROR",
                "mensaje": f"Error durante la ejecución del proceso: {str(e)}"
            }
