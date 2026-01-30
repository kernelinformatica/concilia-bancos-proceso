from datetime import datetime
import re
import pandas as pd
import logging
from io import BytesIO
import os
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from dateutil.parser import parse
from dotenv import load_dotenv
from conectorManagerDB import ConectorManagerDB



load_dotenv()

class Conciliador:
    # Clase consolidada: elimina duplicados y centraliza la lógica de conciliación
    def __init__(self, bancos_stream: BytesIO, mayor_stream: BytesIO, salida = "/var/www/clients/client4/web28/web/conciliaciones-bancarias/upload/", id_empresa=0, id_usuario=0, id_tipo_conicliacion=1, cuenta_concilia=0):
        self.bancos_stream = bancos_stream
        self.mayor_stream = mayor_stream
        self.salida = salida
        self.id_empresa = id_empresa
        self.id_usuario = id_usuario
        # Normalizamos el nombre del atributo a `id_tipo_concilia` (usado en todo el código)
        self.id_tipo_concilia = id_tipo_conicliacion
        self.cuenta_concilia = cuenta_concilia
        self.columnas_equivalentes = {}
        if not self.salida.endswith('/'):
            self.salida += '/'
        self.plataforma = int(os.getenv("PLATAFORMA", 1))
        # DataFrames iniciales
        self.df_bancos = pd.DataFrame()
        self.df_mayor = pd.DataFrame()
        self.resultado_concilia = pd.DataFrame()
        self.unicos_empresa = pd.DataFrame()
        self.unicos_banco = pd.DataFrame()
        self.totales_banco = pd.Series()



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
            else:
                return None  # Retornar None si no se puede determinar el motor
        except Exception as e:
            raise ValueError(f"Error al detectar el motor: {e}")

    def cargar_datos(self):
        """Carga y normaliza columnas usando `columnas_equivalentes`."""
        # Mapa de nombres alternativos
        self.columnas_equivalentes = {
            "importe": ["importe", "m_importe", "saldo"],
            "comprobante": ["comprobante", "nro_comp_asoc", "nro_comp", "nro_comp_preimp", "nro", "numero"],
            "detalle": ["detalle", "m_detalle", "descripcion", "m_descripcion", "m_glosa"],
            "cuit": ["cuit", "cuit_proveedor", "cuit_cliente", "cuit_beneficiario"],
            "concepto": ["concepto", "m_concepto", "concepto_codigo", "concepto_descripcion", "concepto_banco",
                         "concepto_bancos", "bancos_concepto", "conce", "concept"],
        }
        bancos_engine = self.detectar_motor(self.bancos_stream)
        mayor_engine = self.detectar_motor(self.mayor_stream)
        self.bancos_stream.seek(0)
        self.mayor_stream.seek(0)
        self.df_bancos = pd.read_excel(self.bancos_stream, dtype={'comprobante': str}, engine=bancos_engine)
        self.df_bancos = self.unificar_columnas(self.df_bancos, self.columnas_equivalentes)
        self.df_mayor = pd.read_excel(self.mayor_stream, dtype={'comprobante': str}, engine=mayor_engine)
        self.df_mayor = self.unificar_columnas(self.df_mayor, self.columnas_equivalentes)
        self.df_bancos = self.limpiar_dataframe(self.df_bancos)
        self.df_mayor = self.limpiar_dataframe(self.df_mayor)

    def unificar_columnas(self, df: pd.DataFrame, columnas_equivalentes: dict):
        """Renombra columnas del DataFrame según el diccionario de equivalentes."""
        if df is None or not isinstance(df, pd.DataFrame):
            return df
        rename_map = {}
        cols = list(df.columns)
        for objetivo, variantes in (columnas_equivalentes or {}).items():
            for variante in variantes:
                vnorm = variante.lower().strip()
                # exact match
                for col in cols:
                    if col.lower().strip() == vnorm:
                        if col not in rename_map:
                            rename_map[col] = objetivo
                        break
                else:
                    for col in cols:
                        if vnorm in col.lower():
                            if col not in rename_map:
                                rename_map[col] = objetivo
                            break
        if rename_map:
            try:
                df = df.rename(columns=rename_map)
            except Exception as e:
                logger.warning(f"No se pudo renombrar columnas: {e}")
        return df

    def limpiar_dataframe(self, df):
        df = df.copy()
        # Eliminar columnas duplicadas
        df = df.loc[:, ~df.columns.duplicated()]
        df.dropna(how='all', inplace=True)
        col_imp = self.get_col(df, 'importe')
        col_comp = self.get_col(df, 'comprobante')
        # Buscar columna de detalle entre equivalentes
        posibles_detalle = ['detalle', 'detalles', 'descripcion', 'descripción']
        col_detalle = None
        for nombre in posibles_detalle:
            if nombre in df.columns:
                col_detalle = nombre
                break
        # Si no existe columna de detalle, crearla con el valor de concepto o texto genérico
        if not col_detalle:
            col_concepto = self.get_col(df, 'concepto')
            if col_concepto:
                df['detalle'] = df[col_concepto].astype(str)
            else:
                df['detalle'] = 'Sin detalle'
        if col_imp:
            df[col_imp] = pd.to_numeric(df[col_imp], errors='coerce').fillna(0)
        if col_comp:
            df[col_comp] = df[col_comp].fillna('').astype(str)
        return df

    def get_col(self, df, key):
        if key not in self.columnas_equivalentes:
            return None
        equivalentes = [e.lower().strip() for e in self.columnas_equivalentes[key]]
        for nombre_equivalente in equivalentes:
            for col in df.columns:
                if col.lower().strip() == nombre_equivalente:
                    return col
        for nombre_equivalente in equivalentes:
            for col in df.columns:
                if nombre_equivalente in col.lower():
                    return col
        return None

    def normalizar_comprobante(self, valor):
        if valor is None:
            return ''
        s = str(valor).strip()
        partes = re.findall(r'\d+', s)
        if not partes:
            return ''
        if len(partes) >= 2:
            p0 = partes[0].lstrip('0') or '0'
            p1 = partes[1].lstrip('0') or '0'
            return p0 + p1
        return partes[0].lstrip('0') or '0'

    def extraer_cuit(self, texto):
        if texto is None:
            return None
        s = str(texto)
        m = re.search(r'(\d{2}[-\s.]?\d{8}[-\s.]?\d{1})', s)
        if m:
            candidate = m.group(1)
        else:
            m2 = re.search(r'(\d{11})', s)
            candidate = m2.group(1) if m2 else None
        if not candidate:
            return None
        digits = re.sub(r'[^0-9]', '', candidate)
        if len(digits) == 11:
            return f"{digits[:2]}-{digits[2:10]}-{digits[10:]}"
        return None

    def normalizar_cuit(self, cuit):
        if not cuit or not isinstance(cuit, str):
            return None
        digits = re.sub(r'[^0-9]', '', cuit)
        if len(digits) == 11:
            return f"{digits[:2]}-{digits[2:10]}-{digits[10:]}"
        return None

    def asignar_columna_equivalente(self, df, objetivo, variantes, fuentes_suffix=None):
        """Asegura que exista la columna `objetivo` en df tomando valores de columnas objetivo_fuente + sufijo si existen.
        Por ejemplo: si objetivo='concepto' buscará 'concepto_banco' o 'concepto_mayor' y asignará a 'concepto'."""
        if df is None or df.empty:
            return
        # Si ya existe objetivo y no está vacía, nada que hacer
        if objetivo in df.columns and df[objetivo].notna().any():
            return
        # Buscar columnas con sufijos
        if fuentes_suffix is None:
            fuentes_suffix = ['banco', 'mayor']
        for suf in fuentes_suffix:
            cand = f"{objetivo}_{suf}"
            if cand in df.columns:
                df[objetivo] = df[cand]
                return
        # Fallback: buscar cualquier columna que contenga la palabra objetivo
        for col in df.columns:
            if objetivo in col.lower():
                df[objetivo] = df[col]
                return

    def normalizarFechas(self, fecha):
        if not fecha or str(fecha).strip().lower() in ["", "none", "nan", "null"]:
            return "1900-01-01"
        try:
            fech = parse(str(fecha), dayfirst=True)
            return fech.strftime('%Y-%m-%d')
        except Exception:
            return "1900-01-01"

    def procesar_datos(self):
        # obtiene columnas reales
        col_comp_banco = self.get_col(self.df_bancos, 'comprobante')
        col_comp_mayor = self.get_col(self.df_mayor, 'comprobante')
        col_imp_banco = self.get_col(self.df_bancos, 'importe')
        col_imp_mayor = self.get_col(self.df_mayor, 'importe')
        col_detalle_banco = self.get_col(self.df_bancos, 'detalle')
        col_detalle_mayor = self.get_col(self.df_mayor, 'detalle')
        col_cuit_mayor = self.get_col(self.df_mayor, 'cuit')

        if not all([col_comp_banco, col_comp_mayor, col_imp_banco, col_imp_mayor, col_detalle_banco, col_detalle_mayor]):
            missing = [
                "comprobante en banco" if not col_comp_banco else "",
                "comprobante en mayor" if not col_comp_mayor else "",
                "importe en banco" if not col_imp_banco else "",
                "importe en mayor" if not col_imp_mayor else "",
                #"detalle en banco" if not col_detalle_banco else "",
                #"detalle en mayor" if not col_detalle_mayor else "",
            ]
            raise ValueError(f"Faltan columnas esenciales para la conciliación: {', '.join(filter(None, missing))}")

        # normalizaciones
        self.df_bancos['comprobante_norm'] = self.df_bancos[col_comp_banco].apply(self.normalizar_comprobante)
        self.df_mayor['comprobante_norm'] = self.df_mayor[col_comp_mayor].apply(self.normalizar_comprobante)
        self.df_bancos['c4'] = self.df_bancos[col_comp_banco].astype(str).str.zfill(12).str[-4:]
        self.df_mayor['c4'] = self.df_mayor[col_comp_mayor].astype(str).str.zfill(12).str[-4:]
        self.df_bancos['c8'] = self.df_bancos[col_comp_banco].astype(str).str.zfill(12).str[-8:]
        self.df_mayor['c8'] = self.df_mayor[col_comp_mayor].astype(str).str.zfill(12).str[-8:]
        self.df_mayor['cuit'] = self.df_mayor[col_cuit_mayor].astype(str).str.zfill(12).str[-11:]
        self.df_bancos['importe_r'] = pd.to_numeric(self.df_bancos[col_imp_banco], errors='coerce').fillna(0).round(2)
        self.df_mayor['importe_r'] = pd.to_numeric(self.df_mayor[col_imp_mayor], errors='coerce').fillna(0).round(2)
        self.df_bancos['importe_abs'] = self.df_bancos['importe_r'].abs()
        self.df_mayor['importe_abs'] = self.df_mayor['importe_r'].abs()

        self.df_bancos['cuit'] = self.df_bancos[col_detalle_banco].apply(self.extraer_cuit).apply(self.normalizar_cuit)
        if col_cuit_mayor:
            self.df_mayor['cuit'] = self.df_mayor[col_cuit_mayor].apply(self.normalizar_cuit)
        else:
            self.df_mayor['cuit'] = self.df_mayor[col_detalle_mayor].apply(self.extraer_cuit).apply(self.normalizar_cuit)

        logging.info(f"Inicial: bancos={len(self.df_bancos)}, mayor={len(self.df_mayor)}")

        # ETAPA 1A: comprobante_norm + importe_abs
        resultado1 = pd.DataFrame()
        try:
            resultado1 = pd.merge(
                self.df_mayor.dropna(subset=['comprobante_norm', 'importe_abs']),
                self.df_bancos.dropna(subset=['comprobante_norm', 'importe_abs']),
                on=['comprobante_norm', 'importe_abs'],
                how='inner',
                indicator=True,
                suffixes=('_mayor', '_banco')
            )
        except Exception as e:
            logging.warning(f"Etapa1A fallo merge comprobante_norm: {e}")
        logging.info(f"Etapa1A conciliados: {len(resultado1)}")

        mayor_rest = self.df_mayor.copy()
        banco_rest = self.df_bancos.copy()
        if not resultado1.empty:
            keys1 = resultado1[['comprobante_norm', 'importe_abs']].apply(tuple, axis=1)
            mayor_rest = mayor_rest[~mayor_rest[['comprobante_norm', 'importe_abs']].apply(tuple, axis=1).isin(keys1)]
            banco_rest = banco_rest[~banco_rest[['comprobante_norm', 'importe_abs']].apply(tuple, axis=1).isin(keys1)]

        # Ajuste: Excluir movimientos que están en el mayor pero no en el banco
        mayor_rest = mayor_rest[mayor_rest['comprobante_norm'].isin(banco_rest['comprobante_norm'])]

        # ETAPA 1B: c8 + importe_abs
        resultado2 = pd.DataFrame()
        try:
            resultado2 = pd.merge(
                mayor_rest.dropna(subset=['c8', 'importe_abs']),
                banco_rest.dropna(subset=['c8', 'importe_abs']),
                on=['c8', 'importe_abs'],
                how='inner',
                indicator=True,
                suffixes=('_mayor', '_banco')
            )
        except Exception as e:
            logging.warning(f"Etapa1B fallo merge c8: {e}")
        logging.info(f"Etapa1B conciliados: {len(resultado2)}")

        if not resultado2.empty:
            keys2 = resultado2[['c8', 'importe_abs']].apply(tuple, axis=1)
            mayor_rest = mayor_rest[~mayor_rest[['c8', 'importe_abs']].apply(tuple, axis=1).isin(keys2)]
            banco_rest = banco_rest[~banco_rest[['c8', 'importe_abs']].apply(tuple, axis=1).isin(keys2)]

        # ETAPA 1C: c4 + importe_abs
        resultado3 = pd.DataFrame()
        try:
            resultado3 = pd.merge(
                mayor_rest.dropna(subset=['c4', 'importe_abs']),
                banco_rest.dropna(subset=['c4', 'importe_abs']),
                on=['c4', 'importe_abs'],
                how='inner',
                indicator=True,
                suffixes=('_mayor', '_banco')
            )
        except Exception as e:
            logging.warning(f"Etapa1C fallo merge c4: {e}")
        logging.info(f"Etapa1C conciliados: {len(resultado3)}")

        partes = [df for df in (resultado1, resultado2, resultado3) if df is not None and not df.empty]
        self.resultado_concilia = pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()
        logging.info(f"TOTAL conciliados etapa 1: {len(self.resultado_concilia)}")

        # ETAPA 2: CUIT + importe_abs sobre los restantes
        if not self.resultado_concilia.empty and 'comprobante_norm' in self.resultado_concilia.columns:
            conc_keys = self.resultado_concilia[['comprobante_norm','importe_abs']].dropna().apply(tuple,axis=1).tolist()
            mayor_no = self.df_mayor[~self.df_mayor[['comprobante_norm','importe_abs']].apply(tuple,axis=1).isin(conc_keys)]
            banco_no = self.df_bancos[~self.df_bancos[['comprobante_norm','importe_abs']].apply(tuple,axis=1).isin(conc_keys)]
        else:
            mayor_no = self.df_mayor.copy()
            banco_no = self.df_bancos.copy()

        mayor_con_cuit = mayor_no.dropna(subset=['cuit', 'importe_abs'])
        banco_con_cuit = banco_no.dropna(subset=['cuit', 'importe_abs'])

        resultado_cuit = pd.DataFrame()
        if not mayor_con_cuit.empty and not banco_con_cuit.empty:
            try:
                resultado_cuit = pd.merge(
                    mayor_con_cuit,
                    banco_con_cuit,
                    on=['cuit', 'importe_abs'],
                    how='inner',
                    indicator=True,
                    suffixes=('_mayor', '_banco')
                )
            except Exception as e:
                logging.warning(f"Etapa2 fallo merge cuit: {e}")
        logging.info(f"Etapa2 conciliados: {len(resultado_cuit)}")

        if not resultado_cuit.empty:
            if not self.resultado_concilia.empty:
                self.resultado_concilia = pd.concat([self.resultado_concilia, resultado_cuit], ignore_index=True)
            else:
                self.resultado_concilia = resultado_cuit.copy()
        logging.info(f"TOTAL conciliados luego de Etapa 2: {len(self.resultado_concilia)}")

        # Post-procesamiento
        if not self.resultado_concilia.empty:
            if 'cuit_mayor' in self.resultado_concilia.columns:
                self.resultado_concilia['cuit'] = self.resultado_concilia['cuit_mayor']
            elif 'cuit_banco' in self.resultado_concilia.columns:
                self.resultado_concilia['cuit'] = self.resultado_concilia['cuit_banco']

            self.asignar_columna_equivalente(self.resultado_concilia, 'concepto', self.columnas_equivalentes.get('concepto', []), ['banco', 'mayor'])
            self.asignar_columna_equivalente(self.resultado_concilia, 'detalle', self.columnas_equivalentes.get('detalle', []), ['banco', 'mayor'])

            col_imp_final = self.get_col(self.resultado_concilia, 'importe')
            if col_imp_final and f'{col_imp_final}_mayor' in self.resultado_concilia.columns:
                self.resultado_concilia['importe'] = self.resultado_concilia[f'{col_imp_final}_mayor']
            elif col_imp_final and f'{col_imp_final}_banco' in self.resultado_concilia.columns:
                self.resultado_concilia['importe'] = self.resultado_concilia[f'{col_imp_final}_banco']

        # cálculo de únicos
        try:
            idx_conciliados = pd.MultiIndex.from_frame(self.resultado_concilia[['c4', 'importe_abs', 'cuit']]) if not self.resultado_concilia.empty else pd.MultiIndex.from_tuples([])
            idx_original_mayor = pd.MultiIndex.from_frame(self.df_mayor[['c4', 'importe_abs', 'cuit']])
            idx_original_banco = pd.MultiIndex.from_frame(self.df_bancos[['c4', 'importe_abs', 'cuit']])
            self.unicos_empresa = self.df_mayor[~idx_original_mayor.isin(idx_conciliados)].copy()
            self.unicos_banco = self.df_bancos[~idx_original_banco.isin(idx_conciliados)].copy()
        except Exception:
            self.unicos_empresa = self.df_mayor[~self.df_mayor.index.isin(self.resultado_concilia.index)].copy() if not self.resultado_concilia.empty else self.df_mayor.copy()
            self.unicos_banco = self.df_bancos[~self.df_bancos.index.isin(self.resultado_concilia.index)].copy() if not self.resultado_concilia.empty else self.df_bancos.copy()

        if 'importe' not in self.unicos_empresa.columns:
            self.unicos_empresa['importe'] = self.unicos_empresa.get('importe_r', self.unicos_empresa.get('importe_abs', 0))
        if 'importe' not in self.unicos_banco.columns:
            self.unicos_banco['importe'] = self.unicos_banco.get('importe_r', self.unicos_banco.get('importe_abs', 0))

        # totales
        col_conc_banco = self.get_col(self.df_bancos, 'concepto')
        col_imp_banco = self.get_col(self.df_bancos, 'importe')
        if col_conc_banco and col_imp_banco:
            self.totales_banco = self.df_bancos.groupby(col_conc_banco)[col_imp_banco].sum().sort_index()
        else:
            self.totales_banco = pd.Series()

    # Mantengo las funciones de guardado aunque simplificadas (copiadas de la original)
    def guardarUnicosEntidad(self, unicos_entidad, cuenta_concilia):
        print("|------------------------> guardarUnicosEntidad("+str(cuenta_concilia)+")  <------------------------|")
        logging.info(unicos_entidad)
        df = unicos_entidad.dropna(how='all')
        if df.empty:
            print("No hay registros únicos de entidad para guardar.")
            return
        # normalizaciones mínimas
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
            delete_sql = "DELETE FROM SisMasterEntidad WHERE idEmpresa = %s AND procesado_sn = 'N' AND estado = 1"
            cursor.execute(delete_sql, (self.id_empresa,))
            db_connection.commit()
            valores = []
            for row in df.itertuples(index=False):
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
                    0,0,importe_val,0,0,0,0,0,0,concepto_val,fecha_actual,'N',"0",cuenta_concilia,self.id_usuario,0,1,0
                ))
            sql = """INSERT INTO SisMasterEntidad (idConcilia, idEmpresa,  m_ingreso, nro_comp, m_asiento_concilia, m_asiento, m_pase,  importe, debito, credito, saldo, codigo, m_minuta, concepto, detalle, fechayhora, procesado_sn, plan_cuentas, plan_cuentas_concilia, idUsuario, c4, estado, padron_codigo
                   ) VALUES (%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                  """
            cursor.executemany(sql, valores)
            db_connection.commit()
            print(f"Se insertaron {cursor.rowcount} registros correctamente.")
        except Exception as e:
            print(f"Error al insertar en la base de datos GuardarUnicosEntidad: {e}")
        finally:
            cursor.close()
            db_connection.close()

    # dejo las otras funciones de guardado tal cual (versión consolidada simplificada)
    def guardarUnicosEmpresa(self, unicos_empresa, cuenta_concilia):
        df = unicos_empresa.copy()
        fill_values = {
            'm_ingreso': '1900-01-01', 'comprobante': '', 'm_asiento': 0, 'm_pase': 0,
            'importe': 0.0, 'm_minuta': '', 'concepto': '', 'detalle': '', 'plan_cuentas': '', 'c4': '', 'padron_codigo': 0
        }
        df.fillna(value=fill_values, inplace=True)
        if 'm_ingreso' in df.columns:
            df['m_ingreso'] = df['m_ingreso'].apply(self.normalizarFechas)
        numerador = self.traerNumeradorActual()
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()
        try:
            delete_sql = "DELETE FROM SisMasterEmpresa WHERE idEmpresa = %s AND procesado_sn = 'N' AND estado = 1"
            cursor.execute(delete_sql, (self.id_empresa,))
            db_connection.commit()
            valores = []
            # Si columnas_equivalentes está definido, usar los nombres equivalentes
            if self.columnas_equivalentes is not None:
                # Normalizar nombres de columnas a minúsculas para evitar errores por mayúsculas/minúsculas
                for key, value in self.columnas_equivalentes.items():
                    if value:
                        for v in (value if isinstance(value, list) else [value]):
                            for col in df.columns:
                                if col.lower() == v.lower():
                                    df.rename(columns={col: key}, inplace=True)

            for row in df.itertuples():
                # Corrección aquí: aceptar str o datetime
                raw_fecha = getattr(row, 'fecha', '1900-01-01')
                if isinstance(raw_fecha, datetime):
                    fecha = raw_fecha.strftime('%Y-%m-%d')
                else:
                    fecha = self.normalizarFechas(raw_fecha)
                comprobante = getattr(row, 'comprobante', 0)
                try:
                    if int(comprobante) > 0:
                        logging.info("Comprobante detectado en unicos empresa -> "+str(comprobante))
                except (ValueError, TypeError):
                    logging.info("Comprobante no válido como numerico en unicos empresa -> " + str(comprobante))
                    pass  # O manejar el caso según tu lógica

                asiento = getattr(row, 'm_asiento', 0)
                pase = getattr(row, 'pase', 0)
                importe = getattr(row, 'importe', 0)
                minuta = getattr(row, 'm_minuta', 0)
                concepto = getattr(row, 'concepto', '')
                detalle = getattr(row, 'detalle', '')
                plan_cuentas = getattr(row, 'plan_cuentas', 0)
                c4 = getattr(row, 'c4', '0')
                padron_codigo = getattr(row, 'padron_codigo', 0)
                valores.append((
                    self.id_tipo_concilia, self.id_empresa, fecha, comprobante, numerador, asiento, pase,
                    importe,  concepto, detalle, fecha_actual, 'N', cuenta_concilia,self.id_usuario,   1
                ))
            sql = """
                  INSERT INTO SisMasterEmpresa (
                  idConcilia, idEmpresa,  m_ingreso, nro_comp, m_asiento_concilia, m_asiento, m_pase,  importe,  concepto, detalle, fechayhora, procesado_sn, plan_cuentas,  idUsuario, estado
                  ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                  """
            cursor.executemany(sql, valores)
            db_connection.commit()
            print(f"Se insertaron en unicos empresa {cursor.rowcount} registros correctamente.")
        except Exception as e:
            print(f"Error al insertar en la base de datos GuardarUnicosEmpresa: {e}")
        finally:
            cursor.close()
            db_connection.close()

    def guardarTotalesBanco(self, resultado_totales_banco, cuenta_concilia):
        numerador = self.traerNumeradorActual()
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()
        try:
            delete_sql = "DELETE FROM SisMasterTotales WHERE idEmpresa = %s AND procesado_sn = 'N' AND estado = 1"
            cursor.execute(delete_sql, (self.id_empresa,))
            db_connection.commit()
            self.guardarUnicosEmpresa(self.unicos_empresa, self.cuenta_concilia)
            self.guardarUnicosEntidad(self.unicos_banco, self.cuenta_concilia)
            df = resultado_totales_banco.to_frame()
            valores = []
            fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            fecha = datetime.now().strftime('%Y-%m-%d')

            for row in df.itertuples():
                index = getattr(row, 'Index', 0)
                importe= getattr(row, 'importe', 0)
                valores.append((
                    self.id_tipo_concilia, self.id_empresa, numerador, fecha, index,importe, str(fecha_actual), 'N', 0, 0, self.id_usuario, 1
                ))
            sql = """
                INSERT INTO SisMasterTotales (
                    idConcilia, idEmpresa, m_asiento_concilia, m_ingreso, concepto, importe, fechayhora, procesado_sn, plan_cuentas, plan_cuentas_concilia, idUsuario, estado
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(sql, valores)
            db_connection.commit()
            print(f"Se insertaron {cursor.rowcount} registros correctamente.")
        except Exception as e:
            print(f"Error al insertar en la base de datos: {e}")
        finally:
            cursor.close()
            db_connection.close()

    def guardaResultadosConciliacion(self, resultado_concilia, cuenta_concilia =0):
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()

        try:
            delete_sql_cab = "DELETE FROM ConciliaCab WHERE idEmpresa = %s AND procesado_sn = 'N' AND estado = 1 and idUsuario = %s and idConcilia = %s"
            cursor.execute(delete_sql_cab, (self.id_empresa, self.id_usuario, self.id_tipo_concilia))
            db_connection.commit()
            delete_sql = "DELETE FROM SisMaster WHERE idEmpresa = %s AND procesado_sn = 'N' AND idUSuario = %s and estado = 1"
            cursor.execute(delete_sql, (self.id_empresa,self.id_usuario))
            db_connection.commit()
            if 'plan_cuentas' in resultado_concilia.columns:
                resultado_concilia["plan_cuentas"] = resultado_concilia["plan_cuentas"].astype(str)
            if not resultado_concilia.empty and not resultado_concilia["plan_cuentas"].eq(cuenta_concilia).all():
                return {"codigo":400, "control":"ERROR", "mensaje": "Error: La cuenta a conciliar en su archivo no coincide con la seleccionada ("+str(cuenta_concilia)+")"}
            numerador = self.proximoNumeroAsientoConcilia()
            sql_insert_cab = """
                        INSERT INTO ConciliaCab (idEmpresa, idConcilia, nombre, descripcion, asiento_concilia, procesado_sn, idUsuario, estado)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
            # corregir posible typo en id_tipo_concia
            try:
                id_tipo_val = getattr(self, 'id_tipo_concilia')
            except Exception:
                id_tipo_val = 0
            cursor.execute(sql_insert_cab, (self.id_empresa, id_tipo_val, numerador, numerador, numerador, "N", self.id_usuario, 1))
            db_connection.commit()
            valores = []
            for index, row in resultado_concilia.iterrows():
                valores.append((
                        self.id_tipo_concilia, self.id_empresa, row.get('m_asiento'), numerador, row.get('m_pase'), self.normalizarFechas(row.get('m_ingreso')), row.get('plan_cuentas'),
                        row['concepto_mayor' if 'concepto_mayor' in row and pd.notna(row['concepto_mayor']) else 'concepto'],
                        row['detalle_mayor' if 'detalle_mayor' in row and pd.notna(row['detalle_mayor']) else 'detalle'],
                        row.get('nro_comp'), 0, 0, 0, row.get('importe'), row.get('importe'), "N", self.id_usuario, 1
               ))

            sql = """
                  INSERT INTO SisMaster (
                      idConcilia, idEmpresa, m_asiento, m_asiento_concilia, m_pase, m_ingreso, plan_cuentas, concepto, detalle, nro_comp, debito,
                      credito, codigo, saldo, importe, procesado_sn, idUsuario, estado
                  ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                  """

            try:
                cursor.executemany(sql, valores)
                db_connection.commit()
                # Grabo totales y diferencias
                try:
                    self.guardarTotalesBanco(self.totales_banco, self.cuenta_concilia)
                except Exception as e:
                    logger.warning(f"No se pudieron guardar totales: {e}")
                return {"codigo":200, "control":"OK", "mensaje": "Proceso de conciliación se completo con éxito. ("+str(
                    len(self.resultado_concilia))+") registros conciliados."}
            except Exception as e:
                db_connection.rollback()
                return {"codigo":400, "control":"ERROR", "mensaje": "GuardarResultadosConciliacion: Error al insertar en la base de datos "+str(e)}
        finally:
            cursor.close()
            db_connection.close()

    def traerNumeradorActual(self):
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()
        try:
            sql_select = "SELECT numerador FROM Numerador WHERE idEmpresa = %s"
            cursor.execute(sql_select, (self.id_empresa,))
            resultado = cursor.fetchone()
            return resultado[0] if resultado else 0
        except Exception:
            return 0
        finally:
            cursor.close()
            db_connection.close()

    def proximoNumeroAsientoConcilia(self):
        conn = ConectorManagerDB(1)
        db_connection = conn.get_connection().conn
        cursor = db_connection.cursor()
        try:
            sql_select = "SELECT numerador FROM Numerador WHERE idEmpresa = %s"
            cursor.execute(sql_select, (self.id_empresa,))
            resultado = cursor.fetchone()
            if resultado:
                numerador_actual = resultado[0]
                numerador_nuevo = numerador_actual + 1
                sql_update = "UPDATE Numerador SET numerador = %s WHERE idEmpresa = %s"
                cursor.execute(sql_update, (numerador_nuevo, self.id_empresa))
                db_connection.commit()
                return numerador_nuevo
            else:
                return 0
        except Exception:
            try:
                db_connection.rollback()
            except Exception:
                pass
            return 0
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
