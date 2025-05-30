
import pandas as pd
from conectorManagerDB import ConectorManagerDB
# NOTA
# bancos.xls debe terner las columnas: concepto, comprobante, importe
# mayor.xls debe tener las columnas: comprobante, importe
# tienen que estar copiados en c:/bancos
#
# Accion
# realiza conciliacion entre las dos planillas
#
# Resultados en c:/bancos formato csv
# "c:/bancos/resultado_concilia.csv" Movimientos que coinciden por importe y comprobante - Sirve para marcar conciliados
# "c:/bancos/resultados-bancos.csv" Movimiento de bancos que no estan en empresa
# "c:/bancos/resultados-empresa.csv" Movimientos de Empresa que no estan en Bancos
# "c:/bancos/totales_banco.csv" Movimientos de bancos agrupados por conceptos - Sirve para hacer asiento gral por concepto
#
#



conn = ConectorManagerDB(1)
db_connection = conn.get_connection().conn  # Obtener la conexión
cursor = db_connection.cursor()

try:
    # Ejecutar el SELECT
    sql = "SELECT * FROM EstadosGenerales"
    cursor.execute(sql)

    # Obtener los resultados
    resultados = cursor.fetchall()

    # Imprimir los resultados
    for fila in resultados:
        print(fila)

except Exception as e:
    print(f"Error al ejecutar el SELECT: {e}")

finally:
    # Cerrar el cursor y la conexión
    cursor.close()
    db_connection.close()





# Ruta al archivo XLS
ruta3_xls = "C:/temp/bancos/base/bancos.xls"
ruta4_xls = "C:/temp/bancos/base/mayor.xls"
#
# Carga el archivo CSV en un DataFrame de Pandas
df3 = pd.read_excel(ruta3_xls, dtype={'comprobante': str})
df4 = pd.read_excel(ruta4_xls, dtype={'comprobante': str})
#
# Crea C4 con los 4 ultimos digitos de columna comprobante
df3['c4'] = df3['comprobante'].astype(str).str.zfill(4).str[-4:]
df4['c4'] = df4['comprobante'].astype(str).str.zfill(4).str[-4:]
#
#sort by c4, importe
df4 = df4.sort_values(by=['c4', 'importe'])
df3 = df3.sort_values(by=['c4', 'importe'])
resultado_concilia4 = pd.merge(df4, df3, on=['c4', 'importe'], how='inner', indicator=True)

#
#
# Encuentra los importes que están en banco pero no en empresa y al reves
importes_unicos_empresa = df4[~((df4['importe'].isin(df3['importe'])) & (df4['c4'].isin(df3['c4'])))]
importes_unicos_banco = df3[~((df3['importe'].isin(df4['importe'])) & (df3['c4'].isin(df4['c4'])))].sort_values(by='concepto')

# Agrupa por DETALLE y calcula la suma de IMPORTE
totales_banco = df3.groupby('concepto')['importe'].sum().sort_index()
print(totales_banco.info())





# Inicializa la conexión y el cursor
conn = ConectorManagerDB(1)
db_connection = conn.get_connection().conn
cursor = db_connection.cursor()

try:
    # Crear lista de tuplas con los valores a insertar
    df = totales_banco.to_frame()
    valores2 = []  # Definir la lista vacía fuera del bucle
    for row in df.itertuples():
        print(f'Concepto: {row.Index}, Importe: {row.importe}')
        valores2.append((
            1,
            2,
            '2025-05-28',
            row.Index,
            row.importe,
            '2025-05-28 00:00:00',
            'N',
            1,
            1,
            1
        ))

    # Nueva estructura de inserción
    sql2 = """
        INSERT INTO SisMasterTotales (
            idConcilia, idEmpresa, m_ingreso, concepto, importe, fechayhora, procesado_sn, plan_cuentas, idUsuario, estado
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Ejecutar la inserción de múltiples filas
    cursor.executemany(sql2, valores2)

    # Confirmar la transacción
    db_connection.commit()

    print(f"Se insertaron {cursor.rowcount} registros correctamente.")

except Exception as e:
    print(f"Error al insertar en la base de datos: {e}")

finally:
    cursor.close()
    db_connection.close()

#
# Guardar los resultados en un archivo CSV
#resultado_nuevo.to_csv('c:/bancos/resultados_importes.csv', sep=",", decimal=".", index=False)
importes_unicos_banco.to_csv("c:/temp/bancos/resultados_bancos.csv", sep=",", decimal=".", index=False)
importes_unicos_empresa.to_csv("c:/temp/bancos/resultados_empresa.csv", sep=",", decimal=".", index=False)
totales_banco.to_csv("c:/temp/bancos/totales_banco.csv", sep=",", decimal=".", index=True)
#resultado_concilia.to_csv("c:/bancos/resultado_concilia.csv", sep=",", decimal=".", index=False)
resultado_concilia4.to_csv("c:/temp/bancos/resultados_concilia.csv", sep=",", decimal=".", index=False)
