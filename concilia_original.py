
import pandas as pd

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
# Ruta al archivo XLS
ruta3_xls = "C:/temp/bancos/bancos.xls"
ruta4_xls = "C:/temp/bancos/mayor.xls"
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
#
# Guardar los resultados en un archivo CSV
#resultado_nuevo.to_csv('c:/bancos/resultados_importes.csv', sep=",", decimal=".", index=False)
importes_unicos_banco.to_csv("c:/temp/bancos/resultados_bancos.csv", sep=",", decimal=".", index=False)
importes_unicos_empresa.to_csv("c:/temp/bancos/resultados_empresa.csv", sep=",", decimal=".", index=False)
totales_banco.to_csv("c:/temp/bancos/totales_banco.csv", sep=",", decimal=".", index=True)
#resultado_concilia.to_csv("c:/bancos/resultado_concilia.csv", sep=",", decimal=".", index=False)
resultado_concilia4.to_csv("c:/temp/bancos/resultados_concilia.csv", sep=",", decimal=".", index=False)
