"""import pandas as pd

df = pd.read_excel('H:\Dario\Proyectos\Python\kernel\ws-rest\concilia_bancos_api_calc\data-test\mayor.xls', dtype={'comprobante': str})

print(df.shape)  # Muestra la cantidad de filas y columnas importadas
print(df.head()) # Muestra las primeras filas para verificar
"""

from dateutil import parser

fecha_entrada = "20/12/2024"
fecha = parser.parse(fecha_entrada, dayfirst=True)
print(fecha.strftime('%Y-%m-%d'))  # Esperado: 2024-12-20
