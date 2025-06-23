import pandas as pd

# Cambia la ruta al archivo Excel correcto
df = pd.read_excel('H:\Dario\Proyectos\Python\kernel\ws-rest\concilia_bancos_api_calc\data-test\mayor.xls', dtype={'comprobante': str})

print(df.shape)  # Muestra la cantidad de filas y columnas importadas
print(df.head()) # Muestra las primeras filas para verificar