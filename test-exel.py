import pandas as pd
p = r"C:\temp\conciliaciones\mayor_concilia.xls"   # ajusta la ruta al archivo .xls que usas
df = pd.read_excel(p, engine='xlrd')
print('shape=', df.shape)
print(df.head(3))