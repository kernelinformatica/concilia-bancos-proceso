from io import BytesIO
import sys
import os
from pathlib import Path

# Asegurar que el paquete local esté en sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from concilia_bancos_api_calc.concilia import Conciliador

# Usar un archivo xlsx del repo (si existe)
xlsx_path = Path(r"D:/Dario/Proyectos/Python/kernel/ws-rest/TARIFA COOP LEON SOLA 04.11.25.xlsx")
if not xlsx_path.exists():
    print('No se encontró el archivo de prueba:', xlsx_path)
    sys.exit(1)

with open(xlsx_path, 'rb') as f:
    data = f.read()

bancos_stream = BytesIO(data)
mayor_stream = BytesIO(data)

c = Conciliador(bancos_stream, mayor_stream, salida='.', id_empresa=1, id_usuario=1, id_tipo_conicliacion=1, cuenta_concilia=0)
try:
    c.cargar_datos_2()
    print('Carga OK. bancos:', len(c.df_bancos), ' mayor:', len(c.df_mayor))
    c.procesar_datos()
    print('Procesado OK. conciliados:', len(c.resultado_concilia), 'unicos_empresa:', len(c.unicos_empresa), 'unicos_banco:', len(c.unicos_banco))
except Exception as e:
    print('Error durante el test:', repr(e))
    raise
