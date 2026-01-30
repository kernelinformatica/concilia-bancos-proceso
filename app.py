import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
from conn.ConciliaDB import DBConnection
from router import concilia_bp, dummy, conciliar, subir_y_conciliar
from io import BytesIO
from flask import Flask  # o FastAPI, etc.
app = Flask(__name__)

import config
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class AppConciliacionBancos(DBConnection):
    def __init__(self):
        super().__init__()
        self.app = Flask(__name__)
        CORS(self.app)
        self.app.register_blueprint(concilia_bp, url_prefix='/api')

    def run(self, debug=True, host="0.0.0.0", port=6050):
        self.app.run(debug=True, host=host, port=port)




"""
if __name__ == "__main__":
    concilia = AppConciliacionBancos()
    try:
        #concilia.run(debug=True, port=6050)

    except Exception as e:
        logging.error(f"Error al iniciar el servicio: {e}")

"""


if __name__ == '__main__':
    with open('c:/temp/conciliaciones/marga_credi_diciembre.xls', 'rb') as f_banco, \
         open('c:/temp/conciliaciones/mayor_concilia.xls', 'rb') as f_mayor:
        bancos_stream = BytesIO(f_banco.read())
        mayor_stream = BytesIO(f_mayor.read())
        from werkzeug.datastructures import FileStorage

        file_banco = FileStorage(stream=bancos_stream, filename='bancos.xls')
        file_mayor = FileStorage(stream=mayor_stream, filename='mayor.xls')

    with app.test_request_context(
            '/conciliar_datos',
            method='POST',
            data={
                'resu-banco': file_banco,
                'resu-contable': file_mayor,
                'empresa': '2',
                'usuario': '1',
                'tipoConciliacion': '1',
                'cuentaConcilia': '11010211'
            },
            content_type='multipart/form-data'
    ):
        resultado = subir_y_conciliar()
        print(resultado)






