import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
from conn.ConciliaDB import DBConnection
from router import concilia_bp, dummy, conciliar
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





if __name__ == "__main__":
    concilia = AppConciliacionBancos()
    try:
        #with concilia.app.app_context():
        concilia.run(debug=True, port=6050)

    except Exception as e:
        logging.error(f"Error al iniciar el servicio: {e}")







