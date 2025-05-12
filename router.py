import os

import paramiko
from flask import Blueprint, request, jsonify
import logging

from werkzeug.utils import secure_filename
from dotenv import load_dotenv
import config
from concilia import Conciliador as concilia, Conciliador

concilia_bp = Blueprint('concilia', __name__)
load_dotenv()

@concilia_bp.route('/conciliar', methods=['GET'])
def conciliar():
    try:
        ruta_bancos = "C:/temp/bancos/bancos.xls"
        ruta_mayor = "C:/temp/bancos/mayor.xls"
        salida = "C:/temp/bancos/"

        # Ejecutar conciliación
        conciliador = Conciliador(ruta_bancos, ruta_mayor, salida)
        conciliador.ejecutar()
        logging.info(f"Conciliación completada con éxito")
        return jsonify({"message": "Conciliación completada con éxito.", "salida": salida}), 200
    except Exception as e:
        logging.error(f"Error en la conciliación: {e}")
        return jsonify({"error": str(e)}), 500


@concilia_bp.route('/subir_y_conciliar', methods=['POST'])
def subir_y_conciliar():
    try:
        # Verificar si los archivos están en la solicitud
        if 'ruta_bancos' not in request.files or 'ruta_mayor' not in request.files:
            return jsonify({"control": "ERROR",
                            "codigo ": "400",
                            "mensaje": "Ambos archivos (ruta_bancos y ruta_mayor) son requeridos."}), 400

        # Obtener los archivos del formulario
        file_bancos = request.files['ruta_bancos']
        file_mayor = request.files['ruta_mayor']
        # Guardar los archivos temporalmente en el servidor
        upload_folder = "C:/temp/uploads/"
        os.makedirs(upload_folder, exist_ok=True)
        bancos_path = os.path.join(upload_folder, secure_filename(file_bancos.filename))
        mayor_path = os.path.join(upload_folder, secure_filename(file_mayor.filename))
        file_bancos.save(bancos_path)
        file_mayor.save(mayor_path)

        # Configuración del servidor SFTP
        sftp_host = os.getenv("SFTP_HOST")
        sftp_port = os.getenv("SFTP_PORT")
        sftp_user = os.getenv("SFTP_USER")
        sftp_password = os.getenv("SFTP_PASSWORD")
        sftp_destino = os.getenv("SFTP_DESTINO")
        print(sftp_host, sftp_port, sftp_user, sftp_password, sftp_destino)
        # Conexión al servidor SFTP
        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_user, password=sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Subir archivos al servidor SFTP
        sftp.put(bancos_path, f"{str(sftp_destino)}bancos.xls")
        sftp.put(mayor_path, f"{str(sftp_destino)}mayor.xls")

        sftp.close()
        transport.close()
        logging.info("Archivos subidos al servidor SFTP con éxito.")

        # Ejecutar la conciliación en el servidor
        conciliador = Conciliador(bancos_path, mayor_path, sftp_destino)
        conciliador.ejecutar()

        return jsonify({"message": "Archivos subidos y conciliación ejecutada con éxito."}), 200

    except Exception as e:
        logging.error(f"Error al subir archivos o ejecutar la conciliación: {e}")
        return jsonify({"error": str(e)}), 500

# **🔹 Test de conexión**
@concilia_bp.route('/dummy', methods=['GET'])
def dummy():
    try:
        logging.info("Validando conexión con el servicio web...")
        conciliador = Conciliador()
        dummy = conciliador.validarConexion()
        return jsonify(dummy)

    except Exception as e:
        logging.error(f"Error en validarConexionEndpoint: {e}")
        return jsonify({"control": "ERROR", "mensaje": str(e)}), 500

