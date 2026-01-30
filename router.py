import os

import paramiko
from flask import Blueprint, request, jsonify
import logging
from io import BytesIO
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


@concilia_bp.route('/conciliar_datos', methods=['POST'])
def subir_y_conciliar():
    try:

        # Verificar si los archivos están en la solicitud
        if 'resu-banco' not in request.files or 'resu-contable' not in request.files:
            return jsonify({"control": "ERROR",
                            "codigo ": "400",
                            "mensaje": "Ambos archivos (ruta_bancos y ruta_mayor) son requeridos."}), 400

        # Obtener los archivos del formulario
        file_bancos = request.files['resu-banco']
        file_mayor = request.files['resu-contable']
        # Obtener empresa y usuario desde request.form
        id_empresa = request.form.get('empresa')
        id_usuario = request.form.get('usuario')
        id_tipo_conciliacion = request.form.get('tipoConciliacion', 1)
        cuenta_concilia = request.form.get('cuentaConcilia', 0)
        # Configuración del servidor SFTP
        sftp_host = os.getenv("SFTP_HOST")
        sftp_port = int(os.getenv("SFTP_PORT"))
        sftp_user = os.getenv("SFTP_USER")
        sftp_password = os.getenv("SFTP_PASSWORD")
        sftp_destino = str(os.getenv("SFTP_DESTINO"))
        print("HOST: "+str(sftp_host))
        print("USER: " + str(sftp_user))
        print("PORT: " + str(sftp_port))
        print("PASSWORD: " + str(sftp_password))
        print("DESTINO: " + str(sftp_destino))
        if not sftp_destino.endswith('/'):
            sftp_destino += '/'

        # Conexión al servidor SFTP
        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_user, password=sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        print(":: sftp destino "+sftp_destino+" :: ")


        # Subir archivos al servidor SFTP

        bancos_stream = BytesIO(file_bancos.read())
        mayor_stream = BytesIO(file_mayor.read())

        sftp.putfo(bancos_stream, f"{sftp_destino}bancos.xls")
        sftp.putfo(mayor_stream, f"{sftp_destino}mayor.xls")
        sftp.close()
        transport.close()
        logging.info("Archivos subidos al servidor SFTP con éxito.")

        # Ejecutar la conciliación en el servidor
        conciliador = Conciliador(bancos_stream, mayor_stream, str(sftp_destino), id_empresa, id_usuario, id_tipo_conciliacion, cuenta_concilia)
        resp = conciliador.ejecutar()
        return resp


    except Exception as e:
        return jsonify({
            "control": "ERROR",
            "codigo": 500,
            "mensaje": "Error al subir los archivos o al generar la conciliación.",
            "estado": "-1"
        }), 500

        return jsonify({"error": str(e)}), 500

# **🔹 Test de conexión**
@concilia_bp.route('/dummy', methods=['GET'])
def dummy():
    import json
    data = {
        "code": "1",
        "version": "1.0",
        "status": 200,
        "description": "Conciliacion Bancaria: Importación y Generación de Conciliaciones.",
        "name": "Conciliacion Bancaria",
        "message": "Conciliacion Bancaria, Importación y Generación de Conciliaciones: Funciona correctamente",

    }
    json_output = json.dumps(data, indent=4)
    logging.info(json_output)
    return json_output

