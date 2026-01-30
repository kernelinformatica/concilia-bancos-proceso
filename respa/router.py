import os
import pandas as pd
from io import BytesIO
import paramiko
from flask import Blueprint, request, jsonify
import logging
from dotenv import load_dotenv
from concilia import Conciliador

concilia_bp = Blueprint('concilia', __name__)
load_dotenv()


def _hexdump_prefix(data: bytes, length: int = 64) -> str:
    """Devuelve una representación hex corta y un snippet decodificado si es posible."""
    h = ' '.join(f"{b:02x}" for b in data[:length])
    try:
        s = data[:256].decode('utf-8', errors='replace')
    except Exception:
        s = repr(data[:256])
    return f"hex={h} snippet={s}"


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
        logging.info("Iniciando el proceso de subida y conciliación de archivos.")
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


        # --- NUEVO: no convertir, exigir .xlsx ---
        # En lugar de ejecutar la función de conversión (que intentaba convertir xls/xlsb/csv)
        # tratamos siempre los inputs como .xlsx. Validamos que openpyxl/pandas pueda leerlos
        bancos_stream = BytesIO(file_bancos.read())
        mayor_stream = BytesIO(file_mayor.read())

        # Validar que sean .xlsx válidos (si no lo son, respondemos con error claro)
        try:
            bancos_stream.seek(0)
            # usar la función centralizada que valida firma PK y realiza la lectura de comprobación
            bancos_stream = convertir_a_xlsx(bancos_stream)
            bancos_stream.seek(0)
        except Exception as e:
            logging.exception("Archivo 'resu-banco' no es un .xlsx válido")
            return jsonify({
                "control": "ERROR",
                "codigo": 400,
                "mensaje": "Archivo 'resu-banco' debe ser .xlsx válido: " + str(e),
                "estado": "-1"
            }), 400

        try:
            mayor_stream.seek(0)
            mayor_stream = convertir_a_xlsx(mayor_stream)
            mayor_stream.seek(0)
        except Exception as e:
            logging.exception("Archivo 'resu-contable' no es un .xlsx válido")
            return jsonify({
                "control": "ERROR",
                "codigo": 400,
                "mensaje": "Archivo 'resu-contable' debe ser .xlsx válido: " + str(e),
                "estado": "-1"
            }), 400

        # Subir archivos al SFTP con extensión .xlsx (mantener nombre claro)
        sftp.putfo(bancos_stream, f"{sftp_destino}bancos.xlsx")
        bancos_stream.seek(0)
        sftp.putfo(mayor_stream, f"{sftp_destino}mayor.xlsx")
        mayor_stream.seek(0)
        sftp.close()
        transport.close()
        logging.info("Archivos subidos al servidor SFTP con éxito.")

        # Ejecutar la conciliación en el servidor
        # Nota: Conciliador puede recibir objetos BytesIO si está preparado para eso;
        # de lo contrario deberá adaptarse a recibir rutas en disco o streams.
        conciliador = Conciliador(bancos_stream, mayor_stream, str(sftp_destino), id_empresa, id_usuario, id_tipo_conciliacion, cuenta_concilia)
        resp = conciliador.ejecutar()
        return resp


    except Exception as e:

        return jsonify({
            "control": "ERROR",
            "codigo": 500,
            "mensaje": "Error general en el formato de los archivos ."+str(e.args[0]),
            "estado": "-1"
        }), 500




def convertir_a_xlsx(stream: BytesIO):

    try:
        stream.seek(0)
        data = stream.read()

        if not data or len(data) == 0:
            raise ValueError("Archivo vacío o sin contenido")

        # Comprobar firma ZIP de formato .xlsx (PK..)
        sig = data[:4]
        if sig != b'PK\x03\x04':
            # encabezado inesperado: registrar hex/snippet y devolver error claro
            header_info = _hexdump_prefix(data, length=64)
            logging.debug(f"convertir_a_xlsx: encabezado inválido para .xlsx: {header_info}")
            raise ValueError(
                "No es un .xlsx válido (cabecera distinta a PK..). "
                f"Encabezado: {header_info}"
            )

        # Si tiene firma PK, intentamos leer con openpyxl para detectar corrupción
        from io import BytesIO as _BytesIO
        try:
            _BytesIO(data).seek(0)
            pd.read_excel(_BytesIO(data), engine='openpyxl')
            out = _BytesIO(data)
            out.seek(0)
            logging.debug("convertir_a_xlsx: archivo válido .xlsx, no se realizó conversión")
            return out
        except Exception as e_open:
            logging.exception("convertir_a_xlsx: archivo con firma PK pero openpyxl falló al leerlo")
            raise ValueError(
                "Archivo con firma PK pero openpyxl no pudo leerlo (posible fichero corrupto). "
                f"openpyxl error: {e_open}"
            )

    except Exception as e:
        logging.exception("convertir_a_xlsx: error en la validación simplificada")
        raise ValueError(f"Error al convertir a .xlsx: {e}")



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

