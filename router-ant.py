import os
from io import BytesIO
import paramiko
from flask import Blueprint, request, jsonify
import logging
from dotenv import load_dotenv
from concilia import Conciliador

concilia_bp = Blueprint('concilia', __name__)
load_dotenv()

# Requerir únicamente archivos .xls BIFF (opción global para este endpoint)
REQUIRE_ONLY_XLS = True


def _hexdump_prefix(data: bytes, length: int = 64) -> str:
    """Devuelve una representación hex corta y un snippet decodificado si es posible.

    Usa latin-1 para que la transformación sea 1:1 y reemplaza bytes no imprimibles
    por '.' en el snippet para que el mensaje sea legible en logs/errores.
    """
    h = ' '.join(f"{b:02x}" for b in data[:length])
    try:
        # latin-1 mantiene los bytes 1:1; luego imprimimos solo caracteres 'visibles'
        raw = data[:256].decode('latin-1', errors='replace')
        # reemplazar caracteres de control por '.' para visibilidad
        s = ''.join((ch if (32 <= ord(ch) <= 126 or ch == '\n' or ch == '\r' or ch == '\t') else '.') for ch in raw)
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

        # Paso 1: comprobar extensión de nombre de archivo: sólo se aceptan .xls
        filename_bancos = (file_bancos.filename or '').strip()
        filename_mayor = (file_mayor.filename or '').strip()
        if REQUIRE_ONLY_XLS:
            if not filename_bancos.lower().endswith('.xls'):
                return jsonify({
                    "control": "ERROR",
                    "codigo": 400,
                    "mensaje": "Archivo 'resu-banco' debe tener extensión .xls (BIFF). Nombre recibido: '" + filename_bancos + "'",
                    "estado": "-1"
                }), 400
            if not filename_mayor.lower().endswith('.xls'):
                return jsonify({
                    "control": "ERROR",
                    "codigo": 400,
                    "mensaje": "Archivo 'resu-contable' debe tener extensión .xls (BIFF). Nombre recibido: '" + filename_mayor + "'",
                    "estado": "-1"
                }), 400

        # Obtener empresa y usuario desde request.form
        id_empresa = request.form.get('empresa')
        id_usuario = request.form.get('usuario')
        id_tipo_conciliacion = request.form.get('tipoConciliacion', 1)
        cuenta_concilia = request.form.get('cuentaConcilia', 0)

        # --- MOVER LA CONEXION SFTP: primero VALIDAR ARCHIVOS, luego conectamos y subimos ---

        # --- NUEVO: no convertir, exigir .xls ---
        # En lugar de ejecutar la función de conversión (que intentaba convertir xls/xlsb/csv)
        # tratamos siempre los inputs como .xls. Validamos que tengan cabecera OLE (D0 CF 11 E0 ...)
        bancos_stream = BytesIO(file_bancos.read())
        mayor_stream = BytesIO(file_mayor.read())

        # Validar que sean .xls válidos (si no lo son, respondemos con error claro)
        def _validate_is_xls(stream: BytesIO, field_name: str):
            """Valida que el stream corresponda a un .xls BIFF (cabecera OLE).
            Heurística ampliada: además de la firma OLE (contenedor .xls típico), aceptamos
            también flujos BIFF "raw" que no están encapsulados en OLE pero que parecen
            provenir de un workbook BIFF (ej. encabezados/strings de columnas dentro del
            binario). Esto evita rechazar algunos .xls generados por sistemas que envían
            el flujo interno en lugar del archivo OLE completo.

            Si ninguna heurística coincide, lanzamos ValueError con un hexdump para diagnóstico.
            """
            stream.seek(0)
            data = stream.read(256)
            # firma OLE BIFF (xls) - Excel 97-2003 en contenedor OLE
            ole_sig = b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1'
            if len(data) >= 8 and data[:8] == ole_sig:
                stream.seek(0)
                logging.debug(f"_validate_is_xls: detectado contenedor OLE BIFF para {field_name}")
                return stream

            # heurística 1: detectar posible stream BIFF "raw" (no OLE) - Excel tiende a tener
            # encabezados legibles seguidos de datos binarios. Aceptamos si encontramos cadenas
            # ASCII típicas de encabezados.
            ascii_indicators = [b'm_ingreso', b'm_importe', b'm_asiento', b'm_pase', b'plan_cuentas']
            if any(ind in data for ind in ascii_indicators):
                stream.seek(0)
                logging.debug(f"_validate_is_xls: detectado flujo BIFF/encabezados internos para {field_name} (heurística) - se acepta")
                return stream

            # heurística 2: algunos BIFF raw empiezan con bytes similares a registros: 0x09 0x00 0x04 0x00 ...
            # comprobamos patrón inicial de bytes pequeños (varios valores < 0x20) que suelen aparecer
            # en encabezado de registros binarios de Excel. No es infalible, así que lo registramos.
            if len(data) >= 8 and all(b < 0x20 for b in data[:8]):
                # Si además contiene texto legible con separadores nulos, lo aceptamos
                try:
                    text_sample = data.decode('latin-1', errors='ignore')
                    # si contiene al menos 3 letras seguidas y alguna palabra clave, lo tomamos como BIFF
                    import re
                    if re.search(r'[A-Za-z]{3,}', text_sample):
                        stream.seek(0)
                        logging.debug(f"_validate_is_xls: detectado posible BIFF raw por bytes pequeños iniciales para {field_name}")
                        return stream
                except Exception:
                    pass

            # detectar ZIP/PK -> muy probablemente un .xlsx (contenedor OpenXML)
            if len(data) >= 4 and data[:4] == b'PK\x03\x04':
                header_info = _hexdump_prefix(data, length=64)
                logging.debug(f"Validación .xls: archivo con cabecera PK (posible .xlsx) ({field_name}): {header_info}")
                raise ValueError(
                    f"Archivo '{field_name}' parece ser un .xlsx (cabecera PK). Este endpoint acepta únicamente .xls BIFF. "
                    f"Por favor envíe un .xls (Excel 97-2003) o ajuste el flujo. Encabezado: {header_info}"
                )

            # detectar muchos bytes nulos -> posible UTF-16LE (CSV/Texto con encoding utf-16)
            null_count = data.count(b"\x00")
            if null_count > (len(data) // 4):
                header_info = _hexdump_prefix(data, length=64)
                logging.debug(f"Validación .xls: archivo con muchos 0x00 (posible UTF-16/texto) ({field_name}): {header_info}")
                raise ValueError(
                    f"Archivo '{field_name}' parece ser un archivo de texto (posible CSV en UTF-16). "
                    f"Este endpoint acepta únicamente .xls BIFF. Encabezado: {header_info}"
                )

            # detectar encabezados XML (<?xml) -> archivo XML/Excel 2003 XML
            if data.startswith(b'<?xml') or b'<?xml' in data[:16]:
                header_info = _hexdump_prefix(data, length=64)
                logging.debug(f"Validación .xls: archivo con cabecera XML (posible Excel XML) ({field_name}): {header_info}")
                raise ValueError(
                    f"Archivo '{field_name}' parece ser un XML/Excel 2003 (no .xls BIFF). "
                    f"Por favor guárdelo como Excel 97-2003 (.xls). Encabezado: {header_info}"
                )

            # si no encaja en lo anterior, devolver diagnóstico genérico con hexdump
            header_info = _hexdump_prefix(data, length=64)
            logging.debug(f"Validación .xls fallida ({field_name}): {header_info}")
            raise ValueError(
                f"Archivo '{field_name}' no tiene cabecera OLE BIFF (.xls) ni coincidencias heurísticas. "
                f"Asegúrese de abrir en Excel y Guardar como -> Excel 97-2003 (.xls). Encabezado: {header_info}"
            )

        try:
            bancos_stream = _validate_is_xls(bancos_stream, 'resu-banco')
        except Exception as e:
            logging.exception("Archivo 'resu-banco' no es un .xls válido")
            return jsonify({
                "control": "ERROR",
                "codigo": 400,
                "mensaje": "Archivo 'resu-banco' debe ser un .xls válido: " + str(e),
                "estado": "-1"
            }), 400

        try:
            mayor_stream = _validate_is_xls(mayor_stream, 'resu-contable')
        except Exception as e:
            logging.exception("Archivo 'resu-contable' no es un .xls válido")
            return jsonify({
                "control": "ERROR",
                "codigo": 400,
                "mensaje": "Archivo 'resu-contable' debe ser un .xls válido: " + str(e),
                "estado": "-1"
            }), 400

        # Ahora que los archivos están validados como .xls, conectamos al SFTP y subimos
        sftp_host = os.getenv("SFTP_HOST")
        sftp_port = int(os.getenv("SFTP_PORT", 22))
        sftp_user = os.getenv("SFTP_USER")
        sftp_password = os.getenv("SFTP_PASSWORD")
        sftp_destino = str(os.getenv("SFTP_DESTINO") or "")
        if not sftp_destino.endswith('/') and sftp_destino != "":
            sftp_destino += '/'

        # Conexión al servidor SFTP
        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_user, password=sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        logging.info("Conexión SFTP establecida")

        # Subir archivos al SFTP con extensión .xls (mantener nombre claro)
        bancos_stream.seek(0)
        sftp.putfo(bancos_stream, f"{sftp_destino}bancos.xls")
        mayor_stream.seek(0)
        sftp.putfo(mayor_stream, f"{sftp_destino}mayor.xls")
        bancos_stream.seek(0)
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
            "mensaje": "Error general en el formato de los archivos ."+str(e),
            "estado": "-1"
        }), 500



def convertir_a_xlsx(stream: BytesIO):

    try:
        stream.seek(0)
        data = stream.read()

        if not data or len(data) == 0:
            raise ValueError("Archivo vacío o sin contenido")

        # Comprobar firma OLE / BIFF de formato .xls (cabecera típica: D0 CF 11 E0 A1 B1 1A E1)
        ole_sig = b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1'
        if data[:8] != ole_sig:
            # encabezado inesperado: registrar hex/snippet y devolver error claro
            header_info = _hexdump_prefix(data, length=64)
            logging.debug(f"convertir_a_xlsx: encabezado inválido para .xls: {header_info}")
            raise ValueError(
                "No es un .xls válido (cabecera distinta a OLE BIFF). "
                f"Encabezado: {header_info}"
            )

        # Si tiene firma OLE, consideramos que es un .xls válido para este flujo.
        # No intentamos convertir ni abrir aquí (evitamos dependencias como xlrd/pyxlsb).
        from io import BytesIO as _BytesIO
        out = _BytesIO(data)
        out.seek(0)
        logging.debug("convertir_a_xlsx: archivo válido .xls detectado (se devuelve sin conversión)")
        return out

    except Exception as e:
        logging.exception("convertir_a_xlsx: error en la validación simplificada")
        raise ValueError(f"Error al validar .xls: {e}")



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

