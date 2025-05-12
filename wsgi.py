import sys
from app import AppFacturacionElectronica


if __name__ == "__main__":
    sys.argv.append("--timeout")
    sys.argv.append('300')
    AppFacturacionElectronica.run(debug=True, host='0.0.0.0', port=6001)
