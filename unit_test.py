import requests

url = "http://localhost:6050/api/conciliar_datos"
files = {
    'resu-banco': open('C:/temp/conciliaciones/bancos.xls', 'rb'),
    'resu-contable': open('C:/temp/conciliaciones/mayor.xls', 'rb')
}
data = {
    'empresa': '1',
    'usuario': '1',
    'tipoConciliacion': '1',
    'cuentaConcilia': '11010211'
}

response = requests.post(url, files=files, data=data)
print(response.status_code)
print(response.json())
