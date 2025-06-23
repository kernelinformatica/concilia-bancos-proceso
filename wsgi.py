from app import AppConciliacionBancos

# Creamos la instancia de la app Flask
concilia_app = AppConciliacionBancos()
app = concilia_app.app  # Esto es lo que Gunicorn necesita
