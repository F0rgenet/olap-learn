import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Корень проекта
DB_PATH = os.path.join(BASE_DIR, "database", "population_db.db")  # Путь к базе данных
EXCEL_DIR = os.path.join(BASE_DIR, "excel_data")  # Папка с Excel-файлами
