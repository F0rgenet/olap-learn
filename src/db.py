import sqlite3
from loguru import logger
from config import DB_PATH

logger.add("../logs/file_{time}.log", rotation="100 MB")

def get_connection():
    """Создаёт подключение к базе данных."""
    logger.debug(f"Попытка подключения к БД: {DB_PATH}")
    try:
        conn = sqlite3.connect(DB_PATH)
        logger.success(f"Успешное подключение к БД: {DB_PATH}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Ошибка подключения к SQLite: {e}")
        raise

def _get_or_insert_id(conn, table_name: str, value: str) -> int:
    """
    Вспомогательная функция для получения ID из справочной таблицы.
    Вставляет значение, если его нет, и возвращает ID.
    Предполагает, что в таблице есть столбцы 'id' (PK) и 'val' (UNIQUE).
    """
    if value is None:
        logger.warning(f"Попытка вставить None в таблицу {table_name}, пропуск.")
        return None # Или обработать иначе

    cursor = conn.cursor()
    try:
        # Пытаемся вставить, игнорируя ошибку, если значение уже есть (из-за UNIQUE)
        cursor.execute(f"INSERT OR IGNORE INTO {table_name} (val) VALUES (?)", (value,))
        # Получаем ID существующей или только что вставленной записи
        cursor.execute(f"SELECT id FROM {table_name} WHERE val = ?", (value,))
        result = cursor.fetchone()
        if result:
            logger.trace(f"Получен ID {result[0]} для '{value}' в таблице '{table_name}'.")
            return result[0]
        else:
            # Эта ситуация не должна возникать при правильной логике INSERT OR IGNORE и NOT NULL
            logger.error(f"Не удалось получить ID для значения '{value}' в таблице '{table_name}' после INSERT OR IGNORE.")
            raise ValueError(f"Не удалось получить ID для '{value}' в '{table_name}'")
    except sqlite3.Error as e:
        logger.error(f"Ошибка SQLite при работе с таблицей {table_name} для значения '{value}': {e}")
        raise

def get_or_insert_gender(conn, gender_name: str) -> int:
    """Возвращает ID для указанного названия пола."""
    return _get_or_insert_id(conn, "gender", gender_name)

def get_or_insert_nation(conn, nation_name: str) -> int:
    """Возвращает ID для указанного названия национальности."""
    return _get_or_insert_id(conn, "nation", nation_name)

def get_or_insert_territory(conn, territory_name: str) -> int:
    """Возвращает ID для указанного названия региона."""
    # Убираем лишние пробелы, которые могут появиться при парсинге Excel
    territory_name_cleaned = territory_name.strip() if territory_name else None
    return _get_or_insert_id(conn, "territory", territory_name_cleaned)

def get_or_insert_year(conn, year_range_str: str) -> int:
    """Возвращает ID для указанной годовой группы."""
    return _get_or_insert_id(conn, "year", year_range_str)

def get_gender_id(conn, gender_name: str) -> int:
    """Получает ID для гендера (предполагается, что он уже есть)."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM gender WHERE val = ?", (gender_name,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            logger.error(f"Гендер '{gender_name}' не найден в таблице gender.")
            raise ValueError(f"Гендер '{gender_name}' не найден.")
    except sqlite3.Error as e:
        logger.error(f"Ошибка SQLite при поиске гендера '{gender_name}': {e}")
        raise

def insert_population_fact(conn, year_id: int, nation_id: int, territory_id: int, gender_id: int, count: int):
    """Вставляет запись в таблицу population_fact."""
    # Убедись, что структура population_fact верна!
    # Предполагаем столбцы: year_id, nation_id, territory_id, gender_id, count
    # Если столбцы называются id_year, id_nation и т.д. - нужно исправить SQL ниже.
    # Если там тоже просто val1, val2, val3... - это будет сложнее поддерживать.
    # Уточни структуру population_fact, если она отличается от стандартной.

    sql = """
        INSERT INTO population_fact (year_id, nation_id, territory_id, gender_id, count)
        VALUES (?, ?, ?, ?, ?)
    """
    # Добавляем ON CONFLICT для обработки дубликатов (если PK настроен)
    # Например, чтобы перезаписывать:
    # sql += " ON CONFLICT(year_id, nation_id, territory_id, gender_id) DO UPDATE SET count=excluded.count"
    # Или чтобы игнорировать:
    sql += " ON CONFLICT(year_id, nation_id, territory_id, gender_id) DO NOTHING"

    cursor = conn.cursor()
    try:
        cursor.execute(sql, (year_id, nation_id, territory_id, gender_id, count))
        logger.trace(f"Вставлена запись: year={year_id}, nation={nation_id}, territory={territory_id}, gender={gender_id}, count={count}")
    except sqlite3.IntegrityError as e:
        # Эта ошибка теперь должна обрабатываться через ON CONFLICT, но оставим лог на всякий случай
        logger.warning(f"Конфликт целостности при вставке (обработано через ON CONFLICT?): {e}")
        logger.warning(f"Данные: year={year_id}, nation={nation_id}, territory={territory_id}, gender={gender_id}, count={count}")
    except sqlite3.Error as e:
        logger.error(f"Ошибка SQLite при вставке в population_fact: {e}")
        logger.error(f"Данные: year={year_id}, nation={nation_id}, territory={territory_id}, gender={gender_id}, count={count}")
        raise

def populate_initial_data(conn):
    """Заполняет таблицу gender начальными значениями, если их нет."""
    logger.info("Проверка/заполнение таблицы gender...")
    try:
        male_id = get_or_insert_gender(conn, "Мужчины")
        female_id = get_or_insert_gender(conn, "Женщины")
        logger.info(f"ID для 'Мужчины': {male_id}, ID для 'Женщины': {female_id}")
    except Exception as e:
        logger.exception("Не удалось заполнить начальные данные для gender.")
        # Решаем, критично ли это для продолжения работы

# --- Функция для отображения таблиц (оставим для отладки) ---
def show_tables(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    logger.info(f"Таблицы в базе: {tables}")
# --- ---

if __name__ == "__main__":
    logger.info("Запуск модуля db.py напрямую...")
    connection = None # Инициализируем переменную
    try:
        connection = get_connection()
        show_tables(connection) # Показываем таблицы
        populate_initial_data(connection) # Добавляем гендеры
        connection.commit() # Сохраняем изменения (вставку гендеров)
        logger.success("Начальные данные gender проверены/добавлены и сохранены.")
    except Exception as e:
        # Логируем ошибку верхнего уровня при инициализации
        logger.exception(f"Произошла ошибка при инициализации БД в db.py: {e}")
    finally:
        if connection:
            connection.close()
            logger.info("Соединение с БД закрыто.")