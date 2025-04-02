import pandas as pd
import re
from loguru import logger
from typing import Dict, Tuple, Optional

logger.add("../logs/file_{time}.log", rotation="100 MB")

def parse_age_group(age_str: str) -> Optional[Tuple[int, int]]:
    """
    Парсит строку возрастной группы и возвращает кортеж (нижняя_граница, верхняя_граница).
    Примеры: "0 - 4" -> (0, 4), "85 и более" -> (85, 120).
    Возвращает None, если формат не распознан.
    """
    if not isinstance(age_str, str):
        logger.warning(f"Некорректный тип для парсинга возрастной группы: {type(age_str)}, значение: {age_str}")
        return None

    age_str = age_str.strip()

    # Проверка на "X и более"
    match_older = re.match(r'(\d+)\s*(и более|и старше|\+)', age_str, re.IGNORECASE)
    if match_older:
        lower = int(match_older.group(1))
        upper = 120  # Условная верхняя граница для "и более" (120 лет вперед)
        logger.trace(f"Распарсена группа 'и более': '{age_str}' -> ({lower}, {upper})")
        return lower, upper

    # Проверка на "X - Y"
    match_range = re.match(r'(\d+)\s*[-–—]\s*(\d+)', age_str)
    if match_range:
        lower = int(match_range.group(1))
        upper = int(match_range.group(2))
        logger.trace(f"Распарсена группа 'диапазон': '{age_str}' -> ({lower}, {upper})")
        return lower, upper

    # Проверка на одиночный возраст "X лет" (маловероятно, но на всякий случай)
    match_single = re.match(r'(\d+)\s*(лет?)?$', age_str)
    if match_single:
        age = int(match_single.group(1))
        logger.trace(f"Распарсена группа 'одиночный возраст': '{age_str}' -> ({age}, {age})")
        return age, age  # Нижняя и верхняя граница совпадают

    logger.warning(f"Не удалось распознать формат возрастной группы: '{age_str}'")
    return None


def calculate_year_range(lower_age: int, upper_age: int, base_year: int = 2010) -> str:
    """
    Вычисляет годовой промежуток по формуле из ТЗ.
    Формула:
    верх.гран.года = base_year - ниж.гран.возраста
    ниж.гран.года = base_year - верх.гран.возраста
    """
    upper_year = base_year - lower_age
    lower_year = base_year - upper_age
    year_range_str = f"({lower_year}, {upper_year})"
    logger.trace(
        f"Рассчитан годовой диапазон: возраст ({lower_age}, {upper_age}), год {base_year} -> '{year_range_str}'")
    return year_range_str


# --- Основные функции парсинга ---

def read_nationality_data(filepath: str) -> Dict[str, Tuple[int, Dict[str, int]]]:
    """
    Читает данные о национальностях и общем населении из файла pub-04-04.xlsx.
    (Версия 4: Четкие состояния парсинга)
    """
    logger.info(f"Начало чтения файла национальностей: {filepath}")
    data = {}
    current_region = None
    total_population = None
    last_federal_district = None

    # Состояния парсера
    STATE_SEARCHING_FO = 0
    STATE_SEARCHING_REGION = 1
    STATE_SEARCHING_NATION_MARKER = 2
    STATE_READING_NATIONS = 3

    state = STATE_SEARCHING_FO

    try:
        df = pd.read_excel(filepath, header=None, sheet_name=0, dtype=str)
        logger.debug(f"Прочитано строк из {filepath}: {len(df)}")

        for index, row in df.iterrows():
            # Чистим данные строки сразу
            row_values = [str(v).replace('\n', ' ').strip() if pd.notna(v) else None for v in row.tolist()]

            col_a = row_values[0]
            col_b = row_values[1]
            col_c = row_values[2]
            col_d = row_values[3]

            # --- Логика состояний ---

            # --- Всегда ищем ФО как высший приоритет для сброса ---
            is_federal_district_row = (
                    (col_b and "федеральный округ" in col_b.lower()) or
                    (col_c and "федеральный округ" in col_c.lower())
            )
            if is_federal_district_row:
                fo_name = f"{col_b or ''} {col_c or ''}".strip()
                logger.info(f"Найден маркер Федерального Округа: '{fo_name}'")
                last_federal_district = fo_name
                state = STATE_SEARCHING_REGION  # Переходим к поиску региона
                current_region = None
                continue  # Обработали строку, идем дальше

            # --- Поиск Региона ---
            if state == STATE_SEARCHING_REGION:
                # Условия: номер в A, B пусто, C - название (не ФО, не маркер), D - число
                is_potential_region = (
                        col_a and col_a.split('.')[0].isdigit() and
                        col_b is None and  # Важное условие!
                        col_c and
                        col_d and col_d.isdigit() and
                        "федеральный округ" not in col_c.lower() and
                        "указавшие национальную" not in col_c.lower() and
                        # Можно добавить проверку, что это не похоже на национальность?
                        len(col_c) > 3  # Отсекаем слишком короткие названия
                )
                if is_potential_region:
                    region_name_cleaned = col_c.replace('\n', ' ').strip()  # Чистим имя региона
                    if region_name_cleaned not in data:
                        current_region = region_name_cleaned
                        total_population = int(col_d)
                        data[current_region] = (total_population, {})
                        logger.debug(
                            f"Найден регион: '{current_region}' в ФО '{last_federal_district}' с населением {total_population}")
                        state = STATE_SEARCHING_NATION_MARKER  # Переходим к поиску маркера
                    else:
                        logger.warning(f"Повторное обнаружение региона '{region_name_cleaned}'. Игнорируется.")
                    continue  # Обработали

            # --- Поиск Маркера Национальностей ---
            elif state == STATE_SEARCHING_NATION_MARKER:
                if current_region and col_c and "указавшие национальную" in col_c.lower():
                    logger.trace(f"Найден маркер начала национальностей для '{current_region}'")
                    state = STATE_READING_NATIONS  # Начинаем читать нации
                    continue  # Обработали

                # Добавим проверку на пропуск маркера: если началась строка, похожая на нацию
                is_like_nation = (
                        col_a and col_a.split('.')[0].isdigit() and col_c and col_d and col_d.isdigit()
                )
                if is_like_nation:
                    logger.warning(
                        f"Обнаружена строка '{col_c}', похожая на национальность, до маркера 'Указавшие...' для региона '{current_region}'. Проверьте структуру файла.")
                    # Можно либо проигнорировать, либо сразу перейти к чтению наций, если уверены
                    # state = STATE_READING_NATIONS

            # --- Чтение Национальностей ---
            elif state == STATE_READING_NATIONS:
                # Условия: номер в A, C - название, D - число
                is_potential_nationality = (
                        current_region and  # Регион должен быть определен
                        col_a and col_a.split('.')[0].isdigit() and
                        col_c and
                        col_d and col_d.isdigit() and
                        col_c.lower() not in ["указавшие национальную принадлежность", "национальность не указана",
                                              "итого", "лица,", "не указавшие национальную", "принадлежность",
                                              "(не перечисленные выше)"]
                )
                if is_potential_nationality:
                    nation_name = col_c.replace('\n', ' ').strip()  # Чистим имя нации
                    nation_pop = int(col_d)
                    if nation_pop > 0:
                        if current_region in data:
                            data[current_region][1][nation_name] = nation_pop
                            logger.trace(f"  Добавлена нация: '{nation_name}', население: {nation_pop}")
                        else:
                            logger.error(
                                f"Критическая ошибка: Попытка добавить нацию '{nation_name}', но текущий регион '{current_region}' отсутствует в data!")
                    else:
                        logger.trace(f"  Пропущена нация с нулевым населением: '{nation_name}'")
                    continue  # Обработали строку как нацию

                # Проверка на конец блока национальностей (например, пустая строка или конец данных)
                # Или если строка уже не похожа на национальность
                elif col_a is None and col_c is None and col_d is None:
                    logger.trace(
                        f"Предположительно конец блока национальностей для '{current_region}' по пустой строке.")
                    state = STATE_SEARCHING_REGION  # Возвращаемся к поиску следующего региона в текущем ФО
                    current_region = None  # Сбрасываем текущий регион
                    continue

    except FileNotFoundError:
        logger.error(f"Файл не найден: {filepath}")
        return {}
    except Exception as e:
        logger.exception(f"Ошибка при чтении или обработке файла {filepath}: {e}")
        return {}

    logger.success(f"Завершено чтение файла национальностей: {filepath}. Найдено регионов: {len(data)}")
    if abs(len(data) - 85) > 10:  # Сверяем с ожидаемым числом регионов РФ (~85-89)
        logger.warning(
            f"Количество найденных регионов ({len(data)}) значительно отличается от ожидаемого (~85). Проверьте логику парсера или структуру файла.")
    return data


def read_age_sex_data(filepath: str) -> Dict[str, Dict[str, Tuple[int, int]]]:
    """
    Читает данные о возрастных группах и поле из файла pub-02-02.xlsx.

    Возвращает словарь:
    {
        "Название Региона": {
            "0 - 4": (Мужчины, Женщины),
            "5 - 9": (Мужчины, Женщины),
            ...
        },
        ...
    }
    """
    logger.info(f"Начало чтения файла по возрасту и полу: {filepath}")
    data = {}
    current_region = None
    reading_ages = False

    try:
        # Читаем все как строки, без заголовка
        df = pd.read_excel(filepath, header=None, sheet_name=0, dtype=str)
        logger.debug(f"Прочитано строк из {filepath}: {len(df)}")

        for index, row in df.iterrows():
            row_values = [str(v).replace('\n', ' ').strip() if pd.notna(v) else None for v in row.tolist()]

            # Ищем строку с названием региона. Эвристика:
            # - Не пустая строка в столбце D (индекс 3)
            # - Не содержит цифр в начале
            # - Не содержит "в том числе в возрасте"
            # - Не является строкой с возрастной группой (проверяем ниже)
            region_candidate = row_values[3]  # Предполагаем, что регион в столбце D

            if region_candidate and \
                    not any(c.isdigit() for c in region_candidate.split()[0]) and \
                    "в том числе" not in region_candidate.lower() and \
                    "возрасте" not in region_candidate.lower() and \
                    not parse_age_group(region_candidate) and \
                    len(region_candidate) > 5:  # Отсекаем короткие строки вроде "лет:"

                # Дополнительно проверим, что следующая строка похожа на "Городское и сельское"
                if index + 1 < len(df):
                    next_row_values = [str(v).strip() if pd.notna(v) else None for v in df.iloc[index + 1].tolist()]
                    if next_row_values[3] and "городское и сельское" in next_row_values[3].lower():
                        current_region = region_candidate.replace('\n', ' ').strip()
                        data[current_region] = {}
                        reading_ages = True  # Начинаем читать возрасты для этого региона
                        logger.debug(f"Найден регион: '{current_region}'")
                        continue  # Переходим к следующей строке (пропускаем "Городское и сельское")

            # Если мы в режиме чтения возрастов для текущего региона
            if reading_ages and current_region:
                age_group_str = row_values[3]  # Возрастная группа в столбце D (индекс 3)
                male_str = row_values[8]  # Мужчины в столбце I (индекс 8)
                female_str = row_values[9]  # Женщины в столбце J (индекс 9)

                # Проверяем, что есть возрастная группа и числовые данные для мужчин и женщин
                if age_group_str and male_str and female_str and \
                        male_str.isdigit() and female_str.isdigit():

                    # Пробуем распарсить возрастную группу
                    age_bounds = parse_age_group(age_group_str)
                    if age_bounds:  # Если успешно распознали
                        males = int(male_str)
                        females = int(female_str)
                        data[current_region][age_group_str] = (males, females)
                        logger.trace(f"  Добавлен возраст: '{age_group_str}', М: {males}, Ж: {females}")
                    # else: пропущено из-за parse_age_group warning

                # Эвристика для окончания блока возрастов:
                # - Пустая строка (или строка без данных в нужных колонках)
                # - Начало нового региона (определяется выше)
                elif age_group_str is None and male_str is None and female_str is None:
                    if current_region and data[current_region]:  # Если были данные для региона
                        logger.debug(
                            f"Завершено чтение возрастов для региона '{current_region}'. Найдено групп: {len(data[current_region])}")
                        reading_ages = False  # Завершили чтение для этого региона
                        current_region = None  # Сбрасываем текущий регион

    except FileNotFoundError:
        logger.error(f"Файл не найден: {filepath}")
        return {}
    except Exception as e:
        logger.exception(f"Ошибка при чтении или обработке файла {filepath}: {e}")
        return {}

    logger.success(f"Завершено чтение файла по возрасту и полу: {filepath}. Найдено регионов: {len(data)}")
    return data


# --- Блок для тестирования парсера ---
if __name__ == "__main__":
    import os
    from config import BASE_DIR

    file_04_04 = os.path.join(BASE_DIR, 'excel_data', 'pub-04-04.xlsx')
    file_02_02 = os.path.join(BASE_DIR, 'excel_data', 'pub-02-02.xlsx')

    logger.info("--- Тестирование read_nationality_data ---")
    nationality_data = read_nationality_data(file_04_04)
    if nationality_data:
        logger.info(f"Пример данных из read_nationality_data (первый регион):")
        first_region = list(nationality_data.keys())[0]
        total_pop, nations = nationality_data[first_region]
        logger.info(f"Регион: {first_region}, Всего: {total_pop}")
        logger.info(f"Первые 5 национальностей: {dict(list(nations.items())[:5])}")
    else:
        logger.warning("read_nationality_data не вернул данных.")

    logger.info("\n--- Тестирование read_age_sex_data ---")
    age_sex_data = read_age_sex_data(file_02_02)
    if age_sex_data:
        logger.info(f"Пример данных из read_age_sex_data (первый регион):")
        first_region_age = list(age_sex_data.keys())[0]
        ages = age_sex_data[first_region_age]
        logger.info(f"Регион: {first_region_age}")
        logger.info(f"Первые 3 возрастные группы: {dict(list(ages.items())[:3])}")
    else:
        logger.warning("read_age_sex_data не вернул данных.")

    logger.info("\n--- Тестирование вспомогательных функций ---")
    test_age_groups = ["0 - 4", "15-19", "85 и более", "75 и старше", " 60 - 64 ", "Некорректно", "5", "100+"]
    for group in test_age_groups:
        bounds = parse_age_group(group)
        if bounds:
            year_range = calculate_year_range(bounds[0], bounds[1])
            logger.info(f"'{group}' -> Возраст: {bounds}, Годы: {year_range}")
        else:
            logger.info(f"'{group}' -> Не распознано")

    logger.info("\n--- Проверка согласованности регионов ---")
    regions_nationality = set(nationality_data.keys())
    regions_age_sex = set(age_sex_data.keys())

    logger.info(f"Регионов в nationality_data: {len(regions_nationality)}")
    logger.info(f"Регионов в age_sex_data: {len(regions_age_sex)}")

    common_regions = regions_nationality.intersection(regions_age_sex)
    logger.info(f"Общих регионов (есть в обоих файлах): {len(common_regions)}")

    only_in_nationality = regions_nationality - regions_age_sex
    if only_in_nationality:
        logger.warning(
            f"Регионы ТОЛЬКО в nationality_data ({len(only_in_nationality)}): {sorted(list(only_in_nationality))[:10]}...")  # Показываем первые 10

    only_in_age_sex = regions_age_sex - regions_nationality
    if only_in_age_sex:
        logger.warning(
            f"Регионы ТОЛЬКО в age_sex_data ({len(only_in_age_sex)}): {sorted(list(only_in_age_sex))[:10]}...")  # Показываем первые 10