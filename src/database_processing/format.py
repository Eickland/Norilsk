import json
import re

def convert_comma_numbers_in_probes(json_file_path):
    """
    Читает JSON файл с данными проб, находит строки с числами вида "123,45"
    и преобразует их в float числа.
    
    Args:
        json_file_path (str): Путь к JSON файлу
    
    Returns:
        dict: Обработанный словарь с преобразованными числами
    """
    # Паттерн для поиска чисел с запятой в качестве разделителя
    comma_number_pattern = re.compile(r'^-?\d+,\d+$')
    comma_number_pattern = re.compile(r'^-?\d+.\d+$')
    integer_pattern = re.compile(r'^-?\d+$')
            
    def convert_value(value):
        """Рекурсивно обходит структуру данных и преобразует строки с числами"""
        if isinstance(value, str):
            # Проверяем, является ли строка целым числом
            if integer_pattern.match(value):
                return int(value)
            # Проверяем, является ли строка числом с запятой
            elif comma_number_pattern.match(value):
                # Заменяем запятую на точку и преобразуем в float
                return float(value.replace(',', '.'))
        if isinstance(value, list):
            return [convert_item(v) for v in value]
        elif isinstance(value, dict):
            return {k: convert_item(v) for k, v in value.items()}

        return value
    
    # Псевдоним для обратной совместимости
    convert_item = convert_value
    
    try:
        # Читаем JSON файл
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # Обрабатываем данные
        processed_data = convert_item(data)
        
        return processed_data
        
    except FileNotFoundError:
        print(f"Файл {json_file_path} не найден")
        return None
    except json.JSONDecodeError as e:
        print(f"Ошибка при чтении JSON: {e}")
        return None

def convert_and_save_comma_numbers(json_file_path):
    """
    Читает JSON файл, преобразует числа и сохраняет обратно в тот же файл
    """
    data = convert_comma_numbers_in_probes(json_file_path)
    
    if data:
        with open(json_file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
        print(f"Файл {json_file_path} успешно обновлен")
        return True
    return False

# Использование
convert_and_save_comma_numbers("data/data.json")