import json
import re
from typing import Any, Dict, List
from pathlib import Path


def convert_comma_decimal_string(value: Any) -> Any:
    """
    Преобразует строки вида "100,100" в числа с плавающей точкой.
    Если значение не является строкой или не соответствует формату, возвращает исходное значение.
    """
    if not isinstance(value, str):
        return value
    
    # Проверяем, соответствует ли строка формату чисел с запятой в качестве разделителя
    # Регулярное выражение для чисел типа "123,456" или "123,45"
    pattern = r'^-?\d{1,3}(?:\.?\d{3})*,\d+$'
    
    if re.match(pattern, value):
        # Заменяем запятую на точку и преобразуем в float
        try:
            # Удаляем возможные точки в качестве разделителей тысяч
            cleaned = value.replace('.', '').replace(',', '.')
            return float(cleaned)
        except (ValueError, TypeError):
            return value
    else:
        return value


def process_probe(probe: Dict[str, Any]) -> Dict[str, Any]:
    """Обрабатывает одну пробу, преобразуя все строки с запятыми в числа."""
    processed_probe = {}
    
    for key, value in probe.items():
        if isinstance(value, list):
            # Рекурсивно обрабатываем вложенные списки
            processed_probe[key] = [convert_comma_decimal_string(item) if isinstance(item, str) else item 
                                   for item in value]
        elif isinstance(value, dict):
            # Рекурсивно обрабатываем вложенные словари
            processed_probe[key] = process_probe(value)
        else:
            # Преобразуем одиночные значения
            processed_probe[key] = convert_comma_decimal_string(value)
    
    return processed_probe


def process_json_file(input_file: str, output_file: str|None = None) -> None:
    """
    Основная функция для обработки JSON файла.
    
    Args:
        input_file: Путь к входному JSON файлу
        output_file: Путь к выходному JSON файлу (если None, заменяет исходный)
    """
    # Проверяем существование файла
    input_path = Path(input_file)
    if not input_path.exists():
        print(f"Ошибка: Файл {input_file} не найден!")
        return
    
    # Загружаем JSON данные
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Ошибка при чтении JSON файла: {e}")
        return
    
    # Проверяем структуру данных
    if isinstance(data, dict):
        # Если данные - словарь, ищем в них пробы
        if 'probes' in data:
            print(f"Обрабатываю {len(data['probes'])} проб...")
            data['probes'] = [process_probe(probe) for probe in data['probes']]
        else:
            # Проверяем все ключи на наличие вложенных структур с пробами
            for key in data:
                if isinstance(data[key], list) and len(data[key]) > 0:
                    if isinstance(data[key][0], dict) and 'id' in data[key][0]:
                        print(f"Обрабатываю пробы в ключе '{key}'...")
                        data[key] = [process_probe(probe) for probe in data[key]]
    
    elif isinstance(data, list):
        # Если данные - список, проверяем, что это список проб
        if len(data) > 0 and isinstance(data[0], dict) and 'id' in data[0]:
            print(f"Обрабатываю {len(data)} проб...")
            data = [process_probe(probe) for probe in data]
    
    # Определяем путь для сохранения
    if output_file is None:
        # Создаем резервную копию исходного файла
        backup_file = input_path.with_suffix('.json.backup')
        input_path.rename(backup_file)
        print(f"Создана резервная копия: {backup_file}")
        output_file = input_file
    
    # Сохраняем обработанные данные
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Данные успешно сохранены в: {output_file}")
        
        # Выводим пример преобразованных данных
        print("\nПример преобразованных значений:")
        if isinstance(data, dict) and 'probes' in data and len(data['probes']) > 0:
            sample_probe = data['probes'][0]
            for key, value in sample_probe.items():
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    print(f"  {key}: {value} (тип: {type(value).__name__})")
                elif isinstance(value, str) and ',' in value and re.match(r'\d+,\d+', value):
                    print(f"  {key}: {value} (осталось строкой)")
        
    except Exception as e:
        print(f"Ошибка при сохранении файла: {e}")


def main():
    """Основная функция для запуска скрипта."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Преобразование строк с запятыми в числа в JSON базе данных проб.')
    parser.add_argument('input_file', help='Путь к входному JSON файлу')
    parser.add_argument('-o', '--output', help='Путь к выходному JSON файлу (опционально)')
    
    args = parser.parse_args()
    
    process_json_file(args.input_file, args.output)


if __name__ == "__main__":
    # Если нужно просто запустить скрипт с указанием файла, можно использовать:
    # main()
    
    # Или задать файл напрямую:
    input_filename = "data/data.json"  # Укажите имя вашего файла
    
    process_json_file(input_filename)