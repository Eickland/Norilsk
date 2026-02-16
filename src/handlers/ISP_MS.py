import pandas as pd
import re

from middleware.raw_data_processing import expand_sample_code, get_base_name, merge_similar_samples, convert_to_mcg_per_l

def process_metal_samples_csv(file_path, output_path=None):
    """
    Обрабатывает CSV файл с данными проб металлов.
    
    Параметры:
    file_path: путь к входному CSV файлу
    output_path: путь для сохранения обработанного файла (если None, возвращает DataFrame)
    
    Возвращает:
    Обработанный DataFrame или сохраняет в файл
    """
    
    # 0) Чтение файла с извлечением единиц измерения из второй строки
    try:
        # Читаем первую строку как заголовок
        df = pd.read_csv(file_path, encoding='utf-8', header=None, sep=';')
        if df.shape[0] < 2:
            raise ValueError("Файл должен содержать как минимум 2 строки")
        
        # Извлекаем единицы измерения из второй строки
        units = df.iloc[1].tolist()
        units[0] = 'Sample'  # Первый столбец - имена проб
        
        # Читаем файл снова, пропуская вторую строку с единицами измерения
        df = pd.read_csv(file_path, encoding='utf-8', skiprows=[1], sep=';')
        df.rename(columns={f'{df.columns[0]}': 'name'}, inplace=True)
        # Запоминаем оригинальные названия столбцов
        original_columns = df.columns.tolist()
        
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return None
    
    # Сохраняем единицы измерения для каждого столбца
    column_units = {}
    for i, col in enumerate(original_columns):
        if i < len(units):
            column_units[col] = units[i]
        else:
            column_units[col] = 'мкг/л'  # значение по умолчанию
    
    # Применяем группировку и объединение
    df['BaseName'] = df['name'].apply(get_base_name)
    
    # Группируем по базовым именам и объединяем
    merged_rows = []
    for base_name, group in df.groupby('BaseName'):
        if len(group) > 1 and re.search(r'\d\d$', str(group['name'].iloc[0])):
            merged_row = merge_similar_samples(group)
            merged_rows.append(merged_row)
        else:
            merged_rows.extend(group.to_dict('records'))
    
    df = pd.DataFrame(merged_rows)
    df = df.drop(columns=['BaseName'], errors='ignore')
    
    # Восстановление полного шифра из короткого
    df['name'] = df['name'].apply(expand_sample_code)
    
    # Пересчет всех концентраций в мкг/л
    for col in df.columns:
        if col != 'name' and col in column_units:
            unit = column_units[col]
            df[col] = df[col].apply(lambda x: convert_to_mcg_per_l(x, unit))
    
    # Обработка названий столбцов (приведение к правильным названиям элементов)
    def normalize_column_name(col_name):
        """Нормализует название столбца"""
        if col_name == 'name':
            return col_name
        
        # Удаляем единицы измерения и лишние символы
        clean_name = re.sub(r'[\(\),;:]', '', str(col_name))
        clean_name = re.sub(r'\s*[мнг]?г/л\s*', '', clean_name, flags=re.IGNORECASE)
        clean_name = clean_name.strip()
        
        # Приводим к стандартному виду (Fe, Cr, Cu и т.д.)
        element_pattern = r'([A-Z][a-z]?)'
        match = re.match(element_pattern, clean_name)
        if match:
            return match.group(1) + '_MS'
        
        return clean_name
    
    # Переименовываем столбцы
    new_columns = {col: normalize_column_name(col) for col in df.columns}
    df = df.rename(columns=new_columns)
    
    # Сохранение или возврат результата
    if output_path:
        df.to_csv(output_path, index=False, encoding='utf-8', sep=";")
        print(f"Обработанный файл сохранен в: {output_path}")
    
    return df

# Пример использования функции
if __name__ == "__main__":
    # Пример вызова функции
    input_file = r"C:\Users\Kirill\Desktop\норникель январь 20261.csv"
    output_file = r"C:\Users\Kirill\Desktop\ИСПМС 2026.csv"
    
    try:
        result = process_metal_samples_csv(input_file, output_file)
            
    except Exception as e:
        print(f"Произошла ошибка: {e}")