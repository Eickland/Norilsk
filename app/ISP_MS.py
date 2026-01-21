import pandas as pd
import re

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
    
    # 1) Объединение проб с двумя цифрами в конце
    def merge_similar_samples(group_df):
        """Объединяет похожие пробы, усредняя значения"""
        if len(group_df) == 1:
            return group_df.iloc[0]
        
        # Усредняем все числовые столбцы
        avg_row = group_df.mean(numeric_only=True)
        avg_row['Sample'] = group_df['Sample'].iloc[0][:-1]  # Убираем последнюю цифру
        return avg_row
    
    # Создаем базовое имя для группировки (без последней цифры)
    def get_base_name(sample_name):
        """Извлекает базовое имя пробы (без последней цифры)"""
        if pd.isna(sample_name):
            return sample_name
        
        sample_str = str(sample_name)
        # Проверяем, заканчивается ли на две цифры
        if re.search(r'\d\d$', sample_str):
            return sample_str[:-1]  # Убираем последнюю цифру
        return sample_str
    
    # Применяем группировку и объединение
    df['BaseName'] = df['Sample'].apply(get_base_name)
    
    # Группируем по базовым именам и объединяем
    merged_rows = []
    for base_name, group in df.groupby('BaseName'):
        if len(group) > 1 and re.search(r'\d\d$', str(group['Sample'].iloc[0])):
            merged_row = merge_similar_samples(group)
            merged_rows.append(merged_row)
        else:
            merged_rows.extend(group.to_dict('records'))
    
    df = pd.DataFrame(merged_rows)
    df = df.drop(columns=['BaseName'], errors='ignore')
    
    # 2) Восстановление полного шифра из короткого
    def expand_sample_code(sample_name):
        """Восстанавливает полный шифр пробы из короткого"""
        if pd.isna(sample_name):
            return sample_name
        
        sample_str = str(sample_name)
        
        # Извлекаем компоненты из короткого имени
        # Формат: T2-4C1 или T2-P4A1
        pattern = r'(T\d+)-([LPFN]?)(\d+)([A-Z])(\d+)'
        match = re.match(pattern, sample_str)
        
        if not match:
            # Если не соответствует паттерну, возвращаем как есть
            return sample_str
        
        prefix = match.group(1)  # T2
        stage = match.group(2)   # стадия (может быть пусто)
        method_num = match.group(3)  # номер методики (5)
        product_type = match.group(4)  # тип продукта (A)
        repeat_num = match.group(5)  # номер повторности (2)
        
        # Если стадия не указана или это L - возвращаем как есть
        if not stage or stage == 'L':
            return sample_str
        
        # Определяем порядок стадий и какие нужно добавить
        stages_order = ['L', 'P', 'F', 'N']
        
        # Находим индекс указанной стадии
        target_index = stages_order.index(stage)
        
        # Берем все стадии от L до указанной включительно
        needed_stages = stages_order[:target_index + 1]
        
        # Формируем строку стадий с номером методики
        stages_str = ''.join([f"{s}{method_num}" for s in needed_stages])
        
        # Собираем полное имя
        full_code = f"{prefix}-{stages_str}{product_type}{repeat_num}"
        
        return full_code
    
    # Применяем восстановление шифра ко всем пробам
    df['Sample'] = df['Sample'].apply(expand_sample_code)
    
    # 3) Пересчет всех концентраций в мкг/л
    def convert_to_mcg_per_l(value, unit):
        """Конвертирует значение в мг/л"""
        if pd.isna(value):
            return value
        
        try:
            value = float(value)
        except:
            return value
        
        unit_lower = str(unit).lower()
        
        if 'мг/л' in unit_lower or 'mg/l' in unit_lower:
            return value  # 1 мг/л = 1000 мкг/л
        elif 'нг/л' in unit_lower or 'ng/l' in unit_lower:
            return value / (1000*1000)  # 1 нг/л = 0.001 мкг/л
        elif 'мкг/л' in unit_lower or 'µg/l' in unit_lower or 'mcg/l' in unit_lower:
            return value / 1000  # уже в мкг/л
        else:
            # Если единица измерения неизвестна, оставляем как есть
            print(f"Неизвестная единица измерения: {unit}. Оставляю значение без изменений.")
            return value
    
    # Применяем конвертацию ко всем столбцам с концентрациями
    for col in df.columns:
        if col != 'Sample' and col in column_units:
            unit = column_units[col]
            df[col] = df[col].apply(lambda x: convert_to_mcg_per_l(x, unit))
    
    # 4) Обработка названий столбцов (приведение к правильным названиям элементов)
    def normalize_column_name(col_name):
        """Нормализует название столбца"""
        if col_name == 'Sample':
            return col_name
        
        # Удаляем единицы измерения и лишние символы
        clean_name = re.sub(r'[\(\),;:]', '', str(col_name))
        clean_name = re.sub(r'\s*[мнг]?г/л\s*', '', clean_name, flags=re.IGNORECASE)
        clean_name = clean_name.strip()
        
        # Приводим к стандартному виду (Fe, Cr, Cu и т.д.)
        element_pattern = r'([A-Z][a-z]?)'
        match = re.match(element_pattern, clean_name)
        if match:
            return match.group(1)
        
        return clean_name
    
    # Переименовываем столбцы
    new_columns = {col: normalize_column_name(col) for col in df.columns}
    df = df.rename(columns=new_columns)
    df.rename(columns={"Sample":"name"},inplace=True)
    # 5) Сохранение или возврат результата
    if output_path:
        df.to_csv(output_path, index=False, encoding='utf-8',sep=";")
        print(f"Обработанный файл сохранен в: {output_path}")
    
    return df


# Пример использования функции
if __name__ == "__main__":
    # Пример вызова функции
    input_file = r"C:\Users\Kirill\Desktop\норникель январь 20261.csv"
    output_file = r"C:\Users\Kirill\Desktop\ИСПМС 2026.csv"
    
    try:
        result = process_metal_samples_csv(input_file, output_file)
        
        if result is not None:
            print("Обработка завершена успешно!")
            print("\nПервые 10 строк обработанных данных:")
            print(result.head(10))
            
            # Выводим информацию о единицах измерения
            print("\nВсе концентрации приведены к мкг/л")
            
    except Exception as e:
        print(f"Произошла ошибка: {e}")