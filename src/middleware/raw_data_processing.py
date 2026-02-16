import re
import pandas as pd
from typing import Optional
from pathlib import Path

def expand_sample_code(sample_name):
    """Восстанавливает полный шифр пробы из короткого используя паттерны из series_worker"""
    if pd.isna(sample_name):
        return sample_name
    
    sample_str = str(sample_name)
    
    # Извлекаем компоненты из короткого имени
    pattern = r'([A-Z]\d+)-([LPFN]?)(\d+)([A-Z])(\d+)'
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

def get_base_name(sample_name):
    """Извлекает базовое имя пробы (без последней цифры)"""
    if pd.isna(sample_name):
        return sample_name
    
    sample_str = str(sample_name)
    # Проверяем, заканчивается ли на две цифры
    if re.search(r'\d\d$', sample_str):
        return sample_str[:-1]  # Убираем последнюю цифру
    return sample_str

def merge_similar_samples(group_df):
    """Объединяет похожие пробы, усредняя значения"""
    if len(group_df) == 1:
        return group_df.iloc[0]
    
    # Усредняем все числовые столбцы
    avg_row = group_df.mean(numeric_only=True)
    avg_row['name'] = group_df['name'].iloc[0][:-1]  # Убираем последнюю цифру
    return avg_row

def clean_value_icp_aes(val):
    """Очищает значения для ИСП-АЭС данных"""
    if pd.isna(val):
        return 0
    val_str = str(val).strip()
    
    # Если содержит 'некал' - возвращаем NaN для последующего удаления
    if 'некал' in val_str.lower():
        return pd.NA
    
    # Если содержит 'uv' или 'ox' - возвращаем 0
    if 'uv' in val_str.lower():
        return 0
    
    # Если содержит 'x' - удаляем 'x' и оставляем число
    if 'ox' in val_str.lower():
        cleaned = val_str.lower().replace('ox', '').strip()
        try:
            return float(cleaned)
        except:
            raise ValueError('Ошибка удаления ox')
               
    # Если содержит 'x' - удаляем 'x' и оставляем число
    if 'x' in val_str.lower():
        cleaned = val_str.lower().replace('x', '').strip()
        try:
            return float(cleaned)
        except:
            print(cleaned)
            raise ValueError('Ошибка удаления x')
    
    # Пытаемся преобразовать в число
    try:
        val_str = val_str.replace(',', '.')
        return float(val_str)
    except:
        return 0

def convert_to_mcg_per_l(value, unit):
    """Конвертирует значение в мкг/л для ИСП-МС данных"""
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