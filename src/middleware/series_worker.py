import json
import re
from pathlib import Path
from typing import Dict, Any, List, Union, Optional
import sys
sys.path.insert(0, r'D:\lab\Norilsk')
from src.database import get_db_connection

# Регулярные выражения для определения всех типов проб
PATTERNS = {
    'start_A': re.compile(r"^[A-Z]\d-(\d+)A(\d+)$"),
    'start_B': re.compile(r"^[A-Z]\d-(\d+)B(\d+)$"),
    'start_C': re.compile(r"^[A-Z]\d-(\d+)C(\d+)$"),
    'st2_A': re.compile(r"^[A-Z]\d-L(\d+)A(\d+)$"),
    'st2_B': re.compile(r"^[A-Z]\d-L(\d+)B(\d+)$"),
    'st2_C': re.compile(r"^[A-Z]\d-L(\d+)C(\d+)$"),        
    'st3_A': re.compile(r"^[A-Z]\d-L(\d+)P\1A(\d+)$"),  # \1 проверяет что номер методики одинаков
    'st3_B': re.compile(r"^[A-Z]\d-L(\d+)P\1B(\d+)$"),
    'st3_C': re.compile(r"^[A-Z]\d-L(\d+)P\1C(\d+)$"),
    'st4_A': re.compile(r"^[A-Z]\d-L(\d+)P\1F\1A(\d+)$"),
    'st4_B': re.compile(r"^[A-Z]\d-L(\d+)P\1F\1B(\d+)$"),
    'st4_D': re.compile(r"^[A-Z]\d-L(\d+)P\1F\1D(\d+)$"),
    'st4_C': re.compile(r"^[A-Z]\d-L(\d+)P\1F\1C(\d+)$"),
    'st5_A': re.compile(r"^[A-Z]\d-L(\d+)P\1F\1A(\d+)$"),
    'st5_B': re.compile(r"^[A-Z]\d-L(\d+)P\1F\1B(\d+)$"),
    'st5_C': re.compile(r"^[A-Z]\d-L(\d+)P\1F\1C(\d+)$"),        
    'st6_E': re.compile(r"^[A-Z]\d-L(\d+)P\1F\1N\1E(\d+)$"),
    'st6_G': re.compile(r"^[A-Z]\d-L(\d+)P\1F\1N\1G(\d+)$")
}

TYPE_NAMES = {
    'start_A' : 'Жидкая фаза исходной пульпы',
    'start_B' : 'Твердая фаза исходной пульпы',
    'start_C' : 'Исходная пульпа',
    'st2_A' : 'Жидкая фаза выщелачивания',
    'st2_B' : 'Твердая фаза выщелачивания',
    'st2_C' : 'Выщелачивание',
    'st3_A' : 'Жидкая фаза сульфидизации',
    'st3_B' : 'Твердая фаза сульфидизации',
    'st3_C' : 'Сульфидизация',
    'st4_A' : 'Жидкая фаза флотации',
    'st4_B' : 'Твердая фаза флотации',
    'st4_D' : 'Флотоконцетрат',
    'st4_C' : 'Флотация',
    'st5_A' : 'Твердая фаза',
    'st5_B' : 'Твердая фаза',
    'st5_C' : 'Твердая фаза',
    'st6_E' : 'ЖКК',
    'st6_G' : 'Оборотная жидкость',
}

BASE_DIR = Path(__file__).parent.parent.parent
DATA_FILE = BASE_DIR / 'data' / 'data.json'

def get_type_name_from_pattern_type(pattern_type):
    
    type_name = TYPE_NAMES[pattern_type]
    return type_name

def get_source_class_from_probe(probe:dict) -> str|None:
    
    probe_name = probe.get('name', '')        
    if probe_name:

        split_name = probe_name.split(sep='-')
        return split_name[0]

def get_probe_type(probe) -> tuple[str,int,int]|None:    
    
    probe_name = probe.get('name', '')        
    if probe_name:
    
        m = None
        n = None
        probe_type = None
        
        # Определяем тип пробы
        for pattern_name, pattern in PATTERNS.items():
            match = pattern.match(probe_name)
            if match:
                probe_type = pattern_name
                m = int(match.group(1))  # Номер методики
                n = int(match.group(2))  # Номер повторности
                break
        
        if probe_type and m and n:
            return probe_type, m, n

def get_probe_from_type(probe_type: str, method_number: int, exp_number: int) -> Optional[dict]:
    """Находит одну конкретную пробу по её типу и номерам методики/эксперимента"""
    with get_db_connection() as conn:
        # Ищем сразу по колонкам таблицы - это мгновенно
        row = conn.execute("""
            SELECT raw_data FROM probes 
            WHERE probe_type = ? AND method_number = ? AND exp_number = ?
            LIMIT 1
        """, (probe_type, method_number, exp_number)).fetchone()
        
        return json.loads(row['raw_data']) if row else None
    
def get_series_probes() -> List[Dict[str, Any]]:
    """
    Возвращает список ВСЕХ проб из серий, в которых есть 'start_C'.
    Использует подзапрос для поиска валидных серий.
    """
    with get_db_connection() as conn:
        # SQL логика: 
        # 1. Найти уникальные ключи (source_class, method, exp) для всех проб типа start_C
        # 2. Выбрать все пробы, которые подходят под эти ключи
        query = """
            SELECT raw_data FROM probes 
            WHERE (source_class, method_number, exp_number) IN (
                SELECT source_class, method_number, exp_number 
                FROM probes 
                WHERE probe_type = 'start_C'
            )
        """
        rows = conn.execute(query).fetchall()
        
        result_probes = [json.loads(row['raw_data']) for row in rows]
        print(f"Найдено проб в валидных сериях: {len(result_probes)}")
        return result_probes
    
def get_series_dicts() -> List[Dict[str, Dict[str, Any]]]:
    """
    Возвращает список серий. Каждая серия — это словарь { тип_пробы: объект_пробы }.
    Условие: в серии обязана быть проба 'start_C'.
    """
    with get_db_connection() as conn:
        # Получаем все данные из серий, где есть start_C
        # Сортируем для удобства группировки
        query = """
            SELECT source_class, method_number, exp_number, probe_type, raw_data 
            FROM probes 
            WHERE (source_class, method_number, exp_number) IN (
                SELECT source_class, method_number, exp_number 
                FROM probes 
                WHERE probe_type = 'start_C'
            )
            ORDER BY source_class, method_number, exp_number
        """
        rows = conn.execute(query).fetchall()
        
        series_groups = {}
        for row in rows:
            # Создаем уникальный ключ для группировки строк в серии
            key = (row['source_class'], row['method_number'], row['exp_number'])
            
            if key not in series_groups:
                series_groups[key] = {}
            
            # Наполняем словарь серии: { 'start_A': {...}, 'st2_B': {...} }
            series_groups[key][row['probe_type']] = json.loads(row['raw_data'])
            
        return list(series_groups.values())
    
def get_product_type(name:str):
    
    if 'A' in name[-4:]: 
        return 'Liquid'
    
    elif 'B' in name[-4:]: 
        return 'Solid'
    
    elif 'D' in name[-4:]: 
        return 'Concetrate'
    
    elif 'E' in name[-4:]: 
        return 'Dump'
    
    elif 'G' in name[-4:]: 
        return 'Recycle'
    
    else:
        return 'Undefined'
    
def get_probe_by_name(target_name):
    with get_db_connection() as conn:
        # Ищем строку, где имя совпадает с нужным
        row = conn.execute(
            "SELECT raw_data FROM probes WHERE name = ?", 
            (target_name,)
        ).fetchone()
        
        # Если нашли — парсим JSON, если нет — возвращаем None
        return json.loads(row['raw_data']) if row else None            