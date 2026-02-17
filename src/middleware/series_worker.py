import json
import re
from pathlib import Path
from typing import Dict, Any, List, Union


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


BASE_DIR = Path(__file__).parent.parent.parent
DATA_FILE = BASE_DIR / 'data' / 'data.json'

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

def get_probe_from_type(type:str, method_number: int, exp_number:int, data_file: str = str(DATA_FILE)) -> dict|None:

    # Загружаем данные
    with open(data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    probes = data.get('probes', [])
        
    if not probes:
        raise ValueError('Нет базы данных или она пуста')

    for probe in probes:
        get_probe_type_out = get_probe_type(probe)
        if get_probe_type_out is not None:
            real_type, real_method, real_exp_number = get_probe_type_out # type: ignore
        else:
            continue
        if type == real_type and method_number == real_method and exp_number == real_exp_number:
            return probe
    
    return

def get_series_probes(data_file: str = str(DATA_FILE)) -> List[Dict[str, Any]]:
    """
    Возвращает список проб, у которых в базе данных есть проба типа 'start_C' 
    из этой же серии (одинаковый source_class, номер методики и номер эксперимента)
    
    Returns:
        List[Dict]: Список проб в формате json таблицы (поле 'probes')
    """
    # Загружаем данные
    with open(data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    probes = data.get('probes', [])
    
    if not probes:
        raise ValueError('Нет базы данных или она пуста')
    
    # Группируем пробы по сериям (source_class + method_number + exp_number)
    series_groups = {}
    
    for probe in probes:
        probe_type_info = get_probe_type(probe)
        if not probe_type_info:
            continue
            
        probe_type, method_number, exp_number = probe_type_info
        source_class = get_source_class_from_probe(probe)
        
        if not source_class:
            continue
        
        # Ключ серии: source_class, method_number, exp_number
        series_key = (source_class, method_number, exp_number)
        
        if series_key not in series_groups:
            series_groups[series_key] = {
                'probes': [],
                'has_start_c': False
            }
        
        series_groups[series_key]['probes'].append(probe)
        
        # Проверяем, есть ли в этой серии проба типа 'start_C'
        if probe_type == 'start_C':
            series_groups[series_key]['has_start_c'] = True
    
    # Собираем все пробы из серий, где есть start_C
    result_probes = []
    for series_info in series_groups.values():
        if series_info['has_start_c']:
            result_probes.extend(series_info['probes'])
    print(len(result_probes))
    return result_probes

def get_series_dicts(data_file: str = str(DATA_FILE)) -> List[Dict[str, Dict]]:
    """
    Возвращает список словарей, где каждый словарь представляет серию проб.
    Ключ - тип пробы из паттернов в get_probe_type, значение - сама проба.
    Условие: в базе данных есть проба типа 'start_C' (необходимое условие существования серии)
    
    Returns:
        List[Dict[str, Dict]]: Список словарей с пробами, сгруппированными по типам
    """
    # Загружаем данные
    with open(data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    probes = data.get('probes', [])
    
    if not probes:
        raise ValueError('Нет базы данных или она пуста')
    
    # Группируем пробы по сериям
    series_dicts = []
    series_groups = {}
    
    # Сначала группируем все пробы по сериям
    for probe in probes:
        probe_type_info = get_probe_type(probe)
        if not probe_type_info:
            continue
            
        probe_type, method_number, exp_number = probe_type_info
        source_class = get_source_class_from_probe(probe)
        
        if not source_class:
            continue
        
        series_key = (source_class, method_number, exp_number)
        
        if series_key not in series_groups:
            series_groups[series_key] = {
                'probes_by_type': {},
                'has_start_c': False
            }
        
        # Добавляем пробу в словарь по её типу
        series_groups[series_key]['probes_by_type'][probe_type] = probe
        
        if probe_type == 'start_C':
            series_groups[series_key]['has_start_c'] = True
    
    # Формируем результат только для серий с start_C
    for series_info in series_groups.values():
        if series_info['has_start_c']:
            series_dicts.append(series_info['probes_by_type'])
    
    return series_dicts