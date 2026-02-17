import json
import os
import re
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass
from middleware.series_worker import get_probe_type, get_source_class_from_probe, PATTERNS

# Конфигурация
BASE_DIR = Path(__file__).parent.parent
DATA_FILE = BASE_DIR / 'data' / 'data.json'

FIELD_VALIDATION_CONFIG = {
    'start_A': {
        'V (ml)': {'min': 50, 'max': 200, 'warning': 'Объем вне нормы'}
    },
    'start_B': {
        'sample_mass': {'min': 10, 'max': 100, 'warning': 'Масса вне оптимального диапазона'},
    },
    'start_C': {
        'sample_mass': {'min': 21, 'max': 200, 'warning': 'Масса вне оптимального диапазона'},
        'V (ml)': {'min': 50, 'max': 200, 'warning': 'Объем вне нормы'},
    },
}

@dataclass
class ProbeInfo:
    """Информация о пробе"""
    probe: Dict[str, Any]
    probe_type: str
    method_number: int
    exp_number: int
    source_class: str
    warnings: List[str]

@dataclass
class SeriesInfo:
    """Информация о серии проб"""
    series_key: Tuple[str, int, int]
    probes_by_type: Dict[str, ProbeInfo]
    missing_types: List[str]
    all_types: List[str]
    has_warnings: bool

def load_data() -> Dict[str, Any]:
    """Загрузка данных из файла"""
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        raise ValueError('Нет данных')

def validate_probe_fields(probe: Dict[str, Any], probe_type: str) -> List[str]:
    """Валидация полей пробы согласно конфигурации"""
    warnings = []
    
    if probe_type not in FIELD_VALIDATION_CONFIG:
        return warnings
    
    config = FIELD_VALIDATION_CONFIG[probe_type]
    
    for field, rules in config.items():
        if field in probe:
            value = probe[field]
            if isinstance(value, (int, float)):
                if 'min' in rules and value < rules['min']:
                    warnings.append(f"{rules['warning']}: {value} < {rules['min']}")
                elif 'max' in rules and value > rules['max']:
                    warnings.append(f"{rules['warning']}: {value} > {rules['max']}")
    
    return warnings

def analyze_series() -> Tuple[List[SeriesInfo], int]:
    """
    Анализ всех серий проб
    Возвращает список серий и общее количество серий
    """
    data = load_data()
    probes = data.get('probes', [])
    
    if not probes:
        raise ValueError('Нет базы данных или она пуста')
    
    # Группируем пробы по сериям
    series_groups = {}
    
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
            series_groups[series_key] = {}
        
        # Валидация полей
        warnings = validate_probe_fields(probe, probe_type)
        
        # Сохраняем информацию о пробе
        series_groups[series_key][probe_type] = ProbeInfo(
            probe=probe,
            probe_type=probe_type,
            method_number=method_number,
            exp_number=exp_number,
            source_class=source_class,
            warnings=warnings
        )
    
    # Список всех возможных типов проб
    all_probe_types = list(PATTERNS.keys())
    
    # Формируем информацию о сериях
    series_list = []
    total_series = 0
    
    for series_key, probes_by_type in series_groups.items():
        # Проверяем наличие хотя бы одной пробы из паттерна
        if not any(pt in probes_by_type for pt in all_probe_types):
            continue
        
        total_series += 1
        
        # Определяем отсутствующие типы
        existing_types = set(probes_by_type.keys())
        missing_types = [pt for pt in all_probe_types if pt not in existing_types]
        
        # Проверяем наличие предупреждений
        has_warnings = any(len(p.warnings) > 0 for p in probes_by_type.values())
        
        series_list.append(SeriesInfo(
            series_key=series_key,
            probes_by_type=probes_by_type,
            missing_types=missing_types,
            all_types=all_probe_types,
            has_warnings=has_warnings
        ))
    
    return series_list, total_series

def get_series_summary(series: SeriesInfo) -> Dict[str, Any]:
    """Формирование сводки по серии для API"""
    source_class, method_number, exp_number = series.series_key
    
    return {
        'id': f"{source_class}-{method_number}-{exp_number}",
        'source_class': source_class,
        'method_number': method_number,
        'exp_number': exp_number,
        'probe_count': len(series.probes_by_type),
        'missing_count': len(series.missing_types),
        'has_warnings': series.has_warnings,
        'missing_types': series.missing_types[:5],  # Только первые 5 для навигации
        'total_missing': len(series.missing_types)
    }

