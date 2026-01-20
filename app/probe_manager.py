import json
import re
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import copy

@dataclass
class Probe:
    """Класс для представления пробы"""
    name: str
    Ca: float
    dCa: float
    Co: float
    dCo: float
    Cu: float
    dCu: float
    Fe: float
    dFe: float
    Ni: float
    dNi: float
    Описание: str
    is_solid: bool
    id: int
    tags: List[str]
    status_id: int
    is_solution: bool
    last_normalized: str
    # Дополнительные поля
    group_id: Optional[int] = None
    custom_fields: Optional[Dict] = None
    
    def __post_init__(self):
        if self.custom_fields is None:
            self.custom_fields = {}
    
    def to_dict(self) -> Dict:
        """Конвертирует объект Probe в словарь"""
        result = asdict(self)
        # Конвертируем dataclass в обычный dict
        result['custom_fields'] = self.custom_fields or {}
        return result


class ProbeManager:
    def __init__(self, json_file_path: str):
        """
        Инициализация менеджера проб
        """
        self.json_file_path = json_file_path
        self.probes = self.load_probes()
        self.groups = {}
        
    def load_probes(self) -> List[Probe]:
        """Загружает пробы из JSON файла"""
        with open(self.json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        probes = []
        for probe_data in data.get("probes", []):
            # Обрабатываем custom_fields
            custom_fields = probe_data.pop('custom_fields', {}) if 'custom_fields' in probe_data else {}
            
            # Создаем объект Probe
            probe_kwargs = {}
            for field in Probe.__annotations__:
                if field in probe_data:
                    probe_kwargs[field] = probe_data[field]
            probe = Probe(**probe_kwargs)
            probe.custom_fields = custom_fields
            probes.append(probe)
        
        return probes
    
    def save_probes(self, output_path: Optional[str] = None):
        """
        Сохраняет пробы в JSON файл
        """
        save_path = output_path or self.json_file_path
        
        data = {
            "probes": [probe.to_dict() for probe in self.probes]
        }
        
        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    
    def find_probes_by_name_substring(self, substring: str, 
                                     case_sensitive: bool = False) -> List[int]:
        """
        Находит пробы по подстроке в имени
        """
        result = []
        
        for probe in self.probes:
            name_to_check = probe.name if case_sensitive else probe.name.lower()
            substr_to_check = substring if case_sensitive else substring.lower()
            
            if substr_to_check in name_to_check:
                result.append(probe.id)
        
        return result
    
    def find_probes_by_concentration_range(self, element: str, 
                                          min_val: Optional[float] = None,
                                          max_val: Optional[float] = None) -> List[int]:
        """
        Находит пробы по диапазону концентраций элемента
        """
        if element not in ['Ca', 'Co', 'Cu', 'Fe', 'Ni']:
            raise ValueError(f"Элемент {element} не поддерживается")
        
        result = []
        
        for probe in self.probes:
            concentration = getattr(probe, element)
            
            if min_val is not None and concentration < min_val:
                continue
            if max_val is not None and concentration > max_val:
                continue
            
            result.append(probe.id)
        
        return result
    
    def add_state_tags(self):
        """Добавляет теги 'твердая' и 'жидкая'"""
        for probe in self.probes:
            # Удаляем старые теги состояния
            probe.tags = [tag for tag in probe.tags 
                         if tag not in ['твердая', 'жидкая', 'solid', 'liquid']]
            
            # Добавляем новые теги
            if probe.is_solid:
                probe.tags.append('твердая')
                probe.tags.append('solid')
            if probe.is_solution:
                probe.tags.append('жидкая')
                probe.tags.append('liquid')
    
    def add_tag_to_probes(self, tag: str, probe_ids: List[int]):
        """
        Добавляет тег к указанным пробам
        """
        probe_id_set = set(probe_ids)
        
        for probe in self.probes:
            if probe.id in probe_id_set:
                if tag not in probe.tags:
                    probe.tags.append(tag)
    
    def remove_tag_from_probes(self, tag: str, probe_ids: List[int]):
        """
        Удаляет тег из указанных проб
        """
        probe_id_set = set(probe_ids)
        
        for probe in self.probes:
            if probe.id in probe_id_set:
                if tag in probe.tags:
                    probe.tags.remove(tag)
    
    def group_probes(self, group_name: str, probe_ids: List[int]) -> int:
        """
        Создает группу из проб
        """
        group_id = len(self.groups) + 1
        
        # Проверяем, что все ID существуют
        existing_ids = {probe.id for probe in self.probes}
        for pid in probe_ids:
            if pid not in existing_ids:
                raise ValueError(f"Проба с ID {pid} не существует")
        
        self.groups[group_id] = {
            'name': group_name,
            'probe_ids': probe_ids
        }
        
        # Добавляем group_id к пробам
        for probe in self.probes:
            if probe.id in probe_ids:
                probe.group_id = group_id
        
        return group_id
    
    def parse_probe_name(self, probe_name: str) -> Dict[str, str]:
        """
        Парсит имя пробы по разделителям
        """
        separators = r'[_\-\s]+'
        parts = re.split(separators, probe_name)
        
        result = {}
        for i, part in enumerate(parts):
            result[f'part_{i}'] = part
        
        return result
    
    def add_field_based_on_name_pattern(self, field_name: str, 
                                       pattern: Dict[str, Any]):
        """
        Добавляет поле на основе паттерна в имени пробы
        """
        position = pattern.get('position')
        substring = pattern.get('substring')
        value = pattern.get('value')
        match_type = pattern.get('match_type', 'contains')
        
        if None in [position, substring, value]:
            raise ValueError("Не все обязательные поля указаны в паттерне")
        
        for probe in self.probes:
            parsed_name = self.parse_probe_name(probe.name)
            name_part_key = f'part_{position}'
            
            if name_part_key not in parsed_name:
                continue
            
            name_part = parsed_name[name_part_key]
            should_set = False
            
            if match_type == 'exact':
                should_set = name_part == substring
            elif match_type == 'contains':
                should_set = substring in name_part # type: ignore
            elif match_type == 'regex':
                should_set = bool(re.match(substring, name_part)) # type: ignore
            else:
                raise ValueError(f"Неизвестный тип сравнения: {match_type}")
            
            if should_set:
                # Добавляем в custom_fields
                probe.custom_fields[field_name] = value # type: ignore
    
    def batch_add_tags_by_rules(self, rules: List[Dict]):
        """
        Пакетное добавление тегов по правилам
        """
        for rule in rules:
            condition = rule['condition']
            tag = rule['tag']
            
            if condition['type'] == 'name_substring':
                probe_ids = self.find_probes_by_name_substring(
                    condition['substring'],
                    condition.get('case_sensitive', False)
                )
            
            elif condition['type'] == 'concentration_range':
                probe_ids = self.find_probes_by_concentration_range(
                    condition['element'],
                    condition.get('min'),
                    condition.get('max')
                )
            
            elif condition['type'] == 'state':
                if condition.get('state') == 'solid':
                    probe_ids = [p.id for p in self.probes if p.is_solid]
                elif condition.get('state') == 'solution':
                    probe_ids = [p.id for p in self.probes if p.is_solution]
                else:
                    continue
            
            else:
                continue
            
            self.add_tag_to_probes(tag, probe_ids)
    
    def get_probes_by_tags(self, tags: List[str], 
                          match_all: bool = True) -> List[Probe]:
        """
        Получает пробы по тегам
        """
        result = []
        
        for probe in self.probes:
            probe_tags = set(probe.tags)
            search_tags = set(tags)
            
            if match_all:
                if search_tags.issubset(probe_tags):
                    result.append(probe)
            else:
                if search_tags.intersection(probe_tags):
                    result.append(probe)
        
        return result
    
    def get_statistics(self) -> Dict:
        """Возвращает статистику по пробам"""
        stats = {
            'total_probes': len(self.probes),
            'solid_probes': sum(1 for p in self.probes if p.is_solid),
            'solution_probes': sum(1 for p in self.probes if p.is_solution),
            'tags_count': {},
            'average_concentrations': {},
            'elements': {}
        }
        
        # Подсчет тегов
        all_tags = []
        for probe in self.probes:
            all_tags.extend(probe.tags)
        
        from collections import Counter
        stats['tags_count'] = dict(Counter(all_tags))
        
        # Средние концентрации
        elements = ['Ca', 'Co', 'Cu', 'Fe', 'Ni']
        for element in elements:
            values = [getattr(p, element) for p in self.probes]
            stats['average_concentrations'][element] = {
                'mean': sum(values) / len(values),
                'min': min(values),
                'max': max(values)
            }
        
        # Группировка по элементам
        for element in elements:
            stats['elements'][element] = {
                'probes': [p.id for p in self.probes if getattr(p, element) > 0]
            }
        
        return stats
    
    def export_to_csv(self, output_path: str, delimiter: str = ','):
        """
        Экспортирует пробы в CSV файл
        """
        import csv
        
        if not self.probes:
            return
        
        # Получаем все возможные поля
        fieldnames = list(self.probes[0].to_dict().keys())
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter)
            writer.writeheader()
            
            for probe in self.probes:
                writer.writerow(probe.to_dict())