import json
import re
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime

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
    # Дополнительные поля, которые могут быть добавлены
    temperature: Optional[float] = None
    pressure: Optional[float] = None
    group_id: Optional[int] = None
    # ... можно добавить другие поля
    
    def to_dict(self) -> Dict:
        """Конвертирует объект Probe в словарь"""
        return asdict(self)

class ProbeManager:
    def __init__(self, json_file_path: str):
        """
        Инициализация менеджера проб
        
        Args:
            json_file_path: путь к JSON файлу с данными
        """
        self.json_file_path = json_file_path
        self.probes = self.load_probes()
        self.groups = {}  # словарь групп: {group_id: [probe_ids]}
        
    def load_probes(self) -> List[Probe]:
        """Загружает пробы из JSON файла"""
        with open(self.json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        probes = []
        for probe_data in data.get("probes", []):
            # Создаем объект Probe, пропуская лишние поля
            probe_kwargs = {k: v for k, v in probe_data.items() 
                          if k in Probe.__annotations__}
            probes.append(Probe(**probe_kwargs))
        
        return probes
    
    def save_probes(self, output_path: Optional[str] = None):
        """
        Сохраняет пробы в JSON файл
        
        Args:
            output_path: путь для сохранения (если None, сохраняет в исходный файл)
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
        
        Args:
            substring: искомая подстрока
            case_sensitive: чувствительность к регистру
            
        Returns:
            Список ID проб
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
        
        Args:
            element: элемент (например, 'Ca', 'Fe', 'Ni')
            min_val: минимальное значение
            max_val: максимальное значение
            
        Returns:
            Список ID проб
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
        """Добавляет теги 'твердая' и 'жидкая' на основе полей is_solid и is_solution"""
        for probe in self.probes:
            # Удаляем старые теги состояния, если они есть
            probe.tags = [tag for tag in probe.tags 
                         if tag not in ['твердая', 'жидкая']]
            
            # Добавляем новые теги
            if probe.is_solid:
                probe.tags.append('твердая')
            if probe.is_solution:
                probe.tags.append('жидкая')
    
    def add_tag_to_probes(self, tag: str, probe_ids: List[int]):
        """
        Добавляет тег к указанным пробам
        
        Args:
            tag: тег для добавления
            probe_ids: список ID проб
        """
        probe_id_set = set(probe_ids)
        
        for probe in self.probes:
            if probe.id in probe_id_set:
                if tag not in probe.tags:
                    probe.tags.append(tag)
    
    def remove_tag_from_probes(self, tag: str, probe_ids: List[int]):
        """
        Удаляет тег из указанных проб
        
        Args:
            tag: тег для удаления
            probe_ids: список ID проб
        """
        probe_id_set = set(probe_ids)
        
        for probe in self.probes:
            if probe.id in probe_id_set:
                if tag in probe.tags:
                    probe.tags.remove(tag)
    
    def group_probes(self, group_name: str, probe_ids: List[int]) -> int:
        """
        Создает группу из проб
        
        Args:
            group_name: имя группы
            probe_ids: список ID проб в группе
            
        Returns:
            ID созданной группы
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
        
        Args:
            probe_name: имя пробы
            
        Returns:
            Словарь с частями имени
        """
        # Используем различные разделители
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
        
        Args:
            field_name: имя добавляемого поля
            pattern: словарь с паттерном, например:
                {
                    'position': 1,  # позиция в разделенном имени (начиная с 0)
                    'subposition': 1, # позиция в позиции
                    'substring': 'AOB',  # искомая подстрока
                    'value': 25.5,  # значение для установки
                    'match_type': 'exact'  # 'exact', 'contains', 'regex'
                }
        """
        position = pattern.get('position')
        subposition = pattern.get('subposition')
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
            
            if subposition:
                
                try:
                    number_part = int(name_part)
                    number_list = [int(digit) for digit in str(number_part)]
                    
                    if number_list[0] == 1:
                        
                        
                    
                except:
                    raise KeyError("Ошибка в определении подпозиции")
                
            
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
                setattr(probe, field_name, value)
    
    def batch_add_tags_by_rules(self, rules: List[Dict]):
        """
        Пакетное добавление тегов по правилам
        
        Args:
            rules: список правил, например:
                [
                    {
                        'name': 'Высокое железо',
                        'condition': {
                            'type': 'concentration_range',
                            'element': 'Fe',
                            'min': 300
                        },
                        'tag': 'высокое_Fe'
                    },
                    {
                        'name': 'Пробы AOB',
                        'condition': {
                            'type': 'name_substring',
                            'substring': 'AOB'
                        },
                        'tag': 'AOB_группа'
                    }
                ]
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
        
        Args:
            tags: список тегов для поиска
            match_all: если True, возвращает пробы со всеми тегами,
                      если False, возвращает пробы с любым из тегов
        
        Returns:
            Список проб
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
            'average_concentrations': {}
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
            stats['average_concentrations'][element] = sum(values) / len(values)
        
        return stats
    
    def export_to_csv(self, output_path: str, delimiter: str = ','):
        """
        Экспортирует пробы в CSV файл
        
        Args:
            output_path: путь для сохранения CSV
            delimiter: разделитель
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


# Пример использования
def main():
    # Инициализация менеджера
    manager = ProbeManager('data/data.json')
    
    # 1. Добавление тегов состояний
    manager.add_state_tags()
    
    # 2. Поиск проб по подстроке в имени
    aob_probes = manager.find_probes_by_name_substring('AOB')
    print(f"Найдено проб AOB: {len(aob_probes)}")
    
    # 3. Поиск проб с высоким содержанием железа
    high_fe_probes = manager.find_probes_by_concentration_range('Fe', min_val=300)
    print(f"Проб с Fe > 300: {len(high_fe_probes)}")
    
    # 4. Добавление тега для проб AOB
    manager.add_tag_to_probes('AOB_группа', aob_probes)
    
    # 5. Добавление поля температура на основе имени
    temperature_pattern = {
        'position': 0,  # первая часть имени
        'substring': 'AOB',
        'value': 25.5,
        'match_type': 'exact'
    }
    manager.add_field_based_on_name_pattern('temperature', temperature_pattern)
    
    # 6. Пакетное добавление тегов по правилам
    rules = [
        {
            'name': 'Высокое железо',
            'condition': {
                'type': 'concentration_range',
                'element': 'Fe',
                'min': 300
            },
            'tag': 'высокое_Fe'
        },
        {
            'name': 'Низкая медь',
            'condition': {
                'type': 'concentration_range',
                'element': 'Cu',
                'max': 1.0
            },
            'tag': 'низкое_Cu'
        }
    ]
    manager.batch_add_tags_by_rules(rules)
    
    # 7. Создание группы
    manager.group_probes('Группа_AOB_высокое_Fe', aob_probes[:5])
    
    # 8. Получение статистики
    stats = manager.get_statistics()
    print(f"Всего проб: {stats['total_probes']}")
    print(f"Теги: {stats['tags_count']}")
    
    # 9. Сохранение изменений
    manager.save_probes('data/data.json')
    
    # Пример поиска по тегам
    probes_with_tags = manager.get_probes_by_tags(['AOB_группа', 'высокое_Fe'])
    print(f"Проб с тегами AOB_группа и высокое_Fe: {len(probes_with_tags)}")


if __name__ == "__main__":
    main()