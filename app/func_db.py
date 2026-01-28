import json
import copy
from typing import Dict, List, Any, Optional
from pathlib import Path

class ProbeDatabase:
    def __init__(self, json_path: str):
        """Инициализация базы данных с загрузкой из JSON файла"""
        self.json_path = Path(json_path)
        self.data = self._load_data()
    
    def _load_data(self) -> Dict:
        """Загрузка данных из JSON файла"""
        if not self.json_path.exists():
            return {"probes": []}
        
        with open(self.json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _save_data(self) -> None:
        """Сохранение данных в JSON файл"""
        with open(self.json_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
    
    def remove_field_from_all_probes(self, field_name: str) -> bool:
        """
        Удаляет поле из всех проб
        
        Args:
            field_name: Название поля для удаления
            
        Returns:
            bool: True если поле было удалено, False если поля не существовало
        """
        if "probes" not in self.data:
            return False
        
        field_exists = False
        for probe in self.data["probes"]:
            if field_name in probe:
                del probe[field_name]
                field_exists = True
        
        if field_exists:
            self._save_data()
            return True
        return False
    
    def rename_field_for_all_probes(self, old_field_name: str, new_field_name: str) -> bool:
        """
        Переименовывает поле во всех пробах
        
        Args:
            old_field_name: Текущее название поля
            new_field_name: Новое название поля
            
        Returns:
            bool: True если переименование успешно, False если старое поле не найдено
        """
        if "probes" not in self.data:
            return False
        
        field_exists = False
        for probe in self.data["probes"]:
            if old_field_name in probe:
                # Сохраняем значение и удаляем старое поле
                value = probe[old_field_name]
                del probe[old_field_name]
                # Добавляем новое поле с сохраненным значением
                probe[new_field_name] = value
                field_exists = True
        
        if field_exists:
            self._save_data()
            return True
        return False
    
    def set_field_value_for_all_probes(self, field_name: str, value: Any, 
                                       overwrite_existing: bool = True) -> int:
        """
        Задает значение поля для всех проб
        
        Args:
            field_name: Название поля
            value: Значение для установки
            overwrite_existing: Перезаписывать существующие значения (True) или только пустые (False)
            
        Returns:
            int: Количество проб, для которых было установлено значение
        """
        if "probes" not in self.data:
            return 0
        
        updated_count = 0
        for probe in self.data["probes"]:
            if field_name not in probe or overwrite_existing:
                probe[field_name] = copy.deepcopy(value) if isinstance(value, (dict, list)) else value
                updated_count += 1
        
        if updated_count > 0:
            self._save_data()
        
        return updated_count
    
    def get_probes(self) -> List[Dict]:
        """Возвращает список всех проб"""
        return self.data.get("probes", [])
    
    def add_probe(self, probe: Dict) -> None:
        """Добавляет новую пробу в базу данных"""
        if "probes" not in self.data:
            self.data["probes"] = []
        
        self.data["probes"].append(probe)
        self._save_data()
    
    def update_probe(self, probe_id: int, updates: Dict) -> bool:
        """
        Обновляет конкретную пробу
        
        Args:
            probe_id: ID пробы
            updates: Словарь с обновлениями
            
        Returns:
            bool: True если проба найдена и обновлена
        """
        for probe in self.data.get("probes", []):
            if probe.get("id") == probe_id:
                probe.update(updates)
                self._save_data()
                return True
        return False

# Пример использования функций
def example_usage():
    # Создаем экземпляр базы данных
    db = ProbeDatabase("data/data.json")
    
    # 1. Удаление поля для всех проб
    print("Удаление поля")
    removed = db.remove_field_from_all_probes("'Описание'")
    removed = db.remove_field_from_all_probes("Sc 361.383")
    removed = db.remove_field_from_all_probes("Ni 221.648")
    removed = db.remove_field_from_all_probes("Co 237.863")
    removed = db.remove_field_from_all_probes("Cu 327.395")
    removed = db.remove_field_from_all_probes("Sc 335.372")
    removed = db.remove_field_from_all_probes("Fe 234.350")
    removed = db.remove_field_from_all_probes("Ca 315.887")
    removed = db.remove_field_from_all_probes("Co 230.786")
    removed = db.remove_field_from_all_probes("Ni 231.604")
    removed = db.remove_field_from_all_probes("Ni 230.299")
    removed = db.remove_field_from_all_probes("Ni 216.555")
    removed = db.remove_field_from_all_probes("Fe 258.588")
    removed = db.remove_field_from_all_probes("Co 258.033")
    removed = db.remove_field_from_all_probes("Sc 357.634")
    removed = db.remove_field_from_all_probes("Cu 324.754")
    removed = db.remove_field_from_all_probes("Ca 317.933")
    removed = db.remove_field_from_all_probes("Объем р-ра Ca(OH)2 (ml)")
    removed = db.remove_field_from_all_probes("Объем р-ра CaCO3 (ml)")                                                          
    print(f"Поле удалено: {removed}")
    """
    # 2. Переименование поля для всех проб
    print("\nПереименование 'dCa' в 'delta_Ca'...")
    renamed = db.rename_field_for_all_probes("dCa", "delta_Ca")
    print(f"Поле переименовано: {renamed}")
        
    # 3. Задание значения поля для всех проб
    print("\nДобавление поля 'lab' для всех проб...")
    count = db.set_field_value_for_all_probes("lab", "Центральная лаборатория")
    print(f"Обновлено проб: {count}")
    
    # 4. Задание значения только для проб, где поле отсутствует
    print("\nДобавление 'comment' только если его нет...")
    count = db.set_field_value_for_all_probes("comment", "Нет комментария", overwrite_existing=False)
    print(f"Добавлено comment в {count} проб")
    """
# Дополнительные утилитарные функции
def batch_rename_fields(json_path: str, rename_mapping: Dict[str, str]) -> None:
    """
    Пакетное переименование нескольких полей
    
    Args:
        json_path: Путь к JSON файлу
        rename_mapping: Словарь {старое_имя: новое_имя}
    """
    db = ProbeDatabase(json_path)
    
    for old_name, new_name in rename_mapping.items():
        db.rename_field_for_all_probes(old_name, new_name)

def set_field_with_condition(json_path: str, field_name: str, value: Any, 
                            condition_func: callable = None) -> int: # type: ignore
    """
    Устанавливает значение поля для проб, удовлетворяющих условию
    
    Args:
        json_path: Путь к JSON файлу
        field_name: Название поля
        value: Значение
        condition_func: Функция условия (принимает пробу, возвращает bool)
        
    Returns:
        int: Количество обновленных проб
    """
    db = ProbeDatabase(json_path)
    
    if "probes" not in db.data:
        return 0
    
    updated_count = 0
    for probe in db.data["probes"]:
        if condition_func is None or condition_func(probe):
            probe[field_name] = copy.deepcopy(value) if isinstance(value, (dict, list)) else value
            updated_count += 1
    
    if updated_count > 0:
        db._save_data()
    
    return updated_count

# Пример с условным обновлением
def conditional_update_example():
    db = ProbeDatabase("database.json")
    
    # Устанавливаем статус только для твердых проб
    def is_solid_probe(probe):
        return probe.get("is_solid", False)
    
    count = set_field_with_condition(
        "database.json", 
        "material_type", 
        "solid", 
        is_solid_probe
    )
    print(f"Обновлено {count} твердых проб")

if __name__ == "__main__":
    with open("data/data.json", 'r', encoding='utf-8') as f:
        data = json.load(f).get('probes', [])
    
    for probe in data:
        # Создаем список ключей для удаления
        keys_to_delete = [field for field in probe if 'Unnamed' in field]
        
        # Удаляем ключи после завершения итерации
        for key in keys_to_delete:
            del probe[key]
    
    with open("data/data.json", 'w', encoding='utf-8') as f:
        json.dump({'probes': data}, f, ensure_ascii=False, indent=2)    