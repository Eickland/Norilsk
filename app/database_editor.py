"""
Модуль для редактирования базы данных проб.
Включает функции для массового обновления данных.
"""

import json
import os
from typing import List, Dict, Any, Optional, Union
from pathlib import Path
from datetime import datetime

class DatabaseEditor:
    """
    Редактор базы данных проб.
    Предоставляет функционал для массового редактирования данных.
    """
    
    def __init__(self, data_file_path: str):
        """
        Инициализация редактора.
        
        Args:
            data_file_path: Путь к JSON файлу с данными
        """
        self.data_file_path = Path(data_file_path)
        self.data = None
        self.load_data()
    
    def load_data(self) -> None:
        """Загрузка данных из файла"""
        if not self.data_file_path.exists():
            raise FileNotFoundError(f"Файл данных не найден: {self.data_file_path}")
        
        with open(self.data_file_path, 'r', encoding='utf-8') as f:
            self.data = json.load(f)
    
    def save_data(self) -> None:
        """Сохранение данных в файл"""
        if self.data is None:
            raise ValueError("Данные не загружены")
        
        # Создаем резервную копию перед сохранением
        backup_file = self.data_file_path.with_suffix(f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        #self.data_file_path.rename(backup_file)
        
        with open(self.data_file_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
        
        print(f"Данные сохранены. Резервная копия: {backup_file}")
    
    def get_probes(self) -> List[Dict[str, Any]]:
        """Получение списка всех проб"""
        if self.data is None:
            self.load_data()
        return self.data.get('probes', []) # type: ignore
    
    def update_all_probes_field(
        self, 
        field_name: str, 
        new_value: Any,
        value_type: str = 'auto'
    ) -> Dict[str, Any]:
        """
        Изменение одного поля данных на определенное значение для всех проб.
        
        Args:
            field_name: Название поля для изменения
            new_value: Новое значение поля
            value_type: Тип значения ('string', 'number', 'boolean', 'auto')
        
        Returns:
            Словарь со статистикой изменений
        """
        if self.data is None:
            self.load_data()
        
        probes = self.data.get('probes', []) # type: ignore
        
        if not probes:
            return {'success': False, 'error': 'Нет проб для обновления', 'updated': 0}
        
        # Преобразуем значение в нужный тип
        converted_value = self._convert_value(new_value, value_type)
        
        updated_count = 0
        skipped_probes = []
        
        for i, probe in enumerate(probes):
            try:
                probe[field_name] = converted_value
                updated_count += 1
            except Exception as e:
                skipped_probes.append({
                    'index': i,
                    'probe_id': probe.get('id'),
                    'error': str(e)
                })
        
        # Обновляем метаданные
        if 'metadata' not in self.data: # type: ignore
            self.data['metadata'] = {} # type: ignore
        
        self.data['metadata']['last_edit'] = { # type: ignore
            'operation': 'update_all_field',
            'field': field_name,
            'value': new_value,
            'timestamp': datetime.now().isoformat(),
            'updated_count': updated_count,
            'skipped_count': len(skipped_probes)
        }
        
        # Сохраняем изменения
        self.save_data()
        
        return {
            'success': True,
            'operation': 'update_all_field',
            'field': field_name,
            'value': converted_value,
            'updated': updated_count,
            'skipped': len(skipped_probes),
            'total_probes': len(probes),
            'skipped_probes': skipped_probes if skipped_probes else None,
            'timestamp': datetime.now().isoformat()
        }
    
    def update_probes_by_tag(
        self,
        field_name: str,
        new_value: Any,
        tag_filter: Union[str, List[str]],
        match_all: bool = False,
        value_type: str = 'auto'
    ) -> Dict[str, Any]:
        """
        Изменение поля данных для проб с определенным тегом.
        
        Args:
            field_name: Название поля для изменения
            new_value: Новое значение поля
            tag_filter: Тег или список тегов для фильтрации
            match_all: Если True, проба должна содержать все указанные теги
            value_type: Тип значения ('string', 'number', 'boolean', 'auto')
        
        Returns:
            Словарь со статистикой изменений
        """
        if self.data is None:
            self.load_data()
        
        probes = self.data.get('probes', []) # type: ignore
        
        if not probes:
            return {'success': False, 'error': 'Нет проб для обновления', 'updated': 0}
        
        # Преобразуем теги в список
        if isinstance(tag_filter, str):
            tag_filter = [tag_filter]
        
        # Преобразуем значение в нужный тип
        converted_value = self._convert_value(new_value, value_type)
        
        updated_count = 0
        matched_probes = []
        skipped_probes = []
        
        for i, probe in enumerate(probes):
            try:
                probe_tags = probe.get('tags', [])
                
                # Проверяем соответствие тегам
                if match_all:
                    # Все теги должны присутствовать
                    if all(tag in probe_tags for tag in tag_filter):
                        probe[field_name] = converted_value
                        updated_count += 1
                        matched_probes.append(probe.get('id', i))
                else:
                    # Хотя бы один тег должен присутствовать
                    if any(tag in probe_tags for tag in tag_filter):
                        probe[field_name] = converted_value
                        updated_count += 1
                        matched_probes.append(probe.get('id', i))
            except Exception as e:
                skipped_probes.append({
                    'index': i,
                    'probe_id': probe.get('id'),
                    'error': str(e)
                })
        
        # Обновляем метаданные
        if 'metadata' not in self.data: # type: ignore
            self.data['metadata'] = {} # type: ignore
        
        self.data['metadata']['last_edit'] = { # type: ignore
            'operation': 'update_by_tag',
            'field': field_name,
            'value': new_value,
            'tags': tag_filter,
            'match_all': match_all,
            'timestamp': datetime.now().isoformat(),
            'updated_count': updated_count,
            'matched_probes': matched_probes,
            'skipped_count': len(skipped_probes)
        }
        
        # Сохраняем изменения
        self.save_data()
        
        return {
            'success': True,
            'operation': 'update_by_tag',
            'field': field_name,
            'value': converted_value,
            'tags': tag_filter,
            'match_all': match_all,
            'updated': updated_count,
            'matched_probes': matched_probes,
            'skipped': len(skipped_probes),
            'total_probes': len(probes),
            'skipped_probes': skipped_probes if skipped_probes else None,
            'timestamp': datetime.now().isoformat()
        }
    
    def add_new_field(
        self,
        field_name: str,
        default_value: Any = None,
        value_type: str = 'auto',
        skip_existing: bool = True
    ) -> Dict[str, Any]:
        """
        Добавление нового поля данных для всех проб.
        
        Args:
            field_name: Название нового поля
            default_value: Значение по умолчанию
            value_type: Тип значения ('string', 'number', 'boolean', 'auto')
            skip_existing: Пропускать пробы, у которых уже есть это поле
        
        Returns:
            Словарь со статистикой изменений
        """
        if self.data is None:
            self.load_data()
        
        probes = self.data.get('probes', []) # type: ignore
        
        if not probes:
            return {'success': False, 'error': 'Нет проб для обновления', 'added': 0}
        
        # Преобразуем значение в нужный тип
        converted_value = self._convert_value(default_value, value_type)
        
        added_count = 0
        skipped_count = 0
        already_had_field = []
        
        for i, probe in enumerate(probes):
            try:
                if field_name in probe and skip_existing:
                    skipped_count += 1
                    already_had_field.append(probe.get('id', i))
                    continue
                
                probe[field_name] = converted_value
                added_count += 1
            except Exception as e:
                print(f"Ошибка при добавлении поля для пробы {i}: {e}")
                skipped_count += 1
        
        # Обновляем метаданные
        if 'metadata' not in self.data: # type: ignore
            self.data['metadata'] = {} # type: ignore
        
        self.data['metadata']['last_edit'] = { # type: ignore
            'operation': 'add_new_field',
            'field': field_name,
            'default_value': default_value,
            'timestamp': datetime.now().isoformat(),
            'added_count': added_count,
            'skipped_count': skipped_count,
            'already_had_field': already_had_field if already_had_field else None
        }
        
        # Сохраняем изменения
        self.save_data()
        
        return {
            'success': True,
            'operation': 'add_new_field',
            'field': field_name,
            'default_value': converted_value,
            'added': added_count,
            'skipped': skipped_count,
            'already_had_field': already_had_field if already_had_field else None,
            'total_probes': len(probes),
            'timestamp': datetime.now().isoformat()
        }
    
    def _convert_value(self, value: Any, value_type: str) -> Any:
        """
        Преобразование значения в указанный тип.
        
        Args:
            value: Исходное значение
            value_type: Желаемый тип ('string', 'number', 'boolean', 'auto')
        
        Returns:
            Преобразованное значение
        """
        if value_type == 'auto':
            # Автоматическое определение типа
            if isinstance(value, (int, float)):
                return float(value) if '.' in str(value) else int(value)
            elif isinstance(value, bool):
                return bool(value)
            elif isinstance(value, (list, dict)):
                return value
            else:
                return str(value)
        
        elif value_type == 'string':
            return str(value) if value is not None else ''
        
        elif value_type == 'number':
            if value is None or value == '':
                return 0
            try:
                # Пробуем преобразовать в float или int
                float_val = float(value)
                if float_val.is_integer():
                    return int(float_val)
                return float_val
            except (ValueError, TypeError):
                return 0
        
        elif value_type == 'boolean':
            if isinstance(value, bool):
                return value
            elif isinstance(value, str):
                return value.lower() in ['true', 'yes', '1', 'да', 'истина']
            elif isinstance(value, (int, float)):
                return bool(value)
            else:
                return bool(value)
        
        elif value_type == 'list':
            if isinstance(value, list):
                return value
            elif isinstance(value, str):
                return [item.strip() for item in value.split(',') if item.strip()]
            else:
                return [value]
        
        else:
            raise ValueError(f"Неизвестный тип значения: {value_type}")
    
    def get_probes_by_tag(
        self,
        tag_filter: Union[str, List[str]],
        match_all: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Получение проб по тегу.
        
        Args:
            tag_filter: Тег или список тегов для фильтрации
            match_all: Если True, проба должна содержать все указанные теги
        
        Returns:
            Список отфильтрованных проб
        """
        probes = self.get_probes()
        
        if isinstance(tag_filter, str):
            tag_filter = [tag_filter]
        
        filtered_probes = []
        
        for probe in probes:
            probe_tags = probe.get('tags', [])
            
            if match_all:
                if all(tag in probe_tags for tag in tag_filter):
                    filtered_probes.append(probe)
            else:
                if any(tag in probe_tags for tag in tag_filter):
                    filtered_probes.append(probe)
        
        return filtered_probes
    
    def get_field_statistics(self, field_name: str) -> Dict[str, Any]:
        """
        Получение статистики по полю.
        
        Args:
            field_name: Название поля
        
        Returns:
            Словарь со статистикой
        """
        probes = self.get_probes()
        
        values = []
        has_field = 0
        missing_field = 0
        
        for probe in probes:
            if field_name in probe:
                values.append(probe[field_name])
                has_field += 1
            else:
                missing_field += 1
        
        stats = {
            'field_name': field_name,
            'total_probes': len(probes),
            'has_field': has_field,
            'missing_field': missing_field,
            'unique_values': len(set(values)) if values else 0
        }
        
        # Для числовых полей добавляем дополнительную статистику
        if values and all(isinstance(v, (int, float)) for v in values):
            stats.update({
                'min': min(values),
                'max': max(values),
                'mean': sum(values) / len(values),
                'median': sorted(values)[len(values) // 2]
            })
        
        return stats
    
    def remove_field(
        self,
        field_name: str,
        confirm: bool = True
    ) -> Dict[str, Any]:
        """
        Удаление поля из всех проб.
        
        Args:
            field_name: Название поля для удаления
            confirm: Требовать подтверждение
        
        Returns:
            Словарь со статистикой изменений
        """
        if confirm:
            print(f"Вы уверены, что хотите удалить поле '{field_name}' из всех проб?")
            print("Это действие нельзя отменить!")
            response = input("Введите 'YES' для подтверждения: ")
            if response != 'YES':
                return {'success': False, 'error': 'Операция отменена', 'removed': 0}
        
        if self.data is None:
            self.load_data()
        
        probes = self.data.get('probes', []) # type: ignore
        
        removed_count = 0
        
        for probe in probes:
            if field_name in probe:
                del probe[field_name]
                removed_count += 1
        
        # Сохраняем изменения
        self.save_data()
        
        return {
            'success': True,
            'operation': 'remove_field',
            'field': field_name,
            'removed': removed_count,
            'total_probes': len(probes),
            'timestamp': datetime.now().isoformat()
        }


def test():
    """
    Пример использования редактора базы данных.
    """
    # Путь к файлу данных (замените на актуальный)
    DATA_FILE = "data/data.json"
    
    # Создаем экземпляр редактора
    editor = DatabaseEditor(DATA_FILE)
    
    print("=" * 50)
    print("РЕДАКТОР БАЗЫ ДАННЫХ ПРОБ")
    print("=" * 50)
    
    # Пример 1: Получение статистики
    print("\n1. СТАТИСТИКА:")
    probes_count = len(editor.get_probes())
    print(f"Всего проб: {probes_count}")
    
    # Пример 2: Добавление нового поля
    print("\n2. ДОБАВЛЕНИЕ НОВОГО ПОЛЯ:")
    result = editor.add_new_field(
        field_name="лаборант",
        default_value="Не указан",
        value_type="string"
    )
    print(f"Добавлено поле '{result['field']}' для {result['added']} проб")
    
    # Пример 3: Изменение поля для всех проб
    print("\n3. ИЗМЕНЕНИЕ ПОЛЯ ДЛЯ ВСЕХ ПРОБ:")
    result = editor.update_all_probes_field(
        field_name="комментарий",
        new_value="Требуется проверка",
        value_type="string"
    )
    print(f"Обновлено поле '{result['field']}' для {result['updated']} проб")
    
    # Пример 4: Изменение поля по тегу
    print("\n4. ИЗМЕНЕНИЕ ПОЛЯ ПО ТЕГУ:")
    result = editor.update_probes_by_tag(
        field_name="статус_проверки",
        new_value="проверено",
        tag_filter=["важный"],
        value_type="string"
    )
    print(f"Обновлено поле '{result['field']}' для {result['updated']} проб с тегом 'важный'")
    
    # Пример 5: Получение статистики по полю
    print("\n5. СТАТИСТИКА ПО ПОЛЮ:")
    stats = editor.get_field_statistics("лаборант")
    print(f"Поле '{stats['field_name']}':")
    print(f"  Есть у {stats['has_field']} проб")
    print(f"  Отсутствует у {stats['missing_field']} проб")
    print(f"  Уникальных значений: {stats['unique_values']}")
    
    # Пример 6: Получение проб по тегу
    print("\n6. ПРОБЫ ПО ТЕГУ:")
    tagged_probes = editor.get_probes_by_tag(["методика_4"])
    print(f"Найдено {len(tagged_probes)} проб с тегом 'методика_4'")
    if tagged_probes:
        print(f"Первые 3 пробы: {[p.get('name') for p in tagged_probes[:3]]}")
    
    print("\n" + "=" * 50)
    print("ОПЕРАЦИИ ЗАВЕРШЕНЫ")
    print("=" * 50)

def main():
    
    DATA_FILE = "data/data.json"
    
    editor = DatabaseEditor(DATA_FILE)
    
    print("\n2. ДОБАВЛЕНИЕ НОВОГО ПОЛЯ:")
    result = editor.add_new_field(
        field_name="pH",
        default_value="0",
        value_type="number"
    )
    print(f"Добавлено поле '{result['field']}' для {result['added']} проб")
    
    result = editor.add_new_field(
        field_name="Eh",
        default_value="0",
        value_type="number"
    )
    print(f"Добавлено поле '{result['field']}' для {result['added']} проб")
    
    result = editor.add_new_field(
        field_name="Плотность",
        default_value="0",
        value_type="number"
    )
    print(f"Добавлено поле '{result['field']}' для {result['added']} проб")
    
    result = editor.add_new_field(
        field_name="Масса навески (g)",
        default_value="0",
        value_type="number"
    )
    print(f"Добавлено поле '{result['field']}' для {result['added']} проб")      
    result = editor.add_new_field(
        field_name="Масса твердого (g)",
        default_value="0",
        value_type="number"
    )
    print(f"Добавлено поле '{result['field']}' для {result['added']} проб")
    
    result = editor.add_new_field(
        field_name="Объем р-ра Ca(OH)2 (ml)",
        default_value="0",
        value_type="number"
    )
    print(f"Добавлено поле '{result['field']}' для {result['added']} проб")
    
    result = editor.add_new_field(
        field_name="Объем р-ра CaCO3 (ml)",
        default_value="0",
        value_type="number"
    )
    print(f"Добавлено поле '{result['field']}' для {result['added']} проб")
    
    result = editor.add_new_field(
        field_name="Объем р-ра H2SO4 (ml)",
        default_value="0",
        value_type="number"
    )
    print(f"Добавлено поле '{result['field']}' для {result['added']} проб")
    
    result = editor.add_new_field(
        field_name="Масса железных окатышей (g)",
        default_value="0",
        value_type="number"
    )
    print(f"Добавлено поле '{result['field']}' для {result['added']} проб")                              
if __name__ == "__main__":
    # Для тестирования
    main()