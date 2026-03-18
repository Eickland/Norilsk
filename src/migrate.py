import sqlite3
import json
import os
from pathlib import Path
# Импортируем ваши функции парсинга
from middleware.series_worker import get_probe_type, get_source_class_from_probe

# Конфигурация путей
BASE_DIR = Path(__file__).parent.parent
DATA_JSON = BASE_DIR / 'data' / 'data.json'
DB_PATH = BASE_DIR / 'data' / 'lab_data.db'

def init_database():
    """Создает файл БД и таблицу с необходимыми индексами"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('PRAGMA journal_mode=WAL;')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS probes (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            source_class TEXT NOT NULL,
            method_number INTEGER NOT NULL,
            exp_number INTEGER NOT NULL,
            probe_type TEXT NOT NULL,
            flag_needs_recalculation INTEGER DEFAULT 1,
            raw_data TEXT
        )
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_recalc ON probes(flag_needs_recalculation) WHERE flag_needs_recalculation = 1;        
    ''')
    conn.commit()
    return conn

def is_valid_value(val):
    """Проверяет, что значение не пустое и не является 'None'/пустой строкой"""
    if val is None:
        return False
    if isinstance(val, str) and val.strip().lower() in ['', 'none', 'null', 'undefined']:
        return False
    return True

def migrate():
    if not DATA_JSON.exists():
        print(f"Ошибка: Файл {DATA_JSON} не найден!")
        return

    conn = init_database()
    cursor = conn.cursor()

    with open(DATA_JSON, 'r', encoding='utf-8') as f:
        full_data = json.load(f)
    
    probes_list = full_data.get('probes', [])
    print(f"Всего проб в исходном файле: {len(probes_list)}")

    processed_ids = set()  # Для фильтрации дубликатов
    count_success = 0
    count_skipped_invalid = 0
    count_skipped_duplicate = 0

    for probe in probes_list:
        probe_id = probe.get('id')
        name = probe.get('name')
        flag_needs_recalculation = 1
        
        # Получаем данные через ваши middleware функции
        type_info = get_probe_type(probe)
        source_class = get_source_class_from_probe(probe)
        
        probe_type = None
        method_num = None
        exp_num = None
        
        if type_info:
            probe_type, method_num, exp_num = type_info
        # 1. Фильтр дубликатов: если ID уже был, пропускаем (берем только первый)
        if probe_id in processed_ids:
            count_skipped_duplicate += 1
            continue
        # 2. Фильтр полноты данных: проверяем все базовые поля
        fields_to_check = {
            'id': probe_id,
            'name': name,
            'source_class': source_class,
            'method_number': method_num,
            'exp_number': exp_num,
            'probe_type': probe_type
        }
        
        is_complete = all(is_valid_value(v) for v in fields_to_check.values())
        
        if not is_complete:
            count_skipped_invalid += 1
            continue

        # Сохранение в БД
        try:
            cursor.execute('''
                INSERT INTO probes 
                (id, name, source_class, method_number, exp_number, probe_type, flag_needs_recalculation, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                probe_id,
                name,
                source_class,
                method_num,
                exp_num,
                probe_type,
                flag_needs_recalculation,
                json.dumps(probe, ensure_ascii=False)
            ))
            processed_ids.add(probe_id)
            count_success += 1
        except sqlite3.IntegrityError:
            # На случай, если в JSON есть два разных объекта с одним ID
            count_skipped_duplicate += 1
        except Exception as e:
            print(f"Критическая ошибка при импорте {name}: {e}")

    conn.commit()
    conn.close()

    print("\n--- Отчет о миграции ---")
    print(f"Успешно перенесено: {count_success}")
    print(f"Пропущено (дубликаты): {count_skipped_duplicate}")
    print(f"Пропущено (пустые поля): {count_skipped_invalid}")
    print(f"Итого записей в новой базе: {count_success}")

if __name__ == "__main__":
    migrate()