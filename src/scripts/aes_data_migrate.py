import sqlite3
import json
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent.parent
DB_PATH = BASE_DIR / 'data' / 'lab_data.db'

def migrate_elements_to_aes(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Элементы для миграции
    elements_to_migrate = ['Fe', 'Ca', 'Cu', 'Ni', 'Co']
    
    try:
        # Получаем все записи из таблицы probes
        cursor.execute("SELECT id, raw_data FROM probes WHERE raw_data IS NOT NULL")
        rows = cursor.fetchall()
        
        updated_count = 0
        
        for row_id, raw_data_json in rows:
            try:
                # Парсим JSON
                raw_data = json.loads(raw_data_json)
                
                # Проверяем, есть ли данные для миграции
                needs_update = False
                
                for element in elements_to_migrate:
                    # Проверяем наличие обычного значения и AES значения
                    if element in raw_data and f"{element}_AES" in raw_data:
                        aes_value = raw_data[f"{element}_AES"]
                        regular_value = raw_data[element]
                        
                        # Если AES значение существует и > 0, оставляем его
                        # Иначе используем обычное значение
                        if aes_value is not None and aes_value > 0:
                            final_value = aes_value
                        else:
                            final_value = regular_value
                        
                        # Обновляем значение в raw_data
                        raw_data[f"{element}_AES"] = final_value
                        needs_update = True
                
                # Если были изменения, сохраняем обновленный JSON
                if needs_update:
                    updated_json = json.dumps(raw_data)
                    cursor.execute(
                        "UPDATE probes SET raw_data = ? WHERE id = ?",
                        (updated_json, row_id)
                    )
                    updated_count += 1
                    
            except json.JSONDecodeError:
                print(f"Ошибка парсинга JSON для записи ID {row_id}")
                continue
        
        # Сохраняем изменения
        conn.commit()
        print(f"Миграция завершена. Обновлено записей: {updated_count}")
        
    except sqlite3.Error as e:
        print(f"Ошибка базы данных: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")
        conn.rollback()
    finally:
        conn.close()

# Запуск
migrate_elements_to_aes(DB_PATH)
# или второй вариант:
# migrate_and_clean_elements_to_aes(DB_PATH)