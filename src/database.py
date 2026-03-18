import sqlite3
import json
from contextlib import contextmanager
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / 'data' / 'lab_data.db'

@contextmanager
def get_db_connection():
    # check_same_thread=False критичен для Flask
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row  # Чтобы обращаться к полям по именам
    try:
        yield conn
    finally:
        conn.close()

def save_probe(probe_data: dict):
    with get_db_connection() as conn:
        # SQLite сам заблокирует базу на время записи
        conn.execute("""
            INSERT OR REPLACE INTO probes 
            (id, name, source_class, method_number, exp_number, probe_type, flag_needs_recalculation, raw_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            probe_data['id'], 
            probe_data['name'], 
            probe_data['source_class'], 
            probe_data['method_num'], 
            probe_data['exp_num'], 
            probe_data['probe_type'],
            probe_data['flag_needs_recalculation'], 
            json.dumps(probe_data['raw_data'])
        ))
        conn.commit()
        
def get_full_database():
    with get_db_connection() as conn:
        # Извлекаем только колонку с полным JSON
        rows = conn.execute("SELECT raw_data FROM probes").fetchall()
        
        # Превращаем каждую строку обратно в словарь Python
        return [json.loads(row['raw_data']) for row in rows]