import time
import json
from database import get_db_connection
from mass_balance import mass_calculate, phase_calculate # ваши функции
import os
import hashlib
from pathlib import Path
import logging
import traceback

BASE_DIR = Path(__file__).parent.parent

def run_worker():
    logger = logging.getLogger('worker')
    
    # Для мониторинга файла коэффициентов
    coefficients_file = BASE_DIR/"src"/"mass_balance"/"algorithm_coeff.json"
    last_hash = None
    last_check_time = 0
    check_interval = 10
    
    logger.info("Worker запущен")
    
    while True:
        try:
            current_time = time.time()
            
            # Проверяем изменения в файле коэффициентов
            if current_time - last_check_time > check_interval:
                if os.path.exists(coefficients_file):
                    try:
                        with open(coefficients_file, 'rb') as f:
                            file_hash = hashlib.md5(f.read()).hexdigest()
                        
                        if last_hash is None:
                            last_hash = file_hash
                            logger.info(f"Мониторинг файла {coefficients_file} запущен")
                        elif file_hash != last_hash:
                            logger.info(f"Обнаружены изменения в файле {coefficients_file}")
                            if validate_coefficients_file(coefficients_file):
                                logger.info("Файл коэффициентов валиден, начинаем пересчет всей базы...")
                                last_hash = file_hash
                                recalc_all_probes()
                            else:
                                logger.error("Файл коэффициентов поврежден, пересчет отменен")
                    except Exception as e:
                        logger.error(f"Ошибка при проверке файла коэффициентов: {e}")
                        logger.error(traceback.format_exc())
                
                last_check_time = current_time
            
            # Основная обработка задач
            with get_db_connection() as conn:
                probes = conn.execute("""
                    SELECT id, raw_data 
                    FROM probes 
                    WHERE flag_needs_recalculation = 1
                    LIMIT 100
                """).fetchall()
                
                if not probes:
                    logger.debug("Нет задач для обработки, спим 2 секунды")
                    time.sleep(2)
                    continue
                    
                logger.info(f"Найдено задач для обработки: {len(probes)}")
                
                for row in probes:
                    probe_id = row['id']
                    try:
                        if not row['raw_data']:
                            logger.warning(f"probe_id={probe_id} имеет пустой raw_data")
                            conn.execute("UPDATE probes SET flag_needs_recalculation = 2 WHERE id = ?", (probe_id,))
                            continue
                        
                        probe = json.loads(row['raw_data'])
                        logger.debug(f"Загружена проба id={probe_id}, name={probe.get('name', 'unknown')}")
                        
                        if 'id' not in probe:
                            logger.debug(f"Добавляем id={probe_id} в пробу")
                            probe['id'] = probe_id
                        
                        if 'tags' not in probe:
                            probe['tags'] = []
                        
                        # Выполняем цепочку расчетов
                        logger.info(f"Начинаем обработку пробы id={probe_id}")
                        
                        try:
                            probe = phase_calculate.process_phase_calculate(probe)
                            logger.debug(f"phase_calculate завершен для id={probe_id}")
                        except Exception as e:
                            logger.error(f"Ошибка в phase_calculate для id={probe_id}: {e}")
                            logger.error(traceback.format_exc())
                            probe['tags'].append('ошибка phase_calculate') # type: ignore
                        
                        try:
                            probe = mass_calculate.process_mass_calculate(probe) # type: ignore
                            logger.debug(f"mass_calculate завершен для id={probe_id}")
                        except Exception as e:
                            logger.error(f"Ошибка в mass_calculate для id={probe_id}: {e}")
                            logger.error(traceback.format_exc())
                            probe['tags'].append('ошибка mass_calculate') # type: ignore
                        
                        # Сохраняем результат
                        conn.execute("""
                            UPDATE probes 
                            SET raw_data = ?, flag_needs_recalculation = 0 
                            WHERE id = ?
                        """, (json.dumps(probe, ensure_ascii=False), probe_id))
                        
                        logger.info(f"Проба id={probe_id} успешно обработана")
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"Ошибка парсинга JSON для probe_id={probe_id}: {e}")
                        logger.error(f"Строка {e.lineno}, позиция {e.colno}")
                        conn.execute("UPDATE probes SET flag_needs_recalculation = 3 WHERE id = ?", (probe_id,))
                    except Exception as e:
                        logger.error(f"Необработанная ошибка для probe_id={probe_id}: {e}")
                        logger.error(traceback.format_exc())
                        conn.execute("UPDATE probes SET flag_needs_recalculation = 4 WHERE id = ?", (probe_id,))
                
                conn.commit()
                logger.info(f"Обработано проб: {len(probes)}")
                
        except Exception as e:
            logger.critical(f"Критическая ошибка в основном цикле worker: {e}")
            logger.critical(traceback.format_exc())
            time.sleep(5)  # Пауза перед повторной попыткой

def validate_coefficients_file(filepath):
    """Проверяет валидность файла с коэффициентами"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Проверяем структуру (адаптируйте под ваш формат)
        if 'coefficients' in data:
            print(f"Файл коэффициентов валиден. Найдено коэффициентов: {len(data['coefficients'])}")
            return True
        else:
            print("Ошибка: в файле коэффициентов отсутствует ключ 'coefficients'")
            return False
            
    except json.JSONDecodeError as e:
        print(f"Ошибка: файл коэффициентов содержит некорректный JSON: {e}")
        return False
    except Exception as e:
        print(f"Ошибка при чтении файла коэффициентов: {e}")
        return False
            
def recalc_all_probes():
    """Пересчитывает все пробы в базе данных"""
    try:
        with get_db_connection() as conn:
            # Получаем все пробы
            probes = conn.execute("SELECT id, raw_data FROM probes").fetchall()
            
            print(f"Начинаем пересчет всех {len(probes)} проб...")
            success_count = 0
            error_count = 0
            
            for row in probes:
                probe_id = row['id']
                try:
                    if not row['raw_data']:
                        print(f"Предупреждение: probe_id={probe_id} имеет пустой raw_data, пропускаем")
                        error_count += 1
                        continue
                    
                    probe = json.loads(row['raw_data'])
                    
                    if 'id' not in probe:
                        probe['id'] = probe_id
                    
                    # Применяем все расчеты с новыми коэффициентами
                    probe = phase_calculate.process_phase_calculate(probe)
                    probe = mass_calculate.process_mass_calculate(probe)
                    
                    # Обновляем запись
                    conn.execute("""
                        UPDATE probes 
                        SET raw_data = ? 
                        WHERE id = ?
                    """, (json.dumps(probe), probe_id))
                    
                    success_count += 1
                    
                    # Прогресс каждые 100 проб
                    if success_count % 100 == 0:
                        print(f"Прогресс: {success_count}/{len(probes)} проб обработано")
                        conn.commit()  # Промежуточный коммит
                        
                except Exception as e:
                    print(f"Ошибка при пересчете probe_id={probe_id}: {e}")
                    error_count += 1
            
            conn.commit()
            print(f"Пересчет завершен. Успешно: {success_count}, Ошибок: {error_count}")
            
            # Сбрасываем флаги после пересчета
            conn.execute("UPDATE probes SET flag_needs_recalculation = 0")
            conn.commit()
            
    except Exception as e:
        print(f"Критическая ошибка при пересчете всей базы: {e}")
if __name__ == "__main__":
    run_worker()