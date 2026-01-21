import pandas as pd
import numpy as np
from scipy.spatial.distance import pdist, squareform
import json
import shutil
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple, Optional
import re

black_list_column = ['Разбавление','sample_mass','Масса навески (g)']

def load_json_data(json_path: Path) -> Dict:
    """Загрузка данных из JSON файла"""
    if not json_path.exists():
        raise FileNotFoundError(f"Файл данных не найден: {json_path}")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json_data(json_path: Path, data: Dict) -> None:
    """Сохранение данных в JSON файл"""
    # Создаем бэкап
    backup_path = json_path.with_suffix(f'.backup_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.json')
    shutil.copy2(json_path, backup_path)
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"[JSON] Бэкап создан: {backup_path}")

def is_number(x: Any) -> bool:
    """Проверка, является ли значение числом"""
    return isinstance(x, (int, float)) and not isinstance(x, bool)

def postprocess_json_database(json_path: Path) -> Dict[str, Any]:
    """
    Постобработка базы данных JSON:
    1. Удаление стандартов (Стандарт 1-10)
    2. Вычитание значений BLNK из всех проб
    3. Удаление BLNK
    """
    if not json_path.exists():
        print(f"\n[JSON] Файл не найден: {json_path} — пропускаю постобработку базы.")
        return {'success': False, 'error': 'Файл не найден'}
    
    data = load_json_data(json_path)
    
    if "probes" not in data or not isinstance(data["probes"], list):
        print("\n[JSON] В базе нет ключа 'probes' (ожидался список проб) — пропускаю.")
        return {'success': False, 'error': 'Нет списка проб'}
    
    probes: List[Dict[str, Any]] = data["probes"]
    original_count = len(probes)
    
    # Статистика
    stats = {
        'original_count': original_count,
        'removed_standards': 0,
        'blank_found': False,
        'blank_subtracted_fields': set(),
        'blank_removed': False
    }
    
    # 1) Удалить стандарты
    standard_names = {f"Стандарт {i}" for i in range(1, 11)}
    removed_standards = [p for p in probes if p.get("name") in standard_names]
    probes = [p for p in probes if p.get("name") not in standard_names]
    stats['removed_standards'] = len(removed_standards)
    
    # 2) Найти BLNK
    blank = None
    for p in probes:
        if p.get("name") == "BLNK":
            blank = p
            stats['blank_found'] = True
            break
    
    metals_subtracted = set()
    if blank is not None:
        # Определяем, какие поля являются концентрациями металлов
        # Ищем поля, которые могут быть металлами (исключаем служебные поля)
        non_metal_keys = {"name", "id", "tags", "status_id", "sample_mass", 
                         "V (ml)", "Масса навески (g)", "Масса твердого (g)", 
                         "Плотность", "created_at", "last_normalized"}
        
        # Вычитаем только по числовым полям BLNK
        for key, bval in blank.items():
            # Пропускаем служебные поля
            if key in non_metal_keys:
                continue
            # Пропускаем поля погрешностей (начинаются с 'd')
            if isinstance(key, str) and key.startswith("d"):
                continue
            # Проверяем, что значение числовое
            if not is_number(bval):
                continue
            
            # Вычитаем из всех проб
            for p in probes:
                if p is blank:  # Пропускаем сам BLNK
                    continue
                if key in p and is_number(p[key]):
                    p[key] = float(p[key]) - float(bval)
                    metals_subtracted.add(key)
        
        # 3) Удалить сам BLNK после вычитания
        probes = [p for p in probes if p.get("name") != "BLNK"]
        stats['blank_removed'] = True
        stats['blank_subtracted_fields'] = metals_subtracted
    
    # Сохраняем обновленные данные
    data["probes"] = probes
    save_json_data(json_path, data)
    
    # Формируем отчет
    report = {
        'success': True,
        'original_probes': original_count,
        'current_probes': len(probes),
        'standards_removed': stats['removed_standards'],
        'blank_found': stats['blank_found'],
        'blank_removed': stats['blank_removed'],
        'fields_subtracted': list(metals_subtracted) if metals_subtracted else [],
        'removed_standard_names': [p.get('name') for p in removed_standards]
    }
    
    # Выводим отчет в терминал
    print("\n" + "="*60)
    print("[JSON] ПОСТОБРАБОТКА БАЗЫ ДАННЫХ ЗАВЕРШЕНА")
    print("="*60)
    print(f"[JSON] Исходное количество проб: {original_count}")
    print(f"[JSON] Текущее количество проб: {len(probes)}")
    print(f"[JSON] Удалено стандартов: {stats['removed_standards']}")
    if removed_standards:
        print("[JSON] Удаленные стандарты:")
        for p in removed_standards[:5]:  # Показываем первые 5
            print(f"  - {p.get('name')} (id={p.get('id')})")
        if len(removed_standards) > 5:
            print(f"  ... и еще {len(removed_standards) - 5} стандартов")
    
    if stats['blank_found']:
        print("[JSON] BLNK найден: значения вычтены, затем BLNK удалён.")
        if metals_subtracted:
            print("[JSON] Поля, по которым вычитали BLNK:")
            for k in sorted(metals_subtracted):
                print(f"  - {k}")
        else:
            print("[JSON] Не нашлось подходящих числовых полей для вычитания.")
    else:
        print("[JSON] BLNK не найден: вычитание не выполнялось.")
    
    print("="*60)
    
    return report

def merge_probes_by_numbering(json_path: Path) -> Dict[str, Any]:
    """
    Объединение проб по нумерации:
    1. Для проб, где два последних символа это цифры
    2. Арифметически усреднить концентрации металлов тех проб, 
       которые отличаются по названию только последней цифрой
    3. В названии усредненной пробы убирается последний символ
    4. Исходные пробы удаляются
    """
    data = load_json_data(json_path)
    probes = data["probes"]
    
    original_count = len(probes)
    
    # Группируем пробы по базовому названию (без последнего символа)
    probes_by_base_name = {}
    
    for probe in probes:
        name = probe.get('name', '')
        if len(name) >= 2 and name[-1].isdigit() and name[-2].isdigit():
            base_name = name[:-1]  # Убираем последний символ
            if base_name not in probes_by_base_name:
                probes_by_base_name[base_name] = []
            probes_by_base_name[base_name].append(probe)
    
    # Создаем новые усредненные пробы
    new_probes = []
    merged_count = 0
    created_count = 0
    
    for base_name, probe_group in probes_by_base_name.items():
        if len(probe_group) > 1:
            # Создаем новую усредненную пробу
            merged_probe = {}
            
            # Копируем общие поля из первой пробы
            first_probe = probe_group[0]
            for key, value in first_probe.items():
                if key != 'id':  # ID будет сгенерирован позже
                    merged_probe[key] = value
            
            # Устанавливаем новое имя
            merged_probe['name'] = base_name
            
            # Усредняем числовые поля (концентрации металлов)
            # Ищем все числовые поля
            numeric_fields = {}
            for probe in probe_group:
                for key, value in probe.items():
                    if key not in ['id', 'name', 'tags', 'status_id', 'created_at', 'last_normalized']:
                        if is_number(value):
                            if key not in numeric_fields:
                                numeric_fields[key] = []
                            numeric_fields[key].append(float(value))
            
            # Вычисляем средние значения
            for field, values in numeric_fields.items():
                if len(values) > 0:
                    merged_probe[field] = np.mean(values)
            
            # Добавляем информацию о слиянии
            if 'tags' not in merged_probe:
                merged_probe['tags'] = []
            merged_probe['tags'].append('автослияние')
            merged_probe['merged_from'] = [p.get('name') for p in probe_group]
            merged_probe['merge_date'] = pd.Timestamp.now().isoformat()
            
            new_probes.append(merged_probe)
            merged_count += len(probe_group)
            created_count += 1
            
            # Удаляем исходные пробы
            for probe in probe_group:
                if probe in probes:
                    probes.remove(probe)
    
    # Добавляем новые пробы к существующим
    probes.extend(new_probes)
    
    # Обновляем данные
    data["probes"] = probes
    
    # Сохраняем изменения
    save_json_data(json_path, data)
    
    report = {
        'success': True,
        'original_probes': original_count,
        'current_probes': len(probes),
        'merged_groups': len(probes_by_base_name),
        'probes_merged': merged_count,
        'new_probes_created': created_count
    }
    
    print("\n" + "="*60)
    print("[JSON] СЛИЯНИЕ ПРОБ ПО НУМЕРАЦИИ ЗАВЕРШЕНО")
    print("="*60)
    print(f"[JSON] Исходное количество проб: {original_count}")
    print(f"[JSON] Текущее количество проб: {len(probes)}")
    print(f"[JSON] Объединено групп проб: {len(probes_by_base_name)}")
    print(f"[JSON] Объединено проб: {merged_count}")
    print(f"[JSON] Создано новых проб: {created_count}")
    print("="*60)
    
    return report

def recalculate_concentrations(json_path: Path) -> Dict[str, Any]:
    """
    Пересчет концентраций металлов по правилам:
    1. Если предпоследний символ названия пробы A (жидкие пробы):
       - Если имя вида T2-{m}A1: концентрация × 50 × (V(ml)/1000)
       - Другие имена: концентрация × 10 × (V(ml)/1000)
    
    2. Если предпоследний символ названия пробы B (твердые пробы):
       - концентрация × 0.05 / масса_навески × масса_твердого
    """
    data = load_json_data(json_path)
    probes = data["probes"]
    
    stats = {
        'total_probes': len(probes),
        'liquid_processed': 0,
        'solid_processed': 0,
        'volume_error': 0,
        'mass_error': 0,
        'solid_mass_error': 0,
        'skipped': 0
    }
    
    for probe in probes:
        name = probe.get('name', '')
        if len(name) < 2:
            stats['skipped'] += 1
            continue
        
        # Определяем тип пробы по предпоследнему символу
        second_last_char = name[-2] if len(name) >= 2 else ''
        
        if second_last_char.upper() == 'A':
            # Жидкая проба
            stats['liquid_processed'] += 1
            process_liquid_probe(probe, name, stats)
            
        elif second_last_char.upper() == 'B':
            # Твердая проба
            stats['solid_processed'] += 1
            process_solid_probe(probe, name, stats)
            
        else:
            stats['skipped'] += 1
    
    # Сохраняем изменения
    data["probes"] = probes
    save_json_data(json_path, data)
    
    print("\n" + "="*60)
    print("[JSON] ПЕРЕСЧЕТ КОНЦЕНТРАЦИЙ ЗАВЕРШЕН")
    print("="*60)
    print(f"[JSON] Всего проб: {stats['total_probes']}")
    print(f"[JSON] Обработано жидких проб: {stats['liquid_processed']}")
    print(f"[JSON] Обработано твердых проб: {stats['solid_processed']}")
    print(f"[JSON] Пропущено (не A/B): {stats['skipped']}")
    print(f"[JSON] Ошибки объема: {stats['volume_error']}")
    print(f"[JSON] Ошибки массы навески: {stats['mass_error']}")
    print(f"[JSON] Ошибки массы твердого: {stats['solid_mass_error']}")
    print("="*60)
    
    return stats

def process_liquid_probe(probe: Dict, name: str, stats: Dict) -> None:
    """Обработка жидкой пробы"""
    # Проверяем наличие объема
    volume = probe.get('V (ml)')
    if volume is None or not is_number(volume):
        # Добавляем тег об ошибке
        if 'tags' not in probe:
            probe['tags'] = []
        probe['tags'].append('ошибка объема пробы')
        stats['volume_error'] += 1
        return
    
    volume = float(volume)
    
    # Определяем коэффициент умножения
    # Проверяем паттерн T2-{m}A1
    import re
    pattern_t2 = r'^T2-\d+A\d+$'
    
    if re.match(pattern_t2, name):
        multiplier = 50
    else:
        multiplier = 10
    
    # Коэффициент пересчета
    conversion_factor = multiplier * (volume / 1000.0)
    
    # Пересчитываем все концентрации металлов
    for key, value in probe.items():
        # Пропускаем служебные поля
        if key in ['name', 'id', 'tags', 'status_id', 'sample_mass', 
                  'V (ml)', 'Масса навески (g)', 'Масса твердого (g)', 
                  'Плотность', 'created_at', 'last_normalized']:
            continue
        # Пропускаем поля погрешностей
        if key.startswith('d'):
            continue
        # Пересчитываем только числовые значения
        if is_number(value):
            probe[key] = float(value) * conversion_factor
            # Также пересчитываем погрешность, если она есть
            dkey = f'd{key}'
            if dkey in probe and is_number(probe[dkey]):
                probe[dkey] = float(probe[dkey]) * conversion_factor
    
    # Добавляем информацию о пересчете
    if 'recalculation_history' not in probe:
        probe['recalculation_history'] = []
    probe['recalculation_history'].append({
        'type': 'liquid_conversion',
        'multiplier': multiplier,
        'volume_ml': volume,
        'conversion_factor': conversion_factor,
        'date': pd.Timestamp.now().isoformat()
    })

def process_solid_probe(probe: Dict, name: str, stats: Dict) -> None:
    """Обработка твердой пробы"""
    # Проверяем наличие массы навески
    aliquot_mass = probe.get('Масса навески (g)')
    if aliquot_mass is None or not is_number(aliquot_mass):
        # Добавляем тег об ошибке
        if 'tags' not in probe:
            probe['tags'] = []
        probe['tags'].append('ошибка массы навески')
        stats['mass_error'] += 1
        return
    
    aliquot_mass = float(aliquot_mass)
    
    # Проверяем наличие массы твердого
    solid_mass = probe.get('Масса твердого (g)')
    if solid_mass is None or not is_number(solid_mass):
        # Добавляем тег об ошибке
        if 'tags' not in probe:
            probe['tags'] = []
        probe['tags'].append('ошибка массы твердого')
        stats['solid_mass_error'] += 1
        return
    
    solid_mass = float(solid_mass)
    
    # Коэффициент пересчета: 0.05 / масса_навески × масса_твердого
    # 0.05 - объем аликвоты в литрах
    conversion_factor = 0.05 / aliquot_mass * solid_mass
    
    # Пересчитываем все концентрации металлов
    for key, value in probe.items():
        # Пропускаем служебные поля
        if key in ['name', 'id', 'tags', 'status_id', 'sample_mass', 
                  'V (ml)', 'Масса навески (g)', 'Масса твердого (g)', 
                  'Плотность', 'created_at', 'last_normalized']:
            continue
        # Пропускаем поля погрешностей
        if key.startswith('d'):
            continue
        # Пересчитываем только числовые значения
        if is_number(value):
            probe[key] = float(value) * conversion_factor
            # Также пересчитываем погрешность, если она есть
            dkey = f'd{key}'
            if dkey in probe and is_number(probe[dkey]):
                probe[dkey] = float(probe[dkey]) * conversion_factor
    
    # Добавляем информацию о пересчете
    if 'recalculation_history' not in probe:
        probe['recalculation_history'] = []
    probe['recalculation_history'].append({
        'type': 'solid_conversion',
        'aliquot_mass_g': aliquot_mass,
        'solid_mass_g': solid_mass,
        'conversion_factor': conversion_factor,
        'date': pd.Timestamp.now().isoformat()
    })

def expand_sample_code(sample_name):
    """Восстанавливает полный шифр пробы из короткого"""
    if pd.isna(sample_name):
        return sample_name
    
    sample_str = str(sample_name)
    
    # Извлекаем компоненты из короткого имени
    # Формат: T2-4C1 или T2-P4A1
    pattern = r'(T\d+)-([LPFN]?)(\d+)([A-Z])(\d+)'
    match = re.match(pattern, sample_str)
    
    if not match:
        # Если не соответствует паттерну, возвращаем как есть
        return sample_str
    
    prefix = match.group(1)  # T2
    stage = match.group(2)   # стадия (может быть пусто)
    method_num = match.group(3)  # номер методики (5)
    product_type = match.group(4)  # тип продукта (A)
    repeat_num = match.group(5)  # номер повторности (2)
    
    # Если стадия не указана или это L - возвращаем как есть
    if not stage or stage == 'L':
        return sample_str
    
    # Определяем порядок стадий и какие нужно добавить
    stages_order = ['L', 'P', 'F', 'N']
    
    # Находим индекс указанной стадии
    target_index = stages_order.index(stage)
    
    # Берем все стадии от L до указанной включительно
    needed_stages = stages_order[:target_index + 1]
    
    # Формируем строку стадий с номером методики
    stages_str = ''.join([f"{s}{method_num}" for s in needed_stages])
    
    # Собираем полное имя
    full_code = f"{prefix}-{stages_str}{product_type}{repeat_num}"
    
    return full_code

def merge_similar_samples(group_df):
    """Объединяет похожие пробы, усредняя значения"""
    if len(group_df) == 1:
        return group_df.iloc[0]
    
    # Усредняем все числовые столбцы
    avg_row = group_df.mean(numeric_only=True)
    avg_row['name'] = group_df['name'].iloc[0][:-1]  # Убираем последнюю цифру
    return avg_row

def get_base_name(sample_name):
    """Извлекает базовое имя пробы (без последней цифры)"""
    if pd.isna(sample_name):
        return sample_name
    
    sample_str = str(sample_name)
    # Проверяем, заканчивается ли на две цифры
    if re.search(r'\d\d$', sample_str):
        return sample_str[:-1]  # Убираем последнюю цифру
    return sample_str
    
def process_icp_aes_data(file_path: str, json_data_path: Optional[str] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Обрабатывает данные ИСП АЭС и интегрирует с базой данных
    
    Args:
        file_path: Путь к CSV файлу с данными ИСП АЭС
        json_data_path: Путь к JSON файлу базы данных (опционально)
    
    Returns:
        Tuple[DataFrame, DataFrame]: обработанные данные и информацию о длинах волн
    """
    # Чтение данных из CSV файла
    df = pd.read_csv(file_path, sep=';', decimal='.', encoding='utf-8')
    df.rename(columns={f'{df.columns[0]}':'name'},inplace=True)
    
    # Удаление строк, где в столбце 'name' есть 'некал' или пустые строки
    df = df[~df[df.columns[0]].astype(str).str.contains('некал', case=False, na=False)]
    df = df[df[df.columns[0]].astype(str).str.strip() != '']
    
    # Функция для очистки значений в ячейках
    def clean_value(val):
        if pd.isna(val):
            return 0
        val_str = str(val).strip()
        
        # Если содержит 'некал' - возвращаем NaN для последующего удаления
        if 'некал' in val_str.lower():
            return np.nan
        
        # Если содержит 'uv' или 'ox' - возвращаем 0
        if 'uv' in val_str.lower():
            return 0
        
        # Если содержит 'x' - удаляем 'x' и оставляем число
        if 'ox' in val_str.lower():
            cleaned = val_str.lower().replace('ox', '').strip()
            try:
                return float(cleaned)
            except:
                raise ValueError('Ошибка удаления ox')
                   
        # Если содержит 'x' - удаляем 'x' и оставляем число
        if 'x' in val_str.lower():
            cleaned = val_str.lower().replace('x', '').strip()
            try:
                return float(cleaned)
            except:
                print(cleaned)
                raise ValueError('Ошибка удаления x')
        
        # Пытаемся преобразовать в число
        try:
            val_str = val_str.replace(',', '.')
            return float(val_str)
        except:
            return 0
    
    black_dict = {}
    

    
    # Применяем очистку ко всем столбцам, кроме 'Проб' и 'name'
    for col in df.columns:
        if col not in ['Проб', 'name']:
            df[col] = df[col].apply(clean_value)
    
    # Удаляем строки, где все значения NaN (после удаления 'некал')
    df = df.dropna(how='all', subset=[col for col in df.columns if col not in ['Проб', 'name']])
    # Применяем группировку и объединение
    df['BaseName'] = df['name'].apply(get_base_name)
    
    # Группируем по базовым именам и объединяем
    merged_rows = []
    for base_name, group in df.groupby('BaseName'):
        if len(group) > 1 and re.search(r'\d\d$', str(group['name'].iloc[0])):
            merged_row = merge_similar_samples(group)
            merged_rows.append(merged_row)
        else:
            merged_rows.extend(group.to_dict('records'))
    
    df = pd.DataFrame(merged_rows)
    df = df.drop(columns=['BaseName'], errors='ignore')
 
    df['name'] = df['name'].apply(expand_sample_code)

    for col in df.columns:
        if col in black_list_column:
            black_dict[col] = df[col].to_list()
            df.drop(columns=col,inplace=True)
    
    # Определяем металлы и их длины волн
    metal_wavelengths = {}
    for col in df.columns:
        if col not in ['Проб', 'name']:
            parts = col.split(' ')
            if len(parts) >= 2:
                metal = parts[0]
                wavelength = ' '.join(parts[1:])
                if metal not in metal_wavelengths:
                    metal_wavelengths[metal] = []
                metal_wavelengths[metal].append((col, wavelength))
    
    # Функция для выбора 3 наиболее близких значений длин волн
    def select_closest_wavelengths(wavelength_list):
        """
        Выбирает 3 наиболее близких по значению длины волны
        wavelength_list: список кортежей (название_столбца, длина_волны)
        Возвращает: список выбранных названий столбцов и их длин волн
        """
        if len(wavelength_list) <= 2:
            return [(col, wl) for col, wl in wavelength_list]
        
        wavelength_values = []
        wl_data = []
        for col, wl in wavelength_list:
            try:
                wl_num = float(''.join(filter(lambda x: x.isdigit() or x == '.', wl)))
                wavelength_values.append(wl_num)
                wl_data.append((col, wl, wl_num))
            except:
                wl_num = 0
                wavelength_values.append(wl_num)
                wl_data.append((col, wl, wl_num))
        
        if len(wavelength_values) > 1:
            distances = pdist(np.array(wavelength_values).reshape(-1, 1))
            distance_matrix = squareform(distances)
            sum_distances = distance_matrix.sum(axis=1)
            closest_indices = np.argsort(sum_distances)[:3]
            return [(wavelength_list[i][0], wavelength_list[i][1]) for i in closest_indices]
        else:
            return [(wavelength_list[0][0], wavelength_list[0][1])]

    def remove_zero_sum_rows_columns_safe(df):
        """
        Безопасное удаление строк и столбцов с нулевой суммой
        (работает только с числовыми данными)
        """
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        non_numeric_cols = df.select_dtypes(exclude=[np.number]).columns
        
        numeric_df = df[numeric_cols].copy()
        
        numeric_df = numeric_df[numeric_df.sum(axis=1) != 0]
        numeric_df = numeric_df.loc[:, numeric_df.sum(axis=0) != 0]
        
        if len(non_numeric_cols) > 0:
            result = pd.concat([df[non_numeric_cols], numeric_df], axis=1)
            result = result.loc[numeric_df.index]
        else:
            result = numeric_df
        
        return result
    
    # Выбираем столбцы для каждого металла
    selected_columns = ['name']
    metal_selected_wavelengths = {}
    
    for metal, wavelengths in metal_wavelengths.items():
        if wavelengths:
            selected_wavelengths = select_closest_wavelengths(wavelengths)
            selected_cols = [col for col, _ in selected_wavelengths]
            selected_columns.extend(selected_cols)
            metal_selected_wavelengths[metal] = selected_cols
    
    result_df = df[selected_columns].copy()
    
    final_df = pd.DataFrame()
    final_df['name'] = result_df['name']
    
    metal_mean_data = {}
    metal_std_data = {}
    metal_raw_data = {}
    
    for metal, cols in metal_selected_wavelengths.items():
        metal_raw_data[metal] = result_df[cols]
    
    for metal, metal_df in metal_raw_data.items():
        if metal_df.shape[1] > 1:
            metal_mean_data[metal] = metal_df.mean(axis=1)
            metal_std_data[metal] = metal_df.std(axis=1)
        else:
            metal_mean_data[metal] = metal_df.iloc[:, 0]
            metal_std_data[metal] = pd.Series(0, index=metal_df.index)
    
    for metal in metal_mean_data.keys():
        final_df[f'{metal}'] = metal_mean_data[metal]
    
    for metal in metal_std_data.keys():
        final_df[f'd{metal}'] = metal_std_data[metal]
    
    sorted_columns = ['name']
    metals_sorted = sorted(metal_mean_data.keys())

    for metal in metals_sorted:
        sorted_columns.append(f'{metal}')
    
    for metal in metals_sorted:
        sorted_columns.append(f'd{metal}')
    
    final_df = final_df[sorted_columns]
    final_df = remove_zero_sum_rows_columns_safe(final_df)
    
    wavelengths_info = []
    for metal in metals_sorted:
        cols = metal_selected_wavelengths.get(metal, [])
        if cols:
            wl_list = []
            for col in cols:
                parts = col.split(' ')
                if len(parts) >= 2:
                    wl_list.append(parts[1])
            wavelengths_info.append({
                'Металл': metal,
                'Количество_длин_волн': len(cols),
                'Длины_волн': ', '.join(wl_list)
            })
    
    wavelengths_df = pd.DataFrame(wavelengths_info)
    
    # Если указан путь к базе данных, выполняем интеграцию
    if json_data_path:
        try:
            print("\n" + "="*60)
            print("[ИНТЕГРАЦИЯ] НАЧАЛО ИНТЕГРАЦИИ С БАЗОЙ ДАННЫХ")
            print("="*60)
            
            # Конвертируем DataFrame в формат базы данных
            json_data = convert_df_to_database_format(final_df, json_data_path)
            
            # Выполняем постобработку базы данных
            postprocess_result = postprocess_json_database(Path(json_data_path))
            
            # Объединяем пробы по нумерации
            merge_result = merge_probes_by_numbering(Path(json_data_path))
            
            # Пересчитываем концентрации
            recalculation_result = recalculate_concentrations(Path(json_data_path))
            
            print("\n" + "="*60)
            print("[ИНТЕГРАЦИЯ] ИНТЕГРАЦИЯ ЗАВЕРШЕНА УСПЕШНО")
            print("="*60)
            
        except Exception as e:
            print(f"\n[ОШИБКА] Ошибка при интеграции с базой данных: {e}")
            import traceback
            traceback.print_exc()
    
    for column_name, value_list in black_dict.items():
        final_df[column_name] = value_list
    
    print(final_df)
    return final_df, wavelengths_df

def convert_df_to_database_format(df: pd.DataFrame, json_data_path: str) -> List[Dict]:
    """
    Конвертирует DataFrame с данными ИСП АЭС в формат базы данных
    и интегрирует с существующей базой
    """
    json_path = Path(json_data_path)
    data = load_json_data(json_path)
    
    # Загружаем существующие пробы
    existing_probes = data.get('probes', [])
    
    # Создаем словарь существующих проб по имени для быстрого поиска
    existing_probes_dict = {}
    for probe in existing_probes:
        name = probe.get('name')
        if name:
            existing_probes_dict[name] = probe
    
    # Конвертируем DataFrame в список проб
    new_probes = []
    for _, row in df.iterrows():
        probe_name = row['Название пробы']
        
        # Создаем новую пробу
        new_probe = {
            'name': probe_name,
            'tags': ['импорт_исп_аэс', pd.Timestamp.now().strftime('%Y-%m-%d')]
        }
        
        # Добавляем концентрации металлов
        for col in df.columns:
            if col != 'Название пробы':
                if pd.notna(row[col]):
                    new_probe[col] = float(row[col])
        
        # Проверяем, существует ли уже такая проба
        if probe_name in existing_probes_dict:
            # Обновляем существующую пробу
            existing_probe = existing_probes_dict[probe_name]
            # Объединяем данные
            for key, value in new_probe.items():
                if key not in ['id', 'created_at']:  # Не перезаписываем системные поля
                    existing_probe[key] = value
            # Обновляем теги
            if 'tags' not in existing_probe:
                existing_probe['tags'] = []
            existing_probe['tags'].extend(new_probe.get('tags', []))
            # Убираем дубликаты тегов
            if 'tags' in existing_probe:
                existing_probe['tags'] = list(set(existing_probe['tags']))
        else:
            # Добавляем новую пробу
            new_probes.append(new_probe)
    
    # Добавляем новые пробы к существующим
    existing_probes.extend(new_probes)
    
    # Обновляем данные
    data['probes'] = existing_probes
    
    # Сохраняем изменения
    save_json_data(json_path, data)
    
    # Отчет
    print(f"[ИНТЕГРАЦИЯ] Загружено строк из CSV: {len(df)}")
    print(f"[ИНТЕГРАЦИЯ] Обновлено существующих проб: {len(df) - len(new_probes)}")
    print(f"[ИНТЕГРАЦИЯ] Добавлено новых проб: {len(new_probes)}")
    print(f"[ИНТЕГРАЦИЯ] Всего проб в базе: {len(existing_probes)}")
    
    return new_probes


# Пример использования функции
if __name__ == "__main__":
    # Сохраните ваш CSV файл и укажите путь к нему
    file_path = r"C:\Users\Kirill\Desktop\all-Norilsk_isp.csv"
    
    # Укажите путь к базе данных
    json_data_path = str(Path(__file__).parent.parent / 'data' / 'data.json')  # Измените на актуальный путь
    
    try:
        processed_data, wavelengths_info = process_icp_aes_data(file_path)
        
        print("\nОбработанные данные (первые 5 строк):")
        print(processed_data.head())
        print(f"\nРазмер таблицы: {processed_data.shape}")
        
        print("\nИнформация о выбранных длинах волн:")
        print(wavelengths_info)
        
        print("\nСтолбцы таблицы с данными:")
        columns_list = list(processed_data.columns)
        for i, col in enumerate(columns_list, 1):
            print(f"{i:3d}. {col}")
        
        output_data_path = "Обработанные_данные_ИСП_АЭС_с_погрешностями.csv"
        output_wl_path = "Информация_о_длинах_волн.csv"
        
        processed_data.to_csv(output_data_path, index=False, encoding='utf-8-sig',sep=';')
        wavelengths_info.to_csv(output_wl_path, index=False, encoding='utf-8-sig')
        
        print(f"\nОсновные данные сохранены в файл: {output_data_path}")
        print(f"Информация о длинах волн сохранена в файл: {output_wl_path}")
        
    except FileNotFoundError:
        print(f"Файл {file_path} не найден. Убедитесь, что файл существует.")
    except Exception as e:
        print(f"Произошла ошибка при обработке данных: {e}")
        import traceback
        traceback.print_exc()