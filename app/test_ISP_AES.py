import pandas as pd
import numpy as np
from scipy.spatial.distance import pdist, squareform
import re

def process_icp_aes_data(file_path):
    """
    Обрабатывает данные ИСП АЭС:
    1. Удаляет строки со значением 'некал'
    2. Заменяет ячейки с 'uv' или 'ox' на 0
    3. Удаляет 'x' из значений, оставляя только число
    4. Группирует данные по металлам, выбирая 3 наиболее близких значения длин волн
    5. Вычисляет среднее значение и стандартную погрешность (стандартное отклонение) для каждого металла
    6. Возвращает таблицу с пробами, средними концентрациями металлов и их погрешностями
    7. Удаляет из data.json пробы с именами "Стандарт 1" … "Стандарт 10"
    8. Находит пробу "BLNK", вычитает её значения металлов из всех остальных проб, после вычитания удаляет BLNK из базы
    9. Печатает в терминал отчёт (сколько удалено, найден ли BLNK, по каким полям вычитали)
    10. Делает бэкап data.json.bak
    """
    encoding = 'utf-8'
    sep = ';'
    # Чтение данных из CSV файла
    try:
        # Пробуем с точкой
        data = pd.read_csv(file_path, sep=sep, decimal='.', encoding=encoding)
        
        # Быстрая проверка на наличие чисел с запятой
        sample = data.head(100).to_string()  # Проверяем первые 100 строк
        if re.search(r'\d,\d{2}\b', sample):  # Ищем паттерн типа 123,45
            data = pd.read_csv(file_path, sep=sep, decimal=',', encoding=encoding)

    except:
        data = pd.read_csv(file_path, sep=sep, decimal=',', encoding=encoding)
    
    df = data
    
    df.rename(columns={f'{df.columns[0]}':'name'}, inplace=True)
    
    # Удаление строк, где в столбце 'Метки Образцов' есть 'некал' или пустые строки
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
    
    # Применяем очистку ко всем столбцам, кроме 'Проб' и 'Метки Образцов'
    for col in df.columns:
        if col not in ['Проб', 'name']:
            df[col] = df[col].apply(clean_value)
    
    # Удаляем строки, где все значения NaN (после удаления 'некал')
    df = df.dropna(how='all', subset=[col for col in df.columns if col not in ['Проб', 'name']])
    
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
    
    return final_df, wavelengths_df


# Пример использования функции
if __name__ == "__main__":
    # Сохраните ваш CSV файл и укажите путь к нему
    file_path = "металлы-01-12-2025 (2).csv"
    
    try:
        processed_data, wavelengths_info = process_icp_aes_data(file_path)
        
        print("Обработанные данные (первые 5 строк):")
        print(processed_data.head()) # type: ignore
        print(f"\nРазмер таблицы: {processed_data.shape}") # type: ignore
        
        print("\nИнформация о выбранных длинах волн:")
        print(wavelengths_info)
        
        print("\nСтолбцы таблицы с данными:")
        columns_list = list(processed_data.columns) # type: ignore
        for i, col in enumerate(columns_list, 1):
            print(f"{i:3d}. {col}")
        
        output_data_path = "Обработанные_данные_ИСП_АЭС_с_погрешностями.csv"
        output_wl_path = "Информация_о_длинах_волн.csv"
        
        processed_data.to_csv(output_data_path, index=False, encoding='utf-8-sig') # type: ignore
        wavelengths_info.to_csv(output_wl_path, index=False, encoding='utf-8-sig') # type: ignore
        
        print(f"\nОсновные данные сохранены в файл: {output_data_path}")
        print(f"Информация о длинах волн сохранена в файл: {output_wl_path}")
        
    except FileNotFoundError:
        print(f"Файл {file_path} не найден. Убедитесь, что файл существует.")
    except Exception as e:
        print(f"Произошла ошибка при обработке данных: {e}")
        import traceback
        traceback.print_exc()

    # ---------------------------------------------------------------------
    # ДОБАВЛЕННЫЙ БЛОК: чистка data.json (удаление стандартов, BLNK-субтракция, удаление BLNK)
    # ---------------------------------------------------------------------
    import json
    import shutil
    from pathlib import Path
    from typing import Any, Dict, List, Set


    JSON_PATH = Path("/mnt/data/data.json")   # при необходимости поменяйте путь
    BACKUP_PATH = JSON_PATH.with_suffix(".json.bak")

    STANDARD_NAMES = {f"Стандарт {i}" for i in range(1, 11)}
    BLANK_NAME = "BLNK"

    # ключи, которые точно не являются концентрациями
    NON_METAL_KEYS = {"name", "id", "tags", "status_id", "sample_mass"}


    def is_number(x: Any) -> bool:
        return isinstance(x, (int, float)) and not isinstance(x, bool)


    def postprocess_json_database(json_path: Path) -> None:
        if not json_path.exists():
            print(f"\n[JSON] Файл не найден: {json_path} — пропускаю постобработку базы.")
            return

        data = json.loads(json_path.read_text(encoding="utf-8"))

        if "probes" not in data or not isinstance(data["probes"], list):
            print("\n[JSON] В базе нет ключа 'probes' (ожидался список проб) — пропускаю.")
            return

        probes: List[Dict[str, Any]] = data["probes"]

        # 1) удалить стандарты
        removed_standards = [p for p in probes if p.get("name") in STANDARD_NAMES]
        probes = [p for p in probes if p.get("name") not in STANDARD_NAMES]

        # 2) найти BLNK
        blank = None
        for p in probes:
            if p.get("name") == BLANK_NAME:
                blank = p
                break

        metals_subtracted: Set[str] = set()
        if blank is not None:
            # вычитаем только по числовым полям BLNK, исключая d* и служебные ключи
            for key, bval in blank.items():
                if key in NON_METAL_KEYS:
                    continue
                if isinstance(key, str) and key.startswith("d"):
                    continue
                if not is_number(bval):
                    continue

                for p in probes:
                    if p is blank:
                        continue
                    if key in p and is_number(p[key]):
                        p[key] = p[key] - bval
                        metals_subtracted.add(key)

            # 3) удалить сам BLNK после вычитания
            probes = [p for p in probes if p.get("name") != BLANK_NAME]

        # сохранить
        data["probes"] = probes
        shutil.copy2(json_path, BACKUP_PATH)
        json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

        # отчёт
        print("\n[JSON] Постобработка базы завершена.")
        print(f"[JSON] Бэкап: {BACKUP_PATH}")
        print(f"[JSON] Удалено стандартов: {len(removed_standards)}")
        if removed_standards:
            for p in removed_standards:
                print(f"  - {p.get('name')} (id={p.get('id')})")

        if blank is None:
            print("[JSON] BLNK не найден: вычитание не выполнялось.")
        else:
            print("[JSON] BLNK найден: значения вычтены, затем BLNK удалён.")
            if metals_subtracted:
                print("[JSON] Поля, по которым вычитали BLNK:")
                for k in sorted(metals_subtracted):
                    print(f"  - {k}")
            else:
                print("[JSON] Не нашлось подходящих числовых полей для вычитания.")


    # запуск постобработки
    postprocess_json_database(JSON_PATH)
