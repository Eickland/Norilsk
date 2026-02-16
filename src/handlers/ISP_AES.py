import pandas as pd
import numpy as np
from scipy.spatial.distance import pdist, squareform
from typing import Any, Dict, List, Set, Tuple, Optional
import traceback
import re

from middleware.raw_data_processing import expand_sample_code, get_base_name, merge_similar_samples, clean_value_icp_aes

black_list_column = ['Разбавление', 'sample_mass', 'Масса навески (g)', 'Valiq, ml']

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
    df.rename(columns={f'{df.columns[0]}': 'name'}, inplace=True)
    
    # Удаление строк, где в столбце 'name' есть 'некал' или пустые строки
    df = df[~df['name'].astype(str).str.contains('некал', case=False, na=False)]
    df = df[df['name'].astype(str).str.strip() != '']
    
    black_dict = {}
    
    # Применяем очистку ко всем столбцам, кроме 'name'
    for col in df.columns:
        if col != 'name':
            df[col] = df[col].apply(clean_value_icp_aes)
    
    # Удаляем строки, где все значения NaN (после удаления 'некал')
    df = df.dropna(how='all', subset=[col for col in df.columns if col != 'name'])
    
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
            df.drop(columns=col, inplace=True)
    
    # Определяем металлы и их длины волн
    metal_wavelengths = {}
    
    for col in df.columns:
        if col != 'name':
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
        final_df[f'{metal}_AES'] = metal_mean_data[metal]
    
    for metal in metal_std_data.keys():
        final_df[f'd{metal}'] = metal_std_data[metal]
    
    sorted_columns = ['name']
    metals_sorted = sorted(metal_mean_data.keys())

    for metal in metals_sorted:
        sorted_columns.append(f'{metal}_AES')
    
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
    
    for column_name, value_list in black_dict.items():
        final_df[column_name] = value_list
        
    if 'Масса навески (g)' in final_df.columns:
        final_df['Масса навески (g)'] = final_df['Масса навески (g)'].apply(lambda x: x/1000 if x > 80 else x)
    
    return final_df, wavelengths_df

# Пример использования функции
if __name__ == "__main__":
    # Сохраните ваш CSV файл и укажите путь к нему
    file_path = r"C:\Users\Kirill\Downloads\Telegram Desktop\21-01-2026-Norilsk.csv"
    
    try:
        processed_data, wavelengths_info = process_icp_aes_data(file_path)
        
    except FileNotFoundError:
        print(f"Файл {file_path} не найден. Убедитесь, что файл существует.")
    except Exception as e:
        print(f"Произошла ошибка при обработке данных: {e}")
        traceback.print_exc()