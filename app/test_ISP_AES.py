import pandas as pd
import numpy as np
from scipy.spatial.distance import pdist, squareform

def process_icp_aes_data(file_path):
    """
    Обрабатывает данные ИСП АЭС:
    1. Удаляет строки со значением 'некал'
    2. Заменяет ячейки с 'uv' или 'ox' на 0
    3. Удаляет 'x' из значений, оставляя только число
    4. Группирует данные по металлам, выбирая 3 наиболее близких значения длин волн
    5. Вычисляет среднее значение и стандартную погрешность (стандартное отклонение) для каждого металла
    6. Возвращает таблицу с пробами, средними концентрациями металлов и их погрешностями
    """
    
    # Чтение данных из CSV файла
    df = pd.read_csv(file_path, sep=';', decimal='.', encoding='utf-8')
    
    # Удаление строк, где в столбце 'Метки Образцов' есть 'некал' или пустые строки
    df = df[~df['Метки Образцов'].astype(str).str.contains('некал', case=False, na=False)]
    df = df[df['Метки Образцов'].astype(str).str.strip() != '']
    
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
            # Удаляем 'x' и пробелы, конвертируем в число
            cleaned = val_str.lower().replace('ox', '').strip()
            try:
                # Заменяем запятую на точку для корректного преобразования в float
                #cleaned = cleaned.replace(',', '.')
                return float(cleaned)
            except:
                raise ValueError('Ошибка удаления ox')
                   
        # Если содержит 'x' - удаляем 'x' и оставляем число
        if 'x' in val_str.lower():
            # Удаляем 'x' и пробелы, конвертируем в число
            cleaned = val_str.lower().replace('x', '').strip()
            try:
                # Заменяем запятую на точку для корректного преобразования в float
                #cleaned = cleaned.replace(',', '.')
                return float(cleaned)
            except:
                print(cleaned)
                raise ValueError('Ошибка удаления x')
            

        
        # Пытаемся преобразовать в число
        try:
            # Заменяем запятую на точку
            val_str = val_str.replace(',', '.')
            return float(val_str)
        except:
            return 0
    
    # Применяем очистку ко всем столбцам, кроме 'Проб' и 'Метки Образцов'
    for col in df.columns:
        if col not in ['Проб', 'Метки Образцов']:
            df[col] = df[col].apply(clean_value)
    
    # Удаляем строки, где все значения NaN (после удаления 'некал')
    df = df.dropna(how='all', subset=[col for col in df.columns if col not in ['Проб', 'Метки Образцов']])
    
    # Определяем металлы и их длины волн
    metal_wavelengths = {}
    for col in df.columns:
        if col not in ['Проб', 'Метки Образцов']:
            # Извлекаем название металла и длину волны
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
        
        # Преобразуем длины волн в числа
        wavelength_values = []
        wl_data = []
        for col, wl in wavelength_list:
            try:
                # Извлекаем числовую часть
                wl_num = float(''.join(filter(lambda x: x.isdigit() or x == '.', wl)))
                wavelength_values.append(wl_num)
                wl_data.append((col, wl, wl_num))
            except:
                wl_num = 0
                wavelength_values.append(wl_num)
                wl_data.append((col, wl, wl_num))
        
        # Вычисляем попарные расстояния между длинами волн
        if len(wavelength_values) > 1:
            distances = pdist(np.array(wavelength_values).reshape(-1, 1))
            distance_matrix = squareform(distances)
            
            # Находим индексы 3 наиболее близких точек
            # Суммируем расстояния до всех других точек для каждой длины волны
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
        # Выбираем только числовые столбцы
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        non_numeric_cols = df.select_dtypes(exclude=[np.number]).columns
        
        # Работаем только с числовой частью
        numeric_df = df[numeric_cols].copy()
        
        # Удаляем нулевые строки и столбцы
        numeric_df = numeric_df[numeric_df.sum(axis=1) != 0]
        numeric_df = numeric_df.loc[:, numeric_df.sum(axis=0) != 0]
        
        # Объединяем с нечисловыми столбцами, если они есть
        if len(non_numeric_cols) > 0:
            result = pd.concat([df[non_numeric_cols], numeric_df], axis=1)
            # Удаляем строки, которые были удалены из числовой части
            result = result.loc[numeric_df.index]
        else:
            result = numeric_df
        
        return result
    
    # Выбираем столбцы для каждого металла
    selected_columns = ['Метки Образцов']
    metal_selected_wavelengths = {}  # Словарь для хранения выбранных длин волн по металлам
    
    for metal, wavelengths in metal_wavelengths.items():
        if wavelengths:
            selected_wavelengths = select_closest_wavelengths(wavelengths)
            selected_cols = [col for col, _ in selected_wavelengths]
            selected_columns.extend(selected_cols)
            metal_selected_wavelengths[metal] = selected_cols
    
    # Создаем DataFrame с выбранными столбцами
    result_df = df[selected_columns].copy()
    
    # Группируем по металлам (вычисляем среднее и стандартное отклонение для одного металла)
    final_df = pd.DataFrame()
    final_df['Название пробы'] = result_df['Метки Образцов']
    
    # Словари для хранения данных по металлам
    metal_mean_data = {}  # Средние значения
    metal_std_data = {}   # Стандартные отклонения
    metal_raw_data = {}   # Исходные данные по выбранным длинам волн
    
    # Сначала собираем все данные по металлам
    for metal, cols in metal_selected_wavelengths.items():
        metal_raw_data[metal] = result_df[cols]
    
    # Теперь вычисляем средние и стандартные отклонения
    for metal, metal_df in metal_raw_data.items():
        if metal_df.shape[1] > 1:
            # Вычисляем среднее значение по всем длинам волн для данного металла
            metal_mean_data[metal] = metal_df.mean(axis=1)
            # Вычисляем стандартное отклонение
            metal_std_data[metal] = metal_df.std(axis=1)
        else:
            # Если только одно значение, используем его как среднее, а погрешность = 0
            metal_mean_data[metal] = metal_df.iloc[:, 0]
            metal_std_data[metal] = pd.Series(0, index=metal_df.index)
    
    # Добавляем столбцы со средними значениями
    for metal in metal_mean_data.keys():
        final_df[f'{metal}'] = metal_mean_data[metal]
    
    # Добавляем столбцы со стандартными погрешностями
    for metal in metal_std_data.keys():
        final_df[f'd{metal}'] = metal_std_data[metal]
    
    # Сортируем столбцы для удобства: сначала средние, затем погрешности
    sorted_columns = ['Название пробы']
    
    # Сортируем металлы в алфавитном порядке
    metals_sorted = sorted(metal_mean_data.keys())

    # Добавляем сначала все средние значения, затем все погрешности
    for metal in metals_sorted:
        sorted_columns.append(f'{metal}')
    
    for metal in metals_sorted:
        sorted_columns.append(f'd{metal}')
    
    # Переупорядочиваем DataFrame
    final_df = final_df[sorted_columns]
    
    final_df = remove_zero_sum_rows_columns_safe(final_df)
    
    # Создаем отдельный DataFrame с информацией о выбранных длинах волн для каждого металла
    wavelengths_info = []
    for metal in metals_sorted:
        cols = metal_selected_wavelengths.get(metal, [])
        if cols:
            # Извлекаем длины волн из названий столбцов
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
        print(processed_data.head())
        print(f"\nРазмер таблицы: {processed_data.shape}")
        
        print("\nИнформация о выбранных длинах волн:")
        print(wavelengths_info)
        
        # Выводим список столбцов
        print("\nСтолбцы таблицы с данными:")
        columns_list = list(processed_data.columns)
        for i, col in enumerate(columns_list, 1):
            print(f"{i:3d}. {col}")
        
        # Сохранение в CSV файлы
        output_data_path = "Обработанные_данные_ИСП_АЭС_с_погрешностями.csv"
        output_wl_path = "Информация_о_длинах_волн.csv"
        
        processed_data.to_csv(output_data_path, index=False, encoding='utf-8-sig')
        wavelengths_info.to_csv(output_wl_path, index=False, encoding='utf-8-sig')
        
        print(f"\nОсновные данные сохранены в файл: {output_data_path}")
        print(f"Информация о длинах волн сохранена в файл: {output_wl_path}")
        
    except FileNotFoundError:
        print(f"Файл {file_path} не найден. Убедитесь, что файл существует.")
    except Exception as e:
        print(f"Произошла ошибка при обработке данных: {e}")
        import traceback
        traceback.print_exc()