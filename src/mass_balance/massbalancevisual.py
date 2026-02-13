import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Dict, Any

class MassBalanceVisualizer:
    def __init__(self, json_data: List[Dict[str, Any]]):
        self.data = json_data
        self.df = self._flatten_data()

    def _flatten_data(self) -> pd.DataFrame:
        """Преобразует сложный JSON в плоскую таблицу для удобного анализа."""
        rows = []
        for series_idx, series in enumerate(self.data):
            series_name = f"Series_{series_idx + 1}"
            elements_data = series.get("elements", {})
            
            for full_name, details in elements_data.items():
                # Определяем тип данных по суффиксу
                if full_name.endswith("_AES"):
                    base_element = full_name.replace("_AES", "")
                    data_type = "AES"
                elif full_name.endswith("_MS"):
                    base_element = full_name.replace("_MS", "")
                    data_type = "MS"
                else:
                    base_element = full_name
                    data_type = "Base"

                # Извлекаем данные по стадиям
                for stage_idx, stage_values in enumerate(details.get("stages", [])):
                    for probe_type, mass in stage_values.items():
                        rows.append({
                            "series": series_name,
                            "element": base_element,
                            "data_type": data_type,
                            "stage": stage_idx + 1,
                            "probe_type": probe_type,
                            "mass": float(mass)
                        })
        return pd.DataFrame(rows)

    def get_detailed_stats(self, elements: List[str] = None, series: List[str] = None): # type: ignore
        """Выводит сводную статистику по массам."""
        temp_df = self.df.copy()
        if elements:
            temp_df = temp_df[temp_df['element'].isin(elements)]
        if series:
            temp_df = temp_df[temp_df['series'].isin(series)]
            
        stats = temp_df.groupby(['element', 'data_type', 'stage'])['mass'].agg(['sum', 'mean', 'max']).reset_index()
        print("\n--- Подробная статистика масс по стадиям ---")
        print(stats.to_string(index=False))
        return stats

    def plot_stacked_mass_bar(self, element: str, data_type: str = "Base", selected_series: List[str] = None): # type: ignore
            """
            Строит столбчатую диаграмму с накоплением (Stacked Bar Chart):
            - X: Группы по стадиям, внутри каждой стадии — столбцы выбранных серий.
            - Y: Масса (сумма продуктов A, B, D, E, G).
            - Подписи: Короткое имя серии над каждым столбцом.
            """
            # Фильтрация данных
            plot_df = self.df[(self.df['element'] == element) & 
                            (self.df['data_type'] == data_type)]
            
            if selected_series:
                plot_df = plot_df[plot_df['series'].isin(selected_series)]

            if plot_df.empty:
                print(f"Данные не найдены.")
                return

            # Создаем сводную таблицу: Стадия + Серия | Продукты (A, B, D...)
            pivot_df = plot_df.pivot_table(
                index=['stage', 'series'], 
                columns='probe_type', 
                values='mass', 
                aggfunc='sum'
            ).fillna(0)

            # Цвета продуктов
            product_palette = {
                'A': '#3498db', 'B': '#e67e22', 'D': "#9aa09d", 'E': "#17daef", 'G': "#e913e1"
            }
            # Берем только те продукты, которые реально есть в данных
            available_probes = [p for p in ['A', 'B', 'D', 'E', 'G'] if p in pivot_df.columns]
            
            fig, ax = plt.subplots(figsize=(14, 8))
            
            # Определяем позиции для столбцов
            stages = pivot_df.index.get_level_values('stage').unique()
            series_in_data = pivot_df.index.get_level_values('series').unique()
            
            n_stages = len(stages)
            n_series = len(series_in_data)
            
            width = 0.8 / n_series  # Ширина одного столбца серии
            short_name_list = ['9-1','9-2','10-1','10-2','11-1','11-2']
            for i, stage in enumerate(stages):
                for j, series in enumerate(series_in_data):
                    if (stage, series) not in pivot_df.index:
                        continue
                    
                    data = pivot_df.loc[(stage, series)]
                    bottom = 0
                    x_pos = i + (j - (n_series - 1) / 2) * width
                    
                    # Рисуем сегменты (накопление)
                    for probe in available_probes:
                        val = data[probe]
                        if val > 0: # type: ignore
                            ax.bar(x_pos, val, width, bottom=bottom, 
                                color=product_palette.get(probe, '#gray'),
                                edgecolor='white', linewidth=0.5)
                            bottom += val
                    
                    # Добавляем укороченное название серии (например, S1, S2)
                    short_name = short_name_list[j]
                    ax.text(x_pos, bottom + (bottom * 0.01), short_name,  # type: ignore
                            ha='center', va='bottom', fontsize=9, rotation=0)

            # Настройка оформления
            ax.set_xticks(range(len(stages)))
            ax.set_xticklabels([f"Стадия {s}" for s in stages], fontsize=12)
            ax.set_title(f"Распределение {element} по продуктам ({data_type}), плотность твердого 3.2", fontsize=16)
            ax.set_ylabel("Масса (г)", fontsize=12)
            
            # Легенда
            from matplotlib.lines import Line2D
            legend_elements = [Line2D([0], [0], color=product_palette[p], lw=4, label=f"Продукт {p}") 
                            for p in available_probes]
            ax.legend(handles=legend_elements, title="Продукты", loc='upper right')

            plt.tight_layout()
# --- Пример использования ---

# Загрузка данных (замените на чтение вашего файла)
with open(r"C:\Users\Kirill\Desktop\massbalance_api.json", 'r', encoding='utf-8') as f:
     raw_data = json.load(f)


viz = MassBalanceVisualizer(raw_data)

# 1. Вывод статистики по серебру (Ag)
#viz.get_detailed_stats(elements=["mNi"])

# 2. Визуализация масс для mAg (базовые данные)
#viz.plot_element_mass_bar(element="mAg", data_type="Base")

# 3. Сравнение данных из ИСП МС
elem_list = ['mNi','mCu','mK','mPt','mPd','mRu','mMn','mCo','mZn','mAl']
for em in elem_list:
    viz.plot_stacked_mass_bar(element=em, data_type="MS",selected_series=['Series_18','Series_19','Series_20','Series_21','Series_22','Series_23'])
    plt.savefig(fr"C:\Users\Kirill\Desktop\MassBalance\Плотность 3500, {em}.png")
    plt.close()