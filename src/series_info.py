import json
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional, Union
from dataclasses import dataclass
from middleware.series_worker import get_probe_type, get_source_class_from_probe, get_series_dicts
from mass_balance.series_analyzer import analyze_series, SeriesInfo, ProbeInfo, load_data

# Конфигурация
BASE_DIR = Path(__file__).parent.parent
DATA_FILE = BASE_DIR / 'data' / 'data.json'

@dataclass
class SearchCondition:
    """Условие поиска серий"""
    probe_type: str  # Тип пробы (например, 'start_B', 'st2_B' и т.д.)
    metal: str       # Название металла (например, 'Au', 'Ag', 'Cu')
    min_value: Optional[float] = None  # Минимальное значение массы
    max_value: Optional[float] = None  # Максимальное значение массы
    exact_value: Optional[float] = None  # Точное значение (если указано, min и max игнорируются)

@dataclass
class RatioCondition:
    """
    Условие поиска серий по отношению (концентрации)
    По умолчанию рассчитывает массу металла / sample_mass
    """
    probe_type: str  # Тип пробы
    metal: str       # Название металла
    denominator_field: str = 'sample_mass'  # Поле-знаменатель (по умолчанию общая масса)
    min_ratio: Optional[float] = None  # Минимальное отношение
    max_ratio: Optional[float] = None  # Максимальное отношение
    exact_ratio: Optional[float] = None  # Точное отношение
    unit: str = 'мг/кг'  # Единица измерения для отображения

@dataclass
class SearchResult:
    """Результат поиска серии"""
    series_key: Tuple[str, int, int]
    probe: Dict[str, Any]
    metal_value: float
    condition: Union[SearchCondition, RatioCondition]
    series_info: Optional[SeriesInfo] = None
    ratio_value: Optional[float] = None  # Для RatioCondition

def find_series_by_metal_mass(
    conditions: Union[SearchCondition, List[SearchCondition]],
    data_file: str = str(DATA_FILE)
) -> List[SearchResult]:
    """
    Поиск серий по условию попадания значения массы металла в заданный диапазон
    в пробах определенного типа.
    """
    # Загружаем данные
    with open(data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    probes = data.get('probes', [])
    
    if not probes:
        raise ValueError('Нет базы данных или она пуста')
    
    # Преобразуем одиночное условие в список
    if isinstance(conditions, SearchCondition):
        conditions = [conditions]
    
    # Получаем все серии для контекста
    series_list, _ = analyze_series()
    series_dict = {(s.series_key): s for s in series_list}
    
    results = []
    
    for probe in probes:
        probe_type_info = get_probe_type(probe)
        if not probe_type_info:
            continue
            
        probe_type, method_number, exp_number = probe_type_info
        source_class = get_source_class_from_probe(probe)
        
        if not source_class:
            continue
        
        series_key = (source_class, method_number, exp_number)
        
        # Проверяем каждое условие
        for condition in conditions:
            # Проверяем тип пробы
            if probe_type != condition.probe_type:
                continue
            
            # Проверяем наличие металла в пробе
            metal_key = f'm_{condition.metal.lower()}'
            if metal_key not in probe:
                # Пробуем другие возможные форматы
                metal_key = condition.metal
                if metal_key not in probe:
                    continue
            
            metal_value = probe.get(metal_key)
            
            # Проверяем, что значение числовое
            if not isinstance(metal_value, (int, float)):
                continue
            
            # Проверяем условие
            if condition.exact_value is not None:
                if abs(metal_value - condition.exact_value) < 1e-10:
                    matches = True
                else:
                    matches = False
            else:
                matches = True
                if condition.min_value is not None:
                    matches = matches and metal_value >= condition.min_value
                if condition.max_value is not None:
                    matches = matches and metal_value <= condition.max_value
            
            if matches:
                results.append(SearchResult(
                    series_key=series_key,
                    probe=probe,
                    metal_value=metal_value,
                    condition=condition,
                    series_info=series_dict.get(series_key)
                ))
                break  # Нашли совпадение по одному из условий
    
    return results

def find_series_by_metal_ratio(
    conditions: Union[RatioCondition, List[RatioCondition]],
    data_file: str = str(DATA_FILE)
) -> List[SearchResult]:
    """
    Поиск серий по условию попадания отношения массы металла к другому полю
    (концентрации) в заданный диапазон.
    
    Args:
        conditions: Одно условие или список условий для поиска
        data_file: Путь к файлу с данными
    
    Returns:
        List[SearchResult]: Список результатов поиска
    """
    # Загружаем данные
    with open(data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    probes = data.get('probes', [])
    
    if not probes:
        raise ValueError('Нет базы данных или она пуста')
    
    # Преобразуем одиночное условие в список
    if isinstance(conditions, RatioCondition):
        conditions = [conditions]
    
    # Получаем все серии для контекста
    series_list, _ = analyze_series()
    series_dict = {(s.series_key): s for s in series_list}
    
    results = []
    
    for probe in probes:
        probe_type_info = get_probe_type(probe)
        if not probe_type_info:
            continue
            
        probe_type, method_number, exp_number = probe_type_info
        source_class = get_source_class_from_probe(probe)
        
        if not source_class:
            continue
        
        series_key = (source_class, method_number, exp_number)
        
        # Проверяем каждое условие
        for condition in conditions:
            # Проверяем тип пробы
            if probe_type != condition.probe_type:
                continue
            
            # Проверяем наличие металла в пробе
            metal_key = f'm_{condition.metal.lower()}'
            if metal_key not in probe:
                metal_key = condition.metal
                if metal_key not in probe:
                    continue
            
            metal_value = probe.get(metal_key)
            
            # Проверяем наличие поля-знаменателя
            denominator_value = probe.get(condition.denominator_field)
            
            # Проверяем, что значения числовые и знаменатель не равен нулю
            if not isinstance(metal_value, (int, float)) or not isinstance(denominator_value, (int, float)):
                continue
            
            if denominator_value == 0:
                continue
            
            # Рассчитываем отношение
            ratio = metal_value*1000 / denominator_value
            
            # Проверяем условие
            if condition.exact_ratio is not None:
                if abs(ratio - condition.exact_ratio) < 1e-10:
                    matches = True
                else:
                    matches = False
            else:
                matches = True
                if condition.min_ratio is not None:
                    matches = matches and ratio >= condition.min_ratio
                if condition.max_ratio is not None:
                    matches = matches and ratio <= condition.max_ratio
            
            if matches:
                results.append(SearchResult(
                    series_key=series_key,
                    probe=probe,
                    metal_value=metal_value,
                    condition=condition,
                    series_info=series_dict.get(series_key),
                    ratio_value=ratio
                ))
                break  # Нашли совпадение по одному из условий
    
    return results

def find_series_by_multiple_conditions(
    conditions: List[Union[SearchCondition, RatioCondition]],
    match_all: bool = False,
    data_file: str = str(DATA_FILE)
) -> List[SearchResult]:
    """
    Поиск серий по нескольким условиям (смешанные типы условий).
    
    Args:
        conditions: Список условий (могут быть SearchCondition и RatioCondition)
        match_all: Если True, серия должна удовлетворять всем условиям.
                  Если False, достаточно любого условия.
        data_file: Путь к файлу с данными
    
    Returns:
        List[SearchResult]: Список результатов поиска
    """
    # Разделяем условия по типам
    mass_conditions = [c for c in conditions if isinstance(c, SearchCondition)]
    ratio_conditions = [c for c in conditions if isinstance(c, RatioCondition)]
    
    if match_all:
        # Получаем результаты для каждого типа условий
        all_results = []
        
        if mass_conditions:
            mass_results = find_series_by_metal_mass(mass_conditions, data_file)
            all_results.append({r.series_key: r for r in mass_results})
        
        if ratio_conditions:
            ratio_results = find_series_by_metal_ratio(ratio_conditions, data_file)
            all_results.append({r.series_key: r for r in ratio_results})
        
        # Находим пересечение множеств
        if all_results:
            common_keys = set.intersection(*[set(r.keys()) for r in all_results])
            
            final_results = []
            for key in common_keys:
                # Берем первый результат (все они относятся к одной серии)
                result = all_results[0][key]
                final_results.append(result)
            
            return final_results
        else:
            return []
    else:
        # Объединяем результаты из всех условий
        all_results = []
        
        if mass_conditions:
            all_results.extend(find_series_by_metal_mass(mass_conditions, data_file))
        
        if ratio_conditions:
            all_results.extend(find_series_by_metal_ratio(ratio_conditions, data_file))
        
        # Удаляем дубликаты (одна серия может попасть под несколько условий)
        unique_results = {}
        for result in all_results:
            if result.series_key not in unique_results:
                unique_results[result.series_key] = result
        
        return list(unique_results.values())

def visualize_search_results(
    results: List[SearchResult],
    title: str = "Результаты поиска серий",
    figsize: Tuple[int, int] = (14, 7),
    save_path: Optional[str] = None,
    show_ratios: bool = True
) -> None:
    """
    Визуализация результатов поиска серий (массы и/или концентрации).
    
    Args:
        results: Список результатов поиска
        title: Заголовок графика
        figsize: Размер графика
        save_path: Путь для сохранения графика
        show_ratios: Показывать ли концентрации (если есть)
    """
    if not results:
        print("Нет результатов для визуализации")
        return
    
    # Разделяем результаты на массовые и концентрационные
    mass_results = [r for r in results if isinstance(r.condition, SearchCondition)]
    ratio_results = [r for r in results if isinstance(r.condition, RatioCondition)]
    
    n_plots = 0
    if mass_results:
        n_plots += 1
    if ratio_results and show_ratios:
        n_plots += 1
    
    if n_plots == 0:
        print("Нет данных для визуализации")
        return
    
    # Создаем фигуру с подграфиками
    fig, axes = plt.subplots(1, n_plots, figsize=figsize, squeeze=False)
    fig.suptitle(title, fontsize=14, fontweight='bold')
    
    plot_idx = 0
    
    # Визуализация массовых результатов
    if mass_results:
        ax = axes[0, plot_idx] if n_plots > 1 else axes[0, 0]
        
        # Группируем по условиям
        condition_groups = {}
        for result in mass_results:
            condition_key = f"{result.condition.probe_type}_{result.condition.metal}"
            if condition_key not in condition_groups:
                condition_groups[condition_key] = {
                    'condition': result.condition,
                    'results': [],
                    'values': []
                }
            condition_groups[condition_key]['results'].append(result)
            condition_groups[condition_key]['values'].append(result.metal_value)
        
        # Создаем grouped bar chart
        x_pos = np.arange(len(condition_groups))
        bar_width = 0.8 / max(len(g['results']) for g in condition_groups.values()) if condition_groups else 1
        
        for idx, (condition_key, group) in enumerate(condition_groups.items()):
            condition = group['condition']
            values = group['values']
            
            # Создаем подписи для осей X
            x_labels = [f"{r.series_key[0]}-{r.series_key[1]}-{r.series_key[2]}" 
                       for r in group['results']]
            
            # Рисуем столбцы для каждой серии
            for i, (x_label, val) in enumerate(zip(x_labels, values)):
                bar_x = idx + (i - len(x_labels)/2) * bar_width
                bar = ax.bar(bar_x, val, width=bar_width, color='skyblue', 
                           edgecolor='navy', alpha=0.7)
                
                # Добавляем значение
                ax.text(bar_x, val, f'{val:.2f}', ha='center', va='bottom', fontsize=8)
            
            # Добавляем диапазон, если задан
            if condition.min_value is not None or condition.max_value is not None:
                if condition.min_value is not None:
                    ax.axhline(y=condition.min_value, xmin=idx/len(condition_groups), 
                             xmax=(idx+1)/len(condition_groups), color='red', 
                             linestyle='--', alpha=0.5, linewidth=1)
                if condition.max_value is not None:
                    ax.axhline(y=condition.max_value, xmin=idx/len(condition_groups), 
                             xmax=(idx+1)/len(condition_groups), color='green', 
                             linestyle='--', alpha=0.5, linewidth=1)
        
        ax.set_xticks(range(len(condition_groups)))
        ax.set_xticklabels([f"{g['condition'].probe_type}\n{g['condition'].metal}" 
                           for g in condition_groups.values()], fontsize=9)
        ax.set_ylabel('Масса металла', fontsize=10)
        ax.set_title('Поиск по массе металла', fontsize=11)
        ax.grid(True, alpha=0.3, axis='y')
        
        plot_idx += 1
    
    # Визуализация концентрационных результатов
    if ratio_results and show_ratios:
        ax = axes[0, plot_idx] if n_plots > 1 else axes[0, 0]
        
        # Группируем по условиям
        condition_groups = {}
        for result in ratio_results:
            condition_key = f"{result.condition.probe_type}_{result.condition.metal}"
            if condition_key not in condition_groups:
                condition_groups[condition_key] = {
                    'condition': result.condition,
                    'results': [],
                    'values': []
                }
            condition_groups[condition_key]['results'].append(result)
            condition_groups[condition_key]['values'].append(result.ratio_value)
        
        # Создаем grouped bar chart
        bar_width = 0.8 / max(len(g['results']) for g in condition_groups.values()) if condition_groups else 1
        
        for idx, (condition_key, group) in enumerate(condition_groups.items()):
            condition = group['condition']
            values = group['values']
            
            # Создаем подписи для осей X
            x_labels = [f"{r.series_key[0]}-{r.series_key[1]}-{r.series_key[2]}" 
                       for r in group['results']]
            x_pos = range(len(x_labels))
            bars = ax.bar(x_pos, values, color='skyblue', edgecolor='navy', alpha=0.7)
            # Рисуем столбцы для каждой серии
            for bar, val in zip(bars, values):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                    f'{val:.2f}', ha='center', va='bottom', fontsize=12)
            
            # Добавляем диапазон, если задан
            if condition.min_ratio is not None or condition.max_ratio is not None:
                if condition.min_ratio is not None:
                    ax.axhline(y=condition.min_ratio, xmin=idx/len(condition_groups), 
                             xmax=(idx+1)/len(condition_groups), color='red', 
                             linestyle='--', alpha=0.5, linewidth=1)
                if condition.max_ratio is not None:
                    ax.axhline(y=condition.max_ratio, xmin=idx/len(condition_groups), 
                             xmax=(idx+1)/len(condition_groups), color='green', 
                             linestyle='--', alpha=0.5, linewidth=1)
        
        #ax.set_xticks(range(len(condition_groups)))
        ax.set_xticks(x_pos) # type: ignore
        ax.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=13) # type: ignore
        
        # Определяем единицы измерения для подписи оси Y
        if condition_groups:
            sample_condition = list(condition_groups.values())[0]['condition']
            denominator = sample_condition.denominator_field
            unit = getattr(sample_condition, 'unit', '')
            ax.set_ylabel(f'Концентрация ({unit})', fontsize=10)
        
        ax.set_title(f'Поиск по концентрации (масса/{denominator if denominator else "sample_mass"})',  # type: ignore
                    fontsize=11)
        ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"График сохранен в {save_path}")
    
    plt.show()
    
def print_search_results_summary(results: List[SearchResult]) -> None:
    """
    Вывод сводки по результатам поиска в консоль.
    """
    if not results:
        print("❌ Не найдено серий, удовлетворяющих условиям")
        return
    
    print(f"\n{'='*60}")
    print(f"РЕЗУЛЬТАТЫ ПОИСКА: НАЙДЕНО {len(results)} СЕРИЙ")
    print(f"{'='*60}")
    
    # Группируем по типам условий
    mass_results = [r for r in results if isinstance(r.condition, SearchCondition)]
    ratio_results = [r for r in results if isinstance(r.condition, RatioCondition)]
    
    if mass_results:
        print(f"\n📊 Поиск по массе металла: {len(mass_results)} серий")
        condition_counts = {}
        for result in mass_results:
            key = f"{result.condition.probe_type} - {result.condition.metal}"
            condition_counts[key] = condition_counts.get(key, 0) + 1
        
        for condition, count in condition_counts.items():
            print(f"  • {condition}: {count} серий")
    
    if ratio_results:
        print(f"\n📈 Поиск по концентрации: {len(ratio_results)} серий")
        condition_counts = {}
        for result in ratio_results:
            denominator = result.condition.denominator_field # type: ignore
            key = f"{result.condition.probe_type} - {result.condition.metal}/{denominator}"
            condition_counts[key] = condition_counts.get(key, 0) + 1
        
        for condition, count in condition_counts.items():
            unit = result.condition.unit if hasattr(result.condition, 'unit') else '' # type: ignore
            print(f"  • {condition}: {count} серий ({unit})")
    
    print("\n📋 Детальная информация:")
    for i, result in enumerate(results, 1):
        source_class, method, exp = result.series_key
        
        print(f"\n  {i}. Серия: {source_class}-{method}-{exp}")
        
        if isinstance(result.condition, SearchCondition):
            print(f"     Тип пробы: {result.condition.probe_type}")
            print(f"     Металл: {result.condition.metal}")
            print(f"     Значение массы: {result.metal_value:.4f}")
            
            # Условия поиска
            cond = result.condition
            if cond.exact_value is not None:
                print(f"     Условие: точно = {cond.exact_value}")
            else:
                range_str = []
                if cond.min_value is not None:
                    range_str.append(f"≥ {cond.min_value}")
                if cond.max_value is not None:
                    range_str.append(f"≤ {cond.max_value}")
                print(f"     Условие: {' и '.join(range_str)}")
        
        elif isinstance(result.condition, RatioCondition):
            print(f"     Тип пробы: {result.condition.probe_type}")
            print(f"     Металл: {result.condition.metal}")
            print(f"     Знаменатель: {result.condition.denominator_field}")
            print(f"     Концентрация: {result.ratio_value:.6f} {result.condition.unit}")
            print(f"     Масса металла: {result.metal_value:.4f}")
            
            # Условия поиска
            cond = result.condition
            if cond.exact_ratio is not None:
                print(f"     Условие: точно = {cond.exact_ratio}")
            else:
                range_str = []
                if cond.min_ratio is not None:
                    range_str.append(f"≥ {cond.min_ratio}")
                if cond.max_ratio is not None:
                    range_str.append(f"≤ {cond.max_ratio}")
                print(f"     Условие: {' и '.join(range_str)}")
        
        # Информация о полноте серии
        if result.series_info:
            print(f"     Проб в серии: {len(result.series_info.probes_by_type)}/{len(result.series_info.all_types)}")
            if result.series_info.has_warnings:
                print(f"     ⚠️ Есть предупреждения по валидации")

def export_search_results_to_csv(
    results: List[SearchResult],
    filename: str = "search_results.csv"
) -> None:
    """
    Экспорт результатов поиска в CSV файл.
    """
    if not results:
        print("Нет результатов для экспорта")
        return
    
    data = []
    for result in results:
        source_class, method, exp = result.series_key
        probe = result.probe
        
        base_row = {
            'source_class': source_class,
            'method_number': method,
            'exp_number': exp,
            'probe_name': probe.get('name', ''),
            'probe_type': result.condition.probe_type,
            'metal': result.condition.metal,
            'metal_value': result.metal_value,
            'has_warnings': result.series_info.has_warnings if result.series_info else False
        }
        
        if isinstance(result.condition, SearchCondition):
            cond = result.condition
            base_row.update({
                'search_type': 'mass',
                'min_value': cond.min_value if cond.min_value is not None else '',
                'max_value': cond.max_value if cond.max_value is not None else '',
                'exact_value': cond.exact_value if cond.exact_value is not None else '',
                'ratio_value': '',
                'denominator_field': '',
                'unit': ''
            })
        
        elif isinstance(result.condition, RatioCondition):
            cond = result.condition
            base_row.update({
                'search_type': 'ratio',
                'min_value': cond.min_ratio if cond.min_ratio is not None else '',
                'max_value': cond.max_ratio if cond.max_ratio is not None else '',
                'exact_value': cond.exact_ratio if cond.exact_ratio is not None else '',
                'ratio_value': result.ratio_value,
                'denominator_field': cond.denominator_field,
                'unit': cond.unit
            })
        
        data.append(base_row)
    
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"Результаты экспортированы в {filename}")

# Пример использования
if __name__ == "__main__":
    print("="*60)
    print("ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ ФУНКЦИЙ ПОИСКА")
    print("="*60)
    """
    # Пример 1: Поиск по массе металла
    print("\n1. Поиск по массе Au в твердой фазе (10-50):")
    condition1 = SearchCondition(
        probe_type='start_B',
        metal='Au',
        min_value=10.0,
        max_value=50.0
    )
       
    results_mass = find_series_by_metal_mass(condition1)
    print_search_results_summary(results_mass)
    """ 
    # Пример 2: Поиск по концентрации (Au / sample_mass)
    print("\n2. Поиск по концентрации Ni:")
    condition2 = RatioCondition(
        probe_type='st6_E',
        metal='mNi_MS',
        denominator_field='sample_mass',
        min_ratio=0.0001,
        max_ratio=500.0,
        unit='мг/кг'
    )
    
    results_ratio = find_series_by_metal_ratio(condition2)
    print_search_results_summary(results_ratio)
    visualize_search_results(results_ratio,title='Концентрация никеля в ЖКК в мг/кг по данным ИСП МС')
    """
    # Пример 3: Смешанный поиск (любое условие)
    print("\n3. Смешанный поиск (Au по массе ИЛИ по концентрации):")
    conditions_mixed = [condition1, condition2]
    results_mixed = find_series_by_multiple_conditions(conditions_mixed, match_all=False)
    print_search_results_summary(results_mixed)
    
    # Визуализация результатов
    if results_mixed:
        visualize_search_results(results_mixed, "Результаты смешанного поиска")
        export_search_results_to_csv(results_mixed, "mixed_search_results.csv")
    """