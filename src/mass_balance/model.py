import pymc as pm
import numpy as np
import pandas as pd
from scipy import optimize
from typing import Dict, List, Tuple, Optional
import aesara.tensor as at
from dataclasses import dataclass

@dataclass
class ProcessParameters:
    """Параметры процесса для одной серии"""
    # Основные неизвестные величины
    solid_mass_st2: float
    solid_mass_st3: float  
    solid_mass_st4: float
    liquid_vol_st2: float
    liquid_vol_st3: float
    liquid_vol_st4: float
    product_D_mass: float  # скорректированная масса концентрата
    product_E_mass: float  # скорректированная масса хвостов
    product_G_vol: float   # скорректированный объем жидкости
    
    # Поправочные коэффициенты
    sampling_loss_factor: float  # потери при отборе проб
    process_loss_factor: float   # технологические потери
    analysis_bias: Dict[str, float]  # смещение анализа для каждого металла

class MassBalanceModel:
    """
    Иерархическая байесовская модель масс-баланса для всей серии экспериментов
    """
    
    def __init__(self, metal_list: List[str], series_data: Dict):
        """
        Args:
            metal_list: список металлов для баланса (15+ элементов)
            series_data: данные по всем сериям экспериментов
        """
        self.metals = metal_list
        self.series_data = series_data
        
        # Физические константы
        self.SOLID_DENSITY = 3.0  # г/мл, можно уточнить
        self.WATER_DENSITY = 1.0   # г/мл
        
        # Известные добавки реагентов
        self.reagents = {
            'H2SO4': {'density': 1.84, 'mass_field': 'Объем р-ра H2SO4 (ml)'},
            'CaOH2': {'density': 1.2, 'mass_field': 'Объем суспензии Ca(OH)2', 
                     'solid_fraction': 'Массовая доля Ca(OH)2'},
            'CaCO3': {'density': 1.2, 'mass_field': 'Объем суспензии CaCO3',
                     'solid_fraction': 'Массовая доля CaCO3'},
            'iron_pellets': {'mass_field': 'Масса железных окатышей (g)'}
        }
        
    def build_model(self):
        """
        Построение вероятностной модели в PyMC
        """
        
        with pm.Model() as model:
            # === 1. ГЛОБАЛЬНЫЕ ГИПЕРПРИОРЫ ===
            # Эти параметры общие для всех серий
            
            # Общие потери при отборе проб (0-5%)
            global_sampling_loss = pm.Beta('global_sampling_loss', 
                                          alpha=2, beta=50, 
                                          transform=pm.distributions.transforms.interval(0, 0.05))
            
            # Общие технологические потери (0-10%)
            global_process_loss = pm.Beta('global_process_loss',
                                        alpha=2, beta=20,
                                        transform=pm.distributions.transforms.interval(0, 0.1))
            
            # Стандартное отклонение для аналитических ошибок
            sigma_measurement = pm.HalfNormal('sigma_measurement', sigma=0.1)
            
            # === 2. ПАРАМЕТРЫ ДЛЯ КАЖДОЙ СЕРИИ ===
            series_params = {}
            
            for series_id, series in self.series_data.items():
                # Массы твердого на стадиях (логарифмически нормальное распределение)
                solid_st2 = pm.Lognormal(f'solid_st2_{series_id}',
                                       mu=np.log(series['initial_solid'] * 0.8),
                                       sigma=0.3)
                solid_st3 = pm.Lognormal(f'solid_st3_{series_id}',
                                       mu=np.log(solid_st2 * 0.9),
                                       sigma=0.2)
                solid_st4 = pm.Lognormal(f'solid_st4_{series_id}',
                                       mu=np.log(solid_st3 * 0.95),
                                       sigma=0.2)
                
                # Объемы жидкости
                liquid_st2 = pm.Lognormal(f'liquid_st2_{series_id}',
                                        mu=np.log(series['initial_liquid'] * 1.2),
                                        sigma=0.2)
                liquid_st3 = pm.Lognormal(f'liquid_st3_{series_id}',
                                        mu=np.log(liquid_st2 + series['reagent_volume_st3']),
                                        sigma=0.1)
                liquid_st4 = pm.Lognormal(f'liquid_st4_{series_id}',
                                        mu=np.log(liquid_st3 * 0.95),
                                        sigma=0.1)
                
                # Массы конечных продуктов
                product_D = pm.Lognormal(f'product_D_{series_id}',
                                       mu=np.log(solid_st4 * 0.3),  # ~30% извлечение в концентрат
                                       sigma=0.3)
                product_E = pm.Lognormal(f'product_E_{series_id}',
                                       mu=np.log(solid_st4 * 0.7),  # ~70% в хвосты
                                       sigma=0.3)
                product_G = pm.Lognormal(f'product_G_{series_id}',
                                       mu=np.log(liquid_st4 * 0.95),
                                       sigma=0.1)
                
                # Поправочные коэффициенты для металлов (смещение анализа)
                metal_biases = {}
                for metal in self.metals:
                    # Лог-нормальное смещение около 1.0
                    bias = pm.Lognormal(f'bias_{metal}_{series_id}',
                                       mu=0, sigma=0.1)  # ~10% вариация
                    metal_biases[metal] = bias
                
                series_params[series_id] = {
                    'solid_st2': solid_st2,
                    'solid_st3': solid_st3,
                    'solid_st4': solid_st4,
                    'liquid_st2': liquid_st2,
                    'liquid_st3': liquid_st3,
                    'liquid_st4': liquid_st4,
                    'product_D': product_D,
                    'product_E': product_E,
                    'product_G': product_G,
                    'metal_biases': metal_biases
                }
            
            # === 3. УРАВНЕНИЯ МАСС-БАЛАНСА ===
            # Для каждой серии и каждого металла
            mass_balance_errors = []
            
            for series_id, series in self.series_data.items():
                params = series_params[series_id]
                
                for metal in self.metals:
                    # Получаем измеренные концентрации и массы
                    measured_data = self._extract_metal_data(series, metal)
                    
                    # 1. Баланс для стадии 2 (выщелачивание)
                    metal_in_st2 = measured_data['st1_B'] * (1 - global_sampling_loss)  # масса в твердой фазе
                    metal_in_st2 += measured_data['st1_A']  # масса в жидкой фазе
                    
                    metal_out_st2 = (params['solid_st2'] * measured_data['st2_B_conc'] + 
                                   params['liquid_st2'] * measured_data['st2_A_conc']) * \
                                   (1 + params['metal_biases'][metal] - 1)  # применяем bias
                    
                    error_st2 = pm.Normal(f'error_st2_{series_id}_{metal}',
                                        mu=metal_in_st2 - metal_out_st2,
                                        sigma=sigma_measurement)
                    mass_balance_errors.append(error_st2)
                    
                    # 2. Баланс для стадии 3 (сульфидизация)
                    # Учитываем добавку реагентов
                    reagent_metal = self._reagent_metal_content(series, metal)
                    
                    metal_in_st3 = metal_out_st2 * (1 - global_process_loss) + reagent_metal
                    metal_out_st3 = (params['solid_st3'] * measured_data['st3_B_conc'] +
                                   params['liquid_st3'] * measured_data['st3_A_conc']) * \
                                   (1 + params['metal_biases'][metal] - 1)
                    
                    error_st3 = pm.Normal(f'error_st3_{series_id}_{metal}',
                                        mu=metal_in_st3 - metal_out_st3,
                                        sigma=sigma_measurement)
                    mass_balance_errors.append(error_st3)
                    
                    # 3. Баланс для стадии 4 (флотация)
                    metal_in_st4 = metal_out_st3 * (1 - global_process_loss)
                    metal_out_st4 = (params['solid_st4'] * measured_data['st4_B_conc'] +
                                   params['liquid_st4'] * measured_data['st4_A_conc'] +
                                   params['product_D'] * measured_data['st4_D_conc']) * \
                                   (1 + params['metal_biases'][metal] - 1)
                    
                    error_st4 = pm.Normal(f'error_st4_{series_id}_{metal}',
                                        mu=metal_in_st4 - metal_out_st4,
                                        sigma=sigma_measurement)
                    mass_balance_errors.append(error_st4)
                    
                    # 4. Баланс для конечных продуктов
                    metal_in_final = metal_out_st4 * (1 - global_process_loss)
                    metal_out_final = (params['product_E'] * measured_data['st6_E_conc'] +
                                     params['product_G'] * measured_data['st6_G_conc']) * \
                                     (1 + params['metal_biases'][metal] - 1)
                    
                    error_final = pm.Normal(f'error_final_{series_id}_{metal}',
                                          mu=metal_in_final - metal_out_final,
                                          sigma=sigma_measurement)
                    mass_balance_errors.append(error_final)
            
            # === 4. ДОПОЛНИТЕЛЬНЫЕ ОГРАНИЧЕНИЯ ===
            # Физическая согласованность объемов и масс
            
            # Общая масса должна сохраняться
            for series_id, series in self.series_data.items():
                params = series_params[series_id]
                
                # Масса твердого + масса жидкости = общая масса суспензии
                total_mass_st2 = params['solid_st2'] + params['liquid_st2'] * self.WATER_DENSITY
                measured_total_st2 = series.get('st2_C_sample_mass', 0)
                
                pm.Normal(f'total_mass_st2_{series_id}',
                         mu=total_mass_st2,
                         observed=measured_total_st2,
                         sigma=measured_total_st2 * 0.05)  # 5% ошибка
                
                # Массы продуктов не могут превышать входную массу
                pm.Potential(f'constraint_D_{series_id}',
                           pm.math.switch(params['product_D'] > params['solid_st4'],
                                        -np.inf, 0))
                pm.Potential(f'constraint_E_{series_id}',
                           pm.math.switch(params['product_E'] > params['solid_st4'],
                                        -np.inf, 0))
            
            return model
    
    def fit(self, n_samples=2000, n_chains=4):
        """
        Обучение модели с помощью MCMC
        """
        model = self.build_model()
        
        with model:
            # Вариационный вывод для быстрой аппроксимации
            approx = pm.fit(n=100000, method='advi')
            trace = approx.sample(draws=n_samples)
            
            # Полный MCMC для точных оценок
            # trace = pm.sample(n_samples, chains=n_chains, 
            #                 target_accept=0.95, return_inferencedata=True)
            
        return trace

class OptimizeMassBalance:
    """
    Альтернативный подход: оптимизационная модель для поиска параметров
    """
    
    def __init__(self, metals: List[str], series_data: Dict):
        self.metals = metals
        self.series_data = series_data
        
    def objective_function(self, params: np.ndarray, series_id: str) -> float:
        """
        Целевая функция - минимизация невязки масс-баланса
        """
        series = self.series_data[series_id]
        
        # Распаковываем параметры
        n_metals = len(self.metals)
        
        solid_st2 = params[0]
        solid_st3 = params[1]
        solid_st4 = params[2]
        liquid_st2 = params[3]
        liquid_st3 = params[4]
        liquid_st4 = params[5]
        product_D = params[6]
        product_E = params[7]
        product_G = params[8]
        
        # Коэффициенты потерь и bias металлов
        sampling_loss = params[9]
        process_loss = params[10]
        metal_biases = params[11:11 + n_metals]
        
        total_error = 0
        
        for i, metal in enumerate(self.metals):
            bias = metal_biases[i]
            data = self._get_metal_data(series, metal)
            
            # Масс-баланс для каждой стадии
            error = self._calculate_balance_error(
                solid_st2, solid_st3, solid_st4,
                liquid_st2, liquid_st3, liquid_st4,
                product_D, product_E, product_G,
                sampling_loss, process_loss, bias,
                data
            )
            
            total_error += error ** 2
            
        # Штрафы за нарушение физических ограничений
        constraints_penalty = self._calculate_constraints_penalty(
            solid_st2, solid_st3, solid_st4,
            liquid_st2, liquid_st3, liquid_st4,
            product_D, product_E, product_G
        )
        
        return total_error + constraints_penalty
    
    def optimize_series(self) -> Dict:
        """
        Оптимизация для всех серий
        """
        results = {}
        
        for series_id in self.series_data:
            # Начальные приближения
            series = self.series_data[series_id]
            
            x0 = self._get_initial_guess(series)
            
            # Границы параметров
            bounds = self._get_bounds(series)
            
            # Оптимизация
            result = optimize.minimize(
                self.objective_function,
                x0,
                args=(series_id,),
                method='SLSQP',
                bounds=bounds,
                options={'maxiter': 1000, 'ftol': 1e-6}
            )
            
            results[series_id] = {
                'success': result.success,
                'params': self._pack_results(result.x, series),
                'error': result.fun,
                'iterations': result.nit
            }
            
        return results
    
    def _calculate_balance_error(self, *args) -> float:
        """
        Расчет невязки масс-баланса для одного металла
        """
        (solid_st2, solid_st3, solid_st4,
         liquid_st2, liquid_st3, liquid_st4,
         product_D, product_E, product_G,
         sampling_loss, process_loss, bias, data) = args
        
        # Входные массы металла
        metal_in_st1 = data['st1_B'] * (1 - sampling_loss) + data['st1_A']
        
        # Выход со стадии 2
        metal_out_st2 = (solid_st2 * data['st2_B_conc'] + 
                        liquid_st2 * data['st2_A_conc']) * bias
        
        # Стадия 3 с добавкой реагентов
        metal_in_st3 = metal_out_st2 * (1 - process_loss) + data.get('reagent_metal', 0)
        metal_out_st3 = (solid_st3 * data['st3_B_conc'] + 
                        liquid_st3 * data['st3_A_conc']) * bias
        
        # Стадия 4 флотация
        metal_in_st4 = metal_out_st3 * (1 - process_loss)
        metal_out_st4 = (solid_st4 * data['st4_B_conc'] + 
                        liquid_st4 * data['st4_A_conc'] +
                        product_D * data['st4_D_conc']) * bias
        
        # Конечные продукты
        metal_in_final = metal_out_st4 * (1 - process_loss)
        metal_out_final = (product_E * data['st6_E_conc'] + 
                          product_G * data['st6_G_conc']) * bias
        
        # Суммарная невязка
        error = (abs(metal_in_st1 - metal_out_st2) + 
                abs(metal_in_st3 - metal_out_st3) +
                abs(metal_in_st4 - metal_out_st4) +
                abs(metal_in_final - metal_out_final))
        
        return error

class SequentialBalanceSolver:
    """
    Пошаговый метод последовательного уточнения
    """
    
    def solve(self, series_data: Dict) -> Dict:
        """
        Решение масс-баланса последовательно для каждой стадии
        """
        results = {}
        
        for series_id, series in series_data.items():
            # Шаг 1: Оценка массы твердого на стадии 2
            solid_st2 = self._estimate_solid_st2(series)
            
            # Шаг 2: Оценка объема жидкости на стадии 2
            liquid_st2 = self._estimate_liquid_st2(series, solid_st2)
            
            # Шаг 3: Последовательное уточнение с итерациями
            params = self._iterative_refinement(series, solid_st2, liquid_st2)
            
            # Шаг 4: Оценка масс конечных продуктов
            products = self._estimate_products(series, params)
            
            results[series_id] = {**params, **products}
            
        return results
    
    def _iterative_refinement(self, series: Dict, 
                             solid_st2_init: float, 
                             liquid_st2_init: float,
                             max_iter: int = 50,
                             tol: float = 1e-4) -> Dict:
        """
        Итеративное уточнение с использованием всех металлов
        """
        params = {
            'solid_st2': solid_st2_init,
            'solid_st3': solid_st2_init * 0.9,
            'solid_st4': solid_st2_init * 0.85,
            'liquid_st2': liquid_st2_init,
            'liquid_st3': liquid_st2_init * 1.1,
            'liquid_st4': liquid_st2_init * 0.95
        }
        
        for iteration in range(max_iter):
            params_old = params.copy()
            
            # Уточнение по каждому металлу
            for metal in self.metals:
                metal_data = self._get_metal_data(series, metal)
                
                # Решаем обратную задачу для каждой стадии
                params = self._update_params_from_metal(params, metal_data)
            
            # Проверка сходимости
            delta = max(abs(params[k] - params_old[k]) / params_old[k] 
                       for k in params.keys())
            
            if delta < tol:
                break
        
        return params

class EnsembleModel:
    """
    Ансамбль моделей для надежного оценивания
    """
    
    def __init__(self, metals: List[str], series_data: Dict):
        self.bayesian = MassBalanceModel(metals, series_data)
        self.optimizer = OptimizeMassBalance(metals, series_data)
        self.sequential = SequentialBalanceSolver(metals, series_data)
        
    def fit_predict(self) -> Dict:
        """
        Комбинированное решение несколькими методами
        """
        # Результаты каждого метода
        results_bayes = self.bayesian.fit()
        results_opt = self.optimizer.optimize_series()
        results_seq = self.sequential.solve()
        
        # Ансамблевое взвешивание
        ensemble_results = {}
        
        for series_id in results_opt.keys():
            ensemble_results[series_id] = self._weighted_average(
                results_bayes.get(series_id, {}),
                results_opt[series_id]['params'],
                results_seq[series_id],
                weights=[0.4, 0.4, 0.2]  # Байес и оптимизация - основные
            )
            
        return ensemble_results

def main_solution_pipeline(data_file: str):
    """
    Основной конвейер решения задачи масс-баланса
    """
    
    # 1. Загрузка и предобработка данных
    with open(data_file, 'r') as f:
        data = json.load(f)
    
    # 2. Группировка проб по сериям
    series_grouped = group_probes_by_series(data['probes'])
    
    # 3. Определение списка металлов
    metals = extract_metal_list(data['probes'])
    
    # 4. Выбор и запуск модели
    model = EnsembleModel(metals, series_grouped)
    results = model.fit_predict()
    
    # 5. Валидация результатов
    validated_results = validate_results(results, series_grouped)
    
    # 6. Сохранение результатов
    save_balance_results(validated_results)
    
    # 7. Визуализация
    create_balance_visualizations(validated_results)
    
    return validated_results