from typing import Dict, Any, List, Optional, Tuple
import json
from pathlib import Path
from middleware.series_worker import get_probe_type, get_probe_from_type, get_source_class_from_probe, get_probe_by_name
from app.params import settings
from mass_balance.algorithm_config import AtomicFileConfig
import logging
import traceback

# Создаем именной логгер для расчетов
logger = logging.getLogger('calculations')
logger.setLevel(logging.DEBUG) # На этапе отладки ставим DEBUG

BASE_DIR = Path(__file__).parent.parent.parent
#DATA_FILE = BASE_DIR / 'data' / 'data.json'

config = AtomicFileConfig()

def get_analysis_coef(probe_name: str, default: float) -> float:
    """Рассчитывает коэффициент на основе объема и аликвоты родительской пробы."""
    probe = get_probe_by_name(probe_name)
    if not probe:
        return default
    
    v_ml = probe.get('V (ml)')
    v_aliq = probe.get('Valiq, ml', 10)
    if v_aliq == 0:
        v_aliq = 10
    
    if v_ml is not None and (v_ml - v_aliq) != 0:
        return v_ml / (v_ml - v_aliq)
    
    return default

def apply_rebalance(probe: Dict[str, Any], field: str, coefs: List[float]) -> bool:
    """Применяет произведение коэффициентов к полю и ставит флаг, чтобы избежать повтора."""
    suffix = "mass" if "mass" in field else "volume"
    flag_name = f'flag_rebalance_{suffix}'
    
    if probe.get(flag_name) is True:
        return False
        
    original_val = probe.get(field)
    if original_val is not None:
        total_coef = 1.0
        for c in coefs:
            total_coef *= c
            
        probe[f'zero_{field}'] = original_val
        probe[field] = original_val * total_coef
        probe[flag_name] = True
        return True
    return False

def get_suspension_data(probe_c: Dict[str, Any]) -> Tuple[float, float, float]:
    """Рассчитывает массу твердой и объем жидкой фаз из данных суспензии."""
    # Ca(OH)2
    m_frac_ca = probe_c.get('Массовая доля Ca(OH)2', 0)
    v_susp_ca = probe_c.get('Объем суспензии Ca(OH)2', 0)
    m_solid_ca = v_susp_ca * settings.CAOH2_SUSPENSION_DENSITY * m_frac_ca
    
    # CaCO3
    m_frac_co3 = probe_c.get('Массовая доля CaCO3', 0)
    v_susp_co3 = probe_c.get('Объем суспензии CaCO3', 0)
    m_solid_co3 = v_susp_co3 * settings.CACO3_SUSPENSION_DENSITY * m_frac_co3
    
    # Жидкая фаза (общая)
    m_susp_total = (v_susp_ca * settings.CAOH2_SUSPENSION_DENSITY + 
                    v_susp_co3 * settings.CACO3_SUSPENSION_DENSITY)
    liquid_vol = float(m_susp_total - (m_solid_ca + m_solid_co3))
    
    return m_solid_ca, m_solid_co3, liquid_vol

# --- Основная функция ---

logger = logging.getLogger('phase_calculate')

def process_phase_calculate(probe: dict):
    """
    Обработка фазовых расчетов для пробы
    """
    try:
        # Базовая проверка входных данных
        if not isinstance(probe, dict):
            logger.error(f"Входные данные не являются словарем: {type(probe)}")
            return probe
            
        if 'name' not in probe:
            logger.error(f"В пробе отсутствует поле 'name': {probe.get('id', 'unknown')}")
            if 'tags' not in probe:
                probe['tags'] = []
            if 'ошибка: отсутствует name' not in probe['tags']:
                probe['tags'].append('ошибка: отсутствует name')
            return probe
            
        p_name = probe['name']
        probe_id = probe.get('id', 'unknown')
        
        logger.info(f"Начало phase_calculate для пробы id={probe_id}, name={p_name}")
        logger.debug(f"Содержимое пробы: {probe}")
        
        # Получаем тип пробы
        try:
            probe_type_out = get_probe_type(probe)
            logger.debug(f"get_probe_type вернул: {probe_type_out}")
        except Exception as e:
            logger.error(f"Ошибка в get_probe_type для {p_name}: {str(e)}")
            logger.error(traceback.format_exc())
            probe_type_out = None
        
        if probe_type_out is None:
            logger.debug(f"Проба {p_name}: тип не определен, пропуск.")
            if 'tags' not in probe:
                probe['tags'] = []
            if 'тип не определен' not in probe['tags']:
                probe['tags'].append('тип не определен')
            return probe
        
        probe_type, m, n = probe_type_out
        logger.info(f"Проба {p_name}: тип={probe_type}, m={m}, n={n}")
        
        # Получаем source_class
        try:
            source_class = get_source_class_from_probe(probe)
            logger.debug(f"get_source_class_from_probe вернул: {source_class}")
        except Exception as e:
            logger.error(f"Ошибка в get_source_class_from_probe для {p_name}: {str(e)}")
            logger.error(traceback.format_exc())
            source_class = None
        
        if source_class is None:
            logger.debug(f"Проба {p_name}: не найден source_class, пропуск.")
            if 'tags' not in probe:
                probe['tags'] = []
            if 'source_class не найден' not in probe['tags']:
                probe['tags'].append('source_class не найден')
            return probe
        
        logger.info(f"Обработка {p_name} (Тип: {probe_type}, Source class: {source_class})")
        is_updated = False    

        # Формируем имена связанных проб
        try:
            names = {
                'st_A': f"{source_class}-{m}A{n}", 
                'st_B': f"{source_class}-{m}B{n}", 
                'st_C': f"{source_class}-{m}C{n}",
                'st2_A': f"{source_class}-L{m}A{n}", 
                'st2_B': f"{source_class}-L{m}B{n}", 
                'st2_C': f"{source_class}-L{m}C{n}",
                'st3_A': f"{source_class}-L{m}P{m}A{n}", 
                'st3_B': f"{source_class}-L{m}P{m}B{n}", 
                'st3_C': f"{source_class}-L{m}P{m}C{n}",
                'st4_A': f"{source_class}-L{m}P{m}F{m}A{n}", 
                'st4_D': f"{source_class}-L{m}P{m}F{m}D{n}"
            }
            logger.debug(f"Сгенерированные имена: {names}")
        except Exception as e:
            logger.error(f"Ошибка при формировании имен для {p_name}: {str(e)}")
            logger.error(traceback.format_exc())
            return probe

        # Получаем коэффициенты из конфига
        try:
            logger.debug("Загрузка коэффициентов из конфига")
            
            solid_dencity_start_param_T_type = config.get_coefficient("solid_dencity_start_param_T_type")
            liquid_dencity_start_param_T_type = config.get_coefficient("liquid_dencity_start_param_T_type")
            solid_dencity_start_param_E_type = config.get_coefficient("solid_dencity_start_param_E_type")
            liquid_dencity_start_param_E_type = config.get_coefficient("liquid_dencity_start_param_E_type")
            leaching_solid_coefficient_T_type = config.get_coefficient("leaching_solid_coefficient_T_type")
            leaching_solid_coefficient_E_type = config.get_coefficient("leaching_solid_coefficient_E_type")
            solid_dencity_st3_param_T_type = config.get_coefficient("solid_dencity_st3_param_T_type")
            liquid_dencity_st3_param_T_type = config.get_coefficient("liquid_dencity_st3_param_T_type")
            solid_dencity_st3_param_E_type = config.get_coefficient("solid_dencity_st3_param_E_type")
            liquid_dencity_st3_param_E_type = config.get_coefficient("liquid_dencity_st3_param_E_type")
            st3_B_calc_type = config.get_coefficient("st3_B_calc_type")    
            contraction_h2so4 = config.get_coefficient("contraction_h2so4")
            st2_solid_dissolution_param = config.get_coefficient("st2_solid_dissolution_param")
            
            logger.debug(f"Загруженные коэффициенты: solid_T={solid_dencity_start_param_T_type}, "
                        f"liquid_T={liquid_dencity_start_param_T_type}, contraction={contraction_h2so4}")
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке коэффициентов: {str(e)}")
            logger.error(traceback.format_exc())
            raise ValueError(f"Не удалось загрузить коэффициенты: {e}")
        
        # Проверка критических коэффициентов
        if solid_dencity_start_param_T_type is None or liquid_dencity_start_param_T_type is None:
            logger.error(f"Критическая ошибка: коэффициенты не загружены: solid_T={solid_dencity_start_param_T_type}, liquid_T={liquid_dencity_start_param_T_type}")
            raise ValueError("Ошибка значения коэффициента")
        
        # Обработка в зависимости от типа пробы
        try:
            if probe_type == 'start_A':
                logger.info(f"Обработка типа start_A для {p_name}")
                
                try:
                    probe_c = get_probe_by_name(names['st_C'])
                    logger.debug(f"Найдена связанная проба start_C: {probe_c.get('name') if probe_c else 'None'}")
                except Exception as e:
                    logger.error(f"Ошибка при поиске start_C для {p_name}: {str(e)}")
                    probe_c = None
                
                if source_class[0] == 'T':
                    coeff_solid = solid_dencity_start_param_T_type
                    coeff_liquid = liquid_dencity_start_param_T_type
                    logger.debug(f"Используем T-коэффициенты: solid={coeff_solid}, liquid={coeff_liquid}")
                    
                elif source_class[0] == 'E':
                    coeff_solid = solid_dencity_start_param_E_type
                    coeff_liquid = liquid_dencity_start_param_E_type
                    logger.debug(f"Используем E-коэффициенты: solid={coeff_solid}, liquid={coeff_liquid}")
                    
                else:
                    coeff_solid = 3
                    coeff_liquid = 1
                    logger.debug(f"Используем стандартные коэффициенты: solid={coeff_solid}, liquid={coeff_liquid}")

                if coeff_solid is None or coeff_liquid is None:
                    logger.error(f"Коэффициенты не определены: solid={coeff_solid}, liquid={coeff_liquid}")
                    raise ValueError("Ошибка значения коэффициента")
                            
                if probe_c:
                    try:
                        if probe_c.get('V (ml)') is not None:
                            logger.debug(f"start_C V(ml)={probe_c['V (ml)']}, sample_mass={probe_c.get('sample_mass')}")
                            
                            # Расчет объема
                            v_c = probe_c['V (ml)']
                            m_c = probe_c.get('sample_mass', 0)
                            
                            probe['V (ml)'] = v_c - (m_c - v_c)/((1 - coeff_liquid/coeff_solid)*coeff_solid)
                            
                            logger.info(f"Рассчитан V(ml) для {p_name}: {probe['V (ml)']}")
                            is_updated = True
                        else:
                            logger.warning(f"В start_C отсутствует поле V(ml)")
                    except Exception as e:
                        logger.error(f"Ошибка при расчете start_A для {p_name}: {str(e)}")
                        logger.error(traceback.format_exc())
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба start_C") 
                    
            elif probe_type == 'start_B':
                logger.info(f"Обработка типа start_B для {p_name}")
                
                if source_class[0] == 'T':
                    coeff_solid = solid_dencity_start_param_T_type
                    coeff_liquid = liquid_dencity_start_param_T_type
                    
                elif source_class[0] == 'E':
                    coeff_solid = solid_dencity_start_param_E_type
                    coeff_liquid = liquid_dencity_start_param_E_type
                    
                else:
                    coeff_solid = 3
                    coeff_liquid = 1

                if coeff_solid is None or coeff_liquid is None:
                    logger.error(f"Коэффициенты не определены: solid={coeff_solid}, liquid={coeff_liquid}")
                    raise ValueError("Ошибка значения коэффициента")  
                          
                try:
                    probe_c = get_probe_by_name(names['st_C'])
                    logger.debug(f"Найдена связанная проба start_C: {probe_c.get('name') if probe_c else 'None'}")
                except Exception as e:
                    logger.error(f"Ошибка при поиске start_C для {p_name}: {str(e)}")
                    probe_c = None
                
                if probe_c:
                    try:
                        m_c = probe_c.get('sample_mass', 0)
                        v_c = probe_c.get('V (ml)', 0)
                        
                        probe['sample_mass'] = (m_c - v_c)/(1 - coeff_liquid/coeff_solid)
                        
                        logger.info(f"Рассчитана sample_mass для {p_name}: {probe['sample_mass']}")
                        is_updated = True
                    except Exception as e:
                        logger.error(f"Ошибка при расчете start_B для {p_name}: {str(e)}")
                        logger.error(traceback.format_exc())
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба start_C")

            elif probe_type == 'st2_A':
                logger.info(f"Обработка типа st2_A для {p_name}")
                
                try:
                    parent = get_probe_by_name(names['st_A'])
                    p_start_c = get_probe_by_name(names['st_C'])
                    p_start_b = get_probe_by_name(names['st_B'])
                    logger.debug(f"Найдена родительская проба st_A: {parent.get('name') if parent else 'None'}")
                except Exception as e:
                    logger.error(f"Ошибка при поиске st_A для {p_name}: {str(e)}")
                    parent = None
                    p_start_c = None
                    p_start_b = None
                
                if source_class[0] == 'T':
                    coeff_leach = leaching_solid_coefficient_T_type
                    
                elif source_class[0] == 'E':
                    coeff_leach = leaching_solid_coefficient_E_type
                    
                else:
                    coeff_leach = 0.33

                if coeff_leach is None:
                    logger.error(f"Коэффициент выщелачивания не определен")
                    raise ValueError("Ошибка значения коэффициента")
                    
                if parent and parent.get('V (ml)') is not None and p_start_c:
                    try:
                        probe['V (ml)'] = (1-contraction_h2so4) * (parent['V (ml)']+p_start_c['Объем р-ра H2SO4 (ml)']) + st2_solid_dissolution_param * p_start_b['sample_mass']/(1-coeff_leach)  # type: ignore
                        logger.info(f"Рассчитана sample_mass для {p_name}: {probe['V (ml)']} (coeff={coeff_leach})")
                        is_updated = True
                    except Exception as e:
                        logger.error(f"Ошибка при расчете st2_B для {p_name}: {str(e)}")
                        logger.error(traceback.format_exc())
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба st_A или отсутствует sample_mass")
                                
            elif probe_type == 'st2_B':
                
                logger.info(f"Обработка типа st2_B для {p_name}")
                
                try:
                    parent = get_probe_by_name(names['st_B'])
                    logger.debug(f"Найдена родительская проба st_B: {parent.get('name') if parent else 'None'}")
                except Exception as e:
                    logger.error(f"Ошибка при поиске st_B для {p_name}: {str(e)}")
                    parent = None
                
                if source_class[0] == 'T':
                    coeff_leach = leaching_solid_coefficient_T_type
                    
                elif source_class[0] == 'E':
                    coeff_leach = leaching_solid_coefficient_E_type
                    
                else:
                    coeff_leach = 0.33

                if coeff_leach is None:
                    logger.error(f"Коэффициент выщелачивания не определен")
                    raise ValueError("Ошибка значения коэффициента")
                    
                if parent and parent.get('sample_mass') is not None:
                    try:
                        probe['sample_mass'] = coeff_leach * parent['sample_mass']
                        logger.info(f"Рассчитана sample_mass для {p_name}: {probe['sample_mass']} (coeff={coeff_leach})")
                        is_updated = True
                    except Exception as e:
                        logger.error(f"Ошибка при расчете st2_B для {p_name}: {str(e)}")
                        logger.error(traceback.format_exc())
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба st_B или отсутствует sample_mass")
                    
            elif probe_type == 'st3_B':
                p_st2_b = get_probe_by_name(names['st2_B'])
                p_st3_c = get_probe_by_name(names['st3_C'])
                p_start_b = get_probe_by_name(names['st_B'])
                
                if source_class[0] == 'T':
                    coeff_solid = solid_dencity_st3_param_T_type
                    coeff_liquid = liquid_dencity_st3_param_T_type
                    
                elif source_class[0] == 'E':
                    coeff_solid = solid_dencity_st3_param_E_type
                    coeff_liquid = liquid_dencity_st3_param_E_type
                    
                else:
                    coeff_solid = 1.9
                    coeff_liquid = 1.05 
                        
                if coeff_solid is None or coeff_liquid is None:
                    raise ValueError("Ошибка значения коэффициента")
                        
                if p_st2_b and p_st3_c:
                    
                    susp_mass = p_st3_c.get('sample_mass',0)
                    susp_volume = p_st3_c.get('V (ml)',0)
                    
                    if st3_B_calc_type == 0 and p_start_b:
                        
                        probe['sample_mass'] = p_start_b['sample_mass']*3
                    
                    elif susp_mass > 0 and susp_volume > 0 and st3_B_calc_type == 1:
                        
                        probe['sample_mass'] = susp_mass - (coeff_solid*susp_volume - susp_mass)/(coeff_solid-coeff_liquid)
                        
                    else:
                        m_ca, m_co3, _ = get_suspension_data(p_st3_c)
                        m_st2 = p_st2_b.get('sample_mass')
                        m_iron = p_st3_c.get('Масса железных окатышей (g)', 0)
                        
                        if m_st2 is not None:
                            probe['sample_mass'] = m_st2 + 2.32 * m_ca + m_iron + 3.03 * m_co3
                            is_updated = True
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба st3_C или st2_B")                        

            elif probe_type == 'st3_A':

                p_st2_a = get_probe_by_name(names['st2_A'])
                p_st3_c = get_probe_by_name(names['st3_C'])

                if source_class[0] == 'T':
                    coeff_solid = solid_dencity_st3_param_T_type
                    coeff_liquid = liquid_dencity_st3_param_T_type
                    
                elif source_class[0] == 'E':
                    coeff_solid = solid_dencity_st3_param_E_type
                    coeff_liquid = liquid_dencity_st3_param_E_type
                    
                else:
                    coeff_solid = 1.9
                    coeff_liquid = 1.05 
                        
                if coeff_solid is None or coeff_liquid is None:
                    raise ValueError("Ошибка значения коэффициента")
                
                if p_st2_a and p_st3_c:
                    
                    susp_mass = p_st3_c.get('sample_mass',0)
                    susp_volume = p_st3_c.get('V (ml)',0)
                    
                    _, _, liq_vol = get_suspension_data(p_st3_c)
                    v_st2 = p_st2_a.get('V (ml)')
                    
                    if v_st2 is not None:
                        probe['V (ml)'] = (v_st2 + liq_vol)
                        is_updated = True
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба st2_A или st3_C")                        

            elif probe_type == 'st4_A':
                
                parent = get_probe_by_name(names['st3_A'])
                if parent and parent.get('V (ml)') is not None:
                    probe['V (ml)'] = parent['V (ml)']
                    is_updated = True
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба st3_A")                    

            elif probe_type == 'st4_B':
                
                p_st3_b = get_probe_by_name(names['st3_B'])
                p_st4_d = get_probe_by_name(names['st4_D'])
                if p_st3_b and p_st4_d:
                    
                    m3 = p_st3_b.get('sample_mass')
                    m4d = p_st4_d.get('sample_mass')
                    
                    if m3 is not None and m4d is not None:
                        probe['sample_mass'] = m3 - m4d
                        is_updated = True
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба st3_B или st4_D")
                    
                c_leaching = get_analysis_coef(names['st2_A'], 1.05)
                c_sulfur = get_analysis_coef(names['st3_A'], 1.025)
                c_flotation = get_analysis_coef(names['st4_A'], 1.025)

                if probe_type == 'st3_B':
                    if apply_rebalance(probe, 'sample_mass', [c_leaching]): is_updated = True
                
                elif probe_type == 'st3_A':
                    if apply_rebalance(probe, 'V (ml)', [c_leaching]): is_updated = True
                
                elif probe_type == 'st4_A':
                    if apply_rebalance(probe, 'V (ml)', [c_leaching, c_sulfur]): is_updated = True
                    
                elif probe_type == 'st4_B':
                    if apply_rebalance(probe, 'sample_mass', [c_leaching, c_sulfur]): is_updated = True
                    
                elif probe_type == 'st4_D':
                    if apply_rebalance(probe, 'sample_mass', [c_leaching, c_sulfur, 1.05]): is_updated = True
                    
                elif probe_type == 'st6_E':
                    coefs = [c_leaching, c_sulfur, c_flotation, 1.0125, 1.05]
                    if apply_rebalance(probe, 'sample_mass', coefs): is_updated = True                                           

            
        except Exception as e:
            logger.error(f"Ошибка при обработке типа {probe_type} для {p_name}: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Добавляем тег об ошибке
            if 'tags' not in probe:
                probe['tags'] = []
            if f'ошибка расчета {probe_type}' not in probe['tags']:
                probe['tags'].append(f'ошибка расчета {probe_type}')
            
    except Exception as e:
        logger.error(f"Критическая ошибка в process_phase_calculate для пробы {probe.get('id', 'unknown')}: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Добавляем тег об ошибке
        if 'tags' not in probe:
            probe['tags'] = []
        if 'критическая ошибка phase_calculate' not in probe['tags']:
            probe['tags'].append('критическая ошибка phase_calculate')
    
    if is_updated: # type: ignore
        logger.info(f"Проба {p_name} успешно обработана с изменениями") # type: ignore
    else:
        logger.debug(f"Проба {p_name} обработана без изменений") # type: ignore
    
    return probe

"""
def calculate_fields_for_series(data_file: str = str(DATA_FILE)) -> Dict[str, Any]:
    
    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        probes = data.get('probes', [])
        if not probes:
            logger.warning("Список проб пуст. Выход.")            
            return {'success': True, 'message': 'Нет проб', 'total_series': 0}

        probe_map = {p.get('name', ''): p for p in probes if p.get('name')}
        stats = {'total_series': 0, 'updated_probes': 0, 'calculated_fields': 0, 'missing_probes': [], 'errors': []}
        
        for probe in probes:
            p_name = probe.get('name', 'Unknown')
            probe_type_out = get_probe_type(probe)
            
            if probe_type_out is None:
                # Теперь мы будем знать, какие пробы функция 
                logger.debug(f"Проба {p_name}: тип не определен, пропуск.")
                continue
            
            probe_type, m, n = probe_type_out
            source_class = get_source_class_from_probe(probe)
            
            if source_class is None:
                logger.debug(f"Проба {p_name}: не найден source_class, пропуск.")
                continue
            
            logger.info(f"Обработка {p_name} (Тип: {probe_type})")
            is_updated = False
                            
            # Генерация имен связанных проб
            names = {
                'st_A': f"{source_class}-{m}A{n}", 'st_B': f"{source_class}-{m}B{n}", 'st_C': f"{source_class}-{m}C{n}",
                'st2_A': f"{source_class}-L{m}A{n}", 'st2_B': f"{source_class}-L{m}B{n}", 'st2_C': f"{source_class}-L{m}C{n}",
                'st3_A': f"{source_class}-L{m}P{m}A{n}", 'st3_B': f"{source_class}-L{m}P{m}B{n}", 'st3_C': f"{source_class}-L{m}P{m}C{n}",
                'st4_A': f"{source_class}-L{m}P{m}F{m}A{n}", 'st4_D': f"{source_class}-L{m}P{m}F{m}D{n}"
            }

            # Логика расчета базовых значений
            if probe_type == 'start_A':
                probe_c = probe_map.get(names['st_C'])
                if probe_c:
                    for field in ['sample_mass', 'V (ml)']:
                        if probe_c.get(field) is not None:
                            probe[field] = probe_c[field]
                            stats['calculated_fields'] += 1
                            is_updated = True
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба start_C")                                            

            elif probe_type == 'start_B':
                parent = probe_map.get(names['st_C'])
                if parent:
                    probe['sample_mass'] = parent.get('Масса твердого (g)')
                    probe['V (ml)'] = parent.get('V (ml)')
                    stats['calculated_fields'] += 2
                    is_updated = True
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба start_C")
                    
            elif probe_type == 'st2_B':
                parent = probe_map.get(names['st_B'])
                
                if source_class[0] == 'T':
                    solid_coeff = 0.3
                elif source_class[0] == 'E':
                    solid_coeff = 0.5
                else:
                    solid_coeff = 0.3
                    
                if parent and parent.get('sample_mass') is not None:
                    probe['sample_mass'] = solid_coeff*parent['sample_mass']
                    probe['mass_calculation_note'] = f"1/2 от {names['st_B']}"
                    stats['calculated_fields'] += 1
                    is_updated = True
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба st_B")                    
                    

            elif probe_type == 'st2_A':
                p_start_a = probe_map.get(names['st_A'])
                p_st2_c = probe_map.get(names['st2_C'])
                if p_start_a and p_st2_c:
                    v_start = p_start_a.get('V (ml)')
                    v_h2so4 = p_st2_c.get('Объем р-ра H2SO4 (ml)')
                    if v_start is not None and v_h2so4 is not None:
                        probe['V (ml)'] = v_start + v_h2so4
                        probe['volume_calculation_note'] = f"{v_start} + {v_h2so4} H2SO4"
                        stats['calculated_fields'] += 1
                        is_updated = True
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба st2_C или st_A")
                    
            elif probe_type == 'st3_B':
                p_st2_b = probe_map.get(names['st2_B'])
                p_st3_c = probe_map.get(names['st3_C'])
                
                if p_st2_b and p_st3_c:
                    
                    susp_mass = p_st3_c.get('sample_mass',0)
                    susp_volume = p_st3_c.get('V (ml)',0)
                    
                    if susp_mass > 0 and susp_volume > 0:
                        
                        probe['sample_mass'] = susp_mass - (1.9*susp_volume - susp_mass)/(1.9-1.05)
                        
                    else:
                        m_ca, m_co3, _ = get_suspension_data(p_st3_c)
                        m_st2 = p_st2_b.get('sample_mass')
                        m_iron = p_st3_c.get('Масса железных окатышей (g)', 0)
                        if m_st2 is not None:
                            probe['sample_mass'] = m_st2 + 2.32 * m_ca + m_iron + 3.03 * m_co3
                            stats['calculated_fields'] += 1
                            is_updated = True
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба st3_C или st2_B")                        

            elif probe_type == 'st3_A':

                p_st2_a = probe_map.get(names['st2_A'])
                p_st3_c = probe_map.get(names['st3_C'])
                if p_st2_a and p_st3_c:
                    
                    susp_mass = p_st3_c.get('sample_mass',0)
                    susp_volume = p_st3_c.get('V (ml)',0)
                    
                    if susp_mass > 0 and susp_volume > 0:
                        
                        probe['V (ml)'] = (1.9*susp_volume - susp_mass)/(1.9-1.05)                                        
                    
                    _, _, liq_vol = get_suspension_data(p_st3_c)
                    v_st2 = p_st2_a.get('V (ml)')
                    if v_st2 is not None:
                        probe['V (ml)'] = (v_st2 + liq_vol)
                        stats['calculated_fields'] += 1
                        is_updated = True
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба st2_A или st3_C")                        

            elif probe_type == 'st4_A':
                parent = probe_map.get(names['st3_A'])
                if parent and parent.get('V (ml)') is not None:
                    probe['V (ml)'] = parent['V (ml)']
                    stats['calculated_fields'] += 1
                    is_updated = True
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба st3_A")                    

            elif probe_type == 'st4_B':
                p_st3_b = probe_map.get(names['st3_B'])
                p_st4_d = probe_map.get(names['st4_D'])
                if p_st3_b and p_st4_d:
                    m3 = p_st3_b.get('sample_mass')
                    m4d = p_st4_d.get('sample_mass')
                    if m3 is not None and m4d is not None:
                        probe['sample_mass'] = m3 - m4d
                        stats['calculated_fields'] += 1
                        is_updated = True
                else:
                    logger.warning(f"  [!] Для {p_name} не найдена связанная проба st3_B или st4_D")                        

            # --- Блок REBALANCE (Коэффициенты пересчета) ---
            
            # Предварительный расчет общих коэффициентов
            c_leaching = get_analysis_coef(names['st2_A'], 1.05)
            c_sulfur = get_analysis_coef(names['st3_A'], 1.025)
            c_flotation = get_analysis_coef(names['st4_A'], 1.025)

            if probe_type == 'st3_B':
                if apply_rebalance(probe, 'sample_mass', [c_leaching]): is_updated = True
            
            elif probe_type == 'st3_A':
                if apply_rebalance(probe, 'V (ml)', [c_leaching]): is_updated = True
            
            elif probe_type == 'st4_A':
                if apply_rebalance(probe, 'V (ml)', [c_leaching, c_sulfur]): is_updated = True
                
            elif probe_type == 'st4_B':
                if apply_rebalance(probe, 'sample_mass', [c_leaching, c_sulfur]): is_updated = True
                
            elif probe_type == 'st4_D':
                if apply_rebalance(probe, 'sample_mass', [c_leaching, c_sulfur, 1.05]): is_updated = True
                
            elif probe_type == 'st6_E':
                coefs = [c_leaching, c_sulfur, c_flotation, 1.0125, 1.05]
                if apply_rebalance(probe, 'sample_mass', coefs): is_updated = True

            if probe_type == 'start_C':
                stats['total_series'] += 1
            if is_updated:
                stats['updated_probes'] += 1

        # Сохранение
        if stats['calculated_fields'] > 0:
            data.setdefault('metadata', {})['fields_calculation_stats'] = stats
            with open(data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

        return {'success': True, 'message': f"Обновлено {stats['updated_probes']} проб", **stats}

    except Exception as e:
        logger.exception(f"Критическая ошибка при расчете файла {data_file}")
        return {'success': False, 'message': str(e)}
"""        