from typing import Dict, Any, List, Optional, Tuple
import json
from pathlib import Path
from middleware.series_worker import get_probe_type, get_probe_from_type, get_source_class_from_probe
from app.params import settings

BASE_DIR = Path(__file__).parent.parent.parent
DATA_FILE = BASE_DIR / 'data' / 'data.json'

def get_analysis_coef(probe_map: Dict[str, Any], probe_name: str, default: float) -> float:
    """Рассчитывает коэффициент на основе объема и аликвоты родительской пробы."""
    probe = probe_map.get(probe_name)
    if not probe:
        return default
    
    v_ml = probe.get('V (ml)')
    v_aliq = probe.get('Valiq, ml', 10)
    
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

def calculate_fields_for_series(data_file: str = str(DATA_FILE)) -> Dict[str, Any]:
    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        probes = data.get('probes', [])
        if not probes:
            return {'success': True, 'message': 'Нет проб', 'total_series': 0}

        probe_map = {p.get('name', ''): p for p in probes if p.get('name')}
        stats = {'total_series': 0, 'updated_probes': 0, 'calculated_fields': 0, 'missing_probes': [], 'errors': []}
        
        for probe in probes:
            probe_type, m, n = get_probe_type(probe) # type: ignore
            if not (probe_type and m and n): continue
            
            source_class = get_source_class_from_probe(probe)
            
            if source_class is not None:
                
                # Генерация имен связанных проб
                names = {
                    'st_A': f"{source_class}-{m}A{n}", 'st_B': f"{source_class}-{m}B{n}", 'st_C': f"{source_class}-{m}C{n}",
                    'st2_A': f"{source_class}-L{m}A{n}", 'st2_B': f"{source_class}-L{m}B{n}", 'st2_C': f"{source_class}-L{m}C{n}",
                    'st3_A': f"{source_class}-L{m}P{m}A{n}", 'st3_B': f"{source_class}-L{m}P{m}B{n}", 'st3_C': f"{source_class}-L{m}P{m}C{n}",
                    'st4_A': f"{source_class}-L{m}P{m}F{m}A{n}", 'st4_D': f"{source_class}-L{m}P{m}F{m}D{n}"
                }
                
            else:
                continue

            is_updated = False

            # Логика расчета базовых значений
            if probe_type == 'start_A':
                probe_c = get_probe_from_type('start_C', m, n)
                if probe_c:
                    for field in ['sample_mass', 'V (ml)']:
                        if probe_c.get(field) is not None:
                            probe[field] = probe_c[field]
                            stats['calculated_fields'] += 1
                            is_updated = True

            elif probe_type == 'start_B':
                parent = probe_map.get(names['st_C'])
                if parent:
                    probe['sample_mass'] = parent.get('Масса твердого (g)')
                    probe['V (ml)'] = parent.get('V (ml)')
                    stats['calculated_fields'] += 2
                    is_updated = True

            elif probe_type == 'st2_B':
                parent = probe_map.get(names['st_B'])
                if parent and parent.get('sample_mass') is not None:
                    probe['sample_mass'] = parent['sample_mass'] / 2
                    probe['mass_calculation_note'] = f"1/2 от {names['st_B']}"
                    stats['calculated_fields'] += 1
                    is_updated = True

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

            elif probe_type == 'st3_B':
                p_st2_b = probe_map.get(names['st2_B'])
                p_st3_c = probe_map.get(names['st3_C'])
                if p_st2_b and p_st3_c:
                    m_ca, m_co3, _ = get_suspension_data(p_st3_c)
                    m_st2 = p_st2_b.get('sample_mass')
                    m_iron = p_st3_c.get('Масса железных окатышей (g)', 0)
                    if m_st2 is not None:
                        probe['sample_mass'] = m_st2 + 2.32 * m_ca + m_iron + 3.03 * m_co3
                        stats['calculated_fields'] += 1
                        is_updated = True

            elif probe_type == 'st3_A':
                p_st2_a = probe_map.get(names['st2_A'])
                p_st3_c = probe_map.get(names['st3_C'])
                if p_st2_a and p_st3_c:
                    _, _, liq_vol = get_suspension_data(p_st3_c)
                    v_st2 = p_st2_a.get('V (ml)')
                    if v_st2 is not None:
                        probe['V (ml)'] = v_st2 + liq_vol
                        stats['calculated_fields'] += 1
                        is_updated = True

            elif probe_type == 'st4_A':
                parent = probe_map.get(names['st3_A'])
                if parent and parent.get('V (ml)') is not None:
                    probe['V (ml)'] = parent['V (ml)']
                    stats['calculated_fields'] += 1
                    is_updated = True

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

            # --- Блок REBALANCE (Коэффициенты пересчета) ---
            
            # Предварительный расчет общих коэффициентов
            c_leaching = get_analysis_coef(probe_map, names['st2_A'], 1.05)
            c_sulfur = get_analysis_coef(probe_map, names['st3_A'], 1.025)
            c_flotation = get_analysis_coef(probe_map, names['st4_A'], 1.025)

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
        return {'success': False, 'message': str(e)}