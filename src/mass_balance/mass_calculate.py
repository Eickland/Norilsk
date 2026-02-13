from pathlib import Path
from typing import Dict, Any
import json
from app.params import settings

BASE_DIR = Path(__file__).parent.parent.parent
DATA_FILE = BASE_DIR / 'data' / 'data.json'

# Черный список полей, которые не должны отображаться как оси
BLACKLIST_FIELDS = {
    "Описание", "is_solid", "id", "last_normalized", 
    "status_id", "is_solution", "name", "tags"
}

def recalculate_metal_mass(data_file: str = str(DATA_FILE)) -> Dict[str, Any]:
    """
    Перерасчет концентраций металлов в абсолютную массу по правилам:
    
    1. Для жидких проб (не нулевое "Разбавление"):
       m(Me) = [Me] * Разбавление * V(ml) / 1000
       Добавляется тег "ошибка расчета жидкой пробы" при ошибке
    
    2. Для твердых проб (не нулевое "Масса твердого (g)"):
       m(Me) = V_aliq(l) * [Me] * 1000 * Масса твердого(g) / Масса навески(mg)
       Добавляется тег "ошибка расчета твердой пробы" при ошибке
    
    Теперь учитываются две концентрации: из ИСП АЭС (с суффиксом '_AES') 
    и из ИСП МС (с суффиксом '_MS')
    
    Прямо изменяет базу данных, добавляя поля mFe_AES, mCu_AES, mFe_MS и т.д.
    
    Args:
        data_file: Путь к JSON файлу с данными
    
    Returns:
        Словарь со статистикой перерасчета
    """
    try:
        # Загружаем данные
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        probes = data.get('probes', [])
        
        if not probes:
            return {
                'success': True,
                'message': 'Нет проб для перерасчета массы металлов',
                'total_probes': 0,
                'liquid_probes': 0,
                'solid_probes': 0,
                'elements_calculated': 0,
                'errors': 0,
                'probes_modified': 0
            }
        
        # Статистика
        stats = {
            'total_probes': len(probes),
            'liquid_probes': 0,           # Пробы с разбавлением
            'solid_probes': 0,            # Пробы с массой твердого
            'elements_calculated': 0,     # Всего рассчитанных полей mX
            'elements_aes_calculated': 0, # Поля mX_AES
            'elements_ms_calculated': 0,  # Поля mX_MS
            'probes_modified': 0,         # Проб, в которых что-то изменилось
            'errors': 0,                  # Количество ошибок
            'liquid_errors': 0,           # Ошибки в жидких пробах
            'solid_errors': 0,            # Ошибки в твердых пробах
            'new_mass_fields': [],        # Созданные поля масс
            'new_mass_fields_aes': [],    # Поля масс AES
            'new_mass_fields_ms': []      # Поля масс MS
        }
        
        # Список химических элементов для расчета (из всех проб)
        # Теперь ищем элементы с суффиксами _AES и _MS
        metal_elements_aes = set()
        metal_elements_ms = set()
        metal_elements_base = set()  # Для совместимости со старыми полями
        
        for probe in probes:
            for key in probe.keys():
                # Проверяем на AES элементы
                if key.endswith('_AES'):
                    base_element = key[:-4]  # Убираем '_AES'
                    if (len(base_element) <= 3 and 
                        base_element[0].isupper() and 
                        base_element not in BLACKLIST_FIELDS and
                        not base_element.endswith('_MS')):
                        metal_elements_aes.add(base_element)
                
                # Проверяем на MS элементы
                elif key.endswith('_MS'):
                    base_element = key[:-3]  # Убираем '_MS'
                    if (len(base_element) <= 3 and 
                        base_element[0].isupper() and 
                        base_element not in BLACKLIST_FIELDS and
                        not base_element.endswith('_AES')):
                        metal_elements_ms.add(base_element)
                
                # Для совместимости со старыми полями без суффиксов
                elif (len(key) <= 3 and 
                      key[0].isupper() and 
                      key not in ['V', 'Ca', 'Co', 'Cu', 'Fe', 'Ni', 'Pd', 'Pt', 'Rh'] and
                      key not in BLACKLIST_FIELDS):
                    if all(c.islower() for c in key[1:]):
                        metal_elements_base.add(key)
        
        # Добавляем основные элементы, которые точно есть
        basic_elements = ['Fe', 'Cu', 'Ni', 'Ca', 'Co', 'Pd', 'Pt', 'Rh', 
                         'Al', 'Mg', 'Zn', 'Pb', 'Cr', 'Mn', 'Ag', 'Au', 'Ti']
        metal_elements_aes.update(basic_elements)
        metal_elements_ms.update(basic_elements)
        metal_elements_base.update(basic_elements)
        
        # Функция для расчета массы
        def calculate_mass_for_element(element, concentration_field, mass_field_prefix, probe, probe_name, 
                                       dilution_float=None, volume_float=None, 
                                       aliquot_float=None, solid_mass_float=None, sample_weight_float=None):
            """Рассчитывает массу для конкретного элемента и метода анализа"""
            
            # Проверяем наличие концентрации
            if concentration_field not in probe or probe[concentration_field] in [None, 'null', '']:
                return False, None
            
            try:
                concentration = float(probe[concentration_field])
                
                # Расчет в зависимости от типа пробы
                if dilution_float is not None and volume_float is not None:
                    # Жидкая проба
                    if "L" not in probe_name:
                        mass = concentration * dilution_float * ((volume_float - probe.get('Масса твердого (g)', 0)/settings.SOLID_DENCITY_PARAM) / 1000.0)
                    else:
                        mass = concentration * dilution_float * volume_float / 1000.0
                
                elif aliquot_float is not None and solid_mass_float is not None and sample_weight_float is not None:
                    # Твердая проба
                    if solid_mass_float == 0:
                        return False, None
                    mass = (aliquot_float * concentration * sample_weight_float) / solid_mass_float
                
                else:
                    return False, None
                
                # Формируем имя поля для массы
                mass_field = f'm{mass_field_prefix}'
                
                # Сохраняем результат
                probe[mass_field] = float(mass)
                
                # Статистика по типу анализа
                if '_AES' in concentration_field:
                    stats['elements_aes_calculated'] += 1
                    if mass_field not in stats['new_mass_fields_aes']:
                        stats['new_mass_fields_aes'].append(mass_field)
                elif '_MS' in concentration_field:
                    stats['elements_ms_calculated'] += 1
                    if mass_field not in stats['new_mass_fields_ms']:
                        stats['new_mass_fields_ms'].append(mass_field)
                else:
                    stats['elements_calculated'] += 1
                
                if mass_field not in stats['new_mass_fields']:
                    stats['new_mass_fields'].append(mass_field)
                
                return True, mass_field
                
            except (ValueError, TypeError, ZeroDivisionError):
                return False, None
        
        # Перебираем все пробы
        for probe in probes:
            probe_id = probe.get('id')
            probe_name = probe.get('name', f'ID: {probe_id}')
            probe_modified = False
            mass_fields_added = []
            
            # Инициализируем теги, если их нет
            if 'tags' not in probe:
                probe['tags'] = []
            
            # Удаляем старые теги ошибок расчета (если были)
            old_error_tags = ['ошибка расчета жидкой пробы', 'ошибка расчета твердой пробы']
            original_tags = probe['tags'].copy()
            probe['tags'] = [tag for tag in probe['tags'] if tag not in old_error_tags]
            if probe['tags'] != original_tags:
                probe_modified = True
            
            # Пытаемся определить тип пробы и выполнить расчет
            try:
                # 1. Проверка для жидкой пробы
                dilution = probe.get('Разбавление')
                if dilution is not None and dilution != 0 and dilution != 'null':
                    try:
                        dilution_float = float(dilution)
                        volume_ml = probe.get('V (ml)', 0)
                        
                        if volume_ml and volume_ml != 'null':
                            volume_float = float(volume_ml)
                            
                            # Расчет для AES элементов
                            for element in metal_elements_aes:
                                # Проверяем наличие поля _AES
                                element_aes = f"{element}_AES"
                                success, mass_field = calculate_mass_for_element(
                                    element, element_aes, element_aes, probe, probe_name,
                                    dilution_float=dilution_float, volume_float=volume_float
                                )
                                if success:
                                    mass_fields_added.append(mass_field)
                                    probe_modified = True
                            
                            # Расчет для MS элементов
                            for element in metal_elements_ms:
                                # Проверяем наличие поля _MS
                                element_ms = f"{element}_MS"
                                success, mass_field = calculate_mass_for_element(
                                    element, element_ms, element_ms, probe, probe_name,
                                    dilution_float=dilution_float, volume_float=volume_float
                                )
                                if success:
                                    mass_fields_added.append(mass_field)
                                    probe_modified = True
                            
                            # Расчет для старых элементов без суффикса (для совместимости)
                            for element in metal_elements_base:
                                if element in probe:
                                    success, mass_field = calculate_mass_for_element(
                                        element, element, element, probe, probe_name,
                                        dilution_float=dilution_float, volume_float=volume_float
                                    )
                                    if success:
                                        mass_fields_added.append(mass_field)
                                        probe_modified = True
                            
                            stats['liquid_probes'] += 1
                            
                    except (ValueError, TypeError) as e:
                        # Ошибка в расчете жидкой пробы
                        if 'ошибка расчета жидкой пробы' not in probe['tags']:
                            probe['tags'].append('ошибка расчета жидкой пробы')
                            probe_modified = True
                        stats['liquid_errors'] += 1
                        stats['errors'] += 1
                
                # 2. Проверка для твердой пробы
                solid_mass = probe.get('Масса навески (g)')
                if solid_mass is not None and solid_mass != 0 and solid_mass != 'null':
                    try:
                        solid_mass_float = float(solid_mass)
                        aliquot_volume = probe.get('V_aliq (l)', 0)
                        sample_weight = probe.get('sample_mass', 0)
                        
                        if (aliquot_volume and aliquot_volume != 'null' and 
                            sample_weight and sample_weight != 'null'):
                            
                            aliquot_float = float(aliquot_volume)
                            sample_weight_float = float(sample_weight)
                            
                            if sample_weight_float == 0:
                                raise ValueError("Масса навески не может быть нулевой")
                            
                            # Расчет для AES элементов
                            for element in metal_elements_aes:
                                # Проверяем наличие поля _AES
                                element_aes = f"{element}_AES"
                                success, mass_field = calculate_mass_for_element(
                                    element, element_aes, element_aes, probe, probe_name,
                                    aliquot_float=aliquot_float, 
                                    solid_mass_float=solid_mass_float, 
                                    sample_weight_float=sample_weight_float
                                )
                                if success:
                                    mass_fields_added.append(mass_field)
                                    probe_modified = True
                            
                            # Расчет для MS элементов
                            for element in metal_elements_ms:
                                # Проверяем наличие поля _MS
                                element_ms = f"{element}_MS"
                                success, mass_field = calculate_mass_for_element(
                                    element, element_ms, element_ms, probe, probe_name,
                                    aliquot_float=aliquot_float, 
                                    solid_mass_float=solid_mass_float, 
                                    sample_weight_float=sample_weight_float
                                )
                                if success:
                                    mass_fields_added.append(mass_field)
                                    probe_modified = True
                            
                            # Расчет для старых элементов без суффикса (для совместимости)
                            for element in metal_elements_base:
                                if element in probe:
                                    success, mass_field = calculate_mass_for_element(
                                        element, element, element, probe, probe_name,
                                        aliquot_float=aliquot_float, 
                                        solid_mass_float=solid_mass_float, 
                                        sample_weight_float=sample_weight_float
                                    )
                                    if success:
                                        mass_fields_added.append(mass_field)
                                        probe_modified = True
                            
                            stats['solid_probes'] += 1
                            
                    except (ValueError, TypeError, ZeroDivisionError) as e:
                        # Ошибка в расчете твердой пробы
                        if 'ошибка расчета твердой пробы' not in probe['tags']:
                            probe['tags'].append('ошибка расчета твердой пробы')
                            probe_modified = True
                        stats['solid_errors'] += 1
                        stats['errors'] += 1
                        
            except Exception as e:
                # Общая ошибка для пробы
                if 'ошибка расчета жидкой пробы' not in probe['tags']:
                    probe['tags'].append('ошибка расчета жидкой пробы')
                    probe_modified = True
                stats['errors'] += 1
            
            # Если пробу изменили, обновляем статистику
            if probe_modified:
                stats['probes_modified'] += 1
                
                # Добавляем метку времени последнего пересчета
        
        # Обновляем общее количество рассчитанных полей
        stats['elements_calculated'] = (stats['elements_calculated'] + 
                                       stats['elements_aes_calculated'] + 
                                       stats['elements_ms_calculated'])
        
        # Обновляем метаданные
        if 'metadata' not in data:
            data['metadata'] = {}
        
        data['metadata'].update({
            'metal_mass_stats': stats
        })
        
        # СОХРАНЯЕМ ИЗМЕНЕННЫЕ ДАННЫЕ ОБРАТНО В ФАЙЛ
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Формируем сообщение о результатах
        message_parts = []
        if stats['liquid_probes'] > 0:
            message_parts.append(f"жидких проб: {stats['liquid_probes']}")
        if stats['solid_probes'] > 0:
            message_parts.append(f"твердых проб: {stats['solid_probes']}")
        if stats['elements_aes_calculated'] > 0:
            message_parts.append(f"полей mX_AES: {stats['elements_aes_calculated']}")
        if stats['elements_ms_calculated'] > 0:
            message_parts.append(f"полей mX_MS: {stats['elements_ms_calculated']}")
        if stats['errors'] > 0:
            message_parts.append(f"ошибок: {stats['errors']}")
        
        message = "Перерасчет массы металлов: " + ", ".join(message_parts) if message_parts else "Изменений не требуется"
        
        # Добавляем информацию о созданных полях
        if stats['new_mass_fields_aes']:
            message += f". Созданы поля AES: {', '.join(sorted(stats['new_mass_fields_aes']))}"
        if stats['new_mass_fields_ms']:
            message += f". Созданы поля MS: {', '.join(sorted(stats['new_mass_fields_ms']))}"
        
        return {
            'success': True,
            'message': message,
            **stats,
        }
        
    except Exception as e:
        return {
            'success': False,
            'message': f"Ошибка перерасчета массы металлов: {str(e)}",
            'total_probes': 0,
            'liquid_probes': 0,
            'solid_probes': 0,
            'elements_calculated': 0,
            'elements_aes_calculated': 0,
            'elements_ms_calculated': 0,
            'errors': 1,
            'probes_modified': 0
        }
 