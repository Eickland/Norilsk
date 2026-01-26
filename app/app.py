from flask import Flask, render_template, request, jsonify, send_file, session, redirect, url_for
from datetime import datetime
import json
import os
from werkzeug.utils import secure_filename
from test_ISP_AES import process_icp_aes_data
from ISP_MS import process_metal_samples_csv
import pandas as pd
from version_control import VersionControlSystem
from io import BytesIO
from dotenv import load_dotenv
import hashlib
import hmac
import time
from pathlib import Path
from typing import Dict, List, Any, Set
import plotly
import plotly.graph_objs as go
from flask_cors import CORS
import re
from collections import defaultdict

load_dotenv()

BASE_DIR = Path(__file__).parent.parent

app = Flask(__name__, template_folder=str(BASE_DIR / 'templates'), static_folder=str(BASE_DIR / 'static'))
CORS(app)

# Черный список полей, которые не должны отображаться как оси
BLACKLIST_FIELDS = {
    "Описание", "is_solid", "id", "last_normalized", 
    "status_id", "is_solution", "name", "tags"
}


app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CONFIG_PATH = 'allowed_users.json'

BASE_DIR = Path(__file__).parent.parent

app.config['UPLOAD_FOLDER'] = BASE_DIR / 'uploads'
app.config['RESULTS_FOLDER'] = BASE_DIR / 'results'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB максимум
app.config['ALLOWED_EXTENSIONS'] = {'csv', 'xlsx', 'xls', 'json'}

app.config['VERSIONS_DIR'] = BASE_DIR / 'versions'

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULTS_FOLDER'], exist_ok=True)

# Файл для хранения данных

DATA_FILE = BASE_DIR / 'data' / 'data.json'
app.config['DATA_FILE'] = DATA_FILE
# Инициализация системы управления версиями
vcs = VersionControlSystem(app.config['DATA_FILE'], app.config['VERSIONS_DIR'])

def recalculate_metal_mass(data_file: str = str(DATA_FILE)) -> Dict[str, Any]:
    """
    Перерасчет концентраций металлов в абсолютную массу по правилам:
    
    1. Для жидких проб (не нулевое "Разбавление"):
       m(Me) = [Me] * Разбавление * V(ml) / 1000
       Добавляется тег "ошибка расчета жидкой пробы" при ошибке
    
    2. Для твердых проб (не нулевое "Масса твердого (g)"):
       m(Me) = V_aliq(l) * [Me] * 1000 * Масса твердого(g) / Масса навески(mg)
       Добавляется тег "ошибка расчета твердой пробы" при ошибке
    
    Прямо изменяет базу данных, добавляя поля mFe, mCu и т.д.
    
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
        
        # Создаем версию перед перерасчетом
        version_info = vcs.create_version(
            description="Перерасчет концентраций металлов в абсолютную массу",
            author="system",
            change_type="metal_mass_recalculation"
        )
        
        # Статистика
        stats = {
            'total_probes': len(probes),
            'liquid_probes': 0,           # Пробы с разбавлением
            'solid_probes': 0,            # Пробы с массой твердого
            'elements_calculated': 0,     # Всего рассчитанных полей mX
            'probes_modified': 0,         # Проб, в которых что-то изменилось
            'errors': 0,                  # Количество ошибок
            'liquid_errors': 0,           # Ошибки в жидких пробах
            'solid_errors': 0,            # Ошибки в твердых пробах
            'new_mass_fields': []         # Созданные поля масс
        }
        
        # Список химических элементов для расчета (из всех проб)
        metal_elements = set()
        for probe in probes:
            for key in probe.keys():
                # Проверяем, является ли поле химическим элементом
                if (len(key) <= 3 and 
                    key[0].isupper() and 
                    key not in ['V', 'Ca', 'Co', 'Cu', 'Fe', 'Ni', 'Pd', 'Pt', 'Rh'] and
                    key not in BLACKLIST_FIELDS):
                    # Проверяем остальные символы (если есть)
                    if all(c.islower() for c in key[1:]):
                        metal_elements.add(key)
        
        # Добавляем основные элементы, которые точно есть
        basic_elements = ['Fe', 'Cu', 'Ni', 'Ca', 'Co', 'Pd', 'Pt', 'Rh', 
                         'Al', 'Mg', 'Zn', 'Pb', 'Cr', 'Mn', 'Ag', 'Au', 'Ti']
        metal_elements.update(basic_elements)
        
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
                            
                            # Расчет для каждого металла, который есть в пробе
                            for element in metal_elements:
                                if element in probe and probe[element] not in [None, 'null', '']:
                                    try:
                                        concentration = float(probe[element])
                                        
                                        # Расчет массы металла
                                        mass = concentration * dilution_float * ((volume_float - probe.get('Масса твердого (g)')/3) / 1000.0)
                                        
                                        # Сохраняем результат
                                        mass_field = f'm{element}'
                                        probe[mass_field] = float(mass)
                                        mass_fields_added.append(mass_field)
                                        stats['elements_calculated'] += 1
                                        probe_modified = True
                                        
                                    except (ValueError, TypeError):
                                        # Пропускаем этот элемент, если нет значения
                                        continue
                            
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
                            
                            # Расчет для каждого металла, который есть в пробе
                            for element in metal_elements:
                                if element in probe and probe[element] not in [None, 'null', '']:
                                    try:
                                        concentration = float(probe[element])
                                        
                                        # Расчет массы металла
                                        mass = (aliquot_float * concentration * 
                                                sample_weight_float) / solid_mass_float
                                        
                                        # Сохраняем результат
                                        mass_field = f'm{element}'
                                        probe[mass_field] = float(mass)
                                        mass_fields_added.append(mass_field)
                                        stats['elements_calculated'] += 1
                                        probe_modified = True
                                        
                                    except (ValueError, TypeError):
                                        # Пропускаем этот элемент, если нет значения
                                        continue
                            
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
                
                # Записываем поля масс, которые были добавлены
                for field in mass_fields_added:
                    if field not in stats['new_mass_fields']:
                        stats['new_mass_fields'].append(field)
                
                # Добавляем метку времени последнего пересчета
                probe['last_mass_recalculation'] = datetime.now().isoformat()
        
        # Обновляем метаданные
        if 'metadata' not in data:
            data['metadata'] = {}
        
        data['metadata'].update({
            'last_metal_mass_recalculation': datetime.now().isoformat(),
            'metal_mass_stats': stats
        })
        
        # СОХРАНЯЕМ ИЗМЕНЕННЫЕ ДАННЫЕ ОБРАТНО В ФАЙЛ
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Создаем финальную версию
        vcs.create_version(
            description=f"Перерасчет массы металлов: жидких - {stats['liquid_probes']}, твердых - {stats['solid_probes']}, полей mX - {stats['elements_calculated']}, ошибок - {stats['errors']}",
            author="system",
            change_type="metal_mass_recalculation_complete"
        )
        
        # Формируем сообщение о результатах
        message_parts = []
        if stats['liquid_probes'] > 0:
            message_parts.append(f"жидких проб: {stats['liquid_probes']}")
        if stats['solid_probes'] > 0:
            message_parts.append(f"твердых проб: {stats['solid_probes']}")
        if stats['elements_calculated'] > 0:
            message_parts.append(f"полей mX: {stats['elements_calculated']}")
        if stats['errors'] > 0:
            message_parts.append(f"ошибок: {stats['errors']}")
        
        message = "Перерасчет массы металлов: " + ", ".join(message_parts) if message_parts else "Изменений не требуется"
        
        # Добавляем информацию о созданных полях
        if stats['new_mass_fields']:
            message += f". Созданы поля: {', '.join(sorted(stats['new_mass_fields']))}"
        
        return {
            'success': True,
            'message': message,
            **stats,
            'version_created': version_info is not None,
            'version_id': version_info['id'] if version_info else None
        }
        
    except Exception as e:
        return {
            'success': False,
            'message': f"Ошибка перерасчета массы металлов: {str(e)}",
            'total_probes': 0,
            'liquid_probes': 0,
            'solid_probes': 0,
            'elements_calculated': 0,
            'errors': 1,
            'probes_modified': 0
        }

def copy_mass_and_volume_from_C_to_AB(data_file: str = str(DATA_FILE)) -> Dict[str, Any]:
    """
    Находит пробы, отличающиеся только предпоследним символом (A, B, C),
    и копирует поля из пробы C в пробы A и B.
    
    Правила:
    1. Ищем группы проб с одинаковыми именами, кроме предпоследнего символа
    2. Предпоследний символ должен быть A, B или C
    3. Из пробы C копируем поля в пробы A и B той же группы
    4. Для пробы A: копируем 'Масса образца (g)' и 'V (ml)'
    5. Для пробы B: копируем 'Масса образца (g)' и 'V (ml)' только если название пробы B
       содержит не более 11 символов (считая предпоследний символ A/B/C).
       При этом проба B получает значение в поле 'Масса твердого (g)' из 'Масса образца (g)' пробы C
    6. Если поля в A/B уже существуют, они перезаписываются значениями из C
    
    Args:
        data_file: Путь к JSON файлу с данными
    
    Returns:
        Словарь со статистикой операции
    """
    try:
        # Загружаем данные
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        probes = data.get('probes', [])
        
        if not probes:
            return {
                'success': True,
                'message': 'Нет проб для обработки',
                'total_groups': 0,
                'updated_probes': 0,
                'copied_fields': 0
            }
        
        # Создаем версию перед изменениями
        version_info = vcs.create_version(
            description="Копирование массы и объема из проб C в пробы A/B",
            author="system",
            change_type="mass_volume_copy"
        )
        
        # Создаем словарь для группировки проб по базовому имени
        # Ключ: базовое имя (без предпоследнего символа)
        # Значение: словарь с пробами A, B, C
        probe_groups = defaultdict(lambda: {'A': None, 'B': None, 'C': None})
        
        # Собираем пробы по группам
        for probe in probes:
            name = probe.get('name', '')
            if not name:
                continue
                
            # Проверяем длину имени (должно быть хотя бы 2 символа для предпоследнего)
            if len(name) < 2:
                continue
                
            # Получаем предпоследний символ
            second_last_char = name[-2] if len(name) >= 2 else ''
            
            # Проверяем, что предпоследний символ - A, B или C
            if second_last_char in ['A', 'B', 'C']:
                # Создаем базовое имя (без предпоследнего символа)
                base_name = name[:-2] + name[-1]  # Удаляем предпоследний символ
                
                # Сохраняем пробу в соответствующую группу
                probe_groups[base_name][second_last_char] = probe
        
        # Статистика
        stats = {
            'total_groups': 0,
            'valid_groups': 0,        # Группы, где есть проба C и хотя бы одна из A/B
            'updated_probes': 0,      # Пробы A/B, в которые скопированы данные
            'copied_fields': 0,       # Всего скопированных полей
            'missing_fields': 0,      # Поля, которых нет в пробе C
            'skipped_B_length': 0,    # Пробы B, пропущенные из-за длины имени > 11
            'errors': []              # Ошибки при обработке
        }
        
        # Обрабатываем каждую группу
        for base_name, group in probe_groups.items():
            stats['total_groups'] += 1
            
            # Проверяем, есть ли проба C в группе
            probe_C = group.get('C')
            if not probe_C:
                continue  # Пропускаем группы без пробы C
            
            # Проверяем наличие полей в пробе C
            mass_C = probe_C.get('sample_mass')
            mass_solid_C = probe_C.get('Масса твердого (g)')
                
            volume_C = probe_C.get('V (ml)')
            
            # Если нет ни одного поля, пропускаем группу
            if mass_C is None and volume_C is None:
                stats['missing_fields'] += 1
                continue
            
            # Функция для копирования полей в целевую пробу
            def copy_fields_to_probe(probe_target, probe_type, mass_to_copy, volume_to_copy, solid_mass):
                """Копирует поля из пробы C в целевую пробу"""
                fields_copied = 0
                try:
                    # Для пробы B: также копируем массу в поле 'Масса твердого (g)'
                    if probe_type == 'B' and mass_to_copy is not None:
                        # Копируем массу образца в поле 'Масса твердого (g)' для пробы B
                        probe_target['sample_mass'] = solid_mass
                        fields_copied += 1
                        # Добавляем мета-информацию о копировании
                        probe_target['mass_source_for_solid'] = 'Масса образца (g) из пробы C'
                    
                    # Копируем массу образца
                    if probe_type == 'A' and mass_to_copy is not None:
                        if 'sample_mass' in probe_target:
                            probe_target['sample_mass'] = mass_to_copy
                            fields_copied += 1
                        else:
                            # Если ни одного поля нет, создаем 'sample_mass'
                            probe_target['sample_mass'] = mass_to_copy
                            fields_copied += 1
                    
                    # Копируем объем
                    if volume_to_copy is not None:
                        if 'V (ml)' in probe_target:
                            probe_target['V (ml)'] = volume_to_copy
                            fields_copied += 1
                        else:
                            # Создаем поле, если его нет
                            probe_target['V (ml)'] = volume_to_copy
                            fields_copied += 1
                    
                    if fields_copied > 0:
                        stats['updated_probes'] += 1
                        stats['copied_fields'] += fields_copied
                        
                        # Добавляем метку времени обновления
                        probe_target['last_mass_volume_update'] = datetime.now().isoformat()
                        probe_target['mass_volume_source'] = probe_C.get('name', 'Unknown') # type: ignore
                        if probe_type == 'B':
                            probe_target['mass_solid_copied_from_C'] = True
                    
                    return fields_copied
                    
                except Exception as e:
                    stats['errors'].append({
                        'group': base_name,
                        'probe': probe_target.get('name', 'Unknown'),
                        'error': str(e)
                    })
                    return 0
            
            # Обрабатываем пробу A (прежняя логика)
            probe_A = group.get('A')
            if probe_A:
                copy_fields_to_probe(probe_A, 'A', mass_C, volume_C,mass_solid_C)
            
            # Обрабатываем пробу B (новая логика с проверкой длины имени)
            probe_B = group.get('B')
            if probe_B:
                probe_B_name = probe_B.get('name', '')
                
                # Проверяем длину имени пробы B
                if len(probe_B_name) <= 11:
                    # Имя не превышает 11 символов - копируем данные
                    copy_fields_to_probe(probe_B, 'B', mass_C, volume_C,mass_solid_C)
                else:
                    # Имя превышает 11 символов - пропускаем
                    stats['skipped_B_length'] += 1
                    # Добавляем метку о пропуске
                    probe_B['mass_volume_copy_skipped'] = True
                    probe_B['skip_reason'] = f'Длина имени ({len(probe_B_name)}) > 11 символов'
                    probe_B['last_copy_check'] = datetime.now().isoformat()
            
            # Увеличиваем счетчик валидных групп, если хотя бы одна проба была обновлена
            # для этой группы (учитываем обновленные после обработки A и B)
            if stats['updated_probes'] > 0:
                stats['valid_groups'] += 1
        
        # Если были изменения, сохраняем данные
        if stats['updated_probes'] > 0:
            # Обновляем метаданные
            if 'metadata' not in data:
                data['metadata'] = {}
            
            data['metadata'].update({
                'last_mass_volume_copy': datetime.now().isoformat(),
                'mass_volume_copy_stats': stats
            })
            
            # Сохраняем обновленные данные
            with open(data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            # Создаем финальную версию
            vcs.create_version(
                description=f"Копирование массы/объема завершено: "
                           f"обновлено {stats['updated_probes']} проб в {stats['valid_groups']} группах, "
                           f"пропущено {stats['skipped_B_length']} проб B из-за длины имени",
                author="system",
                change_type="mass_volume_copy_complete"
            )
        
        # Формируем сообщение о результатах
        if stats['updated_probes'] > 0:
            message = (f"Обновлено {stats['updated_probes']} проб (A/B) в {stats['valid_groups']} группах. "
                      f"Скопировано {stats['copied_fields']} полей из проб C.")
        else:
            message = "Нет подходящих групп для копирования или поля уже совпадают"
        
        if stats['missing_fields'] > 0:
            message += f" Пропущено {stats['missing_fields']} групп без полей в пробе C."
        
        if stats['skipped_B_length'] > 0:
            message += f" Пропущено {stats['skipped_B_length']} проб B из-за длины имени > 11 символов."
        
        return {
            'success': True,
            'message': message,
            **stats,
            'version_created': version_info is not None,
            'version_id': version_info['id'] if version_info else None,
            'data_modified': stats['updated_probes'] > 0
        }
        
    except Exception as e:
        return {
            'success': False,
            'message': f"Ошибка копирования массы и объема: {str(e)}",
            'total_groups': 0,
            'valid_groups': 0,
            'updated_probes': 0,
            'copied_fields': 0,
            'skipped_B_length': 0,
            'errors': [{'error': str(e)}]
        }

def find_probe_groups_by_name_pattern(data_file: str = str(DATA_FILE)) -> Dict[str, Any]:
    """
    Вспомогательная функция для поиска и отладки групп проб.
    Находит все группы проб, отличающиеся только предпоследним символом.
    
    Args:
        data_file: Путь к JSON файлу
    
    Returns:
        Словарь с найденными группами для отладки
    """
    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        probes = data.get('probes', [])
        
        # Группируем пробы
        probe_groups = defaultdict(lambda: {'A': None, 'B': None, 'C': None})
        found_probes = []
        
        for probe in probes:
            name = probe.get('name', '')
            if not name or len(name) < 2:
                continue
                
            second_last_char = name[-2]
            if second_last_char in ['A', 'B', 'C']:
                base_name = name[:-2] + name[-1]
                probe_groups[base_name][second_last_char] = { # type: ignore
                    'name': name,
                    'id': probe.get('id'),
                    'sample_mass': probe.get('sample_mass') or probe.get('Масса образца (g)'),
                    'V_ml': probe.get('V (ml)')
                }
                found_probes.append(name)
        
        # Фильтруем только полные группы
        complete_groups = {}
        for base_name, group in probe_groups.items():
            if group['C'] and (group['A'] or group['B']):
                complete_groups[base_name] = group
        
        return {
            'success': True,
            'total_probes': len(found_probes),
            'total_groups': len(probe_groups),
            'complete_groups': len(complete_groups),
            'groups': complete_groups,
            'sample_probes': found_probes[:10]  # Первые 10 проб для примера
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }    
# Глобальная функция для создания версии при изменениях
def create_version_on_change(description, author="system"):
    """Обертка для создания версии при изменениях"""
    return vcs.create_version(description=description, author=author)

def allowed_file(filename):
    """Проверяем разрешенные расширения"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def generate_result_filename(original_filename):
    """Генерация уникального имени для результата"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    name, ext = os.path.splitext(original_filename)
    return f"{name}_result_{timestamp}.json"

def convert_df_to_dict(df:pd.DataFrame, add_mass=True):
    
    df['id'] = df.index + 1
    df['tags'] = [[] for _ in range(len(df))]
    if 'status_id' not in df.columns:
        df['status_id'] = 3
    
    if add_mass:
        
        df['sample_mass'] = 1
    
    df.rename(columns={df.columns[0]: 'name'}, inplace=True)
        
    df['name'].dropna(inplace=True)
    df.dropna(axis=1,how='all', inplace=True)
    
    df = df.fillna('null') 

    new_probes = df.to_dict('records')
    
    return new_probes

def load_data():
    """Загрузка данных из файла"""
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        raise ValueError('Нет данных')

def save_data(data):
    """Сохранение данных в файл"""
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def normalize_probe_ids(data_file=DATA_FILE):
    """
    Принудительно обновляет все ID проб в базе данных
    начиная с 1 и далее по порядку
    
    Args:
        data_file: путь к JSON файлу с данными
    
    Returns:
        tuple: (success, message, changes_count)
    """
    
    try:
        # Загружаем данные
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        probes = data.get('probes', [])
        
        if not probes:
            return True, "Нет проб для обновления", 0
        
        # Проверяем текущие ID
        current_ids = []
        has_duplicates = False
        has_gaps = False
        
        for probe in probes:
            current_id = probe.get('id')
            if current_id is not None:
                if current_id in current_ids:
                    has_duplicates = True
                current_ids.append(current_id)
            else:
                # Пробе без ID
                has_gaps = True
        
        # Сортируем список ID
        current_ids_sorted = sorted([i for i in current_ids if i is not None])
        
        # Проверяем есть ли пропуски или некорректные ID
        expected_ids = list(range(1, len(probes) + 1))
        needs_normalization = (
            has_duplicates or 
            has_gaps or 
            current_ids != expected_ids or
            any(id <= 0 for id in current_ids if id is not None)
        )
        
        if not needs_normalization:
            return True, "ID уже в правильном порядке", 0
        
        # Создаем версию перед изменением
        version_info = vcs.create_version(
            description="Автоматическая нормализация ID проб",
            author="system",
            change_type="normalization"
        )
        
        # Обновляем ID
        changes_count = 0
        id_mapping = {}  # Старый ID -> Новый ID для отслеживания изменений
        
        for index, probe in enumerate(probes):
            old_id = probe.get('id')
            new_id = index + 1
            
            if old_id != new_id:
                probe['id'] = new_id
                if old_id is not None:
                    id_mapping[old_id] = new_id
                
                # Добавляем информацию об изменении
                probe['last_normalized'] = datetime.now().isoformat()
                changes_count += 1
        
        # Обновляем метаданные файла
        if 'metadata' not in data:
            data['metadata'] = {}
        
        data['metadata'].update({
            'last_normalization': datetime.now().isoformat(),
            'normalization_changes': changes_count,
            'total_probes': len(probes),
            'id_mapping': id_mapping if id_mapping else None
        })
        
        # Сохраняем обновленные данные
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Создаем финальную версию
        vcs.create_version(
            description=f"Нормализация завершена: обновлено {changes_count} ID",
            author="system",
            change_type="normalization_complete"
        )
        
        return True, f"Обновлено {changes_count} ID проб", changes_count
        
    except Exception as e:
        return False, f"Ошибка нормализации: {str(e)}", 0

    # Функция ниже - ищет недостающие поля у проб и заполняет их нулями. На выходе также даёт статистику - сколько недостающих полей добавлено у скольких суммарно проб.

def normalize_probe_structure(
        data_file: str = str(DATA_FILE),
        default_value: Any = 0,
) -> Dict[str, Any]:
    with open(data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    probes=data['probes']
    all_fields: Set[str] = set()
    for probe in probes:
        if isinstance(probe,dict):
            all_fields.update(probe.keys())
    
    normalization_stats = {
        'fields_added_total': 0,
        'probes_modified': 0,
    }

    for probe in probes:
        if not isinstance(probe,dict):
            continue
        fields_added = 0
        for field in all_fields:
            if field not in probe:
                probe[field] = default_value
                fields_added += 1
        if fields_added > 0:
            normalization_stats['fields_added_total'] += fields_added
            normalization_stats['probes_modified'] += 1

    with open(data_file,'w',encoding='utf-8') as f:
        json.dump(data,f,ensure_ascii=False,indent=2)

    return {
        "data": data,
        "stats": normalization_stats,
        }

def check_id_consistency(data_file=str(DATA_FILE)):
    """
    Проверяет целостность ID проб
    
    Returns:
        dict: информация о проблемах
    """
    
    with open(data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    probes = data.get('probes', [])
    
    if not probes:
        return {'valid': True, 'message': 'Нет проб для проверки'}
    
    issues = {
        'duplicate_ids': [],
        'missing_ids': [],
        'non_sequential': False,
        'negative_ids': [],
        'zero_ids': []
    }
    
    # Собираем ID
    ids = []
    for i, probe in enumerate(probes):
        probe_id = probe.get('id')
        
        if probe_id is None:
            issues['missing_ids'].append(i + 1)
        elif probe_id < 0:
            issues['negative_ids'].append(probe_id)
        elif probe_id == 0:
            issues['zero_ids'].append(probe_id)
        else:
            ids.append(probe_id)
    
    # Проверяем дубликаты
    seen = set()
    duplicates = set()
    for probe_id in ids:
        if probe_id in seen:
            duplicates.add(probe_id)
        seen.add(probe_id)
    
    issues['duplicate_ids'] = list(duplicates)
    
    # Проверяем последовательность
    if ids:
        expected_min = 1
        expected_max = len(ids)
        actual_min = min(ids)
        actual_max = max(ids)
        
        issues['non_sequential'] = (
            actual_min != expected_min or 
            actual_max != expected_max or
            len(set(range(actual_min, actual_max + 1))) != len(ids)
        )
    
    # Формируем результат
    has_issues = any(
        issues['duplicate_ids'] or
        issues['missing_ids'] or
        issues['negative_ids'] or
        issues['zero_ids'] or
        issues['non_sequential'] # type: ignore
    )
    
    return {
        'valid': not has_issues,
        'issues': issues,
        'total_probes': len(probes),
        'probes_with_ids': len([p for p in probes if p.get('id') is not None]),
        'recommend_normalization': has_issues
    }

def get_next_probe_id(data_file=DATA_FILE):
    """Получение следующего доступного ID для пробы"""
    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        probes = data.get('probes', [])
        
        if not probes:
            return 1
        
        # Находим максимальный ID
        max_id = 0
        for probe in probes:
            current_id = probe.get('id')
            if current_id and current_id > max_id:
                max_id = current_id
        
        return max_id + 1
                    
    except Exception as e:
        print(f"Ошибка получения ID: {str(e)}")
        return 1
def verify_telegram_auth(auth_data):
    """Проверка хеша данных от Telegram"""
    check_hash = auth_data.get('hash')
    # Формируем строку для проверки (все поля, кроме hash, в алфавитном порядке)
    data_check_list = []
    for key, value in sorted(auth_data.items()):
        if key != 'hash':
            data_check_list.append(f'{key}={value}')
    data_check_string = '\n'.join(data_check_list)

    # Вычисляем секретный ключ на основе токена бота
    secret_key = hashlib.sha256(TELEGRAM_TOKEN.encode()).digest() # type: ignore
    # Вычисляем HMAC-SHA256
    hash_v = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    
    return hash_v == check_hash

def is_user_allowed(user_data):
    """Проверяет, есть ли пользователь в файле-секрете"""
    if not os.path.exists(CONFIG_PATH):
        return False
    
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
    
    user_id = int(user_data.get('id'))
    username = user_data.get('username')

    # Проверяем и по ID (надежно), и по логину (удобно)
    if user_id in config.get('allowed_ids', []):
        return True
    if username in config.get('allowed_usernames', []):
        return True
        
    return False

@app.route('/login/telegram')
def telegram_login():
    auth_data = request.args.to_dict()
    
    # 1. Сначала проверяем цифровую подпись (функция из предыдущего шага)
    if not verify_telegram_auth(auth_data):
        return "Ошибка безопасности", 400
        
    # 2. Проверяем по нашему файл-секрету
    if is_user_allowed(auth_data):
        session['user'] = auth_data.get('username') or auth_data.get('id')
        return redirect('/')
    
    return "Доступ запрещен", 403

# API для копирования массы и объема из проб C в A/B
@app.route('/api/copy_mass_volume', methods=['POST'])
def api_copy_mass_volume():
    """API endpoint для копирования массы и объема из проб C в пробы A/B"""
    result = copy_mass_and_volume_from_C_to_AB()
    return jsonify(result)

# API для отладки и поиска групп проб
@app.route('/api/debug/probe_groups', methods=['GET'])
def api_debug_probe_groups():
    """API endpoint для отладки - поиск групп проб"""
    result = find_probe_groups_by_name_pattern()
    return jsonify(result)

# Добавьте также в главную функцию index() вызов этой функции:
@app.route('/')
def index():
    """
    Главная страница с нормализацией, пересчетом и форматированием
    """
    try:
        # 1. Принудительно нормализуем ID
        success, message, changes = normalize_probe_ids()
        
        if changes > 0:
            app.logger.info(f"Normalized {changes} probe IDs: {message}")

        # 2. Заполняем у проб недостающие поля
        normalize_result = normalize_probe_structure(
            data_file=str(DATA_FILE),
            default_value=0
        )

        stats = normalize_result.get('stats',{})
        if stats.get('fields_added_total',0) > 0:
            app.logger.info(f"Normalized structure: added {stats['fields_added_total']} fields")

        # 3. Копирование массы и объема из проб C в A/B (НОВАЯ ФУНКЦИЯ)
        mass_volume_result = copy_mass_and_volume_from_C_to_AB(str(DATA_FILE))
        
        if mass_volume_result.get('success') and mass_volume_result.get('data_modified', False):
            app.logger.info(f"Copied mass/volume: {mass_volume_result.get('message')}")

        # 4. Пересчет массы металлов
        metal_mass_result = recalculate_metal_mass(str(DATA_FILE))
        
        if metal_mass_result.get('success'):
            metal_stats = metal_mass_result
            if metal_stats.get('liquid_probes', 0) > 0 or metal_stats.get('solid_probes', 0) > 0:
                app.logger.info(f"Recalculated metal mass: {metal_mass_result.get('message')}")

        # 5. Пересчитываем зависимые поля
        recalculation_result = check_and_recalculate_dependent_fields(str(DATA_FILE))
        
        if recalculation_result.get('success'):
            app.logger.info(f"Recalculated dependent fields: {recalculation_result.get('message')}")

        # 6. Форматируем числовые значения
        formatting_result = check_and_format_numeric_values(str(DATA_FILE))
        
        if formatting_result.get('success') and not formatting_result.get('skipped', False):
            app.logger.info(f"Formatted numeric values: {formatting_result.get('message')}")
            if formatting_result.get('errors'):
                app.logger.warning(f"Formatting errors: {formatting_result.get('errors')}")

        # 7. Загружаем данные для отображения
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        probes = data.get('probes', [])
        
        return render_template('index.html', 
                            probes=probes,
                            normalization_info={
                                'success': success,
                                'message': message,
                                'changes': changes
                            },
                            
                            mass_volume_info={
                                'success': mass_volume_result.get('success', False),
                                'message': mass_volume_result.get('message', ''),
                                'stats': mass_volume_result
                            },
                            metal_mass_info={
                                'success': metal_mass_result.get('success', False),
                                'message': metal_mass_result.get('message', ''),
                                'stats': metal_mass_result
                            },
                            recalculation_info={
                                'success': recalculation_result.get('success', False),
                                'message': recalculation_result.get('message', ''),
                                'stats': recalculation_result
                            },
                            formatting_info={
                                'success': formatting_result.get('success', False),
                                'message': formatting_result.get('message', ''),
                                'skipped': formatting_result.get('skipped', False)
                            })
        
    except Exception as e:
        app.logger.error(f"Error loading index: {str(e)}")
        return render_template('index.html', probes=[], error=str(e))
    
@app.route('/table')
def render_table():
    
    try:
        return render_template('data_table.html')
    
    except Exception as e:
        
        app.logger.error(f"Error loading table: {str(e)}")
        return render_template('index.html', error=str(e))        

@app.route('/api/data')
def get_data():
    data = load_data()
    return jsonify(data)

@app.route('/api/update_status', methods=['POST'])
def update_status():
    data = request.json
    probe_id = data.get('probe_id') # type: ignore
    status_id = data.get('status_id') # type: ignore
    
    db_data = load_data()
    
    # Обновление статуса пробы
    for probe in db_data['probes']:
        if probe['id'] == probe_id:
            probe['status_id'] = status_id
            break
    
    save_data(db_data)
    return jsonify({"success": True})

@app.route('/api/update_priority', methods=['POST'])
def update_priority():
    data = request.json
    probe_id = data.get('probe_id') # type: ignore
    priority_id = data.get('priority_id') # type: ignore
    
    db_data = load_data()
    
    # Обновление приоритета пробы
    for probe in db_data['probes']:
        if probe['id'] == probe_id:
            probe['priority'] = priority_id
            break
    
    save_data(db_data)   
    return jsonify({"success": True})

@app.route('/api/add_status', methods=['POST'])
def add_status():
    data = request.json
    name = data.get('name') # type: ignore
    color = data.get('color') # type: ignore
    
    if not name or not color:
        return jsonify({"success": False, "error": "Не указано имя или цвет"})
    
    db_data = load_data()
    
    # Генерация нового ID
    new_id = max([s['id'] for s in db_data['statuses']], default=0) + 1
    
    new_status = {
        "id": new_id,
        "name": name,
        "color": color
    }
    
    db_data['statuses'].append(new_status)
    save_data(db_data)
    
    return jsonify({"success": True, "status": new_status})

@app.route('/api/add_probe', methods=['POST'])
def add_probe():
    data = request.json
    
    db_data = load_data()
    
    new_probe = {
        "id": get_next_probe_id() + 1, # type: ignore
        "name": data.get('name', 'Новая проба'), # type: ignore
        "Fe": float(data.get('Fe', 0)), # type: ignore
        "Ni": float(data.get('Ni', 0)), # type: ignore
        "Cu": float(data.get('Cu', 0)), # type: ignore
        "sample_mass": float(data.get('sample_mass', 0)), # type: ignore
        "status_id": 1,
        "priority": 1,
        "tags": data.get('tags', []), # type: ignore
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    db_data['probes'].append(new_probe)
    save_data(db_data)
    
    return jsonify({"success": True, "probe": new_probe})

@app.route('/api/upload_ISPAES', methods=['POST'])
def upload_file():
    """
    API endpoint для загрузки файла
    """
    
    # Проверяем наличие файла в запросе
    if 'file' not in request.files:
        return jsonify({
            'success': False,
            'error': 'No file part in the request'
        }), 400
    
    file = request.files['file']
    
    # Проверяем что файл выбран
    if file.filename == '':
        return jsonify({
            'success': False,
            'error': 'No file selected'
        }), 400
    
    # Проверяем расширение файла
    if not allowed_file(file.filename):
        return jsonify({
            'success': False,
            'error': f'File type not allowed. Allowed types: {", ".join(app.config["ALLOWED_EXTENSIONS"])}'
        }), 400
    
    try:
        # Безопасное сохранение файла
        original_filename = secure_filename(file.filename) # type: ignore
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], original_filename)
        file.save(file_path)
        
        # Получаем дополнительные параметры из запроса
        processing_type = request.form.get('processing_type', 'default')
        parameters_str = request.form.get('parameters', '{}')
        
        try:
            parameters = json.loads(parameters_str)
        except json.JSONDecodeError:
            parameters = {}
        
        # Обрабатываем файл с помощью Python-скрипта
        result_data, _ = process_icp_aes_data(
            file_path=file_path
        )
        
        json_data = convert_df_to_dict(result_data,add_mass=False) # type: ignore
        
        # ЗАГРУЖАЕМ ТЕКУЩИЕ ДАННЫЕ ПЕРЕД ИЗМЕНЕНИЕМ
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 1. СОЗДАЕМ ВЕРСИЮ ТЕКУЩЕГО СОСТОЯНИЯ (до изменений)
        vcs.create_version(
            description=f"Импорт файла '{original_filename}' - состояние до импорта",
            author=request.form.get('author', 'anonymous'),
            change_type='pre_import'
        )
        
        # Создаем словарь для быстрого поиска проб по имени
        existing_probes_dict = {}
        for probe in data['probes']:
            name = probe.get('name')
            if name:
                existing_probes_dict[name] = probe
        
        # Статистика для отчета
        updated_count = 0
        added_count = 0
        
        # 2. ОБРАБАТЫВАЕМ НОВЫЕ ДАННЫЕ - ОБЪЕДИНЯЕМ ИЛИ ДОБАВЛЯЕМ
        for new_probe in json_data:
            probe_name = new_probe.get('name')
            
            if probe_name and probe_name in existing_probes_dict:
                # ОБЪЕДИНЕНИЕ: если проба с таким именем уже существует
                existing_probe = existing_probes_dict[probe_name]
                
                # Объединяем данные, новые значения перезаписывают старые
                for key, value in new_probe.items():
                    existing_probe[key] = value
                
                updated_count += 1
            else:
                # ДОБАВЛЕНИЕ: если это новая проба
                data['probes'].append(new_probe)
                if probe_name:
                    existing_probes_dict[probe_name] = new_probe
                added_count += 1
        
        # 3. СОХРАНЯЕМ ОБНОВЛЕННЫЕ ДАННЫЕ
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # 4. СОЗДАЕМ ВЕРСИЮ ПОСЛЕ ИМПОРТА
        version_info = vcs.create_version(
            description=f"Импорт файла '{original_filename}' - обновлено: {updated_count}, добавлено: {added_count}",
            author=request.form.get('author', 'anonymous'),
            change_type='import'
        )
        
        # Сохраняем результат в JSON файл
        result_filename = generate_result_filename(original_filename)
        result_path = os.path.join(app.config['RESULTS_FOLDER'], result_filename)
        
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        # Формируем URL для скачивания результата
        download_url = f"/api/download/{result_filename}"
        
        return jsonify({
            'success': True,
            'message': 'File processed successfully',
            'original_filename': original_filename,
            'result_filename': result_filename,
            'download_url': download_url,
            'version_created': version_info is not None,
            'version_id': version_info['id'] if version_info else None,
            'metadata': {
                'processing_type': processing_type,
                'rows_processed': len(json_data),
                'probes_updated': updated_count,
                'probes_added': added_count,
                'total_probes_after': len(data['probes']),
                'timestamp': datetime.now().isoformat()
            }
        })
        
    except Exception as e:
        # Логируем ошибку
        app.logger.error(f"Error processing file: {str(e)}")
        
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Error processing file'
        }), 500

@app.route('/api/upload_ISPMS', methods=['POST'])
def upload_file_MS():
    """
    API endpoint для загрузки файла ИСПМС
    """
    
    # Проверяем наличие файла в запросе
    if 'file' not in request.files:
        return jsonify({
            'success': False,
            'error': 'No file part in the request'
        }), 400
    
    file = request.files['file']
    
    # Проверяем что файл выбран
    if file.filename == '':
        return jsonify({
            'success': False,
            'error': 'No file selected'
        }), 400
    
    # Проверяем расширение файла
    if not allowed_file(file.filename):
        return jsonify({
            'success': False,
            'error': f'File type not allowed. Allowed types: {", ".join(app.config["ALLOWED_EXTENSIONS"])}'
        }), 400
    
    try:
        # Безопасное сохранение файла
        original_filename = secure_filename(file.filename) # type: ignore
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], original_filename)
        file.save(file_path)
        
        # Получаем дополнительные параметры из запроса
        processing_type = request.form.get('processing_type', 'default')
        parameters_str = request.form.get('parameters', '{}')
        
        try:
            parameters = json.loads(parameters_str)
        except json.JSONDecodeError:
            parameters = {}
        
        # Обрабатываем файл с помощью Python-скрипта
        result_data= process_metal_samples_csv(
            file_path=file_path
        )
        
        json_data = convert_df_to_dict(result_data,add_mass=False) # type: ignore
        
        # ЗАГРУЖАЕМ ТЕКУЩИЕ ДАННЫЕ ПЕРЕД ИЗМЕНЕНИЕМ
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 1. СОЗДАЕМ ВЕРСИЮ ТЕКУЩЕГО СОСТОЯНИЯ (до изменений)
        vcs.create_version(
            description=f"Импорт файла '{original_filename}' - состояние до импорта",
            author=request.form.get('author', 'anonymous'),
            change_type='pre_import'
        )
        
        # Создаем словарь для быстрого поиска проб по имени
        existing_probes_dict = {}
        for probe in data['probes']:
            name = probe.get('name')
            if name:
                existing_probes_dict[name] = probe
        
        # Статистика для отчета
        updated_count = 0
        added_count = 0
        
        # 2. ОБРАБАТЫВАЕМ НОВЫЕ ДАННЫЕ - ОБЪЕДИНЯЕМ ИЛИ ДОБАВЛЯЕМ
        for new_probe in json_data:
            probe_name = new_probe.get('name')
            
            if probe_name and probe_name in existing_probes_dict:
                # ОБЪЕДИНЕНИЕ: если проба с таким именем уже существует
                existing_probe = existing_probes_dict[probe_name]
                
                # Объединяем данные, новые значения перезаписывают старые
                for key, value in new_probe.items():
                    existing_probe[key] = value
                
                updated_count += 1
            else:
                # ДОБАВЛЕНИЕ: если это новая проба
                data['probes'].append(new_probe)
                if probe_name:
                    existing_probes_dict[probe_name] = new_probe
                added_count += 1
        
        # 3. СОХРАНЯЕМ ОБНОВЛЕННЫЕ ДАННЫЕ
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # 4. СОЗДАЕМ ВЕРСИЮ ПОСЛЕ ИМПОРТА
        version_info = vcs.create_version(
            description=f"Импорт файла '{original_filename}' - обновлено: {updated_count}, добавлено: {added_count}",
            author=request.form.get('author', 'anonymous'),
            change_type='import'
        )
        
        # Сохраняем результат в JSON файл
        result_filename = generate_result_filename(original_filename)
        result_path = os.path.join(app.config['RESULTS_FOLDER'], result_filename)
        
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        # Формируем URL для скачивания результата
        download_url = f"/api/download/{result_filename}"
        
        return jsonify({
            'success': True,
            'message': 'File processed successfully',
            'original_filename': original_filename,
            'result_filename': result_filename,
            'download_url': download_url,
            'version_created': version_info is not None,
            'version_id': version_info['id'] if version_info else None,
            'metadata': {
                'processing_type': processing_type,
                'rows_processed': len(json_data),
                'probes_updated': updated_count,
                'probes_added': added_count,
                'total_probes_after': len(data['probes']),
                'timestamp': datetime.now().isoformat()
            }
        })
        
    except Exception as e:
        # Логируем ошибку
        app.logger.error(f"Error processing file: {str(e)}")
        
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Error processing file'
        }), 500


@app.route('/api/upload_data', methods=['POST'])
def upload_data():
    """
    API endpoint для загрузки файла
    """
    
    # Проверяем наличие файла в запросе
    if 'file' not in request.files:
        return jsonify({
            'success': False,
            'error': 'No file part in the request'
        }), 400
    
    file = request.files['file']
    
    # Проверяем что файл выбран
    if file.filename == '':
        return jsonify({
            'success': False,
            'error': 'No file selected'
        }), 400
    
    # Проверяем расширение файла
    if not allowed_file(file.filename):
        return jsonify({
            'success': False,
            'error': f'File type not allowed. Allowed types: {", ".join(app.config["ALLOWED_EXTENSIONS"])}'
        }), 400
    
    try:
        # Безопасное сохранение файла
        original_filename = secure_filename(file.filename) # type: ignore
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], original_filename)
        file.save(file_path)
        
        # Получаем дополнительные параметры из запроса
        processing_type = request.form.get('processing_type', 'default')
        parameters_str = request.form.get('parameters', '{}')
        
        try:
            parameters = json.loads(parameters_str)
        except json.JSONDecodeError:
            parameters = {}
        
        result_data = pd.read_csv(file_path,sep=';')
        
        json_data = convert_df_to_dict(result_data,add_mass=False)
        
        # ЗАГРУЖАЕМ ТЕКУЩИЕ ДАННЫЕ ПЕРЕД ИЗМЕНЕНИЕМ
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 1. СОЗДАЕМ ВЕРСИЮ ТЕКУЩЕГО СОСТОЯНИЯ (до изменений)
        vcs.create_version(
            description=f"Импорт файла '{original_filename}' - состояние до импорта",
            author=request.form.get('author', 'anonymous'),
            change_type='pre_import'
        )
        
        # Создаем словарь для быстрого поиска проб по имени
        existing_probes_dict = {}
        for probe in data['probes']:
            name = probe.get('name')
            if name:
                existing_probes_dict[name] = probe
        
        # Статистика для отчета
        updated_count = 0
        added_count = 0
        
        # 2. ОБРАБАТЫВАЕМ НОВЫЕ ДАННЫЕ - ОБЪЕДИНЯЕМ ИЛИ ДОБАВЛЯЕМ
        for new_probe in json_data:
            probe_name = new_probe.get('name')
            
            if probe_name and probe_name in existing_probes_dict:
                # ОБЪЕДИНЕНИЕ: если проба с таким именем уже существует
                existing_probe = existing_probes_dict[probe_name]
                
                # Объединяем данные, новые значения перезаписывают старые
                for key, value in new_probe.items():
                    existing_probe[key] = value
                
                updated_count += 1
            else:
                # ДОБАВЛЕНИЕ: если это новая проба
                data['probes'].append(new_probe)
                if probe_name:
                    existing_probes_dict[probe_name] = new_probe
                added_count += 1
        
        # 3. СОХРАНЯЕМ ОБНОВЛЕННЫЕ ДАННЫЕ
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # 4. СОЗДАЕМ ВЕРСИЮ ПОСЛЕ ИМПОРТА
        version_info = vcs.create_version(
            description=f"Импорт файла '{original_filename}' - обновлено: {updated_count}, добавлено: {added_count}",
            author=request.form.get('author', 'anonymous'),
            change_type='import'
        )
        
        # Сохраняем результат в JSON файл
        result_filename = generate_result_filename(original_filename)
        result_path = os.path.join(app.config['RESULTS_FOLDER'], result_filename)
        
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        # Формируем URL для скачивания результата
        download_url = f"/api/download/{result_filename}"
        
        return jsonify({
            'success': True,
            'message': 'File processed successfully',
            'original_filename': original_filename,
            'result_filename': result_filename,
            'download_url': download_url,
            'version_created': version_info is not None,
            'version_id': version_info['id'] if version_info else None,
            'metadata': {
                'processing_type': processing_type,
                'rows_processed': len(json_data),
                'probes_updated': updated_count,
                'probes_added': added_count,
                'total_probes_after': len(data['probes']),
                'timestamp': datetime.now().isoformat()
            }
        })
        
    except Exception as e:
        # Логируем ошибку
        app.logger.error(f"Error processing file: {str(e)}")
        
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Error processing file'
        }), 500



@app.route('/api/download/<filename>', methods=['GET'])
def download_result(filename):
    """API для скачивания обработанного JSON файла"""
    file_path = os.path.join(app.config['RESULTS_FOLDER'], filename)
    
    if not os.path.exists(file_path):
        return jsonify({
            'success': False,
            'error': 'File not found'
        }), 404
    
    return send_file(
        file_path,
        as_attachment=True,
        download_name=filename,
        mimetype='application/json'
    )
 
# API для обновления проб
@app.route('/api/probes/update', methods=['POST'])
def update_probes():
    """Обновление данных проб"""
    try:
        data = request.json
        
        # Сохраняем текущую версию перед изменением
        vcs.create_version(
            description=data.get('change_description', 'Update probes'), # type: ignore
            author=request.headers.get('X-User', 'anonymous'),
            change_type='update'
        )
        
        # Сохраняем новые данные
        with open(app.config['DATA_FILE'], 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        return jsonify({
            'success': True,
            'message': 'Probes updated successfully',
            'version_created': True
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Страница управления версиями
@app.route('/versions')
def versions_page():
    """Страница управления версиями"""
    all_versions = vcs.get_all_versions()
    return render_template('versions.html', 
                         versions=all_versions,
                         total_versions=len(all_versions))

# API для получения информации о версиях
@app.route('/api/versions')
def get_versions():
    """API для получения списка версий"""
    versions = vcs.get_all_versions()
    return jsonify({
        'success': True,
        'versions': versions,
        'total': len(versions)
    })

# API для получения конкретной версии
@app.route('/api/versions/<int:version_id>')
def get_version(version_id):
    """API для получения конкретной версии"""
    version_data = vcs.get_version(version_id)
    
    if version_data:
        return jsonify({
            'success': True,
            'version': version_data['metadata'],
            'preview_data': version_data['data']
        })
    else:
        return jsonify({
            'success': False,
            'error': 'Version not found'
        }), 404

# API для восстановления версии
@app.route('/api/versions/<int:version_id>/restore', methods=['POST'])
def restore_version(version_id):
    """API для восстановления версии"""
    try:
        success = vcs.restore_version(version_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': f'Version {version_id} restored successfully',
                'redirect': '/'
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Version not found'
            }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# API для сравнения версий
@app.route('/api/versions/compare')
def compare_versions():
    """API для сравнения двух версий"""
    version1 = request.args.get('v1', type=int)
    version2 = request.args.get('v2', type=int)
    
    if not version1 or not version2:
        return jsonify({
            'success': False,
            'error': 'Both version parameters (v1, v2) are required'
        }), 400
    
    comparison = vcs.compare_versions(version1, version2)
    
    return jsonify({
        'success': True,
        'comparison': comparison
    })

# API для создания версии вручную
@app.route('/api/versions/create', methods=['POST'])
def create_version():
    """Ручное создание версии"""
    try:
        description = request.json.get('description', 'Manual version creation') # type: ignore
        author = request.json.get('author', 'anonymous') # type: ignore
        
        version = vcs.create_version(
            description=description,
            author=author,
            change_type='manual'
        )
        
        if version:
            return jsonify({
                'success': True,
                'message': 'Version created successfully',
                'version': version
            })
        else:
            return jsonify({
                'success': False,
                'message': 'No changes detected'
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/probes/<int:probe_id>/update_probe', methods=['PUT'])
def update_probe(probe_id):
    """API для обновления данных пробы
    
    Args:
        probe_id (int): id пробы для обновления
    """
    try:
        # Получаем данные из запроса
        update_data = request.get_json()
        
        if not update_data:
            return jsonify({
                'success': False,
                'error': 'Нет данных для обновления'
            }), 400
            
        # Проверяем ID в данных
        if 'id' not in update_data or update_data['id'] != probe_id:
            return jsonify({
                'success': False,
                'error': 'Несоответствие ID пробы'
            }), 400
            
        # Загружаем текущие данные
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # СОЗДАЕМ ВЕРСИЮ перед редактированием
        version_info = vcs.create_version(
            description=f"Редактирование пробы #{probe_id}",
            author=request.headers.get('X-User-Email', 'anonymous'),
            change_type='edit'
        )        
  
        # Находим пробу для обновления
        probes = data.get('probes', [])
        probe_index = -1
        
        for i, probe in enumerate(probes):
            if probe.get('id') == probe_id:
                probe_index = i
                break
                
        if probe_index == -1:
            return jsonify({
                'success': False,
                'error': f'Проба с ID {probe_id} не найдена'
            }), 404
            
        # Обновляем пробу
        # Сохраняем старые значения для полей, которые не пришли в обновлении
        old_probe = probes[probe_index]
        updated_probe = {**old_probe, **update_data}
        
        # Обновляем timestamp
        updated_probe['last_normalized'] = datetime.now().isoformat()
        
        # Заменяем пробу в списке
        probes[probe_index] = updated_probe
        
        # Сохраняем обратно в файл
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        # Логируем действие
        user_email = request.headers.get('X-User-Email', 'anonymous')
        app.logger.info(f"Probe #{probe_id} updated by {user_email}")
        
        return jsonify({
            'success': True,
            'message': f'Проба #{probe_id} успешно обновлена',
            'probe': updated_probe,
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'user': user_email
            }
        })
        
    except json.JSONDecodeError as e:
        return jsonify({
            'success': False,
            'error': f'Неверный формат JSON: {str(e)}'
        }), 400
    except Exception as e:
        app.logger.error(f"Error updating probe {probe_id}: {str(e)}")
        
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Ошибка при обновлении пробы'
        }), 500


# Функция для валидации данных пробы
def validate_probe_data(probe_data):
    """Валидация данных пробы"""
    required_fields = ['name', 'sample_mass']
    
    for field in required_fields:
        if field not in probe_data or not probe_data[field]:
            return False, f'Отсутствует обязательное поле: {field}'
    
    # Проверка числовых полей
    numeric_fields = ['sample_mass', 'Ca', 'Fe', 'Ni', 'Cu', 'Co', 
                     'dCa', 'dFe', 'dNi', 'dCu', 'dCo']
    
    for field in numeric_fields:
        if field in probe_data and probe_data[field] is not None:
            try:
                float(probe_data[field])
            except (ValueError, TypeError):
                return False, f'Некорректное значение в поле {field}'
    
    return True, 'Данные валидны'

@app.route('/api/probes/<int:probe_id>/upload_to_edit', methods=['GET'])
def upload_to_edit_prob(probe_id):
    """API для загрузки информации для редактирования пробы
    
    Args:
        probe_id (int): id пробы из базы данных
    """
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        probe_to_edit = None
        probes = data.get('probes', [])
    
        for i, probe in enumerate(probes):
            if probe.get('id') == probe_id:
                probe_to_edit = probe
                break
        
        if not probe_to_edit:
            return jsonify({
                'success': False,
                'error': f'Проба с ID {probe_id} не найдена'
            }), 404
        
        # Получаем все возможные поля из всех проб для динамического создания формы
        all_fields = set()
        for probe in probes:
            all_fields.update(probe.keys())
        
        # Определяем типы полей на основе первой пробы с таким полем
        field_types = {}
        for field in all_fields:
            for probe in probes:
                if field in probe:
                    value = probe[field]
                    if isinstance(value, bool):
                        field_types[field] = 'boolean'
                    elif isinstance(value, (int, float)):
                        field_types[field] = 'number'
                    elif isinstance(value, list):
                        field_types[field] = 'array'
                    else:
                        field_types[field] = 'string'
                    break
        
        # Определяем человекочитаемые названия полей
        field_labels = {
            'id': 'ID пробы',
            'name': 'Название пробы',
            'status_id': 'Статус',
            'priority': 'Приоритет',
            'last_normalized': 'Последнее обновление',
            'is_solid': 'Твердая проба',
            'is_solution': 'Раствор',
            'sample_mass': 'Масса образца (g)',
            'V (ml)': 'Объем (ml)',
            'Масса навески (mg)': 'Масса навески (mg)',
            'Разбавление': 'Разбавление',
            'Ca': 'Кальций (Ca)',
            'Fe': 'Железо (Fe)',
            'Ni': 'Никель (Ni)',
            'Cu': 'Медь (Cu)',
            'Co': 'Кобальт (Co)',
            'dCa': 'Погрешность Ca',
            'dFe': 'Погрешность Fe',
            'dNi': 'Погрешность Ni',
            'dCu': 'Погрешность Cu',
            'dCo': 'Погрешность Co',
            'Кто готовил': 'Кто готовил',
            'Среда': 'Среда',
            'Аналиты': 'Аналиты',
            'Описание': 'Описание',
            'tags': 'Теги',
        }
        
        return jsonify({
            'success': True,
            'message': f'Проба #{probe_id} успешно загружена',
            'probe': probe_to_edit,
            'metadata': {
                'field_types': field_types,
                'field_labels': field_labels,
                'all_fields': list(all_fields),
                'timestamp': datetime.now().isoformat(),
                'user': request.headers.get('X-User-Email', 'anonymous')
            }
        })
        
    except Exception as e:
        app.logger.error(f"Error upload probe {probe_id}: {str(e)}")
        
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Ошибка при загрузке пробы'
        }), 500
        
@app.route('/api/probes/<int:probe_id>/delete', methods=['DELETE'])
def delete_probe(probe_id):
    """
    API для удаления одной пробы
    
    Пример запроса:
    DELETE /api/probes/123/delete
    """
    
    try:
        # Загружаем текущие данные
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Ищем пробу для удаления
        probe_to_delete = None
        probes = data.get('probes', [])
        
        for i, probe in enumerate(probes):
            if probe.get('id') == probe_id:
                probe_to_delete = probe
                break
        
        if not probe_to_delete:
            return jsonify({
                'success': False,
                'error': f'Проба с ID {probe_id} не найдена'
            }), 404
        
        # Информация о удаляемой пробе (для описания версии)
        probe_info = {
            'id': probe_id,
            'name': probe_to_delete.get('name', 'Без названия'),
            'sample_id': probe_to_delete.get('sample_id', ''),
            'date': probe_to_delete.get('date', '')
        }
        
        # СОЗДАЕМ ВЕРСИЮ перед удалением
        version_info = vcs.create_version(
            description=f"Удаление пробы #{probe_id} ({probe_info['name']})",
            author=request.headers.get('X-User-Email', 'anonymous'),
            change_type='delete'
        )
        
        # Удаляем пробу
        data['probes'] = [p for p in probes if p.get('id') != probe_id]
        
        # Сохраняем обновленные данные
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Подтверждаем удаление с дополнительной версией (опционально)
        vcs.create_version(
            description=f"Подтверждение удаления пробы #{probe_id}",
            author='system',
            change_type='delete_confirm'
        )
        
        return jsonify({
            'success': True,
            'message': f'Проба #{probe_id} успешно удалена',
            'deleted_probe': probe_info,
            'version_created': version_info is not None,
            'version_id': version_info['id'] if version_info else None,
            'metadata': {
                'total_probes_before': len(probes),
                'total_probes_after': len(data['probes']),
                'timestamp': datetime.now().isoformat(),
                'user': request.headers.get('X-User-Email', 'anonymous')
            }
        })
        
    except Exception as e:
        app.logger.error(f"Error deleting probe {probe_id}: {str(e)}")
        
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Ошибка при удалении пробы'
        }), 500

# API для удаления версии (с осторожностью!)
@app.route('/api/versions/<int:version_id>/delete', methods=['DELETE'])
def delete_version(version_id):
    """Удаление версии"""
    try:
        success = vcs.delete_version(version_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': f'Version {version_id} deleted successfully'
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Version not found'
            }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Экспорт версии
@app.route('/api/versions/<int:version_id>/export')
def export_version(version_id):
    """Экспорт версии в виде файла"""
    version_data = vcs.get_version(version_id)
    
    if not version_data:
        return jsonify({'error': 'Version not found'}), 404
    
    # Создаем временный файл для экспорта
    export_filename = f"probes_version_{version_id}_{datetime.now().strftime('%Y%m%d')}.json"
    export_path = os.path.join(BASE_DIR / 'temp', export_filename)
    
    os.makedirs(BASE_DIR / 'temp', exist_ok=True)
    
    with open(export_path, 'w', encoding='utf-8') as f:
        json.dump(version_data, f, ensure_ascii=False, indent=2)
    
    return send_file(
        export_path,
        as_attachment=True,
        download_name=export_filename,
        mimetype='application/json'
    )

@app.route('/api/probes/save', methods=['POST'])
def save_probes():
    data = request.json
    
    # Автоматическое создание версии
    vcs.create_version(
        description=data.get('change_description', 'Изменение данных проб'), # type: ignore
        author=request.headers.get('X-User', 'anonymous'),
        change_type='update'
    )
    
    # Сохранение данных
    with open(DATA_FILE, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    return jsonify({'success': True})

@app.route('/api/export/excel', methods=['GET'])
def export_excel():
    """Экспорт всей базы данных в Excel"""
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        df = pd.json_normalize(data=data['probes'])
        filename = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        output = BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        
        return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=filename
            )
    
    except Exception as e:
        app.logger.error(f"Ошибка при экспорте: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Health check endpoint
@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'upload_folder': app.config['UPLOAD_FOLDER'],
        'results_folder': app.config['RESULTS_FOLDER']
    })

def get_available_columns():
    """Получение списка доступных колонок для осей"""
    data = load_data()
    if not data["probes"]:
        return [], []
    
    df = pd.DataFrame(data["probes"])
    
    # Фильтрация числовых колонок
    numeric_cols = []
    for col in df.columns:
        if col not in BLACKLIST_FIELDS:
            # Проверяем, есть ли числовые значения в колонке
            sample_values = df[col].dropna().head(10)
            if len(sample_values) > 0:
                # Проверяем, можно ли преобразовать значения в числа
                try:
                    pd.to_numeric(sample_values)
                    numeric_cols.append(col)
                except:
                    continue
    
    return sorted(numeric_cols)

@app.route('/graph')
def plot_graph():
    """Главная страница"""
    return render_template('plot_graph.html')

@app.route('/api/columns', methods=['GET'])
def get_columns():
    """API для получения доступных колонок"""
    columns = get_available_columns()
    return jsonify({"columns": columns})

@app.route('/api/plot', methods=['POST'])
def create_plot():
    """API для создания графика"""
    try:
        data = request.json
        x_axis = data.get('x_axis') # type: ignore
        y_axis = data.get('y_axis') # type: ignore
        
        print(f"API called with x_axis={x_axis}, y_axis={y_axis}")
        
        if not x_axis or not y_axis:
            return jsonify({"error": "Не указаны оси X и Y"}), 400
        
        # Загружаем данные
        db_data = load_data()
        if not db_data["probes"]:
            print("No probes data found")
            return jsonify({"error": "Нет данных"}), 404
        
        df = pd.DataFrame(db_data["probes"])
        print(f"DataFrame loaded with {len(df)} rows, columns: {list(df.columns)}")
        
        # Проверяем наличие колонок
        if x_axis not in df.columns:
            print(f"X axis column '{x_axis}' not found in DataFrame")
            return jsonify({"error": f"Колонка X '{x_axis}' не найдена"}), 404
        
        if y_axis not in df.columns:
            print(f"Y axis column '{y_axis}' not found in DataFrame")
            return jsonify({"error": f"Колонка Y '{y_axis}' не найдена"}), 404
        
        print(f"Data sample for {x_axis}: {df[x_axis].head().tolist()}")
        print(f"Data sample for {y_axis}: {df[y_axis].head().tolist()}")
        
        # Проверяем, есть ли числовые данные
        print(f"X data type: {df[x_axis].dtype}, NaN count: {df[x_axis].isna().sum()}")
        print(f"Y data type: {df[y_axis].dtype}, NaN count: {df[y_axis].isna().sum()}")
        
        # Создаем график
        fig = go.Figure()
        
        # Разделяем точки по статусу (если есть)
        if 'status_id' in df.columns:
            statuses = df['status_id'].unique()
            colors = ['#00ff88', '#0088ff', '#ff8800', '#ff0088', '#8800ff']
            
            print(f"Found {len(statuses)} unique statuses: {statuses}")
            
            for i, status in enumerate(statuses):
                mask = df['status_id'] == status
                mask_data = df.loc[mask]
                print(f"Status {status}: {mask.sum()} points")
                print(f"X values: {mask_data[x_axis].head().tolist()}")
                print(f"Y values: {mask_data[y_axis].head().tolist()}")
                
                fig.add_trace(go.Scatter(
                    x=mask_data[x_axis].tolist(),
                    y=mask_data[y_axis].tolist(),
                    mode='markers',
                    name=f'Status {status}',
                    marker=dict(
                        size=12,
                        color=colors[i % len(colors)],
                        line=dict(width=2, color='white')
                    ),
                    hovertext=mask_data['name'].tolist() if 'name' in mask_data.columns else None,
                    hoverinfo='text+x+y'
                ))
        else:
            # Все точки одним цветом
            print(f"Total points: {len(df)}")
            print(f"X values: {df[x_axis].head().tolist()}")
            print(f"Y values: {df[y_axis].head().tolist()}")
            
            fig.add_trace(go.Scatter(
                x=df[x_axis].tolist(),
                y=df[y_axis].tolist(),
                mode='markers',
                marker=dict(
                    size=12,
                    color='#00ff88',
                    line=dict(width=2, color='white')
                ),
                hovertext=df['name'].tolist() if 'name' in df.columns else None,
                hoverinfo='text+x+y'
            ))
        
        # Настройка стиля графика
        fig.update_layout(
            plot_bgcolor='rgba(10, 10, 20, 0.9)',
            paper_bgcolor='rgba(10, 10, 20, 0.7)',
            font=dict(color='#ffffff', family='Arial, sans-serif'),
            title=dict(
                text=f'{y_axis} vs {x_axis}',
                font=dict(size=20, color='#00ff88')
            ),
            xaxis=dict(
                title=dict(text=x_axis, font=dict(size=14, color='#ffffff')),
                gridcolor='rgba(255, 255, 255, 0.1)',
                zerolinecolor='rgba(255, 255, 255, 0.3)'
            ),
            yaxis=dict(
                title=dict(text=y_axis, font=dict(size=14, color='#ffffff')),
                gridcolor='rgba(255, 255, 255, 0.1)',
                zerolinecolor='rgba(255, 255, 255, 0.3)'
            ),
            hovermode='closest',
            showlegend='status_id' in df.columns,
            legend=dict(
                font=dict(color='#ffffff'),
                bgcolor='rgba(0, 0, 0, 0.5)'
            ),
            margin=dict(l=50, r=50, t=50, b=50)
        )
        
        # Конвертируем в JSON
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        
        print(f"Generated plot JSON length: {len(graphJSON)}")
        
        return jsonify({
            "plot": graphJSON,
            "statistics": {
                "total_points": len(df),
                "x_mean": float(df[x_axis].mean()),
                "y_mean": float(df[y_axis].mean()),
                "x_std": float(df[x_axis].std()),
                "y_std": float(df[y_axis].std())
            }
        })
        
    except Exception as e:
        print(f"Error in create_plot: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/data/sample', methods=['GET'])
def get_data_sample():
    """API для получения образца данных"""
    data = load_data()
    sample = data["probes"]
    return jsonify({"sample": sample})

@app.route('/api/add_series', methods=['POST'])
def add_series():
    """API для создания серии проб"""
    try:
        data = request.json
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'Нет данных'
            }), 400
        
        base_name = data.get('base_name')
        method_number = data.get('method_number')
        repeat_number = data.get('repeat_number')
        probes_data = data.get('probes', [])
        
        if not base_name or not probes_data:
            return jsonify({
                'success': False,
                'error': 'Отсутствуют обязательные данные'
            }), 400
        
        # Загружаем текущие данные
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            db_data = json.load(f)
        
        # Создаем версию перед изменением
        vcs.create_version(
            description=f"Создание серии проб '{base_name}' (методика {method_number})",
            author=request.headers.get('X-User-Email', 'anonymous'),
            change_type='series_creation'
        )
        
        # Получаем следующий доступный ID
        current_probes = db_data.get('probes', [])
        max_id = max([p.get('id', 0) for p in current_probes], default=0)
        
        created_count = 0
        new_probes = []
        
        # Добавляем каждую пробу из серии
        for probe_data in probes_data:
            max_id += 1
            
            new_probe = {
                'id': max_id,
                'name': probe_data['name'],
                'sample_mass': probe_data.get('sample_mass', 1.0),
                'V (ml)': probe_data.get('V_ml', 100.0),
                'status_id': probe_data.get('status_id', 1),
                'priority': probe_data.get('priority', 1),
                'Fe': probe_data.get('Fe', 0),
                'Ni': probe_data.get('Ni', 0),
                'Cu': probe_data.get('Cu', 0),
                'tags': probe_data.get('tags', []),
                'method_number': method_number,
                'repeat_number': repeat_number,
                'is_series': True,
                'series_base': base_name,
                'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'last_normalized': datetime.now().isoformat()
            }
            
            new_probes.append(new_probe)
            created_count += 1
        
        # Добавляем новые пробы к существующим
        db_data['probes'].extend(new_probes)
        
        # Сохраняем обновленные данные
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(db_data, f, ensure_ascii=False, indent=2)
        
        # Создаем финальную версию
        vcs.create_version(
            description=f"Серия '{base_name}' создана: {created_count} проб",
            author='system',
            change_type='series_complete'
        )
        
        return jsonify({
            'success': True,
            'message': f'Серия проб создана успешно',
            'created_count': created_count,
            'base_name': base_name,
            'method_number': method_number,
            'probes_created': [p['name'] for p in new_probes],
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'user': request.headers.get('X-User-Email', 'anonymous'),
                'total_probes': len(db_data['probes'])
            }
        })
        
    except Exception as e:
        app.logger.error(f"Error creating series: {str(e)}")
        
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Ошибка при создании серии проб'
        }), 500


@app.route('/api/validate_series_name', methods=['POST'])
def validate_series_name():
    """API для валидации названия серии"""
    try:
        data = request.json
        base_name = data.get('base_name', '').strip() # type: ignore
        
        # Паттерн для проверки: T2-{число}C{число}
        import re
        pattern = r'^T2-(\d+)C(\d+)$'
        
        if not re.match(pattern, base_name):
            return jsonify({
                'valid': False,
                'error': 'Неверный формат. Используйте: T2-{номер_методики}C{номер_повторности}'
            })
        
        # Проверяем, нет ли уже проб с таким именем
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            db_data = json.load(f)
        
        existing_names = [p.get('name') for p in db_data.get('probes', [])]
        
        match = re.match(pattern, base_name)
        method_number = match.group(1) # type: ignore
        repeat_number = match.group(2) # type: ignore
        
        # Генерируем предварительные названия для проверки
        templates = [
            'T2-{m}A{r}', 'T2-{m}B{r}', 'T2-L{m}C{r}', 'T2-L{m}A{r}',
            'T2-L{m}B{r}', 'T2-L{m}P{m}C{r}', 'T2-L{m}P{m}A{r}',
            'T2-L{m}P{m}B{r}', 'T2-L{m}P{m}F{m}C{r}', 'T2-L{m}P{m}F{m}A{r}',
            'T2-L{m}P{m}F{m}B{r}', 'T2-L{m}P{m}F{m}D{r}', 'T2-L{m}P{m}F{m}N{m}C{r}',
            'T2-L{m}P{m}F{m}N{m}A{r}', 'T2-L{m}P{m}F{m}N{m}B{r}', 'T2-L{m}P{m}F{m}N{m}E{r}'
        ]
        
        generated_names = []
        for template in templates:
            name = template.replace('{m}', method_number).replace('{r}', repeat_number)
            generated_names.append(name)
        
        # Проверяем, какие имена уже существуют
        existing_in_series = [name for name in generated_names if name in existing_names]
        
        return jsonify({
            'valid': True,
            'method_number': method_number,
            'repeat_number': repeat_number,
            'generated_names': generated_names,
            'existing_in_series': existing_in_series,
            'warning': f'Найдено {len(existing_in_series)} существующих проб в этой серии' if existing_in_series else None
        })
        
    except Exception as e:
        return jsonify({
            'valid': False,
            'error': str(e)
        }), 500

def recalculate_dependent_fields(data_file: str = str(DATA_FILE)) -> Dict[str, Any]:
    """
    Пересчитывает зависимые поля для всех проб:
    - "Масса твердого (g)" = 1.5 * ("sample_mass" - "V (ml)")
    - "Плотность" = "sample_mass" / "V (ml)"
    
    Args:
        data_file: Путь к JSON файлу с данными
    
    Returns:
        Словарь со статистикой пересчета
    """
    try:
        # Загружаем данные
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        probes = data.get('probes', [])
        
        if not probes:
            return {
                'success': True,
                'message': 'Нет проб для пересчета',
                'updated_mass': 0,
                'updated_density': 0,
                'total_probes': 0
            }
        
        # Создаем версию перед пересчетом
        version_info = vcs.create_version(
            description="Автоматический пересчет зависимых полей",
            author="system",
            change_type="recalculation"
        )
        
        # Статистика
        stats = {
            'total_probes': len(probes),
            'updated_mass': 0,      # Обновлено поле "Масса твердого (g)"
            'updated_density': 0,    # Обновлено поле "Плотность"
            'skipped_mass': 0,       # Пропущено из-за ошибок
            'skipped_density': 0,    # Пропущено из-за ошибок
            'errors': []
        }
        
        # Пересчитываем для каждой пробы
        for index, probe in enumerate(probes):
            probe_id = probe.get('id', index + 1)
            
            try:
                # 1. Расчет "Масса твердого (g)"
                if 'sample_mass' in probe and 'V (ml)' in probe:
                    try:
                        sample_mass = float(probe['sample_mass'])
                        volume = float(probe['V (ml)'])
                        
                        # Проверяем корректность значений
                        if not (isinstance(sample_mass, (int, float)) and 
                                isinstance(volume, (int, float))):
                            raise ValueError("Значения не являются числами")
                        
                        # Рассчитываем массу твердого
                        solid_mass = 1.5 * (sample_mass - volume)
                        
                        # Сохраняем значение
                        probe['Масса твердого (g)'] = float(solid_mass)
                        stats['updated_mass'] += 1
                        
                    except (ValueError, TypeError, KeyError) as e:
                        stats['skipped_mass'] += 1
                        stats['errors'].append({
                            'probe_id': probe_id,
                            'field': 'Масса твердого (g)',
                            'error': str(e)
                        })
                
                # 2. Расчет "Плотность"
                if 'sample_mass' in probe and 'V (ml)' in probe:
                    try:
                        sample_mass = float(probe['sample_mass'])
                        volume = float(probe['V (ml)'])
                        
                        # Проверяем корректность значений
                        if not (isinstance(sample_mass, (int, float)) and 
                                isinstance(volume, (int, float))):
                            raise ValueError("Значения не являются числами")
                        
                        # Проверяем, что объем не ноль (деление на ноль)
                        if volume == 0:
                            raise ValueError("Объем равен нулю, деление невозможно")
                        
                        # Рассчитываем плотность
                        density = sample_mass / volume
                        
                        # Сохраняем значение
                        probe['Плотность'] = float(density)
                        stats['updated_density'] += 1
                        
                    except (ValueError, TypeError, ZeroDivisionError, KeyError) as e:
                        stats['skipped_density'] += 1
                        stats['errors'].append({
                            'probe_id': probe_id,
                            'field': 'Плотность',
                            'error': str(e)
                        })
                        
            except Exception as e:
                # Общая ошибка для пробы
                stats['errors'].append({
                    'probe_id': probe_id,
                    'field': 'общая',
                    'error': f'Необработанная ошибка: {str(e)}'
                })
        
        # Обновляем метаданные
        if 'metadata' not in data:
            data['metadata'] = {}
        
        data['metadata'].update({
            'last_recalculation': datetime.now().isoformat(),
            'recalculation_stats': stats
        })
        
        # Сохраняем обновленные данные
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Создаем финальную версию
        vcs.create_version(
            description=f"Пересчет зависимых полей завершен: масса - {stats['updated_mass']}, плотность - {stats['updated_density']}",
            author="system",
            change_type="recalculation_complete"
        )
        
        # Формируем сообщение о результатах
        message_parts = []
        if stats['updated_mass'] > 0:
            message_parts.append(f"обновлена 'Масса твердого (g)' у {stats['updated_mass']} проб")
        if stats['updated_density'] > 0:
            message_parts.append(f"обновлена 'Плотность' у {stats['updated_density']} проб")
        
        message = "Пересчет зависимых полей: " + ", ".join(message_parts) if message_parts else "Изменений не требуется"
        
        return {
            'success': True,
            'message': message,
            **stats,
            'version_created': version_info is not None,
            'version_id': version_info['id'] if version_info else None
        }
        
    except Exception as e:
        return {
            'success': False,
            'message': f"Ошибка пересчета: {str(e)}",
            'total_probes': 0,
            'updated_mass': 0,
            'updated_density': 0,
            'errors': [{'field': 'система', 'error': str(e)}]
        }


def check_and_recalculate_dependent_fields(data_file: str = str(DATA_FILE)) -> Dict[str, Any]:
    """
    Проверяет необходимость пересчета и выполняет его.
    Можно добавить логику проверки временных меток.
    """
    try:
        # Проверяем, когда был последний пересчет
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        metadata = data.get('metadata', {})
        last_recalc = metadata.get('last_recalculation')
        
        # Здесь можно добавить логику для определения необходимости пересчета
        # Например, если прошло больше суток с последнего пересчета
        # или если были изменения в данных
        
        # Пока всегда выполняем пересчет
        return recalculate_dependent_fields(data_file)
        
    except Exception as e:
        return {
            'success': False,
            'message': f"Ошибка проверки необходимости пересчета: {str(e)}"
        }
from probe_manager import ProbeManager


probe_manager = ProbeManager(str(DATA_FILE)) # type: ignore

@app.route('/probe_manager')
def render_manager():
    """Главная страница"""
    return render_template('probe_manager.html')

# API endpoints

@app.route('/api/probes', methods=['GET'])
def get_probes():
    """Получить все пробы"""
    try:
        probes = probe_manager.probes # type: ignore
        return jsonify({
            'success': True,
            'data': [probe.to_dict() for probe in probes],
            'count': len(probes)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/probes/search', methods=['POST'])
def search_probes():
    """Поиск проб по различным критериям"""
    try:
        data = request.json
        result_ids = []
        
        if data.get('name_substring'): # pyright: ignore[reportOptionalMemberAccess]
            result_ids = probe_manager.find_probes_by_name_substring( # type: ignore
                data['name_substring'], # type: ignore
                data.get('case_sensitive', False) # type: ignore
            )
        
        elif data.get('concentration_range'): # type: ignore
            range_data = data['concentration_range'] # type: ignore
            result_ids = probe_manager.find_probes_by_concentration_range( # type: ignore
                range_data['element'],
                range_data.get('min'),
                range_data.get('max')
            )
        
        # Фильтруем пробы по ID
        filtered_probes = [p for p in probe_manager.probes if p.id in result_ids] # type: ignore
        
        return jsonify({
            'success': True,
            'data': [probe.to_dict() for probe in filtered_probes],
            'count': len(filtered_probes)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/probes/tags', methods=['POST'])
def manage_tags():
    """Управление тегами проб"""
    try:
        data = request.json
        action = data.get('action')  # pyright: ignore[reportOptionalMemberAccess] # 'add' или 'remove'
        tag = data.get('tag') # type: ignore
        probe_ids = data.get('probe_ids', []) # type: ignore
        
        if action == 'add':
            probe_manager.add_tag_to_probes(tag, probe_ids) # type: ignore
        elif action == 'remove':
            probe_manager.remove_tag_from_probes(tag, probe_ids) # type: ignore
        else:
            return jsonify({'success': False, 'error': 'Неизвестное действие'}), 400
        
        probe_manager.save_probes() # type: ignore
        
        return jsonify({
            'success': True,
            'message': f'Тег "{tag}" успешно {action}'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/probes/state-tags', methods=['POST'])
def add_state_tags():
    """Добавить теги состояний (твердая/жидкая)"""
    try:
        probe_manager.add_state_tags() # type: ignore
        probe_manager.save_probes() # type: ignore
        
        return jsonify({
            'success': True,
            'message': 'Теги состояний добавлены'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/probes/group', methods=['POST'])
def create_group():
    """Создать группу проб"""
    try:
        data = request.json
        group_name = data.get('name') # pyright: ignore[reportOptionalMemberAccess]
        probe_ids = data.get('probe_ids', []) # pyright: ignore[reportOptionalMemberAccess]
        
        group_id = probe_manager.group_probes(group_name, probe_ids) # pyright: ignore[reportFunctionMemberAccess]
        probe_manager.save_probes() # pyright: ignore[reportFunctionMemberAccess]
        
        return jsonify({
            'success': True,
            'message': f'Группа "{group_name}" создана',
            'group_id': group_id
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/probes/add-field', methods=['POST'])
def add_field():
    """Добавить поле на основе имени пробы"""
    try:
        data = request.json
        field_name = data.get('field_name') # pyright: ignore[reportOptionalMemberAccess]
        pattern = data.get('pattern') # pyright: ignore[reportOptionalMemberAccess]
        
        probe_manager.add_field_based_on_name_pattern(field_name, pattern) # pyright: ignore[reportFunctionMemberAccess]
        probe_manager.save_probes() # pyright: ignore[reportFunctionMemberAccess]
        
        return jsonify({
            'success': True,
            'message': f'Поле "{field_name}" добавлено'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/probes/batch-tags', methods=['POST'])
def batch_tags():
    """Пакетное добавление тегов по правилам"""
    try:
        data = request.json
        rules = data.get('rules', []) # pyright: ignore[reportOptionalMemberAccess]
        
        probe_manager.batch_add_tags_by_rules(rules) # pyright: ignore[reportFunctionMemberAccess]
        probe_manager.save_probes() # pyright: ignore[reportFunctionMemberAccess]
        
        return jsonify({
            'success': True,
            'message': f'Применено {len(rules)} правил'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/probes/filter', methods=['POST'])
def filter_probes():
    """Фильтрация проб по тегам"""
    try:
        data = request.json
        tags = data.get('tags', []) # pyright: ignore[reportOptionalMemberAccess]
        match_all = data.get('match_all', True) # pyright: ignore[reportOptionalMemberAccess]
        
        filtered_probes = probe_manager.get_probes_by_tags(tags, match_all) # pyright: ignore[reportFunctionMemberAccess]
        
        return jsonify({
            'success': True,
            'data': [probe.to_dict() for probe in filtered_probes],
            'count': len(filtered_probes)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/statistics', methods=['GET'])
def get_statistics():
    """Получить статистику по пробам"""
    try:
        stats = probe_manager.get_statistics() # pyright: ignore[reportFunctionMemberAccess]
        return jsonify({
            'success': True,
            'data': stats
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/probes/update_probes_manager', methods=['POST'])
def update_probe_by_manager():
    """Обновить данные пробы"""
    try:
        data = request.json
        probe_id = data.get('id') # type: ignore
        
        # Находим пробу
        probe = next((p for p in probe_manager.probes if p.id == probe_id), None) # type: ignore
        if not probe:
            return jsonify({'success': False, 'error': 'Проба не найдена'}), 404
        
        # Обновляем поля (кроме id)
        for key, value in data.items(): # type: ignore
            if key != 'id' and hasattr(probe, key):
                setattr(probe, key, value)
        
        probe_manager.save_probes() # type: ignore
        
        return jsonify({
            'success': True,
            'message': 'Проба обновлена',
            'data': probe.to_dict()
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/probes/parse-name', methods=['POST'])
def parse_probe_name():
    """Парсинг имени пробы"""
    try:
        data = request.json
        probe_name = data.get('name', '') # type: ignore
        
        parsed = probe_manager.parse_probe_name(probe_name) # type: ignore
        
        return jsonify({
            'success': True,
            'data': parsed
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/mass')
def render_mass():
    return render_template('mass.html')


@app.route('/api/calculate_balance')
def calculate_balance():
    probes = load_data().get("probes", [])
    # Создаем словарь для быстрого поиска: "NAME": {probe_data}
    probe_map = {p['name']: p for p in probes}
    
    series_list = []
    
    # Регулярное выражение для поиска "нулевых" образцов вида T2-{m}C{n}
    # Пример: T2-4C1 -> m=4, n=1
    root_pattern = re.compile(r"^T2-(\d+)C(\d+)$")

    for probe in probes:
        match = root_pattern.match(probe['name'])
        if match:
            m = match.group(1)  # Номер методики
            n = match.group(2)  # Номер повторности
            
            # Определяем имена проб для данной серии
            names = {
                # Исходные (Input)
                "start_A": f"T2-{m}A{n}",
                "start_B": f"T2-{m}B{n}",
                
                # Стадия 2
                "st2_A": f"T2-L{m}A{n}",
                "st2_B": f"T2-L{m}B{n}",
                
                # Стадия 3
                "st3_A": f"T2-L{m}P{m}A{n}",
                "st3_B": f"T2-L{m}P{m}B{n}",
                
                # Стадия 4
                "st4_A": f"T2-L{m}P{m}F{m}A{n}",
                "st4_B": f"T2-L{m}P{m}F{m}B{n}",
                "st4_D": f"T2-L{m}P{m}F{m}D{n}",  # Камерный продукт
                
                # Стадия 5
                "st5_A": f"T2-L{m}P{m}F{m}N{m}A{n}",  # Оборотная жидкость
                "st5_B": f"T2-L{m}P{m}F{m}N{m}B{n}",
                
                # Стадия 6 (ЖКК)
                "st6_E": f"T2-L{m}P{m}F{m}N{m}E{n}",
                "st6_G": f"T2-L{m}P{m}F{m}N{m}G{n}"  # Новая оборотная жидкость
            }

            # Вспомогательная функция для получения значения пробы с учетом правил связи стадий
            def get_probe_value_with_fallback(probe_name, element, fallback_rules=None):
                """
                Получает значение элемента из пробы.
                Если проба не найдена и заданы правила fallback, ищет альтернативную пробу.
                """
                # Пытаемся получить значение из целевой пробы
                value = get_probe_value(probe_map, probe_name, element)
                
                # Если значение найдено (не ноль) или правила fallback не заданы - возвращаем
                if value != 0 or not fallback_rules:
                    return value
                
                # Применяем правила fallback
                for fallback_name in fallback_rules:
                    fallback_value = get_probe_value(probe_map, fallback_name, element)
                    if fallback_value != 0:
                        # Записываем информацию о том, откуда взято значение
                        if probe_name in probe_map:
                            probe_map[probe_name].setdefault('fallback_info', {})[element] = {
                                'source': fallback_name,
                                'value': fallback_value
                            }
                        return fallback_value
                
                return 0

            # Правила связи между стадиями 5 и 6:
            # Если пробы из 6 стадии нет, берем из 5 стадии:
            # - Продукт B 5 стадии → предпродукт E 6 стадии
            # - Продукт A 5 стадии → продукт G 6 стадии
            fallback_rules_st6 = {
                "st6_E": [names["st5_B"]],  # E из 6 стадии можно взять из B 5 стадии
                "st6_G": [names["st5_A"]]   # G из 6 стадии можно взять из A 5 стадии
            }

            elements = ['mFe', 'mCu', 'mNi', 'mPd', 'mPt', 'mRh', 'mAu', 'mAg', 'mOs', 'mRu', 'mIr']
            series_data = {
                "id": f"Series-{m}-{n}",
                "method": m,
                "repeat": n,
                "elements": {},
                "probe_availability": {  # Добавляем информацию о доступности проб
                    "st5_A": names["st5_A"] in probe_map,
                    "st5_B": names["st5_B"] in probe_map,
                    "st6_E": names["st6_E"] in probe_map,
                    "st6_G": names["st6_G"] in probe_map,
                    "used_fallback": False
                }
            }

            for el in elements:
                # 1. Расчет входной массы (Input)
                input_val = get_probe_value(probe_map, names["start_A"], el) + \
                            get_probe_value(probe_map, names["start_B"], el)
                
                # 2. Расчет выходных продуктов для баланса с учетом fallback правил
                out_D = get_probe_value(probe_map, names["st4_D"], el)  # Камерный
                
                # Используем значения с fallback для стадии 6
                out_E = get_probe_value_with_fallback(
                    names["st6_E"], 
                    el, 
                    fallback_rules_st6["st6_E"]
                )
                
                out_G = get_probe_value_with_fallback(
                    names["st6_G"], 
                    el, 
                    fallback_rules_st6["st6_G"]
                )
                
                # Оборотная жидкость - теперь это st6_G или st5_A если st6_G нет
                out_Recycle = out_G  # st6_G - новая оборотная жидкость
                
                total_out = out_D + out_E + out_Recycle
                loss = input_val - total_out
                
                # Предотвращение деления на ноль
                calc_base = input_val if input_val != 0 else 1

                # 3. Расчет стадий (Bar Plot Data) с учетом новых правил
                stages = [
                    # Стадия 1 (Input)
                    get_probe_value(probe_map, names["start_A"], el) + 
                    get_probe_value(probe_map, names["start_B"], el),
                    
                    # Стадия 2
                    get_probe_value(probe_map, names["st2_A"], el) + 
                    get_probe_value(probe_map, names["st2_B"], el),
                    
                    # Стадия 3
                    get_probe_value(probe_map, names["st3_A"], el) + 
                    get_probe_value(probe_map, names["st3_B"], el),
                    
                    # Стадия 4
                    get_probe_value(probe_map, names["st4_A"], el) + 
                    get_probe_value(probe_map, names["st4_B"], el) + 
                    get_probe_value(probe_map, names["st4_D"], el),
                    
                    # Стадия 5 (теперь это промежуточная стадия)
                    get_probe_value(probe_map, names["st5_A"], el) + 
                    get_probe_value(probe_map, names["st5_B"], el),
                    
                    # Стадия 6 (финальная стадия с учетом fallback)
                    out_E + out_G  # E и G из 6 стадии
                ]

                # Определяем, использовались ли fallback значения
                used_fallback_E = (out_E != 0 and get_probe_value(probe_map, names["st6_E"], el) == 0 
                                  and out_E == get_probe_value(probe_map, names["st5_B"], el))
                used_fallback_G = (out_G != 0 and get_probe_value(probe_map, names["st6_G"], el) == 0 
                                  and out_G == get_probe_value(probe_map, names["st5_A"], el))
                
                if used_fallback_E or used_fallback_G:
                    series_data["probe_availability"]["used_fallback"] = True

                series_data["elements"][el] = {
                    "balance": {
                        "input": round(input_val, 9),
                        "D": round(out_D, 9),         # Камерный продукт
                        "E": round(out_E, 9),         # Продукт E (из st6_E или st5_B)
                        "G": round(out_G, 9),         # Оборотная жидкость G (из st6_G или st5_A)
                        "Recycle": round(out_Recycle, 9),  # Алиас для G для обратной совместимости
                        "Loss": round(loss, 9),
                        "D_pct": round((out_D / calc_base) * 100, 9),
                        "E_pct": round((out_E / calc_base) * 100, 9),
                        "G_pct": round((out_G / calc_base) * 100, 9),
                        "Recycle_pct": round((out_Recycle / calc_base) * 100, 9),  # Для обратной совместимости
                        "Loss_pct": round((loss / calc_base) * 100, 9),
                        "fallback_used": {
                            "E": used_fallback_E,
                            "G": used_fallback_G
                        }
                    },
                    "stages": [round(x, 9) for x in stages],
                    "probe_details": {
                        "st5_A": get_probe_value(probe_map, names["st5_A"], el),
                        "st5_B": get_probe_value(probe_map, names["st5_B"], el),
                        "st6_E": get_probe_value(probe_map, names["st6_E"], el),
                        "st6_G": get_probe_value(probe_map, names["st6_G"], el),
                        "st6_E_actual": get_probe_value(probe_map, names["st6_E"], el),
                        "st6_G_actual": get_probe_value(probe_map, names["st6_G"], el),
                        "st6_E_used": out_E,
                        "st6_G_used": out_G
                    }
                }
            
            series_list.append(series_data)

    # Добавляем общую статистику по использованию fallback
    if series_list:
        total_series = len(series_list)
        series_with_fallback = sum(1 for s in series_list if s["probe_availability"]["used_fallback"])
        
        for series in series_list:
            series["stats"] = {
                "total_series": total_series,
                "series_with_fallback": series_with_fallback,
                "fallback_percentage": round((series_with_fallback / total_series * 100), 2) if total_series > 0 else 0
            }

    return jsonify(series_list)


# Вспомогательная функция для получения значений из проб
def get_probe_value(probe_map, probe_name, element_key):
    """
    Получает значение элемента из пробы по имени.
    Возвращает 0, если проба или элемент не найдены.
    """
    if probe_name not in probe_map:
        return 0
    
    probe = probe_map[probe_name]
    
    # Пробуем найти элемент в различных возможных форматах
    if element_key in probe:
        value = probe[element_key]
    elif element_key.lower() in probe:
        value = probe[element_key.lower()]
    elif element_key.upper() in probe:
        value = probe[element_key.upper()]
    else:
        return 0
    
    # Преобразуем в число, если это возможно
    try:
        return float(value) if value not in [None, ""] else 0
    except (ValueError, TypeError):
        return 0
    
def format_numeric_values(data_file: str = str(DATA_FILE), decimal_places: int = 4) -> Dict[str, Any]:
    """
    Форматирует все числовые значения в базе данных до указанного количества знаков после запятой.
    
    Args:
        data_file: Путь к JSON файлу с данными
        decimal_places: Количество знаков после запятой (по умолчанию 4)
    
    Returns:
        Словарь со статистикой форматирования
    """
    try:
        # Загружаем данные
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        probes = data.get('probes', [])
        
        if not probes:
            return {
                'success': True,
                'message': 'Нет проб для форматирования',
                'formatted_count': 0,
                'total_probes': 0
            }
        
        # Создаем версию перед форматированием
        version_info = vcs.create_version(
            description=f"Автоматическое форматирование числовых значений до {decimal_places} знаков",
            author="system",
            change_type="formatting"
        )
        
        # Статистика
        stats = {
            'total_probes': len(probes),
            'formatted_values': 0,
            'formatted_probes': 0,
            'errors': []
        }
        
        # Поля, которые не нужно форматировать (строковые, булевы и т.д.)
        non_numeric_fields = {
            'id', 'name', 'tags', 'created_at', 'last_normalized',
            'Описание', 'Кто готовил', 'Среда', 'Аналиты',
            'merged_from', 'merge_date', 'recalculation_history'
        }
        
        # Функция для определения, является ли поле числовым
        def is_numeric_field(field_name, value):
            # Пропускаем специальные поля
            if field_name in non_numeric_fields:
                return False
            
            # Пропускаем поля, которые заведомо не числовые
            if field_name.startswith('d') and len(field_name) > 1:
                # Это поле погрешности - проверяем значение
                return isinstance(value, (int, float))
            
            # Проверяем тип значения
            return isinstance(value, (int, float))
        
        # Форматируем значения для каждой пробы
        for index, probe in enumerate(probes):
            probe_id = probe.get('id', index + 1)
            probe_formatted = False
            
            try:
                for field_name, value in list(probe.items()):
                    if is_numeric_field(field_name, value):
                        try:
                            # Форматируем значение
                            formatted_value = round(float(value), decimal_places)
                            probe[field_name] = formatted_value
                            stats['formatted_values'] += 1
                            probe_formatted = True
                        except (ValueError, TypeError) as e:
                            # Оставляем оригинальное значение при ошибке
                            stats['errors'].append({
                                'probe_id': probe_id,
                                'field': field_name,
                                'value': value,
                                'error': str(e)
                            })
                
                if probe_formatted:
                    stats['formatted_probes'] += 1
                    
            except Exception as e:
                stats['errors'].append({
                    'probe_id': probe_id,
                    'field': 'общая',
                    'error': f'Необработанная ошибка: {str(e)}'
                })
        
        # Обновляем метаданные
        if 'metadata' not in data:
            data['metadata'] = {}
        
        data['metadata'].update({
            'last_formatting': datetime.now().isoformat(),
            'formatting_decimal_places': decimal_places,
            'formatting_stats': stats
        })
        
        # Сохраняем обновленные данные
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Создаем финальную версию
        vcs.create_version(
            description=f"Форматирование завершено: {stats['formatted_values']} значений в {stats['formatted_probes']} пробах",
            author="system",
            change_type="formatting_complete"
        )
        
        # Формируем сообщение о результатах
        message = f"Отформатировано {stats['formatted_values']} числовых значений в {stats['formatted_probes']} пробах до {decimal_places} знаков"
        
        return {
            'success': True,
            'message': message,
            **stats,
            'version_created': version_info is not None,
            'version_id': version_info['id'] if version_info else None
        }
        
    except Exception as e:
        return {
            'success': False,
            'message': f"Ошибка форматирования: {str(e)}",
            'formatted_values': 0,
            'formatted_probes': 0,
            'errors': [{'field': 'система', 'error': str(e)}]
        }


def check_and_format_numeric_values(data_file: str = str(DATA_FILE), force: bool = False) -> Dict[str, Any]:
    """
    Проверяет необходимость форматирования и выполняет его.
    
    Args:
        data_file: Путь к JSON файлу
        force: Принудительное форматирование, даже если уже было выполнено
    
    Returns:
        Результат форматирования
    """
    try:
        # Проверяем, когда было последнее форматирование
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        metadata = data.get('metadata', {})
        last_formatting = metadata.get('last_formatting')
        
        # Если force=False, проверяем необходимость
        if not force and last_formatting:
            # Можно добавить логику, например, форматировать раз в день
            from datetime import datetime, timedelta
            last_date = datetime.fromisoformat(last_formatting.replace('Z', '+00:00'))
            if datetime.now() - last_date < timedelta(days=1):
                return {
                    'success': True,
                    'message': 'Форматирование не требуется (выполнялось менее суток назад)',
                    'skipped': True,
                    'last_formatting': last_formatting
                }
        
        # Выполняем форматирование
        return format_numeric_values(data_file, decimal_places=4)
        
    except Exception as e:
        return {
            'success': False,
            'message': f"Ошибка проверки необходимости форматирования: {str(e)}"
        }

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True, port=5000)