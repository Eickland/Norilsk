from flask import Flask, render_template, request, jsonify, send_file
from datetime import datetime
import json
import os
import logging
from werkzeug.utils import secure_filename
from handlers.ISP_MS import process_metal_samples_csv, expand_sample_code
from handlers.ISP_AES import process_icp_aes_data
from mass_balance.phase_calculate import calculate_fields_for_series
from mass_balance.mass_calculate import recalculate_metal_mass
from middleware.series_worker import get_series_dicts, get_source_class_from_probe, get_probe_type
import pandas as pd
from version_control.version_control import VersionControlSystem
from io import BytesIO
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, Any, Set
import plotly
import plotly.graph_objs as go
from flask_cors import CORS
import re
import numpy as np
from datetime import datetime, timedelta
import traceback
from app.params import settings
from database_processing.func_db import ProbeDatabase # Импорт вашего класса


load_dotenv()

BASE_DIR = Path(__file__).parent.parent

app = Flask(__name__, template_folder=str(BASE_DIR /'src'/ 'templates'), static_folder=str(BASE_DIR /'src'/ 'static'))
CORS(app)
# Черный список полей, которые не должны отображаться как оси
BLACKLIST_FIELDS = {
    "Описание", "is_solid", "id", "last_normalized", 
    "status_id", "is_solution", "name", "tags"
} 

app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY')
app.config['UPLOAD_FOLDER'] = BASE_DIR / 'uploads'
app.config['RESULTS_FOLDER'] = BASE_DIR / 'results'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB максимум
app.config['ALLOWED_EXTENSIONS'] = {'csv', 'xlsx', 'xls', 'json'}
app.config['VERSIONS_DIR'] = BASE_DIR / 'versions'

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULTS_FOLDER'], exist_ok=True)

DATA_FILE = BASE_DIR / 'data' / 'data.json'
app.config['DATA_FILE'] = DATA_FILE
# Инициализация системы управления версиями
vcs = VersionControlSystem(app.config['DATA_FILE'], app.config['VERSIONS_DIR'])
db_manager = ProbeDatabase(app.config['DATA_FILE'])       
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

def convert_df_to_dict(df:pd.DataFrame):
    
    df['id'] = df.index + 1
    df['tags'] = [[] for _ in range(len(df))]
    if 'status_id' not in df.columns:
        df['status_id'] = 3
    
    df.rename(columns={df.columns[0]: 'name'}, inplace=True)
        
    df['name'].dropna(inplace=True)
    df.dropna(axis=1,how='all', inplace=True)
    
    df = df.fillna(0) 

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
            probe["V_aliq (l)"] = 0.05
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
    
# API для копирования массы и объема из проб C в A/B
@app.route('/api/copy_mass_volume', methods=['POST'])
def api_copy_mass_volume():
    """API endpoint для копирования массы и объема из проб C в пробы A/B"""
    result = calculate_fields_for_series()
    return jsonify(result)

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
        mass_volume_result = calculate_fields_for_series(str(DATA_FILE))
        
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
        
        json_data = convert_df_to_dict(result_data) # type: ignore
        
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
        
        json_data = convert_df_to_dict(result_data) # type: ignore
        
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

@app.route('/api/preview_ISPMS', methods=['POST'])
def preview_ISPMS():
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file'}), 400
    
    file = request.files['file']
    # Сохраняем временно для анализа
    temp_path = os.path.join(app.config['UPLOAD_FOLDER'], 'temp_' + secure_filename(file.filename)) # type: ignore
    file.save(temp_path)
    
    try:
        # Обрабатываем данные (используем вашу существующую функцию)
        result_data = process_metal_samples_csv(file_path=temp_path)
        new_probes = convert_df_to_dict(result_data) # type: ignore
        
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            db_data = json.load(f)
        
        # Проверка на новые поля с помощью логики из func_db
        existing_fields = set()
        for p in db_data['probes']:
            existing_fields.update(p.keys())
        
        new_fields = set()
        for p in new_probes:
            for key in p.keys():
                if key not in existing_fields:
                    new_fields.add(key)
        
        if new_fields:
            os.remove(temp_path)
            return jsonify({
                'success': False,
                'error': 'NEW_FIELDS_DETECTED',
                'details': list(new_fields),
                'message': f'Обнаружены новые поля: {", ".join(new_fields)}. Загрузка запрещена.'
            }), 200 # Возвращаем 200, чтобы JS обработал это как бизнес-логику

        # Анализ серий
        existing_series = set()
        for p in db_data['probes']:
            info = get_probe_type(p)
            source = get_source_class_from_probe(p)
            if info and source:
                existing_series.add((source, info[1], info[2])) # source, method, exp

        changed_series = set()
        new_series = set()
        
        for p in new_probes:
            info = get_probe_type(p)
            source = get_source_class_from_probe(p)
            if info and source:
                series_key = (source, info[1], info[2])
                if series_key in existing_series:
                    changed_series.add(series_key)
                else:
                    new_series.add(series_key)

        return jsonify({
            'success': True,
            'stats': {
                'changed_series_count': len(changed_series),
                'new_series_count': len(new_series),
                'total_probes': len(new_probes)
            },
            'temp_file': temp_path # Передаем путь для последующего подтверждения
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

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
        
        result_data['name'] = result_data['name'].apply(expand_sample_code)
        
        json_data = convert_df_to_dict(result_data)
        
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

@app.route('/api/probes/batch_delete', methods=['POST'])
def batch_delete_probes():
    try:
        data_request = request.get_json()
        probe_ids = data_request.get('ids', [])
        
        if not probe_ids:
            return jsonify({'success': False, 'error': 'Список ID пуст'}), 400

        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        probes = data.get('probes', [])
        initial_count = len(probes)
        
        # Создаем версию VCS для группового действия
        vcs.create_version(
            description=f"Массовое удаление проб: {len(probe_ids)} шт.",
            author=request.headers.get('X-User-Email', 'anonymous'),
            change_type='batch_delete'
        )
        
        # Фильтруем список, оставляя только те, чьих ID нет в списке на удаление
        data['probes'] = [p for p in probes if p.get('id') not in probe_ids]
        
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            
        return jsonify({
            'success': True,
            'deleted_count': initial_count - len(data['probes']),
            'message': f'Удалено {initial_count - len(data["probes"])} проб'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
        
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
    """Экспорт всей базы данных в CSV"""
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        df = pd.json_normalize(data=data['probes'])
        filename = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        output = BytesIO()
        df.to_csv(output, index=False, encoding='utf-8-sig',sep=";")
        output.seek(0)
        
        return send_file(
                output,
                mimetype='text/csv',
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

def extract_series_info() -> Dict:
    """Извлечение информации о сериях из данных"""
    probes = load_data().get("probes", [])
    probe_map = {p['name']: p for p in probes}
    
    series_dict = {}
    root_pattern = re.compile(r"^T2-(\d+)C(\d+)$")
    
    patterns = {
        'start_A': re.compile(r"^T2-(\d+)A(\d+)$"),
        'start_B': re.compile(r"^T2-(\d+)B(\d+)$"),
        'start_C': re.compile(r"^T2-(\d+)C(\d+)$"),
        'st2_A': re.compile(r"^T2-L(\d+)A(\d+)$"),
        'st2_B': re.compile(r"^T2-L(\d+)B(\d+)$"),
        'st3_A': re.compile(r"^T2-L(\d+)P\1A(\d+)$"),  # \1 проверяет что номер методики одинаков
        'st3_B': re.compile(r"^T2-L(\d+)P\1B(\d+)$"),
        'st3_C': re.compile(r"^T2-L(\d+)P\1C(\d+)$"),
        'st4_A': re.compile(r"^T2-L(\d+)P\1F\1A(\d+)$"),
        'st4_B': re.compile(r"^T2-L(\d+)P\1F\1B(\d+)$"),
        'st4_D': re.compile(r"^T2-L(\d+)P\1F\1D(\d+)$")
    }    

    # Проходим по всем пробам и определяем их тип
    for probe in probes:
        probe_name = probe.get('name', '')
        if not probe_name:
            continue
        
        m = None
        n = None
        probe_type = None
        
        # Определяем тип пробы
        for pattern_name, pattern in patterns.items():
            match = pattern.match(probe_name)
            if match:
                probe_type = pattern_name
                m = match.group(1)  # Номер методики
                n = match.group(2)  # Номер повторности
                break
        
        if not probe_type or not m or not n:
            continue
            
        series_key = f"T2-{m}C{n}"
        
        if series_key not in series_dict:
            series_dict[series_key] = {
                'method': m,
                'replicate': n,
                'probes': {},
                'stages': {}
            }
        
        # Определяем стадию и тип пробы
        name = probe['name']
        print(name)
        if f"T2-{m}A{n}" in name or f"T2-{m}B{n}" in name:
            stage = 'start'
            sample_type = 'A' if 'A' in name else 'B'
        elif f"T2-L{m}A{n}" in name or f"T2-L{m}B{n}" in name:
            stage = 'st2'
            sample_type = 'A' if 'A' in name else 'B'
        elif f"T2-L{m}P{m}A{n}" in name or f"T2-L{m}P{m}B{n}" in name:
            stage = 'st3'
            sample_type = 'A' if 'A' in name else 'B'
        elif f"T2-L{m}P{m}F{m}A{n}" in name or f"T2-L{m}P{m}F{m}B{n}" in name or f"T2-L{m}P{m}F{m}D{n}" in name:
            stage = 'st4'
            if 'A' in name:
                sample_type = 'A'
            elif 'B' in name:
                sample_type = 'B'
            else:
                sample_type = 'D'
        elif f"T2-L{m}P{m}F{m}N{m}A{n}" in name or f"T2-L{m}P{m}F{m}N{m}B{n}" in name:
            stage = 'st5'
            sample_type = 'A' if 'A' in name else 'B'
        elif f"T2-L{m}P{m}F{m}N{m}E{n}" in name or f"T2-L{m}P{m}F{m}N{m}G{n}" in name:
            stage = 'st6'
            sample_type = 'E' if 'E' in name else 'G'
        else:
            continue
        
        series_dict[series_key]['probes'][name] = probe
        if stage not in series_dict[series_key]['stages']:
            series_dict[series_key]['stages'][stage] = {}
        series_dict[series_key]['stages'][stage][sample_type] = probe
    
    return series_dict

@app.route('/api/columns')
def get_columns():
    """Получение списка числовых колонок"""
    try:
        db_data = load_data()
        if not db_data["probes"]:
            return jsonify({"columns": []}), 404
        
        df = pd.DataFrame(db_data["probes"])
        
        # Исключаем нечисловые колонки и черный список
        blacklist = [
            'Описание', 'is_solid', 'id', 'last_normalized', 
            'status_id', 'is_solution', 'name', 'tags', 'method', 'replicate','is_series','series_base'
            ,'priority','repeat_number','method_number','created_at','Масса навески (g)','merge_date','merged_from','mass_volume_source',
            'last_mass_volume_update','масса','общая','V_aliq (l)','Масса навески (mg)','Разбавление','volume_calculation_note','Масса','mass_source_for_solid',
            'mass_solid_copied_from_C','last_copy_check','skip_reason','mass_volume_copy_skipped','mass_source_for_solid','mass_calculation_note'
        ]
        
        numeric_columns = []
        for col in df.columns:
            if col in blacklist:
                continue
            if pd.api.types.is_numeric_dtype(df[col]):
                numeric_columns.append(col)
            else:
                try:
                    # Пробуем преобразовать в числовой тип
                    pd.to_numeric(df[col], errors='raise')
                    numeric_columns.append(col)
                except:
                    continue
        
        return jsonify({"columns": sorted(numeric_columns)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/series')
def get_series():
    """Получение списка доступных серий"""
    try:
        series_dict = extract_series_info()
        series_list = sorted(list(series_dict.keys()))
        return jsonify({"series": series_list})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/plot', methods=['POST'])
def create_plot():
    """API для создания графика с поддержкой серий"""
    try:
        data = request.json
        analysis_mode = data.get('analysis_mode', 'single') # type: ignore
        x_axis = data.get('x_axis') # type: ignore
        y_axis = data.get('y_axis') # type: ignore
        selected_series = data.get('series', []) # type: ignore
        filters = data.get('filters', {}) # type: ignore
        
        if not x_axis or not y_axis:
            return jsonify({"error": "Не указаны оси X и Y"}), 400
        
        series_dict = extract_series_info()
        
        if analysis_mode == 'average':
            # Используем все серии для усреднения
            selected_series = list(series_dict.keys())
        
        # Фильтрация данных в зависимости от режима
        plot_data = []
        colors = [
            '#2b6cb0', '#3182ce', '#4299e1', '#63b3ed', '#90cdf4',  # Blue scale
            '#38a169', '#48bb78', '#68d391', '#9ae6b4',  # Green scale
            '#d69e2e', '#ed8936', '#f6ad55', '#fbd38d',  # Orange scale
            '#9f7aea', '#b794f4', '#d6bcfa',  # Purple scale
        ]
        
        for i, series_name in enumerate(selected_series):
            if series_name not in series_dict:
                continue
            
            series_info = series_dict[series_name]
            print(series_info)
            series_data = []
            
            # Собираем данные для серии
            for probe_name, probe_data in series_info['probes'].items():
                # Применяем фильтры
                if filters.get('hide_zero', True):
                    if (x_axis in probe_data and (probe_data[x_axis] == 0 or pd.isna(probe_data[x_axis]))):
                        continue
                    if (y_axis in probe_data and (probe_data[y_axis] == 0 or pd.isna(probe_data[y_axis]))):
                        continue
                
                # Фильтр по типу пробы (A/B)
                sample_type = probe_name[-1]  # Последний символ - тип пробы
                if sample_type == 'A' and not filters.get('show_liquid', True):
                    continue
                if sample_type == 'B' and not filters.get('show_solid', True):
                    continue
                
                if x_axis in probe_data and y_axis in probe_data:
                    try:
                        x_val = float(probe_data[x_axis])
                        y_val = float(probe_data[y_axis])
                        
                        # Пропускаем NaN значения
                        if pd.isna(x_val) or pd.isna(y_val):
                            continue
                        
                        series_data.append({
                            'x': x_val,
                            'y': y_val,
                            'name': probe_name,
                            'sample_type': sample_type,
                            'series': series_name
                        })
                    except (ValueError, TypeError):
                        continue
            
            if series_data:
                plot_data.append({
                    'series_name': series_name,
                    'data': series_data,
                    'color': colors[i % len(colors)]
                })
        
        # Создаем график в зависимости от режима
        fig = go.Figure()
        
        if analysis_mode == 'single':
            # Одна серия - один график
            if plot_data:
                series_data = plot_data[0]['data']
                fig.add_trace(go.Scatter(
                    x=[d['x'] for d in series_data],
                    y=[d['y'] for d in series_data],
                    mode='markers+lines',
                    name=plot_data[0]['series_name'],
                    marker=dict(
                        size=10,
                        color=plot_data[0]['color'],
                        line=dict(width=2, color='white')
                    ),
                    line=dict(width=2, color=plot_data[0]['color']),
                    hovertext=[d['name'] for d in series_data],
                    hoverinfo='text+x+y',
                    customdata=[[d['series'], d['sample_type']] for d in series_data]
                ))
        
        elif analysis_mode == 'multiple':
            # Несколько серий - несколько линий
            for series_info in plot_data:
                series_data = series_info['data']
                fig.add_trace(go.Scatter(
                    x=[d['x'] for d in series_data],
                    y=[d['y'] for d in series_data],
                    mode='markers+lines',
                    name=series_info['series_name'],
                    marker=dict(
                        size=8,
                        color=series_info['color'],
                        line=dict(width=1, color='white')
                    ),
                    line=dict(width=2, color=series_info['color'], dash='dash'),
                    hovertext=[d['name'] for d in series_data],
                    hoverinfo='text+x+y',
                    customdata=[[d['series'], d['sample_type']] for d in series_data]
                ))
        
        elif analysis_mode == 'average':
            # Среднее по сериям
            # Группируем по типам проб
            sample_types = {}
            for series_info in plot_data:
                for point in series_info['data']:
                    sample_type = point['sample_type']
                    if sample_type not in sample_types:
                        sample_types[sample_type] = []
                    sample_types[sample_type].append(point)
            
            for sample_type, points in sample_types.items():
                if len(points) < 2:
                    continue
                
                # Сортируем по X для правильного построения линии
                points.sort(key=lambda p: p['x'])
                
                fig.add_trace(go.Scatter(
                    x=[p['x'] for p in points],
                    y=[p['y'] for p in points],
                    mode='lines+markers',
                    name=f'Avg {sample_type}-type',
                    marker=dict(size=10),
                    line=dict(width=3),
                    hovertext=[f"Average of {len(selected_series)} series" for _ in points],
                    hoverinfo='text+x+y'
                ))
        
        elif analysis_mode == 'percentage':
            # Процентный анализ
            reference_type = data.get('reference_type', 'start') # type: ignore
            sample_type = data.get('sample_type', 'A') # type: ignore
            
            percentages = []
            for series_name in selected_series:
                if series_name not in series_dict:
                    continue
                
                series_info = series_dict[series_name]
                
                # Находим референсное значение
                reference_value = None
                if reference_type in series_info['stages']:
                    ref_probes = series_info['stages'][reference_type]
                    for probe_type, probe_data in ref_probes.items():
                        if y_axis in probe_data:
                            try:
                                reference_value = float(probe_data[y_axis])
                                break
                            except:
                                continue
                
                if reference_value is None or reference_value == 0:
                    continue
                
                # Находим значения для указанного типа пробы на всех стадиях
                for stage_name, stage_probes in series_info['stages'].items():
                    if sample_type in stage_probes:
                        probe_data = stage_probes[sample_type]
                        if x_axis in probe_data and y_axis in probe_data:
                            try:
                                x_val = float(probe_data[x_axis])
                                y_val = float(probe_data[y_axis])
                                percentage = (y_val / reference_value) * 100
                                
                                percentages.append({
                                    'x': x_val,
                                    'y': percentage,
                                    'series': series_name,
                                    'stage': stage_name,
                                    'reference': reference_value,
                                    'actual': y_val
                                })
                            except:
                                continue
            
            if percentages:
                # Группируем по стадиям
                stages = {}
                for p in percentages:
                    stage = p['stage']
                    if stage not in stages:
                        stages[stage] = []
                    stages[stage].append(p)
                
                for i, (stage_name, stage_points) in enumerate(stages.items()):
                    stage_points.sort(key=lambda p: p['x'])
                    
                    fig.add_trace(go.Scatter(
                        x=[p['x'] for p in stage_points],
                        y=[p['y'] for p in stage_points],
                        mode='markers+lines',
                        name=f'{stage_name} ({sample_type})',
                        marker=dict(size=10, color=colors[i % len(colors)]),
                        line=dict(width=2, color=colors[i % len(colors)]),
                        hovertext=[f"{p['series']}: {p['actual']:.2f}/{p['reference']:.2f}" for p in stage_points],
                        hoverinfo='text+x+y'
                    ))
        
        # Настройка стиля графика (светлая тема)
        fig.update_layout(
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(color='#2d3748', family='Arial, sans-serif'),
            title=dict(
                text=f'{y_axis} vs {x_axis} - {analysis_mode.title()} Mode',
                font=dict(size=20, color='#2b6cb0')
            ),
            xaxis=dict(
                title=dict(text=x_axis, font=dict(size=14, color='#4a5568')),
                gridcolor='#e2e8f0',
                zerolinecolor='#cbd5e0',
                linecolor='#cbd5e0',
                mirror=True
            ),
            yaxis=dict(
                title=dict(text=y_axis, font=dict(size=14, color='#4a5568')),
                gridcolor='#e2e8f0',
                zerolinecolor='#cbd5e0',
                linecolor='#cbd5e0',
                mirror=True
            ),
            hovermode='closest',
            showlegend=True,
            legend=dict(
                font=dict(color='#4a5568'),
                bgcolor='rgba(255, 255, 255, 0.8)',
                bordercolor='#e2e8f0'
            ),
            margin=dict(l=50, r=50, t=80, b=50)
        )
        
        # Рассчитываем статистику
        all_x = []
        all_y = []
        for series_info in plot_data:
            for point in series_info['data']:
                all_x.append(point['x'])
                all_y.append(point['y'])
        
        if all_x and all_y:
            # Вычисляем R²
            if len(all_x) > 1:
                correlation_matrix = np.corrcoef(all_x, all_y)
                r_squared = correlation_matrix[0, 1] ** 2
            else:
                r_squared = 0
            
            statistics = {
                'series_count': len(plot_data),
                'total_points': len(all_x),
                'x_mean': float(np.mean(all_x)),
                'y_mean': float(np.mean(all_y)),
                'x_std': float(np.std(all_x)),
                'y_std': float(np.std(all_y)),
                'r_squared': float(r_squared)
            }
        else:
            statistics = {
                'series_count': 0,
                'total_points': 0,
                'x_mean': None,
                'y_mean': None,
                'x_std': None,
                'y_std': None,
                'r_squared': None
            }
        
        # Конвертируем в JSON
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        
        return jsonify({
            "plot": graphJSON,
            "statistics": statistics
        })
        
    except Exception as e:
        print(f"Error in create_plot: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/data/sample')
def get_sample_data():
    """Получение образца данных"""
    try:
        db_data = load_data()
        return jsonify({"sample": db_data["probes"] if db_data["probes"] else []})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
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
                        solid_mass = 1/(1-(1/settings.SOLID_DENCITY_PARAM)) * (sample_mass - volume)
                        
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
    
    # Получаем все серии с start_C
    series_dicts = get_series_dicts()
    
    series_list = []
    
    for series in series_dicts:
        # Находим start_C пробу для определения m и n
        start_c_probe = series.get('start_C')
        if not start_c_probe:
            continue
            
        # Получаем информацию о типе пробы
        probe_type_info = get_probe_type(start_c_probe)
        if not probe_type_info:
            continue
            
        probe_type, m, n = probe_type_info
        source_class = get_source_class_from_probe(start_c_probe)
        
        # Формируем словарь с именами проб для данной серии
        names = {
            "start_A": series.get('start_A', {}).get('name', ''),
            "start_B": series.get('start_B', {}).get('name', ''),
            "st2_A": series.get('st2_A', {}).get('name', ''),
            "st2_B": series.get('st2_B', {}).get('name', ''),
            "st3_A": series.get('st3_A', {}).get('name', ''),
            "st3_B": series.get('st3_B', {}).get('name', ''),
            "st4_A": series.get('st4_A', {}).get('name', ''),
            "st4_B": series.get('st4_B', {}).get('name', ''),
            "st4_D": series.get('st4_D', {}).get('name', ''),
            "st5_A": series.get('st5_A', {}).get('name', ''),
            "st5_B": series.get('st5_B', {}).get('name', ''),
            "st6_E": series.get('st6_E', {}).get('name', ''),
            "st6_G": series.get('st6_G', {}).get('name', '')
        }
        
        # Создаем probe_map для быстрого доступа к пробам
        probe_map = {}
        for probe in series.values():
            if probe and 'name' in probe:
                probe_map[probe['name']] = probe
        
        # Вспомогательная функция для получения значения пробы с учетом правил связи стадий
        def get_probe_value_with_fallback(probe_name, element, fallback_rules=None, method_suffix=""):
            """
            Получает значение элемента из пробы.
            Если проба не найдена и заданы правила fallback, ищет альтернативную пробу.
            """
            # Пытаемся получить значение из целевой пробы (с суффиксом метода)
            element_with_suffix = f"{element}{method_suffix}" if method_suffix else element
            value = get_probe_value(probe_map, probe_name, element_with_suffix)
            
            # Если значение найдено (не ноль) или правила fallback не заданы - возвращаем
            if value != 0 or not fallback_rules:
                return value
            
            # Применяем правила fallback
            for fallback_name in fallback_rules:
                fallback_value = get_probe_value(probe_map, fallback_name, element_with_suffix)
                if fallback_value != 0:
                    # Записываем информацию о том, откуда взято значение
                    if probe_name in probe_map:
                        probe_map[probe_name].setdefault('fallback_info', {})[element_with_suffix] = {
                            'source': fallback_name,
                            'value': fallback_value
                        }
                    return fallback_value
            
            return 0
        
        # Правила связи между стадиями 5 и 6:
        fallback_rules_st6 = {
            "st6_E": [names["st5_B"]],  # E из 6 стадии можно взять из B 5 стадии
            "st6_G": [names["st5_A"]]   # G из 6 стадии можно взять из A 5 стадии
        }
        
        # Базовые элементы без суффиксов
        base_elements = ['Fe', 'Cu', 'Ni', 'Pd', 'Pt', 'Rh', 'Au', 'Ag', 'Os', 'Ru', 'Ir', 'K', 'Al', 'Mg', 'Co', 'Zn', 'Ca', 'Mn']
        
        # Массы с суффиксами методов
        elements_aes = [f'm{element}_AES' for element in base_elements]
        elements_ms = [f'm{element}_MS' for element in base_elements]
        elements_base = [f'm{element}' for element in base_elements]
        
        series_data = {
            "id": f"Series-{source_class}-{m}-{n}",
            "source_class": source_class,
            "method": m,
            "repeat": n,
            "elements": {},
            "probe_availability": {
                "st5_A": names["st5_A"] in probe_map,
                "st5_B": names["st5_B"] in probe_map,
                "st6_E": names["st6_E"] in probe_map,
                "st6_G": names["st6_G"] in probe_map,
                "used_fallback": False
            }
        }
        
        # Функция для расчета данных для набора элементов
        def calculate_for_elements(elements_list, method_suffix=""):
            elements_data = {}
            
            for element in elements_list:
                # Базовое имя элемента (без m и суффикса метода)
                base_element_name = element.replace('m', '').replace('_AES', '').replace('_MS', '')
                
                # 1. Расчет входной массы (Input)
                input_val = get_probe_value(probe_map, names["start_A"], element) + \
                            get_probe_value(probe_map, names["start_B"], element)
                
                # 2. Расчет выходных продуктов для баланса с учетом fallback правил
                out_D = get_probe_value(probe_map, names["st4_D"], element)  # Камерный
                
                # Используем значения с fallback для стадии 6
                out_E = get_probe_value_with_fallback(
                    names["st6_E"], 
                    base_element_name, 
                    fallback_rules_st6["st6_E"],
                    method_suffix
                )
                
                out_G = get_probe_value_with_fallback(
                    names["st6_G"], 
                    base_element_name, 
                    fallback_rules_st6["st6_G"],
                    method_suffix
                )
                
                # Оборотная жидкость
                out_Recycle = out_G
                
                total_out = out_D + get_probe_value(probe_map, names["st6_E"], element) + out_Recycle
                loss = input_val - total_out
                
                # Предотвращение деления на ноль
                calc_base = input_val if input_val != 0 else 1
                
                # 3. Расчет стадий (Bar Plot Data)
                stages = []
                # Стадия 1
                stages.append({
                    'A': get_probe_value(probe_map, names["start_A"], element),
                    'B': get_probe_value(probe_map, names["start_B"], element),
                    'D': 0.0, 'E': 0.0, 'G': 0.0
                })
                # Стадия 2
                stages.append({
                    'A': get_probe_value(probe_map, names["st2_A"], element),
                    'B': get_probe_value(probe_map, names["st2_B"], element),
                    'D': 0.0, 'E': 0.0, 'G': 0.0
                })
                # Стадия 3
                stages.append({
                    'A': get_probe_value(probe_map, names["st3_A"], element),
                    'B': get_probe_value(probe_map, names["st3_B"], element),
                    'D': 0.0, 'E': 0.0, 'G': 0.0
                })
                # Стадия 4
                stages.append({
                    'A': get_probe_value(probe_map, names["st4_A"], element),
                    'B': get_probe_value(probe_map, names["st4_B"], element),
                    'D': get_probe_value(probe_map, names["st4_D"], element),
                    'E': 0.0, 'G': 0.0
                })
                # Стадия 5
                stages.append({
                    'A': get_probe_value(probe_map, names["st5_A"], element),
                    'B': get_probe_value(probe_map, names["st5_B"], element),
                    'D': 0.0, 'E': 0.0, 'G': 0.0
                })
                # Стадия 6 (с учётом fallback)
                stages.append({
                    'A': 0.0, 'B': 0.0, 'D': 0.0,
                    'E': get_probe_value(probe_map, names["st6_E"], element),
                    'G': out_G
                })
                
                # Округление значений
                for stage in stages:
                    for k in stage:
                        stage[k] = round(stage[k], 9)
                
                # Определяем, использовались ли fallback значения
                element_for_check = f"m{base_element_name}{method_suffix}"
                used_fallback_E = (out_E != 0 and get_probe_value(probe_map, names["st6_E"], element_for_check) == 0 
                                  and out_E == get_probe_value(probe_map, names["st5_B"], element_for_check))
                used_fallback_G = (out_G != 0 and get_probe_value(probe_map, names["st6_G"], element_for_check) == 0 
                                  and out_G == get_probe_value(probe_map, names["st5_A"], element_for_check))
                
                if used_fallback_E or used_fallback_G:
                    series_data["probe_availability"]["used_fallback"] = True
                
                elements_data[element] = {
                    "balance": {
                        "input": round(input_val, 9),
                        "D": round(out_D, 9),
                        "E": get_probe_value(probe_map, names["st6_E"], element),
                        "G": round(out_G, 9),
                        "Recycle": round(out_Recycle, 9),
                        "Loss": round(loss, 9),
                        "D_pct": round((out_D / calc_base) * 100, 9),
                        "E_pct": round((get_probe_value(probe_map, names["st6_E"], element) / calc_base) * 100, 9),
                        "G_pct": round((out_G / calc_base) * 100, 9),
                        "Recycle_pct": round((out_Recycle / calc_base) * 100, 9),
                        "Loss_pct": round((loss / calc_base) * 100, 9),
                        "fallback_used": {
                            "E": used_fallback_E,
                            "G": used_fallback_G
                        }
                    },
                    "stages": stages,
                    "probe_details": {
                        "st5_A": get_probe_value(probe_map, names["st5_A"], element),
                        "st5_B": get_probe_value(probe_map, names["st5_B"], element),
                        "st6_E": get_probe_value(probe_map, names["st6_E"], element),
                        "st6_G": get_probe_value(probe_map, names["st6_G"], element),
                        "st6_E_actual": get_probe_value(probe_map, names["st6_E"], element),
                        "st6_G_actual": get_probe_value(probe_map, names["st6_G"], element),
                        "st6_E_used": get_probe_value(probe_map, names["st6_E"], element),
                        "st6_G_used": get_probe_value(probe_map, names["st6_G"], element)
                    }
                }
            
            return elements_data
        
        # Рассчитываем данные для всех методов
        series_data["elements"].update(calculate_for_elements(elements_aes, "_AES"))
        series_data["elements"].update(calculate_for_elements(elements_ms, "_MS"))
        series_data["elements"].update(calculate_for_elements(elements_base, ""))
        
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def is_valid_json(file_path):
    """Проверяем, что файл содержит валидный JSON"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            json.load(f)
        return True
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error(f"Invalid JSON file: {e}")
        return False

@app.route('/upload_replace', methods=['GET'])
def upload_form():
    """Страница с формой для загрузки файла"""
    return render_template('replace.html')

@app.route('/api/database/replace', methods=['POST'])
def replace_database():
    """
    Эндпоинт для замены базы данных новым JSON файлом
    
    Ожидается multipart/form-data с файлом под ключом 'file'
    """
    # Проверяем, есть ли файл в запросе
    if 'file' not in request.files:
        return jsonify({
            'success': False,
            'message': 'No file part in the request'
        }), 400
    
    file = request.files['file']
    
    # Проверяем, что файл был выбран
    if file.filename == '':
        return jsonify({
            'success': False,
            'message': 'No file selected'
        }), 400
    
    # Проверяем расширение файла
    if not allowed_file(file.filename):
        return jsonify({
            'success': False,
            'message': 'File type not allowed. Only JSON files are accepted.'
        }), 400
    
    try:
        # Сохраняем файл во временную папку
        filename = secure_filename(file.filename) # type: ignore
        temp_file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(temp_file_path)
        
        # Проверяем, что файл содержит валидный JSON
        if not is_valid_json(temp_file_path):
            # Удаляем временный файл
            os.remove(temp_file_path)
            return jsonify({
                'success': False,
                'message': 'Invalid JSON format in uploaded file'
            }), 400
        
        # Создаем резервную копию текущей базы данных
        backup_file = None
        if os.path.exists(DATA_FILE):
            backup_file = f"{DATA_FILE}.backup"
            with open(DATA_FILE, 'r', encoding='utf-8') as src, \
                 open(backup_file, 'w', encoding='utf-8') as dst:
                dst.write(src.read())
        
        # Заменяем текущую базу данных новым файлом
        with open(temp_file_path, 'r', encoding='utf-8') as src, \
             open(DATA_FILE, 'w', encoding='utf-8') as dst:
            dst.write(src.read())
        
        # Удаляем временный файл
        os.remove(temp_file_path)
        
        # Логируем успешную операцию
        logger.info(f"Database successfully replaced from file: {filename}")
        
        return jsonify({
            'success': True,
            'message': 'Database successfully replaced',
            'backup_created': backup_file is not None,
            'backup_file': backup_file
        }), 200
        
    except Exception as e:
        logger.error(f"Error replacing database: {e}")
        
        # В случае ошибки восстанавливаем из резервной копии, если она была создана
        if 'backup_file' in locals() and backup_file and os.path.exists(backup_file): # type: ignore
            try:
                with open(backup_file, 'r', encoding='utf-8') as src, \
                     open(DATA_FILE, 'w', encoding='utf-8') as dst:
                    dst.write(src.read())
                logger.info("Database restored from backup after error")
            except Exception as restore_error:
                logger.error(f"Failed to restore from backup: {restore_error}")
        
        # Удаляем временный файл, если он существует
        if 'temp_file_path' in locals() and os.path.exists(temp_file_path): # type: ignore
            try:
                os.remove(temp_file_path) # type: ignore
            except:
                pass
        
        return jsonify({
            'success': False,
            'message': f'Internal server error: {str(e)}'
        }), 500

@app.route('/api/schema/fields', methods=['GET'])
def get_all_fields():
    """Возвращает список всех уникальных полей, существующих в пробах"""
    probes = db_manager.get_probes()
    all_fields = set()
    for probe in probes:
        all_fields.update(probe.keys())
    return jsonify(sorted(list(all_fields)))

@app.route('/api/schema/rename', methods=['POST'])
def rename_field():
    data = request.json
    success = db_manager.rename_field_for_all_probes(data['old_name'], data['new_name']) # type: ignore
    return jsonify({'success': success})

@app.route('/api/schema/delete', methods=['POST'])
def delete_field():
    data = request.json
    # Предотвращаем удаление критических полей
    if data['field_name'] in ['id', 'name']: # type: ignore
        return jsonify({'success': False, 'error': 'Нельзя удалять системные поля'}), 400
    success = db_manager.remove_field_from_all_probes(data['field_name']) # type: ignore
    return jsonify({'success': success})

@app.route('/api/schema/set_value', methods=['POST'])
def set_global_value():
    data = request.json
    count = db_manager.set_field_value_for_all_probes(
        data['field_name'],  # type: ignore
        data['value'],  # type: ignore
        overwrite_existing=data.get('overwrite', True) # type: ignore
    )
    return jsonify({'success': True, 'updated_count': count})

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True, port=5000)