from flask import Flask, render_template, request, jsonify, send_file, session, redirect, url_for
from datetime import datetime
import json
import os
from werkzeug.utils import secure_filename
from test_ISP_AES import process_icp_aes_data
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
        default_value: Any = False,
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
            default_value=False
        )

        stats = normalize_result.get('stats',{})
        if stats.get('fields_added_total',0) > 0:
            app.logger.info(f"Normalized structure: added {stats['fields_added_total']} fields")

        # 3. Пересчитываем зависимые поля
        recalculation_result = check_and_recalculate_dependent_fields(str(DATA_FILE))
        
        if recalculation_result.get('success'):
            app.logger.info(f"Recalculated dependent fields: {recalculation_result.get('message')}")

        # 4. Форматируем числовые значения
        formatting_result = check_and_format_numeric_values(str(DATA_FILE))
        
        if formatting_result.get('success') and not formatting_result.get('skipped', False):
            app.logger.info(f"Formatted numeric values: {formatting_result.get('message')}")
            if formatting_result.get('errors'):
                app.logger.warning(f"Formatting errors: {formatting_result.get('errors')}")

        # 5. Загружаем данные для отображения
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

@app.route('/api/upload', methods=['POST'])
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

'''
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
'''
@app.route('/mass')
def render_mass():
    return render_template('mass.html')

# Масс баланс, Функция для парсинга имени пробы
def parse_probe_name_mass(name):
    # Ищем нулевую пробу вида T2-{m}C{n}
    base_match = re.match(r'T2-(\d+)C(\d+)$', name)
    if base_match:
        return {
            'type': 'base',
            'method': int(base_match.group(1)),
            'repeat': int(base_match.group(2)),
            'stage': 'C',
            'full_method': base_match.group(1)
        }
    
    # Ищем пробы типа A и B
    simple_match = re.match(r'T2-(\d+)([AB])(\d+)$', name)
    if simple_match:
        return {
            'type': 'simple',
            'method': int(simple_match.group(1)),
            'stage': simple_match.group(2),
            'repeat': int(simple_match.group(3)),
            'full_method': simple_match.group(1)
        }
    
    # Ищем пробы с L
    l_match = re.match(r'T2-L(\d+)([ABCP])(\d+)$', name)
    if l_match:
        return {
            'type': 'L_simple',
            'method': int(l_match.group(1)),
            'stage': l_match.group(2),
            'repeat': int(l_match.group(3)),
            'full_method': l_match.group(1)
        }
    
    # Ищем пробы с LPF
    lpf_match = re.match(r'T2-L(\d+)P\1F\1([D])(\d+)$', name)
    if lpf_match:
        return {
            'type': 'final1',
            'method': int(lpf_match.group(1)),
            'stage': lpf_match.group(2),
            'repeat': int(lpf_match.group(3)),
            'full_method': lpf_match.group(1)
        }
    
    # Ищем пробы с LPFN
    lpfne_match = re.match(r'T2-L(\d+)P\1F\1N\1([E])(\d+)$', name)
    if lpfne_match:
        return {
            'type': 'final2',
            'method': int(lpfne_match.group(1)),
            'stage': lpfne_match.group(2),
            'repeat': int(lpfne_match.group(3)),
            'full_method': lpfne_match.group(1)
        }
    
    # Ищем другие пробы с более сложными названиями
    complex_match = re.match(r'T2-L(\d+)P\1F\1([ABC])(\d+)$', name)
    if complex_match:
        return {
            'type': 'complex',
            'method': int(complex_match.group(1)),
            'stage': complex_match.group(2),
            'repeat': int(complex_match.group(3)),
            'full_method': complex_match.group(1)
        }
    
    complex_match2 = re.match(r'T2-L(\d+)P(\d+)F(\d+)([ABC])(\d+)$', name)
    if complex_match2:
        return {
            'type': 'complex',
            'method': int(complex_match2.group(1)),
            'stage': complex_match2.group(4),
            'repeat': int(complex_match2.group(5)),
            'full_method': f"{complex_match2.group(1)}-{complex_match2.group(2)}-{complex_match2.group(3)}"
        }
    
    return None

# Поиск серий
@app.route('/api/series', methods=['GET'])
def get_series():
    data = load_data()
    probes = data.get('probes', [])
    
    # Группируем пробы по методике и повторности
    series_dict = defaultdict(list)
    
    for probe in probes:
        parsed = parse_probe_name_mass(probe['name'])
        if parsed:
            key = (parsed['method'], parsed['repeat'])
            probe['parsed'] = parsed
            series_dict[key].append(probe)
    
    # Фильтруем только полные серии (есть нулевая проба C)
    complete_series = []
    for (method_num, repeat_num), probes_list in series_dict.items():
        # Проверяем наличие нулевой пробы T2-{m}C{n}
        has_base = any(p['parsed']['type'] == 'base' for p in probes_list)
        
        if has_base:
            # Сортируем пробы по типам для удобства
            series_probes = {
                'method': method_num,
                'repeat': repeat_num,
                'probes': probes_list,
                'base': next(p for p in probes_list if p['parsed']['type'] == 'base'),
                'source_A': next((p for p in probes_list if 'A' in p['parsed']['stage'] and p['parsed']['type'] in ['simple', 'complex']), None),
                'source_B': next((p for p in probes_list if 'B' in p['parsed']['stage'] and p['parsed']['type'] in ['simple', 'complex']), None),
                'final_D': next((p for p in probes_list if p['parsed']['stage'] == 'D'), None),
                'final_E': next((p for p in probes_list if p['parsed']['stage'] == 'E'), None)
            }
            complete_series.append(series_probes)
    
    return jsonify({'series': complete_series})

# Расчет масс-баланса
@app.route('/api/mass-balance/<int:method>/<int:repeat>', methods=['GET'])
def calculate_mass_balance(method, repeat):
    data = load_data()
    probes = data.get('probes', [])
    
    # Находим нужные пробы
    series_probes = []
    for probe in probes:
        parsed = parse_probe_name_mass(probe['name'])
        if parsed and parsed['method'] == method and parsed['repeat'] == repeat:
            probe['parsed'] = parsed
            series_probes.append(probe)
    
    if not series_probes:
        return jsonify({'error': 'Series not found'}), 404
    
    # Извлекаем конкретные пробы
    source_A = next((p for p in series_probes if 'A' in p['parsed']['stage'] and p['parsed']['type'] in ['simple', 'complex']), None)
    source_B = next((p for p in series_probes if 'B' in p['parsed']['stage'] and p['parsed']['type'] in ['simple', 'complex']), None)
    final_D = next((p for p in series_probes if p['parsed']['stage'] == 'D'), None)
    final_E = next((p for p in series_probes if p['parsed']['stage'] == 'E'), None)
    
    if not all([source_A, source_B]):
        return jsonify({'error': 'Source probes not found'}), 400
    
    metals = ['Fe', 'Cu', 'Ni']
    results = {}
    
    for metal in metals:
        # Сумма исходных проб
        source_total = source_A.get(metal, 0) + source_B.get(metal, 0) # type: ignore
        
        # Сумма конечных продуктов (если есть)
        final_total = 0
        if final_D:
            final_total += final_D.get(metal, 0)
        if final_E:
            final_total += final_E.get(metal, 0)
        
        if source_total > 0:
            percentage_remaining = (final_total / source_total) * 100 if final_total > 0 else 0
            percentage_lost = 100 - percentage_remaining
        else:
            percentage_remaining = 0
            percentage_lost = 0
        
        results[metal] = {
            'source_total': round(source_total, 2),
            'final_total': round(final_total, 2),
            'percentage_remaining': round(percentage_remaining, 2),
            'percentage_lost': round(percentage_lost, 2),
            'source_A': round(source_A.get(metal, 0), 2), # type: ignore
            'source_B': round(source_B.get(metal, 0), 2), # type: ignore
            'final_D': round(final_D.get(metal, 0), 2) if final_D else 0,
            'final_E': round(final_E.get(metal, 0), 2) if final_E else 0
        }
    
    return jsonify({
        'method': method,
        'repeat': repeat,
        'results': results,
        'probes_found': {
            'source_A': source_A['name'] if source_A else 'Not found',
            'source_B': source_B['name'] if source_B else 'Not found',
            'final_D': final_D['name'] if final_D else 'Not found',
            'final_E': final_E['name'] if final_E else 'Not found'
        }
    })

# Данные для barplot
@app.route('/api/metal-content/<int:method>/<int:repeat>', methods=['GET'])
def get_metal_content(method, repeat):
    data = load_data()
    probes = data.get('probes', [])
    
    series_probes = []
    for probe in probes:
        parsed = parse_probe_name_mass(probe['name'])
        if parsed and parsed['method'] == method and parsed['repeat'] == repeat:
            probe['parsed'] = parsed
            series_probes.append(probe)
    
    if not series_probes:
        return jsonify({'error': 'Series not found'}), 404
    
    # Группируем пробы по стадиям (буквам)
    stages = defaultdict(list)
    for probe in series_probes:
        stage = probe['parsed']['stage']
        stages[stage].append(probe)
    
    # Подготавливаем данные для графика
    metals = ['Fe', 'Cu', 'Ni']
    chart_data = {}
    
    for metal in metals:
        stage_data = []
        for stage, probes_list in sorted(stages.items()):
            if stage in ['A', 'B', 'C', 'D', 'E']:  # Только интересующие нас стадии
                values = [p.get(metal, 0) for p in probes_list]
                if values:
                    avg_value = sum(values) / len(values)
                    stage_data.append({
                        'stage': stage,
                        'value': round(avg_value, 2),
                        'individual_values': [round(v, 2) for v in values],
                        'probe_names': [p['name'] for p in probes_list]
                    })
        
        chart_data[metal] = stage_data
    
    return jsonify({
        'method': method,
        'repeat': repeat,
        'chart_data': chart_data,
        'stages_found': list(stages.keys())
    })

# Обновление базы данных
@app.route('/api/update-database', methods=['POST'])
def update_database():
    try:
        data = request.get_json()
        save_data(data)
        return jsonify({'status': 'success', 'message': 'Database updated'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Получение всей базы данных
@app.route('/api/database', methods=['GET'])
def get_database():
    return jsonify(load_data())

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