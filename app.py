from flask import Flask, render_template, request, jsonify, send_file
from datetime import datetime
import json
import os
from werkzeug.utils import secure_filename
from test_ISP_AES import process_icp_aes_data
import pandas as pd
from version_control import VersionControlSystem
from io import BytesIO

app = Flask(__name__)

app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['RESULTS_FOLDER'] = 'results'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB максимум
app.config['ALLOWED_EXTENSIONS'] = {'csv', 'xlsx', 'xls', 'json'}
app.config['DATA_FILE'] = 'data/data.json'
app.config['VERSIONS_DIR'] = 'versions'

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULTS_FOLDER'], exist_ok=True)

# Файл для хранения данных
DATA_FILE = 'data/data.json'

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

def convert_df_to_dict(df:pd.DataFrame):
    
    df['id'] = df.index + 1
    df['tags'] = [[] for _ in range(len(df))]
    df['status_id'] = 3
    df['sample_mass'] = 1
    
    try:
        df.rename(columns={'Название пробы': 'name'}, inplace=True)
        
    except:
        df.rename(columns={df.columns[0]: 'name'}, inplace=True) 
    
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

import json
from datetime import datetime

def normalize_probe_ids(data_file='data/data.json'):
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

def check_id_consistency(data_file='data/data.json'):
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

@app.route('/')
def index():
    """
    Главная страница с принудительной нормализацией ID при каждой загрузке
    """
    
    try:
        # Принудительно нормализуем ID
        success, message, changes = normalize_probe_ids()
        
        # Логируем результат
        if changes > 0:
            app.logger.info(f"Normalized {changes} probe IDs on page load: {message}")
        
        # Загружаем данные для отображения
        with open('data/data.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        probes = data.get('probes', [])
        
        return render_template('index.html', 
                             probes=probes,
                             normalization_info={
                                 'success': success,
                                 'message': message,
                                 'changes': changes
                             })
        
    except Exception as e:
        app.logger.error(f"Error loading index: {str(e)}")
        return render_template('index.html', probes=[], error=str(e))

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
        "id": db_data['next_probe_id'],
        "name": data.get('name', 'Новая проба'), # type: ignore
        "Fe": float(data.get('Fe', 0)), # type: ignore
        "Ni": float(data.get('Ni', 0)), # type: ignore
        "Cu": float(data.get('Cu', 0)), # type: ignore
        "sample_mass": float(data.get('sample_mass', 0)), # type: ignore
        "status_id": 1,
        "tags": data.get('tags', []), # type: ignore
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    db_data['probes'].append(new_probe)
    db_data['next_probe_id'] += 1
    save_data(db_data)
    
    return jsonify({"success": True, "probe": new_probe})

from version_control import VersionControlSystem  # Импортируем систему управления версиями

# Инициализируем систему управления версиями (добавьте в начало)
vcs = VersionControlSystem('data/data.json', 'versions')

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
        
        json_data = convert_df_to_dict(result_data)
        
        # ЗАГРУЖАЕМ ТЕКУЩИЕ ДАННЫЕ ПЕРЕД ИЗМЕНЕНИЕМ
        with open('data/data.json', 'r', encoding='utf-8') as f:
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
        with open('data/data.json', 'w', encoding='utf-8') as f:
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
        
        result_data = pd.read_excel(file_path)
        
        json_data = convert_df_to_dict(result_data)
        
        # ЗАГРУЖАЕМ ТЕКУЩИЕ ДАННЫЕ ПЕРЕД ИЗМЕНЕНИЕМ
        with open('data/data.json', 'r', encoding='utf-8') as f:
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
        with open('data/data.json', 'w', encoding='utf-8') as f:
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

@app.route('/api/probes/<int:probe_id>/delete', methods=['DELETE'])
def delete_probe(probe_id):
    """
    API для удаления одной пробы
    
    Пример запроса:
    DELETE /api/probes/123/delete
    """
    
    try:
        # Загружаем текущие данные
        with open('data/data.json', 'r', encoding='utf-8') as f:
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
        with open('data/data.json', 'w', encoding='utf-8') as f:
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
    export_path = os.path.join('temp', export_filename)
    
    os.makedirs('temp', exist_ok=True)
    
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
    with open('data/data.json', 'w') as f:
        json.dump(data, f)
    
    return jsonify({'success': True})

@app.route('/api/export/excel', methods=['GET'])
def export_excel():
    """Экспорт всей базы данных в Excel"""
    try:
        with open('data.json', 'r', encoding='utf-8') as f:
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

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True, port=5000)