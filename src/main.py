from flask import Flask, render_template, request, jsonify, send_file,render_template_string
from datetime import datetime
import json
import os
import logging
from werkzeug.utils import secure_filename
from handlers.ISP_MS import process_metal_samples_csv, expand_sample_code
from handlers.ISP_AES import process_icp_aes_data
from middleware.series_worker import get_series_dicts, get_source_class_from_probe, get_probe_type,get_type_name_from_pattern_type
from mass_balance.series_analyzer import analyze_series, get_series_summary, FIELD_VALIDATION_CONFIG
from database_processing.format import convert_and_save_comma_numbers
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
from database_processing.func_db import ProbeDatabase
from logger.logging import HTTPHandler
from logging.handlers import RotatingFileHandler
from database import get_db_connection, get_full_database
import shutil
import tempfile
load_dotenv()

BASE_DIR = Path(__file__).parent.parent

app = Flask(__name__, template_folder=str(BASE_DIR /'src'/ 'templates'), static_folder=str(BASE_DIR /'src'/ 'static'))
CORS(app)
# Черный список полей, которые не должны отображаться как оси
BLACKLIST_FIELDS = {
    "Описание", "is_solid", "id", "last_normalized", 
    "status_id", "is_solution", "name", "tags"
} 
DB_PATH = str(BASE_DIR/"data"/"lab_data.db")
LOG_FILE = str(BASE_DIR/"app_local.log")

app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY')
app.config['UPLOAD_FOLDER'] = BASE_DIR / 'uploads'
app.config['RESULTS_FOLDER'] = BASE_DIR / 'results'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB максимум
app.config['ALLOWED_EXTENSIONS'] = {'csv', 'xlsx', 'xls', 'json'}
app.config['VERSIONS_DIR'] = BASE_DIR / 'versions'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_logging(app):
    # 1. Создаем форматтер
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # 2. Локальный файл (ограничение 1 МБ, храним 3 старых копии)
    file_handler = RotatingFileHandler(
        LOG_FILE, 
        maxBytes=1*1024*1024, # 1 Мегабайт
        backupCount=3, 
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)    
    # Создаем ваш обработчик
    http_handler = HTTPHandler(url="http://server.internal.error")
    
    # Устанавливаем формат (обязательно, чтобы self.format(record) работал корректно)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    http_handler.setFormatter(formatter)
    app.logger.addHandler(file_handler)
    
    # Уровень логирования
    http_handler.setLevel(logging.INFO)
    
    # Добавляем к логгеру Flask
    app.logger.addHandler(http_handler)
    app.logger.setLevel(logging.DEBUG)

setup_logging(app)

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULTS_FOLDER'], exist_ok=True)

#DATA_FILE = BASE_DIR / 'data' / 'data.json'
#app.config['DATA_FILE'] = DATA_FILE
# Инициализация системы управления версиями

@app.route('/view-logs')
def view_logs():
    if not os.path.exists(LOG_FILE):
        return "Файл логов еще не создан."

    # Читаем файл (последние 200 строк, чтобы не вешать браузер)
    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()[-200:]
    except Exception as e:
        return f"Ошибка чтения логов: {e}"

    # Простой HTML-шаблон
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Системные логи</title>
        <style>
            body { background: #1e1e1e; color: #d4d4d4; font-family: monospace; padding: 20px; }
            .log-entry { border-bottom: 1px solid #333; padding: 5px 0; }
            .INFO { color: #4caf50; }
            .WARNING { color: #ff9800; }
            .ERROR { color: #f44336; }
            .DEBUG { color: #2196f3; }
            .header { position: sticky; top: 0; background: #1e1e1e; padding: 10px; border-bottom: 2px solid #555; }
        </style>
    </head>
    <body>
        <div class="header">
            <h2>Последние 200 записей лога (Размер файла: {{ size }} байт)</h2>
            <a href="/view-logs" style="color: #aaa;">Обновить</a>
        </div>
        <div style="margin-top: 20px;">
            {% for line in lines %}
                <div class="log-entry">
                    {% if 'INFO' in line %}<span class="INFO">{{ line }}</span>
                    {% elif 'WARNING' in line %}<span class="WARNING">{{ line }}</span>
                    {% elif 'ERROR' in line %}<span class="ERROR">{{ line }}</span>
                    {% elif 'DEBUG' in line %}<span class="DEBUG">{{ line }}</span>
                    {% else %}<span>{{ line }}</span>{% endif %}
                </div>
            {% endfor %}
        </div>
    </body>
    </html>
    """
    
    file_size = os.path.getsize(LOG_FILE)
    return render_template_string(html_template, lines=lines, size=file_size)
       

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

@app.route('/')
def index():
    """
    Главная страница с нормализацией, пересчетом и форматированием
    """
    return render_template('index.html')
    
@app.route('/table')
def render_table():
    
    try:
        return render_template('data_table.html')
    
    except Exception as e:
        
        app.logger.error(f"Error loading table: {str(e)}")
        return render_template('index.html', error=str(e))        

@app.route('/api/data')
def get_data():
    try:
        with get_db_connection() as conn:
            # Получаем все пробы. Мы берем только raw_data, 
            # так как фронтенду нужны полные данные объекта
            rows = conn.execute("SELECT raw_data FROM probes").fetchall()
            
            # Превращаем строки JSON из базы в список объектов Python
            probes_list = [json.loads(row['raw_data']) for row in rows]
            
            # Возвращаем структуру, к которой привык ваш JS
            return jsonify({
                "status": "success",
                "probes": probes_list
            })
    except Exception as e:
        return jsonify(({"status": "error", "message": str(e)}), 500)

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
        result_data, _ = process_icp_aes_data(file_path=file_path)
        json_data = convert_df_to_dict(result_data) 
        
        updated_count = 0
        added_count = 0
        
        with get_db_connection() as conn:
            for new_probe in json_data:
                # Извлекаем метаданные для колонок
                p_id = new_probe.get('id')
                name = new_probe.get('name')
                s_class = get_source_class_from_probe(new_probe)
                p_info = get_probe_type(new_probe) # (type, method, exp)
                
                if not p_info or not s_class: continue

                # Проверяем существование для статистики (опционально)
                exists = conn.execute("SELECT 1 FROM probes WHERE id = ?", (p_id,)).fetchone()
                if exists: updated_count += 1
                else: added_count += 1

                # Сохраняем в БД. Флаг flag_needs_recalculation=1 запустит Воркера!
                conn.execute("""
                    INSERT OR REPLACE INTO probes 
                    (id, name, source_class, method_number, exp_number, probe_type, raw_data, flag_needs_recalculation)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """, (
                    p_id, name, s_class, p_info[1], p_info[2], p_info[0], 
                    json.dumps(new_probe)
                ))
            conn.commit()

        return jsonify({
            'success': True,
            'message': 'Данные успешно загружены в базу и поставлены в очередь на расчет',
            'metadata': {
                'probes_updated': updated_count,
                'probes_added': added_count,
                'timestamp': datetime.now().isoformat()
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/preview_ISPAES', methods=['POST'])
def preview_ISPAES():
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file'}), 400
    
    file = request.files['file']
    # Сохраняем временно для анализа
    temp_path = os.path.join(app.config['UPLOAD_FOLDER'], 'temp_' + secure_filename(file.filename)) # type: ignore
    file.save(temp_path)
    
    try:
        result_data, _ = process_icp_aes_data(file_path=temp_path)
        new_probes = convert_df_to_dict(result_data)
        
        with get_db_connection() as conn:
            # 1. Проверка на НОВЫЕ ПОЛЯ (спрашиваем у самой БД)
            # Получаем список всех колонок в JSON (если вы используете SQLite 3.38+)
            # Или просто проверяем ключи из первого объекта в базе
            sample = conn.execute("SELECT raw_data FROM probes LIMIT 1").fetchone()
            existing_fields = set(json.loads(sample['raw_data']).keys()) if sample else set()
            
            new_fields = set()
            for p in new_probes:
                for key in p.keys():
                    if key not in existing_fields and existing_fields: # если база не пуста
                        new_fields.add(key)
            
            if new_fields:
                return jsonify({
                    'success': False,
                    'error': 'NEW_FIELDS_DETECTED',
                    'details': list(new_fields)
                }), 200

            # 2. Анализ СЕРИЙ (через SQL это один запрос)
            changed_series_count = 0
            new_series_count = 0
            
            # Собираем уникальные серии из загруженного файла
            incoming_series = set()
            for p in new_probes:
                info = get_probe_type(p)
                source = get_source_class_from_probe(p)
                if info and source:
                    incoming_series.add((source, info[1], info[2]))

            for s_class, m_num, e_num in incoming_series:
                exists = conn.execute("""
                    SELECT 1 FROM probes 
                    WHERE source_class = ? AND method_number = ? AND exp_number = ? 
                    LIMIT 1
                """, (s_class, m_num, e_num)).fetchone()
                
                if exists: changed_series_count += 1
                else: new_series_count += 1

        return jsonify({
            'success': True,
            'stats': {
                'changed_series_count': changed_series_count,
                'new_series_count': new_series_count,
                'total_probes': len(new_probes)
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

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

        
        # Обрабатываем файл с помощью Python-скрипта
        result_data= process_metal_samples_csv(
            file_path=file_path
        )
        
        updated_count = 0
        added_count = 0
        
        json_data = convert_df_to_dict(result_data) # type: ignore
        
        with get_db_connection() as conn:
            for new_probe in json_data:
                # Извлекаем метаданные для колонок
                p_id = new_probe.get('id')
                name = new_probe.get('name')
                s_class = get_source_class_from_probe(new_probe)
                p_info = get_probe_type(new_probe) # (type, method, exp)
                
                if not p_info or not s_class: continue

                # Проверяем существование для статистики (опционально)
                exists = conn.execute("SELECT 1 FROM probes WHERE id = ?", (p_id,)).fetchone()
                if exists: updated_count += 1
                else: added_count += 1

                # Сохраняем в БД. Флаг flag_needs_recalculation=1 запустит Воркера!
                conn.execute("""
                    INSERT OR REPLACE INTO probes 
                    (id, name, source_class, method_number, exp_number, probe_type, raw_data, flag_needs_recalculation)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """, (
                    p_id, name, s_class, p_info[1], p_info[2], p_info[0], 
                    json.dumps(new_probe)
                ))
            conn.commit()

        return jsonify({
            'success': True,
            'message': 'Данные успешно загружены в базу и поставлены в очередь на расчет',
            'metadata': {
                'probes_updated': updated_count,
                'probes_added': added_count,
                'timestamp': datetime.now().isoformat()
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/preview_ISPMS', methods=['POST'])
def preview_ISPMS():
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file'}), 400
    
    file = request.files['file']
    # Сохраняем временно для анализа
    temp_path = os.path.join(app.config['UPLOAD_FOLDER'], 'temp_' + secure_filename(file.filename)) # type: ignore
    file.save(temp_path)
    
    try:
        result_data = process_metal_samples_csv(file_path=temp_path)
        new_probes = convert_df_to_dict(result_data) # type: ignore
        
        with get_db_connection() as conn:
            # 1. Проверка на НОВЫЕ ПОЛЯ (спрашиваем у самой БД)
            # Получаем список всех колонок в JSON (если вы используете SQLite 3.38+)
            # Или просто проверяем ключи из первого объекта в базе
            sample = conn.execute("SELECT raw_data FROM probes LIMIT 1").fetchone()
            existing_fields = set(json.loads(sample['raw_data']).keys()) if sample else set()
            
            new_fields = set()
            for p in new_probes:
                for key in p.keys():
                    if key not in existing_fields and existing_fields: # если база не пуста
                        new_fields.add(key)
            
            if new_fields:
                return jsonify({
                    'success': False,
                    'error': 'NEW_FIELDS_DETECTED',
                    'details': list(new_fields)
                }), 200

            # 2. Анализ СЕРИЙ (через SQL это один запрос)
            changed_series_count = 0
            new_series_count = 0
            
            # Собираем уникальные серии из загруженного файла
            incoming_series = set()
            for p in new_probes:
                info = get_probe_type(p)
                source = get_source_class_from_probe(p)
                if info and source:
                    incoming_series.add((source, info[1], info[2]))

            for s_class, m_num, e_num in incoming_series:
                exists = conn.execute("""
                    SELECT 1 FROM probes 
                    WHERE source_class = ? AND method_number = ? AND exp_number = ? 
                    LIMIT 1
                """, (s_class, m_num, e_num)).fetchone()
                
                if exists: changed_series_count += 1
                else: new_series_count += 1

        return jsonify({
            'success': True,
            'stats': {
                'changed_series_count': changed_series_count,
                'new_series_count': new_series_count,
                'total_probes': len(new_probes)
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/upload_data_synthes', methods=['POST'])
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
        
        result_data = pd.read_csv(file_path,sep=';')
        
        result_data['name'] = result_data['name'].apply(expand_sample_code)
        
        json_data = convert_df_to_dict(result_data)
        
        updated_count = 0
        added_count = 0
        
        with get_db_connection() as conn:
            for new_probe in json_data:
                # Извлекаем метаданные для колонок
                p_id = new_probe.get('id')
                name = new_probe.get('name')
                s_class = get_source_class_from_probe(new_probe)
                p_info = get_probe_type(new_probe) # (type, method, exp)
                
                if not p_info or not s_class: continue

                # Проверяем существование для статистики (опционально)
                exists = conn.execute("SELECT 1 FROM probes WHERE id = ?", (p_id,)).fetchone()
                if exists: updated_count += 1
                else: added_count += 1

                # Сохраняем в БД. Флаг flag_needs_recalculation=1 запустит Воркера!
                conn.execute("""
                    INSERT OR REPLACE INTO probes 
                    (id, name, source_class, method_number, exp_number, probe_type, raw_data, flag_needs_recalculation)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """, (
                    p_id, name, s_class, p_info[1], p_info[2], p_info[0], 
                    json.dumps(new_probe)
                ))
            conn.commit()

        return jsonify({
            'success': True,
            'message': 'Данные успешно загружены в базу и поставлены в очередь на расчет',
            'metadata': {
                'probes_updated': updated_count,
                'probes_added': added_count,
                'timestamp': datetime.now().isoformat()
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/preview_upload_synthes', methods=['POST'])
def preview_synthes():
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file'}), 400
    
    file = request.files['file']
    # Сохраняем временно для анализа
    temp_path = os.path.join(app.config['UPLOAD_FOLDER'], 'temp_' + secure_filename(file.filename)) # type: ignore
    file.save(temp_path)
    
    try:
        # Обрабатываем данные (используем вашу существующую функцию)
        result_data = pd.read_csv(temp_path,sep=';')
        result_data['name'] = result_data['name'].apply(expand_sample_code)
        
        new_probes = convert_df_to_dict(result_data) # type: ignore
        
        updated_count = 0
        added_count = 0
                
        with get_db_connection() as conn:
            
            for new_probe in new_probes:
                # Извлекаем метаданные для колонок
                p_id = new_probe.get('id')
                name = new_probe.get('name')
                s_class = get_source_class_from_probe(new_probe)
                p_info = get_probe_type(new_probe) # (type, method, exp)
                
                if not p_info or not s_class: continue

                # Проверяем существование для статистики (опционально)
                exists = conn.execute("SELECT 1 FROM probes WHERE id = ?", (p_id,)).fetchone()
                if exists: updated_count += 1
                else: added_count += 1

                # Сохраняем в БД. Флаг flag_needs_recalculation=1 запустит Воркера!
                conn.execute("""
                    INSERT OR REPLACE INTO probes 
                    (id, name, source_class, method_number, exp_number, probe_type, raw_data, flag_needs_recalculation)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """, (
                    p_id, name, s_class, p_info[1], p_info[2], p_info[0], 
                    json.dumps(new_probe)
                ))
            conn.commit()

        return jsonify({
            'success': True,
            'message': 'Данные успешно загружены в базу и поставлены в очередь на расчет',
            'metadata': {
                'probes_updated': updated_count,
                'probes_added': added_count,
                'timestamp': datetime.now().isoformat()
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

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


@app.route('/api/probes/<int:probe_id>/update_probe', methods=['PUT'])
def update_probe(probe_id):
    """API для обновления данных пробы в SQLite"""
    try:
        update_data = request.get_json()
        
        if not update_data:
            return jsonify({'success': False, 'error': 'Нет данных для обновления'}), 400
            
        if str(update_data.get('id')) != str(probe_id):
            return jsonify({'success': False, 'error': 'Несоответствие ID пробы'}), 400

        with get_db_connection() as conn:
            # 1. Получаем текущую версию пробы из БД
            row = conn.execute(
                "SELECT raw_data FROM probes WHERE id = ?", 
                (probe_id,)
            ).fetchone()
            
            if not row:
                return jsonify({'success': False, 'error': f'Проба с ID {probe_id} не найдена'}), 404
            
            current_probe = json.loads(row['raw_data'])
            
            # 2. Склеиваем старые данные с новыми (как и раньше)
            updated_probe = {**current_probe, **update_data}
            
            # Обновляем техническое поле времени изменения (в самом JSON)
            updated_probe['last_updated'] = datetime.now().isoformat()
            
            # 3. Сохраняем обратно в БД
            # Обновляем саму строку, ставим флаг пересчета и обновляем колонку name (на случай если ее сменили)
            conn.execute("""
                UPDATE probes 
                SET name = ?, 
                    raw_data = ?, 
                    flag_needs_recalculation = 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (
                updated_probe.get('name', 'Unnamed'), 
                json.dumps(updated_probe, ensure_ascii=False), 
                probe_id
            ))
            
            conn.commit()
            
        # Логирование
        user_email = request.headers.get('X-User-Email', 'anonymous')
        app.logger.info(f"Probe #{probe_id} updated and queued for recalculation by {user_email}")
        
        return jsonify({
            'success': True,
            'message': f'Проба #{probe_id} обновлена и поставлена в очередь на пересчет',
            'probe': updated_probe
        })
        
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
        with get_db_connection() as conn:
            # 1. Получаем текущую версию пробы из БД
            row = conn.execute(
                "SELECT raw_data FROM probes WHERE id = ?", 
                (probe_id,)
            ).fetchone()
            
            if not row:
                return jsonify({'success': False, 'error': f'Проба с ID {probe_id} не найдена'}), 404
            
            current_probe = json.loads(row['raw_data'])
            
        probe = current_probe
    
        
        if not probe:
            return jsonify({
                'success': False,
                'error': f'Проба с ID {probe_id} не найдена'
            }), 404
        
        # Определяем типы полей на основе первой пробы с таким полем
        field_types = {}
        for field in probe.keys():
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
            'probe': probe,
            'metadata': {
                'field_types': field_types,
                'field_labels': field_labels,
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
        # Список ID, которые нужно удалить
        probe_ids = data_request.get('ids', [])
        
        if not probe_ids:
            return jsonify({'success': False, 'error': 'Список ID пуст'}), 400

        with get_db_connection() as conn:
            # Выполняем удаление. SQLite эффективно обработает список ID.
            # Нам нужно создать строку с вопросиками (?, ?, ?) по количеству ID
            placeholders = ', '.join(['?'] * len(probe_ids))
            query = f"DELETE FROM probes WHERE id IN ({placeholders})"
            
            cursor = conn.execute(query, probe_ids)
            deleted_count = cursor.rowcount  # Узнаем, сколько строк было реально удалено
            
            conn.commit()
            
        return jsonify({
            'success': True,
            'deleted_count': deleted_count,
            'message': f'Удалено {deleted_count} проб'
        })
        
    except Exception as e:
        app.logger.error(f"Error in batch delete: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500
        
@app.route('/api/export/excel', methods=['GET'])
@app.route('/api/export/db', methods=['GET'])
def export_database():
    """Экспорт всей базы данных в формате .db (SQLite)"""
    try:
        source_db = DB_PATH 
        
        # Создаем имя файла для скачивания с временной меткой
        download_name = f"norilsk_probes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        
        # Создаем временный файл, чтобы не мешать работе основного процесса
        # Это гарантирует, что мы отправим целостный файл без блокировок
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, download_name)
        
        # Копируем файл базы данных
        # Если вы используете WAL-режим, лучше использовать специальный BACKUP API,
        # но для обычного экспорта shutil.copy2 обычно достаточно.
        shutil.copy2(source_db, temp_path)
        
        return send_file(
            temp_path,
            mimetype='application/x-sqlite3',
            as_attachment=True,
            download_name=download_name
        )

    except Exception as e:
        app.logger.error(f"Ошибка при экспорте базы: {str(e)}")
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

@app.route('/graph')
def plot_graph():
    """Главная страница"""
    return render_template('plot_graph.html')

def extract_series_info() -> Dict:
    """Извлечение информации о сериях из данных"""
    probes = get_full_database()
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
    # Получаем все серии с start_C одной оптимизированной функцией
    series_dicts = get_series_dicts()
    series_list = []
    
    # Предварительно компилируем список элементов для всех методов
    base_elements = ['Fe', 'Cu', 'Ni', 'Pd', 'Pt', 'Rh', 'Au', 'Ag', 'Os', 'Ru', 'Ir', 
                     'K', 'Al', 'Mg', 'Co', 'Zn', 'Ca', 'Mn']
    
    # Создаем маппинг методов к суффиксам и полным именам элементов
    method_configs = [
        {'suffix': '_AES', 'elements': [f'm{e}_AES' for e in base_elements]},
        {'suffix': '_MS', 'elements': [f'm{e}_MS' for e in base_elements]},
        {'suffix': '', 'elements': [f'm{e}' for e in base_elements]}
    ]
    
    # Правила связи между стадиями 5 и 6 (выносим за цикл как константу)
    FALLBACK_RULES_ST6 = {
        "st6_E": "st5_B",  # E из 6 стадии можно взять из B 5 стадии
        "st6_G": "st5_A"   # G из 6 стадии можно взять из A 5 стадии
    }
    
    # Типы проб, которые нас интересуют для баланса
    PROBE_TYPES = ['start_A', 'start_B', 'st2_A', 'st2_B', 'st3_A', 'st3_B', 
                   'st4_A', 'st4_B', 'st4_D', 'st5_A', 'st5_B', 'st6_E', 'st6_G']
    
    for series in series_dicts:
        # Проверяем наличие start_C через оптимизированный доступ
        start_c_probe = series.get('start_C')
        if not start_c_probe:
            continue
        
        # Получаем информацию о типе пробы через новую функцию
        probe_type_info = get_probe_type(start_c_probe)
        if not probe_type_info:
            continue
        
        probe_type, m, n = probe_type_info
        source_class = get_source_class_from_probe(start_c_probe)
        
        # Формируем словарь с именами проб и создаем probe_map одной операцией
        names = {}
        probe_map = {}
        
        for probe_type in PROBE_TYPES:
            probe = series.get(probe_type, {})
            name = probe.get('name', '')
            names[probe_type] = name
            if name and probe:
                probe_map[name] = probe
        
        # Создаем lookup для fallback правил
        fallback_lookup = {
            names['st6_E']: names.get(FALLBACK_RULES_ST6['st6_E']),
            names['st6_G']: names.get(FALLBACK_RULES_ST6['st6_G'])
        }
        
        # Инициализируем данные серии
        series_data = {
            "id": f"Series-{source_class}-{m}-{n}",
            "source_class": source_class,
            "method": m,
            "repeat": n,
            "elements": {},
            "probe_availability": {
                "st5_A": bool(names.get('st5_A')),
                "st5_B": bool(names.get('st5_B')),
                "st6_E": bool(names.get('st6_E')),
                "st6_G": bool(names.get('st6_G')),
                "used_fallback": False
            }
        }
        
        # Оптимизированная функция получения значения с кэшированием
        def get_probe_value_cached(probe_name, element_key, cache={}):
            cache_key = f"{probe_name}:{element_key}"
            if cache_key in cache:
                return cache[cache_key]
            
            if probe_name not in probe_map:
                cache[cache_key] = 0
                return 0
            
            probe = probe_map[probe_name]
            
            # Пробуем разные варианты ключа
            for key_variant in [element_key, element_key.lower(), element_key.upper()]:
                if key_variant in probe:
                    value = probe[key_variant]
                    try:
                        result = float(value) if value not in [None, ""] else 0
                        cache[cache_key] = result
                        return result
                    except (ValueError, TypeError):
                        pass
            
            cache[cache_key] = 0
            return 0
        
        # Функция для расчета данных для одного элемента
        def calculate_element_data(element, base_element, method_suffix=""):
            # Получаем значения с кэшированием
            input_val = (get_probe_value_cached(names['start_A'], element) + 
                        get_probe_value_cached(names['start_B'], element))
            
            # Основные выходные продукты
            out_D = get_probe_value_cached(names['st4_D'], element)
            out_E_actual = get_probe_value_cached(names['st6_E'], element)
            out_G_actual = get_probe_value_cached(names['st6_G'], element)
            
            # Применяем fallback правила
            out_E_used = out_E_actual
            out_G_used = out_G_actual
            fallback_E = False
            fallback_G = False
            
            if out_E_actual == 0 and fallback_lookup[names['st6_E']]:
                out_E_used = get_probe_value_cached(fallback_lookup[names['st6_E']], element)
                fallback_E = (out_E_used != 0)
            
            if out_G_actual == 0 and fallback_lookup[names['st6_G']]:
                out_G_used = get_probe_value_cached(fallback_lookup[names['st6_G']], element)
                fallback_G = (out_G_used != 0)
            
            if fallback_E or fallback_G:
                series_data["probe_availability"]["used_fallback"] = True
            
            # Расчет баланса
            total_out = out_D + out_E_used + out_G_used
            loss = input_val - total_out
            calc_base = input_val if input_val != 0 else 1
            
            # Создаем данные стадий одной операцией
            stages = [
                {'A': get_probe_value_cached(names['start_A'], element), 'B': get_probe_value_cached(names['start_B'], element), 'D': 0.0, 'E': 0.0, 'G': 0.0},
                {'A': get_probe_value_cached(names['st2_A'], element), 'B': get_probe_value_cached(names['st2_B'], element), 'D': 0.0, 'E': 0.0, 'G': 0.0},
                {'A': get_probe_value_cached(names['st3_A'], element), 'B': get_probe_value_cached(names['st3_B'], element), 'D': 0.0, 'E': 0.0, 'G': 0.0},
                {'A': get_probe_value_cached(names['st4_A'], element), 'B': get_probe_value_cached(names['st4_B'], element), 'D': out_D, 'E': 0.0, 'G': 0.0},
                {'A': get_probe_value_cached(names['st5_A'], element), 'B': get_probe_value_cached(names['st5_B'], element), 'D': 0.0, 'E': 0.0, 'G': 0.0},
                {'A': 0.0, 'B': 0.0, 'D': 0.0, 'E': out_E_used, 'G': out_G_used}
            ]
            
            # Округляем значения
            for stage in stages:
                for k in stage:
                    stage[k] = round(stage[k], 9)
            
            return {
                "balance": {
                    "input": round(input_val, 9),
                    "D": round(out_D, 9),
                    "E": round(out_E_used, 9),
                    "G": round(out_G_used, 9),
                    "Recycle": round(out_G_used, 9),
                    "Loss": round(loss, 9),
                    "D_pct": round((out_D / calc_base) * 100, 9),
                    "E_pct": round((out_E_used / calc_base) * 100, 9),
                    "G_pct": round((out_G_used / calc_base) * 100, 9),
                    "Recycle_pct": round((out_G_used / calc_base) * 100, 9),
                    "Loss_pct": round((loss / calc_base) * 100, 9),
                    "fallback_used": {"E": fallback_E, "G": fallback_G}
                },
                "stages": stages,
                "probe_details": {
                    "st5_A": get_probe_value_cached(names['st5_A'], element),
                    "st5_B": get_probe_value_cached(names['st5_B'], element),
                    "st6_E": out_E_actual,
                    "st6_G": out_G_actual,
                    "st6_E_actual": out_E_actual,
                    "st6_G_actual": out_G_actual,
                    "st6_E_used": out_E_used,
                    "st6_G_used": out_G_used
                }
            }
        
        # Рассчитываем данные для всех методов и элементов
        for config in method_configs:
            for element in config['elements']:
                base_element = element.replace('m', '').replace('_AES', '').replace('_MS', '')
                series_data["elements"][element] = calculate_element_data(element, base_element, config['suffix'])
        
        series_list.append(series_data)
    
    # Добавляем статистику по использованию fallback
    if series_list:
        total_series = len(series_list)
        series_with_fallback = sum(1 for s in series_list if s["probe_availability"]["used_fallback"])
        fallback_percentage = round((series_with_fallback / total_series * 100), 2) if total_series > 0 else 0
        
        for series in series_list:
            series["stats"] = {
                "total_series": total_series,
                "series_with_fallback": series_with_fallback,
                "fallback_percentage": fallback_percentage
            }
    
    return jsonify(series_list)

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

@app.route('/analyzer')
def render_analyzer():
    """Главная страница"""
    try:
        series_list, total_series = analyze_series()
        return render_template('series_analyzer.html', total_series=total_series)
    except Exception as e:
        return render_template('series_analyzer.html', error=str(e), total_series=0)

@app.route('/api/series_analyzer')
def get_series_analyzer():
    """API: Получение списка всех серий (для боковой панели)"""
    try:
        series_list, total_series = analyze_series()
        series_summaries = [get_series_summary(s) for s in series_list]
        return jsonify({
            'success': True,
            'series': series_summaries,
            'total': total_series
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/series/<series_id>')
def get_series_details(series_id):
    """API: Получение детальной информации о конкретной серии"""
    try:
        # Парсим ID серии
        parts = series_id.split('-')
        if len(parts) != 3:
            return jsonify({'success': False, 'error': 'Invalid series ID'}), 400
        
        source_class, method_str, exp_str = parts
        method_number = int(method_str)
        exp_number = int(exp_str)
        
        # Получаем все серии
        series_list, _ = analyze_series()
        
        # Ищем нужную серию
        target_series = None
        for series in series_list:
            if (series.series_key[0] == source_class and 
                series.series_key[1] == method_number and 
                series.series_key[2] == exp_number):
                target_series = series
                break
        
        if not target_series:
            return jsonify({'success': False, 'error': 'Series not found'}), 404
        
        # Формируем детальный ответ
        result = {
            'success': True,
            'series': {
                'id': series_id,
                'source_class': source_class,
                'method_number': method_number,
                'exp_number': exp_number,
                'probes': [],
                'missing_types': target_series.missing_types,
                'has_warnings': target_series.has_warnings
            }
        }
        
        # Добавляем информацию о пробах
        for probe_type, probe_info in target_series.probes_by_type.items():
            probe_data = {
                'type': get_type_name_from_pattern_type(probe_type),
                'name': probe_info.probe.get('name', 'Unknown'),
                'data': {k: v for k, v in probe_info.probe.items() if k != 'name'},
                'warnings': probe_info.warnings
            }
            result['series']['probes'].append(probe_data)
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True, port=5000)