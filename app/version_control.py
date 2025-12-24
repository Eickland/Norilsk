import os
import json
import shutil
from datetime import datetime
from typing import Dict, List, Optional
import hashlib

class VersionControlSystem:
    def __init__(self, data_file: str, versions_dir: str = "versions"):
        """
        Инициализация системы управления версиями
        
        Args:
            data_file: путь к основному JSON файлу
            versions_dir: папка для хранения версий
        """
        self.data_file = data_file
        self.versions_dir = versions_dir
        self.history_file = os.path.join(self.versions_dir, "version_history.json")
        
        # Создаем директории если их нет
        os.makedirs(self.versions_dir, exist_ok=True)
        
        # Загружаем историю версий или создаем новую
        self.history = self._load_history()
    
    def _load_history(self) -> List[Dict]:
        """Загрузка истории версий из файла"""
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                return []
        return []
    
    def _save_history(self):
        """Сохранение истории версий в файл"""
        with open(self.history_file, 'w', encoding='utf-8') as f:
            json.dump(self.history, f, ensure_ascii=False, indent=2)
    
    def _calculate_hash(self, data: Dict) -> str:
        """Вычисление хеша содержимого JSON"""
        content = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def _get_current_data(self) -> Dict:
        """Получение текущих данных из основного файла"""
        if os.path.exists(self.data_file):
            with open(self.data_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    
    def _save_version_file(self, version_data: Dict, version_id: str):
        """Сохранение версии в отдельный файл"""
        version_filename = f"v{version_id}_{os.path.basename(self.data_file)}"
        version_path = os.path.join(self.versions_dir, version_filename)
        
        with open(version_path, 'w', encoding='utf-8') as f:
            json.dump(version_data, f, ensure_ascii=False, indent=2)
    
    def create_version(self, description: str = "", author: str = "system", 
                       change_type: str = "manual") -> Optional[Dict]:
        """
        Создание новой версии текущего состояния
        
        Args:
            description: описание изменений
            author: автор изменений
            change_type: тип изменения (manual, auto, import, etc.)
        
        Returns:
            Созданная версия или None если изменений нет
        """
        current_data = self._get_current_data()
        
        if not current_data:
            return None
        
        # Вычисляем хеш текущих данных
        current_hash = self._calculate_hash(current_data)
        
        # Проверяем, есть ли изменения по сравнению с последней версией
        if self.history:
            last_version = self.history[-1]
            if last_version.get('hash') == current_hash:
                return None  # Изменений нет
        
        # Генерируем ID новой версии
        version_id = len(self.history) + 1
        
        # Создаем объект версии
        version = {
            'id': version_id,
            'timestamp': datetime.now().isoformat(),
            'description': description,
            'author': author,
            'change_type': change_type,
            'hash': current_hash,
            'filename': f"v{version_id}_{os.path.basename(self.data_file)}",
            'data_size': len(json.dumps(current_data)),
            'probes_count': len(current_data.get('probes', []))
        }
        
        # Сохраняем версию в отдельный файл
        self._save_version_file(current_data, version_id) # type: ignore
        
        # Добавляем в историю
        self.history.append(version)
        self._save_history()
        
        # Ограничиваем количество хранимых версий (опционально)
        self._cleanup_old_versions(max_versions=50)
        
        return version
    
    def get_version(self, version_id: int) -> Optional[Dict]:
        """Получение данных конкретной версии"""
        for version in self.history:
            if version['id'] == version_id:
                version_file = os.path.join(self.versions_dir, version['filename'])
                if os.path.exists(version_file):
                    with open(version_file, 'r', encoding='utf-8') as f:
                        return {
                            'metadata': version,
                            'data': json.load(f)
                        }
        return None
    
    def restore_version(self, version_id: int) -> bool:
        """
        Восстановление конкретной версии как текущей
        
        Args:
            version_id: ID версии для восстановления
        
        Returns:
            True если успешно, False если версия не найдена
        """
        version_data = self.get_version(version_id)
        
        if not version_data:
            return False
        
        # Сохраняем текущее состояние как резервную копию перед восстановлением
        backup_version = self.create_version(
            description=f"Backup before restoring version {version_id}",
            author="system",
            change_type="backup"
        )
        
        # Восстанавливаем версию в основной файл
        with open(self.data_file, 'w', encoding='utf-8') as f:
            json.dump(version_data['data'], f, ensure_ascii=False, indent=2)
        
        # Создаем запись о восстановлении
        restore_version = {
            'id': len(self.history) + 1,
            'timestamp': datetime.now().isoformat(),
            'description': f'Restored from version {version_id}',
            'author': 'system',
            'change_type': 'restore',
            'hash': self._calculate_hash(version_data['data']),
            'filename': f"restore_v{version_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            'data_size': len(json.dumps(version_data['data'])),
            'probes_count': len(version_data['data'].get('probes', []))
        }
        
        # Сохраняем восстановленную версию
        self._save_version_file(version_data['data'], f"restore_{version_id}")
        
        self.history.append(restore_version)
        self._save_history()
        
        return True
    
    def compare_versions(self, version_id1: int, version_id2: int) -> Dict:
        """Сравнение двух версий"""
        v1_data = self.get_version(version_id1)
        v2_data = self.get_version(version_id2)
        
        if not v1_data or not v2_data:
            return {'error': 'Version not found'}
        
        v1_probes = v1_data['data'].get('probes', [])
        v2_probes = v2_data['data'].get('probes', [])
        
        # Простое сравнение количества проб
        comparison = {
            'version1': v1_data['metadata'],
            'version2': v2_data['metadata'],
            'probes_count_diff': len(v2_probes) - len(v1_probes),
            'data_size_diff': v2_data['metadata']['data_size'] - v1_data['metadata']['data_size'],
            'is_same_hash': v1_data['metadata']['hash'] == v2_data['metadata']['hash']
        }
        
        # Более детальное сравнение (опционально)
        comparison['detailed'] = self._detailed_comparison(v1_probes, v2_probes)
        
        return comparison
    
    def _detailed_comparison(self, probes1: List, probes2: List) -> Dict:
        """Детальное сравнение списков проб"""
        # Простая реализация - можно расширить
        ids1 = {p.get('id') for p in probes1 if 'id' in p}
        ids2 = {p.get('id') for p in probes2 if 'id' in p}
        
        return {
            'added_probes': list(ids2 - ids1),
            'removed_probes': list(ids1 - ids2),
            'common_probes': list(ids1 & ids2)
        }
    
    def get_all_versions(self) -> List[Dict]:
        """Получение всех версий с возможностью пагинации"""
        return self.history
    
    def get_version_count(self) -> int:
        """Получение количества версий"""
        return len(self.history)
    
    def _cleanup_old_versions(self, max_versions: int = 50):
        """Удаление старых версий (оставляет только max_versions последних)"""
        if len(self.history) > max_versions:
            # Удаляем старые файлы версий
            versions_to_keep = self.history[-max_versions:]
            
            # Получаем имена файлов для удаления
            all_files = {v['filename'] for v in self.history}
            keep_files = {v['filename'] for v in versions_to_keep}
            delete_files = all_files - keep_files
            
            # Удаляем файлы
            for filename in delete_files:
                file_path = os.path.join(self.versions_dir, filename)
                if os.path.exists(file_path):
                    os.remove(file_path)
            
            # Обновляем историю
            self.history = versions_to_keep
            # Переиндексируем ID
            for i, version in enumerate(self.history, 1):
                version['id'] = i
            
            self._save_history()
    
    def export_version(self, version_id: int, export_path: str) -> bool:
        """Экспорт версии в отдельный файл"""
        version_data = self.get_version(version_id)
        
        if not version_data:
            return False
        
        with open(export_path, 'w', encoding='utf-8') as f:
            json.dump(version_data, f, ensure_ascii=False, indent=2)
        
        return True
    
    def delete_version(self, version_id: int) -> bool:
        """Удаление конкретной версии (с осторожностью!)"""
        version_to_delete = None
        for version in self.history:
            if version['id'] == version_id:
                version_to_delete = version
                break
        
        if not version_to_delete:
            return False
        
        # Удаляем файл версии
        version_file = os.path.join(self.versions_dir, version_to_delete['filename'])
        if os.path.exists(version_file):
            os.remove(version_file)
        
        # Удаляем из истории
        self.history = [v for v in self.history if v['id'] != version_id]
        
        # Переиндексируем оставшиеся версии
        for i, version in enumerate(self.history, 1):
            version['id'] = i
        
        self._save_history()
        return True