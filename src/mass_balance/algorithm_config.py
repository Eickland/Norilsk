import json
import os
import tempfile
import shutil
from typing import Dict, Any, Optional
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent.parent

class AtomicFileConfig:
    """
    Безопасное хранение конфигурации с атомарными операциями записи
    Использует временные файлы для предотвращения повреждения данных
    """
    
    def __init__(self, filename=BASE_DIR/"src"/"mass_balance"/"algorithm_coeff.json"):
        self.filename = filename
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        """Создает файл с базовой структурой, если его нет"""
        if not os.path.exists(self.filename):
            self.write({'coefficients': {}})
    
    def read(self) -> Dict[str, Any]:
        """Безопасное чтение конфигурации"""
        try:
            with open(self.filename, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            # Возвращаем пустую структуру в случае ошибки
            return {'coefficients': {}}
    
    def write(self, data: Dict[str, Any]) -> bool:
        """
        Атомарная запись конфигурации через временный файл
        Возвращает True при успешной записи
        """
        # Создаем временный файл в той же директории
        temp_dir = os.path.dirname(self.filename) or '.'
        temp_path = None
        
        try:
            # Создаем временный файл
            with tempfile.NamedTemporaryFile(
                mode='w',
                dir=temp_dir,
                delete=False,
                encoding='utf-8'
            ) as temp_file:
                temp_path = temp_file.name
                json.dump(data, temp_file, indent=4, ensure_ascii=False)
                temp_file.flush()
                os.fsync(temp_file.fileno())  # Принудительная запись на диск
            
            # Атомарно заменяем старый файл новым
            shutil.move(temp_path, self.filename)
            return True
            
        except Exception as e:
            # В случае ошибки удаляем временный файл
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)
            print(f"Ошибка при записи конфигурации: {e}")
            return False
    
    # ========== Методы для работы с коэффициентами ==========
    
    def get_all_coefficients(self) -> Dict[str, float]:
        """Получить все коэффициенты"""
        data = self.read()
        return data.get('coefficients', {})
    
    def get_coefficient(self, name: str, default: Optional[float] = None) -> Optional[float]:
        """Получить конкретный коэффициент"""
        coefficients = self.get_all_coefficients()
        return coefficients.get(name, default)
    
    def set_coefficient(self, name: str, value: float) -> bool:
        """Установить значение одного коэффициента"""
        data = self.read()
        if 'coefficients' not in data:
            data['coefficients'] = {}
        data['coefficients'][name] = value
        return self.write(data)
    
    def set_coefficients(self, coeff_dict: Dict[str, float]) -> bool:
        """Установить несколько коэффициентов сразу"""
        data = self.read()
        if 'coefficients' not in data:
            data['coefficients'] = {}
        data['coefficients'].update(coeff_dict)
        return self.write(data)
    
    def delete_coefficient(self, name: str) -> bool:
        """Удалить коэффициент"""
        data = self.read()
        if 'coefficients' in data and name in data['coefficients']:
            del data['coefficients'][name]
            return self.write(data)
        return False
    
    def update_coefficient(self, name: str, delta: float) -> Optional[float]:
        """Изменить коэффициент на дельту (прибавить/отнять)"""
        data = self.read()
        if 'coefficients' not in data:
            data['coefficients'] = {}
        
        current = data['coefficients'].get(name, 0.0)
        new_value = current + delta
        data['coefficients'][name] = new_value
        
        if self.write(data):
            return new_value
        return None