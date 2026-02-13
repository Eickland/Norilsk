class AdminLab {
    constructor() {
        this.fields = [];
        this.init();
    }

    async init() {
        await this.loadFields();
        this.initAnimations();
    }

    initAnimations() {
        // Добавляем эффект появления для строк таблицы
        const rows = document.querySelectorAll('#schemaFieldsBody tr');
        rows.forEach((row, index) => {
            row.style.animation = `fadeIn 0.3s ease-out ${index * 0.05}s both`;
        });
    }

    async loadFields() {
        const response = await fetch('/api/schema/fields');
        this.fields = await response.json();
        this.renderFields();
    }

    renderFields() {
        const tbody = document.getElementById('schemaFieldsBody');
        tbody.innerHTML = this.fields.map(field => `
            <tr>
                <td><strong>${field}</strong></td>
                <td>
                    <button class="btn-edit" onclick="adminLab.openEditModal('${field}')">
                        <i class="fas fa-cog"></i> Управление
                    </button>
                    <button class="btn-delete" onclick="adminLab.deleteField('${field}')">
                        <i class="fas fa-trash-alt"></i> Удалить поле
                    </button>
                </td>
            </tr>
        `).join('');
        
        this.initAnimations();
    }

    openEditModal(fieldName) {
        document.getElementById('currentFieldName').value = fieldName;
        document.getElementById('fieldEditTitle').textContent = `Управление полем: ${fieldName}`;
        document.getElementById('newFieldName').value = fieldName;
        document.getElementById('fieldEditModal').style.display = 'flex';
        
        // Добавляем эффект пульсации для заголовка
        const title = document.getElementById('fieldEditTitle');
        title.style.animation = 'pulse 2s infinite';
    }

    async renameField() {
        const oldName = document.getElementById('currentFieldName').value;
        const newName = document.getElementById('newFieldName').value;
        
        if (oldName === newName) return;

        const response = await fetch('/api/schema/rename', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ old_name: oldName, new_name: newName })
        });

        if ((await response.json()).success) {
            this.showNotification('Поле переименовано во всей базе', 'success');
            setTimeout(() => location.reload(), 1500);
        }
    }

    async deleteField(fieldName) {
        if (!confirm(`Вы уверены, что хотите УДАЛИТЬ поле "${fieldName}" из ВСЕХ записей? Это действие необратимо.`)) return;

        const response = await fetch('/api/schema/delete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ field_name: fieldName })
        });

        if (response.ok) {
            this.showNotification('Поле успешно удалено', 'success');
            this.loadFields();
        } else {
            const err = await response.json();
            this.showNotification(err.error, 'error');
        }
    }

    async setGlobalValue() {
        const fieldName = document.getElementById('currentFieldName').value;
        const value = document.getElementById('globalFieldValue').value;
        const overwrite = document.getElementById('overwriteCheckbox').checked;

        const response = await fetch('/api/schema/set_value', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ field_name: fieldName, value, overwrite })
        });

        const result = await response.json();
        this.showNotification(`Обновлено проб: ${result.updated_count}`, 'success');
        setTimeout(() => {
            document.getElementById('fieldEditModal').style.display = 'none';
        }, 1500);
    }

    showNotification(message, type) {
        // Создаем элемент уведомления
        const notification = document.createElement('div');
        notification.textContent = message;
        notification.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 15px 25px;
            background: ${type === 'success' ? 'rgba(0, 255, 157, 0.2)' : 'rgba(255, 78, 78, 0.2)'};
            border: 1px solid ${type === 'success' ? '#00ff9d' : '#ff4e4e'};
            border-radius: 10px;
            color: ${type === 'success' ? '#a7ffd7' : '#ffa7a7'};
            backdrop-filter: blur(10px);
            z-index: 2000;
            animation: slideIn 0.3s ease-out;
            box-shadow: 0 0 20px ${type === 'success' ? 'rgba(0, 255, 157, 0.3)' : 'rgba(255, 78, 78, 0.3)'};
        `;
        
        document.body.appendChild(notification);
        
        setTimeout(() => {
            notification.style.animation = 'slideOut 0.3s ease-out';
            setTimeout(() => notification.remove(), 300);
        }, 3000);
    }
}

const adminLab = new AdminLab();