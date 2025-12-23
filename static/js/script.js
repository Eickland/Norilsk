class ProbeLab {
    constructor() {
        this.data = null;
        this.currentFilter = '';
        this.currentStatusFilter = null;
        this.init();
    }

    async init() {
        await this.loadData();
        this.renderTable();
        this.renderStatusFilter();
        this.renderStats();
        this.setupEventListeners();
    }

    async loadData() {
        try {
            const response = await fetch('/api/data');
            this.data = await response.json();
        } catch (error) {
            console.error('Ошибка загрузки данных:', error);
        }
    }

    async updateProbeStatus(probeId, statusId) {
        try {
            const response = await fetch('/api/update_status', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    probe_id: probeId,
                    status_id: statusId
                })
            });
            
            if (response.ok) {
                await this.loadData();
                this.renderTable();
                this.renderStats();
            }
        } catch (error) {
            console.error('Ошибка обновления статуса:', error);
        }
    }

    renderTable() {
        const tbody = document.getElementById('probeTableBody');
        if (!tbody || !this.data) return;

        tbody.innerHTML = '';

        // Безопасный вывод чисел с форматированием
        const safeToFixed = (value, decimals = 1) => {
            if (value === null || value === undefined || isNaN(value)) {
                return '—'; // или 'N/A', 'нет данных'
            }
            return Number(value).toFixed(decimals);
        };

        // Безопасный доступ к свойствам
        const getSafeValue = (obj, property, defaultValue = '—') => {
            return obj && obj[property] !== undefined ? obj[property] : defaultValue;
        };

        const filteredProbes = this.data.probes.filter(probe => {
            // Безопасная проверка на существование свойств
            const probeName = getSafeValue(probe, 'name', '');
            const probeTags = getSafeValue(probe, 'tags', []);
            
            const matchesSearch = this.currentFilter === '' || 
                probeName.toLowerCase().includes(this.currentFilter.toLowerCase()) ||
                probeTags.some(tag => tag && tag.toLowerCase().includes(this.currentFilter.toLowerCase()));
            
            const matchesStatus = this.currentStatusFilter === null || 
                getSafeValue(probe, 'status_id') === this.currentStatusFilter;

            return matchesSearch && matchesStatus;
        });

        filteredProbes.forEach(probe => {
            const status = this.data.statuses.find(s => s.id === probe.status_id);
            const row = document.createElement('tr');
            
            row.innerHTML = `
                <td>${probe.id ? `#${probe.id}` : '—'}</td>
                <td>
                    <strong>${this.escapeHtml(getSafeValue(probe, 'name', 'Без названия'))}</strong>
                </td>
                <td class="concentration">${safeToFixed(probe.Fe)}</td>
                <td class="concentration">${safeToFixed(probe.Ni)}</td>
                <td class="concentration">${safeToFixed(probe.Cu)}</td>
                <td>${safeToFixed(probe.sample_mass, 2)}</td>
                <td>
                    <select class="status-select" data-probe-id="${probe.id || ''}" 
                            style="background-color: ${status && status.color ? status.color : '#ccc'}; color: white; border: none; padding: 8px 15px; border-radius: 20px; cursor: pointer; font-weight: 600; min-width: 140px;">
                        ${(this.data.statuses || []).map(s => 
                            `<option value="${s.id}" ${s.id === probe.status_id ? 'selected' : ''}
                            style="background-color: ${s.color || '#ccc'}; color: white;">
                                ${this.escapeHtml(s.name || 'Неизвестно')}
                            </option>`
                        ).join('')}
                    </select>
                </td>
                <td>
                    <div class="tags-container">
                        ${(probe.tags || []).map(tag => 
                            tag ? `<span class="tag">${this.escapeHtml(tag)}</span>` : ''
                        ).join('')}
                    </div>
                </td>
                <td>
                    <button class="btn-action" onclick="lab.editProbe(${probe.id || 0})" title="Редактировать">
                        <i class="fas fa-edit"></i>
                    </button>
                    <button class="btn-action" onclick="lab.deleteProbe(${probe.id || 0})" title="Удалить">
                        <i class="fas fa-trash"></i>
                    </button>
                </td>
            `;
            
            tbody.appendChild(row);
        });

        // Добавляем обработчики для select'ов
        document.querySelectorAll('.status-select').forEach(select => {
            select.addEventListener('change', (e) => {
                const probeId = parseInt(e.target.dataset.probeId);
                const statusId = parseInt(e.target.value);
                this.updateProbeStatus(probeId, statusId);
            });
        });
    }

    renderStatusFilter() {
        const container = document.getElementById('statusFilter');
        if (!container) return;

        container.innerHTML = `
            <button class="filter-btn ${this.currentStatusFilter === null ? 'active' : ''}" 
                    onclick="lab.filterByStatus(null)">
                Все статусы
            </button>
            ${this.data.statuses.map(status => `
                <button class="filter-btn ${this.currentStatusFilter === status.id ? 'active' : ''}" 
                        onclick="lab.filterByStatus(${status.id})"
                        style="border-left: 3px solid ${status.color}">
                    ${this.escapeHtml(status.name)}
                </button>
            `).join('')}
        `;
    }

    renderStats() {
        const totalProbes = this.data.probes.length;
        const activeProbes = this.data.probes.filter(p => p.status_id === 2).length;
        const avgFe = this.data.probes.reduce((sum, p) => sum + p.fe_concentration, 0) / totalProbes || 0;
        const avgCu = this.data.probes.reduce((sum, p) => sum + p.cu_concentration, 0) / totalProbes || 0;

        document.getElementById('totalProbes').textContent = totalProbes;
        document.getElementById('activeProbes').textContent = activeProbes;
        document.getElementById('avgFe').textContent = avgFe.toFixed(1);
        document.getElementById('avgCu').textContent = avgCu.toFixed(1);
        document.getElementById('footerTotal').textContent = totalProbes;
    }

    filterByStatus(statusId) {
        this.currentStatusFilter = statusId;
        this.renderTable();
        this.renderStatusFilter();
    }

    setupEventListeners() {
        // Поиск
        const searchInput = document.getElementById('searchInput');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                this.currentFilter = e.target.value;
                this.renderTable();
            });
        }

        // Добавление статуса
        const addStatusBtn = document.getElementById('addStatusBtn');
        const statusModal = document.getElementById('statusModal');
        const cancelStatusBtn = document.getElementById('cancelStatusBtn');
        const saveStatusBtn = document.getElementById('saveStatusBtn');


        if (addStatusBtn) {
            addStatusBtn.addEventListener('click', () => {
                statusModal.style.display = 'flex';
            });
        }

        if (cancelStatusBtn) {
            cancelStatusBtn.addEventListener('click', () => {
                statusModal.style.display = 'none';
            });
        }

        if (saveStatusBtn) {
            saveStatusBtn.addEventListener('click', async () => {
                const name = document.getElementById('statusName').value;
                const color = document.getElementById('statusColor').value;

                if (!name.trim()) {
                    alert('Введите название статуса');
                    return;
                }

                try {
                    const response = await fetch('/api/add_status', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify({ name: name.trim(), color })
                    });

                    if (response.ok) {
                        await this.loadData();
                        this.renderTable();
                        this.renderStatusFilter();
                        statusModal.style.display = 'none';
                        
                        // Сброс формы
                        document.getElementById('statusName').value = '';
                    }
                } catch (error) {
                    console.error('Ошибка добавления статуса:', error);
                }
            });
        }

        // Добавление пробы
        const addProbeBtn = document.getElementById('addProbeBtn');
        const probeModal = document.getElementById('probeModal');
        const cancelProbeBtn = document.getElementById('cancelProbeBtn');
        const saveProbeBtn = document.getElementById('saveProbeBtn');

        if (addProbeBtn) {
            addProbeBtn.addEventListener('click', () => {
                probeModal.style.display = 'flex';
            });
        }

        if (cancelProbeBtn) {
            cancelProbeBtn.addEventListener('click', () => {
                probeModal.style.display = 'none';
            });
        }

        if (saveProbeBtn) {
            saveProbeBtn.addEventListener('click', async () => {
                const name = document.getElementById('probeName').value;
                const fe = document.getElementById('probeFe').value;
                const ni = document.getElementById('probeNi').value;
                const cu = document.getElementById('probeCu').value;
                const mass = document.getElementById('probeMass').value;
                const tags = document.getElementById('probeTags').value
                    .split(',')
                    .map(tag => tag.trim())
                    .filter(tag => tag.length > 0);

                if (!name.trim()) {
                    alert('Введите название пробы');
                    return;
                }

                const probeData = {
                    name: name.trim(),
                    fe_concentration: parseFloat(fe) || 0,
                    ni_concentration: parseFloat(ni) || 0,
                    cu_concentration: parseFloat(cu) || 0,
                    sample_mass: parseFloat(mass) || 1.0,
                    tags: tags
                };

                try {
                    const response = await fetch('/api/add_probe', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify(probeData)
                    });

                    if (response.ok) {
                        await this.loadData();
                        this.renderTable();
                        this.renderStats();
                        probeModal.style.display = 'none';
                        
                        // Сброс формы
                        document.getElementById('probeName').value = '';
                        document.getElementById('probeFe').value = '0.0';
                        document.getElementById('probeNi').value = '0.0';
                        document.getElementById('probeCu').value = '0.0';
                        document.getElementById('probeMass').value = '1.00';
                        document.getElementById('probeTags').value = '';
                    }
                } catch (error) {
                    console.error('Ошибка добавления пробы:', error);
                }
            });
        }

    const exportTable = document.getElementById('exportTable');
    const exportModal = document.getElementById('exportModal');    
    const cancelExportBtn = document.getElementById('cancelExportBtn');
    const downloadExportBtn = document.getElementById('downloadExportBtn');

        // Открытие модального окна
    if (exportTable) {
        exportTable.addEventListener('click', () => {
            exportModal.style.display = 'flex';
            // Сброс при открытии
            resetFileInput();
        });
    }

    // Закрытие модального окна
    if (cancelExportBtn) {
        cancelExportBtn.addEventListener('click', () => {
            exportModal.style.display = 'none';
            resetFileInput();
        });
    }

    if (downloadExportBtn) {
        downloadExportBtn.addEventListener('click', function() {
            // Просто открываем URL API в новом окне/вкладке
            window.open('/api/export/excel', '_blank');
        });
    }

    // Импорт данных ИСП АЭС
    const importTable = document.getElementById('importTable');
    const importModal = document.getElementById('importModal');
    const cancelImportBtn = document.getElementById('cancelImportBtn');
    const fileInput = document.getElementById('fileInput');
    const dropArea = document.getElementById('dropArea');
    const fileInfo = document.getElementById('fileInfo');
    const fileName = document.getElementById('fileName');
    const fileSize = document.getElementById('fileSize');
    const removeFileBtn = document.getElementById('removeFileBtn');
    const uploadSubmit = document.getElementById('uploadSubmit');

    // Открытие модального окна
    if (importTable) {
        importTable.addEventListener('click', () => {
            importModal.style.display = 'flex';
            // Сброс при открытии
            resetFileInput();
        });
    }

    // Закрытие модального окна
    if (cancelImportBtn) {
        cancelImportBtn.addEventListener('click', () => {
            importModal.style.display = 'none';
            resetFileInput();
        });
    }

    // Клик по области загрузки
    dropArea.addEventListener('click', () => fileInput.click());

    // Обработка выбора файла
    fileInput.addEventListener('change', handleFile);

    // Drag & Drop функционал
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropArea.addEventListener(eventName, preventDefaults, false);
    });

    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    ['dragenter', 'dragover'].forEach(eventName => {
        dropArea.addEventListener(eventName, highlight, false);
    });

    ['dragleave', 'drop'].forEach(eventName => {
        dropArea.addEventListener(eventName, unhighlight, false);
    });

    function highlight() {
        dropArea.classList.add('dragover');
    }

    function unhighlight() {
        dropArea.classList.remove('dragover');
    }

    // Обработка drop (только один файл)
    dropArea.addEventListener('drop', handleDrop, false);

    function handleDrop(e) {
        const dt = e.dataTransfer;
        const files = dt.files;
        
        if (files.length > 1) {
            alert('Пожалуйста, загружайте только один файл за раз');
            return;
        }
        
        if (files.length === 1) {
            fileInput.files = files;
            handleFile({ target: fileInput });
        }
    }

    // Обработка выбранного файла (только один)
    function handleFile(e) {
        const files = e.target.files;
        
        if (!files.length) {
            hideFileInfo();
            return;
        }
        
        // Проверяем, что загружен только один файл
        if (files.length > 1) {
            alert('Можно загрузить только один файл. Будет использован первый файл из списка.');
            // Оставляем только первый файл
            const dt = new DataTransfer();
            dt.items.add(files[0]);
            fileInput.files = dt.files;
        }
        
        const file = files[0];
        
        // Показываем информацию о файле
        fileName.textContent = file.name;
        fileSize.textContent = `Размер: ${formatFileSize(file.size)}`;
        fileInfo.style.display = 'block';
    }

    // Форматирование размера файла
    function formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    // Удаление файла
    if (removeFileBtn) {
        removeFileBtn.addEventListener('click', () => {
            resetFileInput();
        });
    }

    // Сброс выбора файла
    function resetFileInput() {
        fileInput.value = '';
        hideFileInfo();
    }

    // Скрытие информации о файле
    function hideFileInfo() {
        fileInfo.style.display = 'none';
        fileName.textContent = '';
        fileSize.textContent = '';
    }

    // Отправка файла на сервер (только один файл)
    if (uploadSubmit) {
        uploadSubmit.addEventListener('click', async function() {
            const files = fileInput.files;
            
            if (!files.length) {
                alert('Пожалуйста, выберите файл для загрузки!');
                return;
            }
            
            // Берем только первый файл (на всякий случай)
            const file = files[0];
            
            // Валидация файла (пример)
            if (!validateFile(file)) {
                alert('Недопустимый тип файла или слишком большой размер');
                return;
            }
            
            const formData = new FormData();
            formData.append('file', file); // Только один файл с ключом 'file'
            
            // Добавляем дополнительные данные, если нужно
            formData.append('userId', '123');
            formData.append('description', 'Загруженный файл');
            
            try {
                // Показываем индикатор загрузки
                uploadSubmit.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Загрузка...';
                uploadSubmit.disabled = true;
                
                const response = await fetch('/api/upload', {
                    method: 'POST',
                    body: formData
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    alert('Файл успешно загружен!');
                    
                    // Закрываем модальное окно после успешной загрузки
                    importModal.style.display = 'none';
                    
                    // Сброс формы
                    resetFileInput();
                    
                    // Опционально: обновить таблицу или показать сообщение
                    if (typeof refreshData === 'function') {
                        refreshData();
                    }
                } else {
                    throw new Error(result.message || 'Ошибка загрузки файла');
                }
            } catch (error) {
                console.error('Ошибка:', error);
                alert('Ошибка при загрузке файла: ' + error.message);
            } finally {
                // Восстанавливаем кнопку
                uploadSubmit.innerHTML = '<i class="fas fa-paper-plane"></i> Отправить';
                uploadSubmit.disabled = false;
            }
        });
    }

    const importData = document.getElementById('importData');
    const importDataModal = document.getElementById('importDataModal');
    const cancelDataImportBtn = document.getElementById('cancelDataImportBtn');
    const fileDataInput = document.getElementById('fileDataInput');
    const dropDataArea = document.getElementById('dropDataArea');
    const DatafileInfo = document.getElementById('DatafileInfo');
    const DatafileName = document.getElementById('DatafileName');
    const DatafileSize = document.getElementById('DatafileSize');
    const removeDataFileBtn = document.getElementById('removeDataFileBtn');
    const uploadDataSubmit = document.getElementById('uploadDataSubmit');
    // Открытие модального окна
    if (importData) {
        importData.addEventListener('click', () => {
            importDataModal.style.display = 'flex';
            // Сброс при открытии
            dataresetFileInput();
        });
    }

    // Закрытие модального окна
    if (cancelDataImportBtn) {
        cancelDataImportBtn.addEventListener('click', () => {
            importDataModal.style.display = 'none';
            dataresetFileInput();
        });
    }

    // Клик по области загрузки
    dropDataArea.addEventListener('click', () => fileDataInput.click());

    // Обработка выбора файла
    fileDataInput.addEventListener('change', datahandleFile);

    // Drag & Drop функционал
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropDataArea.addEventListener(eventName, DatapreventDefaults, false);
    });

    function DatapreventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    ['dragenter', 'dragover'].forEach(eventName => {
        dropDataArea.addEventListener(eventName, datahighlight, false);
    });

    ['dragleave', 'drop'].forEach(eventName => {
        dropDataArea.addEventListener(eventName, dataunhighlight, false);
    });

    function datahighlight() {
        dropDataArea.classList.add('dragover');
    }

    function dataunhighlight() {
        dropDataArea.classList.remove('dragover');
    }

    // Обработка drop (только один файл)
    dropDataArea.addEventListener('drop', datahandleDrop, false);

    function datahandleDrop(e) {
        const dt = e.dataTransfer;
        const files = dt.files;
        
        if (files.length > 1) {
            alert('Пожалуйста, загружайте только один файл за раз');
            return;
        }
        
        if (files.length === 1) {
            fileDataInput.files = files;
            datahandleFile({ target: fileDataInput });
        }
    }

    // Обработка выбранного файла (только один)
    function datahandleFile(e) {
        const files = e.target.files;
        
        if (!files.length) {
            hideFileInfo();
            return;
        }
        
        // Проверяем, что загружен только один файл
        if (files.length > 1) {
            alert('Можно загрузить только один файл. Будет использован первый файл из списка.');
            // Оставляем только первый файл
            const dt = new DataTransfer();
            dt.items.add(files[0]);
            fileDataInput.files = dt.files;
        }
        
        const file = files[0];
        
        // Показываем информацию о файле
        DatafileName.textContent = file.name;
        DatafileSize.textContent = `Размер: ${formatFileSize(file.size)}`;
        DatafileInfo.style.display = 'block';
    }

    // Удаление файла
    if (removeDataFileBtn) {
        removeDataFileBtn.addEventListener('click', () => {
            dataresetFileInput();
        });
    }

    // Сброс выбора файла
    function dataresetFileInput() {
        fileDataInput.value = '';
        datahideFileInfo();
    }

    // Скрытие информации о файле
    function datahideFileInfo() {
        DatafileInfo.style.display = 'none';
        DatafileName.textContent = '';
        DatafileSize.textContent = '';
    }

    // Отправка файла на сервер (только один файл)
    if (uploadDataSubmit) {
        uploadDataSubmit.addEventListener('click', async function() {
            const files = fileDataInput.files;
            
            if (!files.length) {
                alert('Пожалуйста, выберите файл для загрузки!');
                return;
            }
            
            // Берем только первый файл (на всякий случай)
            const file = files[0];
            
            // Валидация файла (пример)
            if (!validateFile(file)) {
                alert('Недопустимый тип файла или слишком большой размер');
                return;
            }
            
            const formData = new FormData();
            formData.append('file', file); // Только один файл с ключом 'file'
            
            // Добавляем дополнительные данные, если нужно
            formData.append('userId', '123');
            formData.append('description', 'Загруженный файл');
            
            try {
                // Показываем индикатор загрузки
                uploadSubmit.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Загрузка...';
                uploadSubmit.disabled = true;
                
                const response = await fetch('/api/upload_data', {
                    method: 'POST',
                    body: formData
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    alert('Файл успешно загружен!');
                    
                    // Закрываем модальное окно после успешной загрузки
                    importDataModal.style.display = 'none';
                    
                    // Сброс формы
                    resetFileInput();
                    
                    // Опционально: обновить таблицу или показать сообщение
                    if (typeof refreshData === 'function') {
                        refreshData();
                    }
                } else {
                    throw new Error(result.message || 'Ошибка загрузки файла');
                }
            } catch (error) {
                console.error('Ошибка:', error);
                alert('Ошибка при загрузке файла: ' + error.message);
            } finally {
                // Восстанавливаем кнопку
                uploadDataSubmit.innerHTML = '<i class="fas fa-paper-plane"></i> Отправить';
                uploadDataSubmit.disabled = false;
            }
        });
    }

    // Функция валидации файла (пример)
    function validateFile(file) {
        // Максимальный размер: 10MB
        const maxSize = 10 * 1024 * 1024;
        
        // Разрешенные типы файлов
        const allowedTypes = [
            'text/csv',
            'application/vnd.ms-excel',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'application/json',
            'text/plain'
        ];
        
        // Разрешенные расширения
        const allowedExtensions = ['.csv', '.xls', '.xlsx', '.json', '.txt'];
        
        // Проверка размера
        if (file.size > maxSize) {
            return false;
        }
        
        // Проверка по расширению
        const fileName = file.name.toLowerCase();
        const hasValidExtension = allowedExtensions.some(ext => fileName.endsWith(ext));
        
        // Проверка по MIME-типу (не всегда надежно)
        const hasValidType = allowedTypes.includes(file.type) || file.type === '';
        
        return hasValidExtension && hasValidType;
    }

            // Вспомогательная функция для форматирования размера файла
            function formatFileSize(bytes) {
                if (bytes === 0) return '0 Bytes';
                const k = 1024;
                const sizes = ['Bytes', 'KB', 'MB', 'GB'];
                const i = Math.floor(Math.log(bytes) / Math.log(k));
                return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
            }
            // Закрытие модальных окон по клику
            window.addEventListener('click', (e) => {
                if (e.target.id === 'statusModal') {
                    document.getElementById('statusModal').style.display = 'none';
                }
                if (e.target.id === 'probeModal') {
                    document.getElementById('probeModal').style.display = 'none';
                }
                if (e.target.id === 'importModal') {
                    document.getElementById('importModal').style.display = 'none';
                }
            });
        }

        editProbe(probeId) {
            // Здесь можно добавить логику редактирования пробы
            alert(`Редактирование пробы #${probeId}. Функция в разработке.`);
        }

        deleteProbe(probeId) {
            if (confirm('Вы уверены, что хотите удалить эту пробу?')) {
                // Запрашиваем причину удаления
                const reason = prompt('Причина удаления:', '');
                
                fetch(`/api/probes/${probeId}/delete`, {
                    method: 'DELETE',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-User-Email': 'place_holder@example.com' // Замените на реальный email
                    }
                })
                .then(response => response.json())
                .then(result => {
                    if (result.success) {
                        alert(`Проба #${probeId} удалена! Версия #${result.version_id} создана.`);
                        // Обновить таблицу
                        if (typeof this.loadData === 'function') {
                            this.loadData();
                        }
                    } else {
                        alert(`Ошибка: ${result.error}`);
                    }
                })
                .catch(error => {
                    alert('Ошибка сети: ' + error.message);
                });
            }
        }

        escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
}

// Инициализация приложения
let lab;
document.addEventListener('DOMContentLoaded', () => {
    lab = new ProbeLab();
});