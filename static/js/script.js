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

    async updateProbePriority(probeId, priorityId) {
        try {
            const response = await fetch('/api/update_priority', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    probe_id: probeId,
                    priority_id: priorityId
                })
            });
            
            if (response.ok) {
                await this.loadData();
                this.renderTable();
            }
        } catch (error) {
            console.error('Ошибка обновления приоритета:', error);
        }
    }

    renderTable() {
        const tbody = document.getElementById('probeTableBody');
        if (!tbody || !this.data) return;

        tbody.innerHTML = '';

        // Проверяем структуру данных
        console.log('Данные загружены:', this.data);
        console.log('Пробы:', this.data.probes);


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
            const priority = this.data.priority?.find(p => p.id === probe.priority) || 
                            { id: probe.priority, name: 'Не указан', color: '#ccc' };
            const row = document.createElement('tr');
            
            row.innerHTML = `
                <td>${probe.id ? `#${probe.id}` : '—'}</td>
                <td>
                    <strong>${this.escapeHtml(getSafeValue(probe, 'name', 'Без названия'))}</strong>
                </td>
                <td class="concentration">${getSafeValue(probe, 'Fe', 'Нет данных')}</td>
                <td class="concentration">${getSafeValue(probe, 'Ni', 'Нет данных')}</td>
                <td class="concentration">${getSafeValue(probe, 'Cu', 'Нет данных')}</td>
                <td>
                    <select class="status-select" data-probe-id="${probe.id || ''}" 
                            style="background-color: ${status && status.color ? status.color : '#ccc'}; color: black; border: none; padding: 8px 15px; border-radius: 20px; cursor: pointer; font-weight: 600; min-width: 140px;">
                        ${(this.data.statuses || []).map(s => 
                            `<option value="${s.id}" ${s.id === probe.status_id ? 'selected' : ''}
                            style="background-color: ${s.color || '#ccc'}; color: black;">
                                ${this.escapeHtml(s.name || 'Неизвестно')}
                            </option>`
                        ).join('')}
                    </select>
                </td>
                <td>
                    <select class="priority-select" data-probe-id="${probe.id || ''}" 
                            style="background-color: ${priority.color}; color: black; border: none; padding: 8px 15px; border-radius: 20px; cursor: pointer; font-weight: 600; min-width: 140px;">
                        ${(this.data.priority || []).map(p => 
                            `<option value="${p.id}" ${p.id === probe.priority ? 'selected' : ''}
                            style="background-color: ${p.color}; color: black;">
                                ${this.escapeHtml(p.name)}
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
        document.querySelectorAll('.priority-select').forEach(select => {
            select.addEventListener('change', (e) => {
                const probeId = parseInt(e.target.dataset.probeId);
                const priorityId = parseInt(e.target.value);
                this.updateProbePriority(probeId, priorityId);
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

        document.getElementById('totalProbes').textContent = totalProbes;
        document.getElementById('activeProbes').textContent = activeProbes;
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
                    Fe: parseFloat(fe) || 0,
                    Ni: parseFloat(ni) || 0,
                    Cu: parseFloat(cu) || 0,
                    sample_mass: parseFloat(mass) || 1.0,
                    tags: tags,
                    priority: 1 // По умолчанию делаем средний приоритет
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

            const canceleditBtn = document.getElementById('canceleditBtn');
            // Закрытие модального окна
            if (canceleditBtn) {
                canceleditBtn.addEventListener('click', () => {
                    editModal.style.display = 'none';

            });
                }
            window.addEventListener('click', (e) => {
                if (e.target.id === 'editModal') {
                    document.getElementById('editModal').style.display = 'none';
                }
            });

        }

    editProbe(probeId) {
        editModal.style.display = 'flex';
        // Функция создания одного поля формы
        function createFormField(fieldName, fieldValue, fieldTypes, fieldLabels) {
            const fieldDiv = document.createElement('div');
            fieldDiv.className = 'form-group';
            
            const label = document.createElement('label');
            label.htmlFor = `edit-${fieldName}`;
            label.textContent = fieldLabels[fieldName] || fieldName;
            fieldDiv.appendChild(label);
            
            let inputElement;
            
            // Определяем тип поля
            if (fieldTypes.checkbox && fieldTypes.checkbox.includes(fieldName)) {

                // Создаем контейнер для чекбокса с меткой
                const container = document.createElement('div');
                container.className = 'checkbox-container';

                inputElement = document.createElement('input');
                inputElement.type = 'checkbox';
                inputElement.id = `edit-${fieldName}`;
                inputElement.name = fieldName;
                inputElement.checked = Boolean(fieldValue);

                // Добавляем класс для стилизации
                inputElement.className = 'custom-checkbox';
                
                // Создаем метку для чекбокса (лучше UX)
                const label = document.createElement('label');
                label.htmlFor = `edit-${fieldName}`;
                label.textContent = fieldName; // или другой текст
                label.className = 'checkbox-label';
                
                // Добавляем обработчик для отслеживания изменений
                inputElement.addEventListener('change', function(e) {
                    console.log(`Checkbox ${fieldName} changed:`, this.checked);
                    // Здесь можно добавить логику обновления данных
                });
                
                // Собираем все элементы
                container.appendChild(inputElement);
                container.appendChild(label);
                
                inputElement = container;                
            } 
            else if (fieldName === 'status_id') {
                inputElement = document.createElement('select');
                inputElement.id = `edit-${fieldName}`;
                inputElement.name = fieldName;
                
                const statuses = [
                    {id: 1, name: 'Сульфидизация'},
                    {id: 2, name: 'Флотация'},
                    {id: 3, name: 'Фильтрование'},
                    {id: 4, name: 'Сушильный шкаф'},
                    {id: 5, name: 'Разложение'},
                    {id: 6, name: 'Анализ'},
                    {id: 7, name: 'Обработка данных'},
                    {id: 8, name: 'Выполнено'},                                        
                ];
                
                statuses.forEach(status => {
                    const option = document.createElement('option');
                    option.value = status.id;
                    option.textContent = status.name;
                    option.selected = status.id === fieldValue;
                    inputElement.appendChild(option);
                });
            }
            else if (fieldTypes.textarea && fieldTypes.textarea.includes(fieldName)) {
                inputElement = document.createElement('textarea');
                inputElement.id = `edit-${fieldName}`;
                inputElement.name = fieldName;
                inputElement.rows = 3;
                inputElement.value = fieldValue || '';
            }
            else if (fieldTypes.number && fieldTypes.number.includes(fieldName)) {
                inputElement = document.createElement('input');
                inputElement.type = 'number';
                inputElement.id = `edit-${fieldName}`;
                inputElement.name = fieldName;
                inputElement.step = '0.001';
                inputElement.value = fieldValue !== null && fieldValue !== undefined ? fieldValue : '';
            }
            else if (Array.isArray(fieldValue)) {
                inputElement = document.createElement('input');
                inputElement.type = 'text';
                inputElement.id = `edit-${fieldName}`;
                inputElement.name = fieldName;
                inputElement.value = fieldValue.join(', ');
                inputElement.dataset.originalType = 'array';
            }
            else {
                // Текстовое поле по умолчанию
                inputElement = document.createElement('input');
                inputElement.type = 'text';
                inputElement.id = `edit-${fieldName}`;
                inputElement.name = fieldName;
                inputElement.value = fieldValue !== null && fieldValue !== undefined ? fieldValue : '';
            }
            
            inputElement.className = 'form-control';
            fieldDiv.appendChild(inputElement);
            
            // Добавляем подсказку с типом данных
            const typeHint = document.createElement('small');
            typeHint.className = 'form-text text-muted';
            typeHint.textContent = `Тип: ${typeof fieldValue}`;
            fieldDiv.appendChild(typeHint);
            
            return fieldDiv;
        }
        // Функция создания динамической формы
        function createDynamicEditForm(probe, probeId) {
            const formContainer = document.getElementById('edit-probe-form-container');
            if (!formContainer) return;
            
            // Создаем форму
            const form = document.createElement('form');
            form.id = 'edit-probe-form';
            
            // Добавляем скрытое поле с ID пробы
            const idInput = document.createElement('input');
            idInput.type = 'hidden';
            idInput.id = 'edit-probe-id';
            idInput.name = 'id';
            idInput.value = probeId;
            form.appendChild(idInput);
            
            // Группируем поля по категориям
            const fieldCategories = {
                'Основная информация': ['id', 'name', 'status_id', 'priority', 'last_normalized'],
                'Характеристики пробы': ['is_solid', 'is_solution', 'sample_mass', 'V (ml)', 'Масса навески (mg)', 'Разбавление'],
                'Концентрации элементов': ['Ca', 'Fe', 'Ni', 'Cu', 'Co'],
                'Погрешности': ['dCa', 'dFe', 'dNi', 'dCu', 'dCo'],
                'Дополнительная информация': ['Кто готовил', 'Среда', 'Аналиты', 'описание', 'tags'],
            };
            
            // Определяем типы полей для разных значений
            const fieldTypes = {
                'checkbox': ['is_solid', 'is_solution'],
                'number': ['Ca', 'Fe', 'Ni', 'Cu', 'Co', 'dCa', 'dFe', 'dNi', 'dCu', 'dCo', 
                        'sample_mass', 'priority', 'Разбавление', 'Масса навески (mg)'],
                'textarea': ['описание'],
                'select': ['status_id'],
            };
            
            // Определяем человекочитаемые названия полей
            const fieldLabels = {
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
                'описание': 'Описание',
                'tags': 'Теги',
            };
            
            // Обрабатываем каждую категорию
            Object.entries(fieldCategories).forEach(([category, fields]) => {
                // Создаем секцию категории
                const categorySection = document.createElement('div');
                categorySection.className = 'form-category';
                
                const categoryTitle = document.createElement('h3');
                categoryTitle.textContent = category;
                categorySection.appendChild(categoryTitle);
                
                // Добавляем поля категории
                fields.forEach(fieldName => {
                    if (probe[fieldName] !== undefined) {
                        const fieldDiv = createFormField(fieldName, probe[fieldName], fieldTypes, fieldLabels);
                        if (fieldDiv) {
                            categorySection.appendChild(fieldDiv);
                        }
                    }
                });
                
                // Добавляем остальные поля, которые не вошли в категории
                if (category === 'Дополнительная информация') {
                    getEditableFields(probe).forEach(key => {
                        // Игнорируем системные поля и поля, которые уже добавлены
                        const allCategorizedFields = Object.values(fieldCategories).flat();
                        if (!allCategorizedFields.includes(key)) {
                            const fieldDiv = createFormField(key, probe[key], fieldTypes, fieldLabels);
                            if (fieldDiv) {
                                categorySection.appendChild(fieldDiv);
                            }
                        }
                    });
                }
                
                form.appendChild(categorySection);
            });
            
            // Добавляем кнопки сохранения
            const buttonsDiv = document.createElement('div');
            buttonsDiv.className = 'form-buttons';
            
            const saveButton = document.createElement('button');
            saveButton.type = 'button';
            saveButton.className = 'btn btn-primary';
            saveButton.textContent = 'Сохранить изменения';
            saveButton.onclick = saveEditedProbe;
            
            const cancelButton = document.createElement('button');
            cancelButton.type = 'button';
            cancelButton.className = 'btn btn-secondary';
            cancelButton.textContent = 'Отмена';
            cancelButton.onclick = closeEditModal;
            
            buttonsDiv.appendChild(saveButton);
            buttonsDiv.appendChild(cancelButton);
            form.appendChild(buttonsDiv);
            
            formContainer.appendChild(form);
        }        
        // Очищаем контейнер формы
        const formContainer = document.getElementById('edit-probe-form-container');
        if (formContainer) {
            formContainer.innerHTML = '';
        }
        
        // Показываем индикатор загрузки
        const loader = document.getElementById('edit-probe-loader');
        if (loader) loader.style.display = 'block';
        
        fetch(`/api/probes/${probeId}/upload_to_edit`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
            }
        })
        .then(response => response.json())
        .then(result => {
            if (result.success) {

                // Сохраняем данные пробы
                currentProbeData = result.probe;
                
                // Динамически создаем форму
                createDynamicEditForm(result.probe, probeId);
                
                console.log(`Проба #${probeId} загружена для редактирования`);
            } else {
                alert(`Ошибка загрузки: ${result.error || result.message}`);
            }
        })
        .catch(error => {
            console.error('Ошибка при загрузке пробы:', error);
            alert('Ошибка при загрузке данных пробы');
        })
        .finally(() => {
            if (loader) loader.style.display = 'none';
        });
    }

        deleteProbe(probeId) {
            if (confirm('Вы уверены, что хотите удалить эту пробу?')) {
                // Запрашиваем причину удаления
                const reason = prompt('Причина удаления:', '');
                
                fetch(`/api/probes/${probeId}/delete`, {
                    method: 'DELETE',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-User-Email': 'place_holder@example.com'
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

// Функция сохранения изменений (динамическая)
function saveEditedProbe() {
    const probeId = document.getElementById('edit-probe-id').value;
    
    if (!probeId || !currentProbeData) {
        alert('Данные пробы не загружены');
        return;
    }
    
    // Собираем данные из динамической формы
    const form = document.getElementById('edit-probe-form');
    const formData = new FormData(form);
    const updateData = {};
    
    // Конвертируем FormData в объект
    for (let [key, value] of formData.entries()) {
        // Пропускаем пустые поля, если они не были изменены
        if (value === '' && currentProbeData[key] !== undefined) {
            continue;
        }
        
        // Обрабатываем специальные типы данных
        const inputElement = document.querySelector(`[name="${key}"]`);
        
        if (inputElement.type === 'checkbox') {
            updateData[key] = inputElement.checked;
        } 
        else if (inputElement.type === 'number') {
            updateData[key] = value !== '' ? parseFloat(value) : null;
        }
        else if (inputElement.dataset.originalType === 'array') {
            updateData[key] = value.split(',').map(item => item.trim()).filter(item => item);
        }
        else if (key === 'status_id' || key === 'priority' || key === 'Разбавление') {
            updateData[key] = value !== '' ? parseInt(value) : null;
        }
        else {
            updateData[key] = value;
        }
    }
    
    // Добавляем обязательные поля, если они не были в форме
    updateData.id = parseInt(probeId);
    updateData.last_normalized = new Date().toISOString();
    
    // Показываем индикатор сохранения
    const saveButton = document.querySelector('#edit-probe-form .btn-primary');
    const originalText = saveButton.textContent;
    saveButton.textContent = 'Сохранение...';
    saveButton.disabled = true;
    
    // Отправляем запрос на сохранение
    fetch(`/api/probes/${probeId}/update_probe`, {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(updateData)
    })
    .then(response => response.json())
    .then(result => {
        if (result.success) {
            alert('Изменения успешно сохранены!');
            closeEditModal();
            
            // Обновляем данные на странице
            if (window.refreshProbesList) {
                window.refreshProbesList();
            }
            
            console.log(`Проба #${probeId} обновлена`, result.probe);
        } else {
            alert(`Ошибка сохранения: ${result.error || result.message}`);
        }
    })
    .catch(error => {
        console.error('Ошибка при сохранении:', error);
        alert('Ошибка при сохранении данных');
    })
    .finally(() => {
        if (saveButton) {
            saveButton.textContent = originalText;
            saveButton.disabled = false;
        }
    });
}

// Функция закрытия модального окна
function closeEditModal() {
    const modal = document.getElementById('editModal');
    if (modal) {
        modal.style.display = 'none';
    }
}

function getEditableFields(probe) {
    // Поля, которые нельзя редактировать
    const readOnlyFields = ['id', 'last_normalized', 'created_at', 'updated_at'];
    
    // Поля, которые требуют специальной обработки
    const specialFields = ['status_id', 'tags'];
    
    return Object.keys(probe).filter(field => 
        !readOnlyFields.includes(field)
    );
}

// Инициализация приложения
let currentProbeData = null; // Храним текущие данные пробы
let lab;
document.addEventListener('DOMContentLoaded', () => {
    lab = new ProbeLab();
});