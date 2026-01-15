class ProbeLab {
    constructor() {
        this.data = null;
        this.currentFilter = '';
        this.currentStatusFilter = null;
        this.currentProbeData = null;
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

    // API методы
    async updateProbeStatus(probeId, statusId) {
        try {
            const response = await fetch('/api/update_status', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ probe_id: probeId, status_id: statusId })
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
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ probe_id: probeId, priority_id: priorityId })
            });
            
            if (response.ok) {
                await this.loadData();
                this.renderTable();
            }
        } catch (error) {
            console.error('Ошибка обновления приоритета:', error);
        }
    }

    // Рендеринг таблицы
    renderTable() {
        const tbody = document.getElementById('probeTableBody');
        if (!tbody || !this.data) return;

        tbody.innerHTML = '';

        const filteredProbes = this.data.probes.filter(probe => {
            const probeName = this.getSafeValue(probe, 'name', '');
            const probeTags = this.getSafeValue(probe, 'tags', []);
            
            const matchesSearch = this.currentFilter === '' || 
                probeName.toLowerCase().includes(this.currentFilter.toLowerCase()) ||
                probeTags.some(tag => tag && tag.toLowerCase().includes(this.currentFilter.toLowerCase()));
            
            const matchesStatus = this.currentStatusFilter === null || 
                this.getSafeValue(probe, 'status_id') === this.currentStatusFilter;

            return matchesSearch && matchesStatus;
        });

        filteredProbes.forEach(probe => {
            const status = this.data.statuses.find(s => s.id === probe.status_id);
            const priority = this.data.priority?.find(p => p.id === probe.priority) || 
                            { id: probe.priority, name: 'Не указан', color: '#ccc' };
            
            const row = document.createElement('tr');
            row.innerHTML = this.createTableRowHTML(probe, status, priority);
            tbody.appendChild(row);
        });

        this.bindTableEventListeners();
    }

    createTableRowHTML(probe, status, priority) {
        return `
            <td>${probe.id ? `#${probe.id}` : '—'}</td>
            <td><strong>${this.escapeHtml(this.getSafeValue(probe, 'name', 'Без названия'))}</strong></td>
            <td class="concentration">${this.getSafeValue(probe, 'Fe', 'Нет данных')}</td>
            <td class="concentration">${this.getSafeValue(probe, 'Ni', 'Нет данных')}</td>
            <td class="concentration">${this.getSafeValue(probe, 'Cu', 'Нет данных')}</td>
            <td>
                <select class="status-select" data-probe-id="${probe.id || ''}" 
                        style="background-color: ${status?.color || '#ccc'}; color: black; border: none; padding: 8px 15px; border-radius: 20px; cursor: pointer; font-weight: 600; min-width: 140px;">
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
    }

    bindTableEventListeners() {
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

    // Рендеринг фильтров и статистики
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

    // Настройка обработчиков событий
    setupEventListeners() {
        this.setupSearch();
        this.setupModal('statusModal', 'addStatusBtn', 'cancelStatusBtn', this.saveStatus.bind(this));
        this.setupModal('probeModal', 'addProbeBtn', 'cancelProbeBtn', this.saveProbe.bind(this));
        this.setupExportModal();
        this.setupImportModal('importModal', 'importTable', 'cancelImportBtn', 'fileInput', 
                            'dropArea', 'fileInfo', 'fileName', 'fileSize', 'removeFileBtn', 
                            'uploadSubmit', '/api/upload');
        this.setupImportModal('importDataModal', 'importData', 'cancelDataImportBtn', 'fileDataInput',
                            'dropDataArea', 'DatafileInfo', 'DatafileName', 'DatafileSize', 
                            'removeDataFileBtn', 'uploadDataSubmit', '/api/upload_data');
        this.setupEditModal();
        this.setupModalCloseListeners();
        this.setupSeriesModal();
    }

    setupSearch() {
        const searchInput = document.getElementById('searchInput');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                this.currentFilter = e.target.value;
                this.renderTable();
            });
        }
    }

    setupModal(modalId, openBtnId, cancelBtnId, saveCallback) {
        const modal = document.getElementById(modalId);
        const openBtn = document.getElementById(openBtnId);
        const cancelBtn = document.getElementById(cancelBtnId);

        if (openBtn) {
            openBtn.addEventListener('click', () => modal.style.display = 'flex');
        }
        if (cancelBtn) {
            cancelBtn.addEventListener('click', () => modal.style.display = 'none');
        }
        
        if (modalId === 'statusModal') {
            const saveBtn = document.getElementById('saveStatusBtn');
            if (saveBtn) saveBtn.addEventListener('click', saveCallback);
        } else if (modalId === 'probeModal') {
            const saveBtn = document.getElementById('saveProbeBtn');
            if (saveBtn) saveBtn.addEventListener('click', saveCallback);
        }
    }

    setupExportModal() {
        const exportTable = document.getElementById('exportTable');
        const exportModal = document.getElementById('exportModal');
        const cancelExportBtn = document.getElementById('cancelExportBtn');
        const downloadExportBtn = document.getElementById('downloadExportBtn');

        if (exportTable) {
            exportTable.addEventListener('click', () => exportModal.style.display = 'flex');
        }
        if (cancelExportBtn) {
            cancelExportBtn.addEventListener('click', () => exportModal.style.display = 'none');
        }
        if (downloadExportBtn) {
            downloadExportBtn.addEventListener('click', () => {
                window.open('/api/export/excel', '_blank');
            });
        }
    }

    setupImportModal(modalId, openBtnId, cancelBtnId, fileInputId, dropAreaId, 
                    fileInfoId, fileNameId, fileSizeId, removeBtnId, submitBtnId, endpoint) {
        const modal = document.getElementById(modalId);
        const openBtn = document.getElementById(openBtnId);
        const cancelBtn = document.getElementById(cancelBtnId);
        const fileInput = document.getElementById(fileInputId);
        const dropArea = document.getElementById(dropAreaId);
        const fileInfo = document.getElementById(fileInfoId);
        const fileName = document.getElementById(fileNameId);
        const fileSize = document.getElementById(fileSizeId);
        const removeBtn = document.getElementById(removeBtnId);
        const submitBtn = document.getElementById(submitBtnId);

        if (!modal || !fileInput || !dropArea) return;

        // Открытие/закрытие модального окна
        if (openBtn) openBtn.addEventListener('click', () => {
            modal.style.display = 'flex';
            this.resetFileInput(fileInput, fileInfo, fileName, fileSize);
        });
        if (cancelBtn) cancelBtn.addEventListener('click', () => {
            modal.style.display = 'none';
            this.resetFileInput(fileInput, fileInfo, fileName, fileSize);
        });

        // Обработка файлов
        dropArea.addEventListener('click', () => fileInput.click());
        fileInput.addEventListener('change', (e) => this.handleFile(e, fileInfo, fileName, fileSize));
        
        // Drag & Drop
        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
            dropArea.addEventListener(eventName, this.preventDefaults, false);
        });
        
        ['dragenter', 'dragover'].forEach(eventName => {
            dropArea.addEventListener(eventName, () => dropArea.classList.add('dragover'), false);
        });
        
        ['dragleave', 'drop'].forEach(eventName => {
            dropArea.addEventListener(eventName, () => dropArea.classList.remove('dragover'), false);
        });
        
        dropArea.addEventListener('drop', (e) => this.handleDrop(e, fileInput, fileInfo, fileName, fileSize), false);

        // Удаление файла
        if (removeBtn) removeBtn.addEventListener('click', () => {
            this.resetFileInput(fileInput, fileInfo, fileName, fileSize);
        });

        // Отправка файла
        if (submitBtn) {
            submitBtn.addEventListener('click', async () => {
                await this.uploadFile(fileInput, submitBtn, endpoint, modal);
            });
        }
    }

    setupEditModal() {
        const editModal = document.getElementById('editModal');
        const cancelEditBtn = document.getElementById('canceleditBtn');
        
        if (cancelEditBtn) {
            cancelEditBtn.addEventListener('click', () => editModal.style.display = 'none');
        }
        
        window.addEventListener('click', (e) => {
            if (e.target.id === 'editModal') {
                editModal.style.display = 'none';
            }
        });
    }

    setupModalCloseListeners() {
        const modals = ['statusModal', 'probeModal', 'importModal'];
        modals.forEach(modalId => {
            window.addEventListener('click', (e) => {
                if (e.target.id === modalId) {
                    document.getElementById(modalId).style.display = 'none';
                }
            });
        });
    }

    // Обработка файлов
    preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    handleFile(e, fileInfo, fileName, fileSize) {
        const files = e.target.files;
        if (!files.length) {
            fileInfo.style.display = 'none';
            return;
        }
        
        // Оставляем только первый файл
        if (files.length > 1) {
            alert('Можно загрузить только один файл. Будет использован первый файл из списка.');
            const dt = new DataTransfer();
            dt.items.add(files[0]);
            e.target.files = dt.files;
        }
        
        const file = files[0];
        fileName.textContent = file.name;
        fileSize.textContent = `Размер: ${this.formatFileSize(file.size)}`;
        fileInfo.style.display = 'block';
    }

    handleDrop(e, fileInput, fileInfo, fileName, fileSize) {
        const dt = e.dataTransfer;
        const files = dt.files;
        
        if (files.length > 1) {
            alert('Пожалуйста, загружайте только один файл за раз');
            return;
        }
        
        if (files.length === 1) {
            fileInput.files = files;
            this.handleFile({ target: fileInput }, fileInfo, fileName, fileSize);
        }
    }

    resetFileInput(fileInput, fileInfo, fileName, fileSize) {
        fileInput.value = '';
        fileInfo.style.display = 'none';
        fileName.textContent = '';
        fileSize.textContent = '';
    }

    async uploadFile(fileInput, submitBtn, endpoint, modal) {
        const files = fileInput.files;
        
        if (!files.length) {
            alert('Пожалуйста, выберите файл для загрузки!');
            return;
        }
        
        const file = files[0];
        
        if (!this.validateFile(file)) {
            alert('Недопустимый тип файла или слишком большой размер');
            return;
        }
        
        const formData = new FormData();
        formData.append('file', file);
        
        try {
            submitBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Загрузка...';
            submitBtn.disabled = true;
            
            const response = await fetch(endpoint, {
                method: 'POST',
                body: formData
            });
            
            const result = await response.json();
            
            if (response.ok) {
                alert('Файл успешно загружен!');
                modal.style.display = 'none';
                this.resetFileInput(fileInput, 
                    document.getElementById(`${fileInput.id}Info`),
                    document.getElementById(`${fileInput.id}Name`),
                    document.getElementById(`${fileInput.id}Size`));
                
                // Обновляем данные
                await this.loadData();
                this.renderTable();
                this.renderStats();
            } else {
                throw new Error(result.message || 'Ошибка загрузки файла');
            }
        } catch (error) {
            console.error('Ошибка:', error);
            alert('Ошибка при загрузке файла: ' + error.message);
        } finally {
            submitBtn.innerHTML = '<i class="fas fa-paper-plane"></i> Отправить';
            submitBtn.disabled = false;
        }
    }

    // Валидация файла
    validateFile(file) {
        const maxSize = 10 * 1024 * 1024;
        const allowedExtensions = ['.csv', '.xls', '.xlsx', '.json', '.txt'];
        const allowedTypes = [
            'text/csv',
            'application/vnd.ms-excel',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'application/json',
            'text/plain'
        ];
        
        if (file.size > maxSize) return false;
        
        const fileName = file.name.toLowerCase();
        const hasValidExtension = allowedExtensions.some(ext => fileName.endsWith(ext));
        const hasValidType = allowedTypes.includes(file.type) || file.type === '';
        
        return hasValidExtension && hasValidType;
    }

    formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    // Сохранение данных
    async saveStatus() {
        const name = document.getElementById('statusName').value;
        const color = document.getElementById('statusColor').value;

        if (!name.trim()) {
            alert('Введите название статуса');
            return;
        }

        try {
            const response = await fetch('/api/add_status', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: name.trim(), color })
            });

            if (response.ok) {
                await this.loadData();
                this.renderTable();
                this.renderStatusFilter();
                document.getElementById('statusModal').style.display = 'none';
                document.getElementById('statusName').value = '';
            }
        } catch (error) {
            console.error('Ошибка добавления статуса:', error);
        }
    }

    async saveProbe() {
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
            priority: 1
        };

        try {
            const response = await fetch('/api/add_probe', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(probeData)
            });

            if (response.ok) {
                await this.loadData();
                this.renderTable();
                this.renderStats();
                document.getElementById('probeModal').style.display = 'none';
                
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
    }

    // Редактирование пробы
    async editProbe(probeId) {
        const editModal = document.getElementById('editModal');
        editModal.style.display = 'flex';
        
        const formContainer = document.getElementById('edit-probe-form-container');
        if (formContainer) formContainer.innerHTML = '';
        
        const loader = document.getElementById('edit-probe-loader');
        if (loader) loader.style.display = 'block';
        
        try {
            const response = await fetch(`/api/probes/${probeId}/upload_to_edit`, {
                method: 'GET',
                headers: { 'Content-Type': 'application/json' }
            });
            
            const result = await response.json();
            
            if (result.success) {
                this.currentProbeData = result.probe;
                this.createDynamicEditForm(result.probe, probeId);
            } else {
                alert(`Ошибка загрузки: ${result.error || result.message}`);
            }
        } catch (error) {
            console.error('Ошибка при загрузке пробы:', error);
            alert('Ошибка при загрузке данных пробы');
        } finally {
            if (loader) loader.style.display = 'none';
        }
    }

    createDynamicEditForm(probe, probeId) {
        const formContainer = document.getElementById('edit-probe-form-container');
        if (!formContainer) return;
        
        const form = document.createElement('form');
        form.id = 'edit-probe-form';
        
        // Скрытое поле с ID
        const idInput = document.createElement('input');
        idInput.type = 'hidden';
        idInput.id = 'edit-probe-id';
        idInput.name = 'id';
        idInput.value = probeId;
        form.appendChild(idInput);
        
        // Категории полей
        const fieldCategories = {
            'Основная информация': ['id', 'name', 'status_id', 'priority', 'last_normalized'],
            'Характеристики пробы': ['is_solid', 'is_solution', 'sample_mass', 'V (ml)', 'Масса навески (g)', 'Разбавление','pH','Eh','Плотность',
                'Масса твердого (g)','Масса Ca(OH)2 (g)','Масса CaCO3 (g)','Плотность','Объем р-ра H2SO4 (ml)','Масса железных окатышей (g)'],
            'Концентрации элементов': ['Ca', 'Fe', 'Ni', 'Cu', 'Co'],
            'Погрешности': ['dCa', 'dFe', 'dNi', 'dCu', 'dCo'],
            'Дополнительная информация': ['Кто готовил', 'Среда', 'Аналиты', 'описание', 'tags'],
        };
        
        const fieldTypes = {
            'checkbox': ['is_solid', 'is_solution'],
            'number': ['Ca', 'Fe', 'Ni', 'Cu', 'Co', 'dCa', 'dFe', 'dNi', 'dCu', 'dCo', 
                      'sample_mass', 'priority', 'Разбавление', 'Масса навески (g)','pH','Eh','Плотность',
                      'Масса твердого (g)','Масса Ca(OH)2 (g)','Масса CaCO3 (g)','Объем р-ра H2SO4 (ml)','Масса железных окатышей (g)'],
            'textarea': ['описание'],
            'select': ['status_id'],
        };
        
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
            'Масса навески (g)': 'Масса навески (g)',
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
        
        // Создаем поля по категориям
        Object.entries(fieldCategories).forEach(([category, fields]) => {
            const categorySection = document.createElement('div');
            categorySection.className = 'form-category';
            
            const categoryTitle = document.createElement('h3');
            categoryTitle.textContent = category;
            categorySection.appendChild(categoryTitle);
            
            fields.forEach(fieldName => {
                if (probe[fieldName] !== undefined) {
                    const fieldDiv = this.createFormField(fieldName, probe[fieldName], fieldTypes, fieldLabels);
                    if (fieldDiv) categorySection.appendChild(fieldDiv);
                }
            });
            
            form.appendChild(categorySection);
        });
        
        // Кнопки формы
        const buttonsDiv = document.createElement('div');
        buttonsDiv.className = 'form-buttons';
        
        const saveButton = document.createElement('button');
        saveButton.type = 'button';
        saveButton.className = 'btn btn-primary';
        saveButton.textContent = 'Сохранить изменения';
        saveButton.onclick = () => this.saveEditedProbe();
        
        const cancelButton = document.createElement('button');
        cancelButton.type = 'button';
        cancelButton.className = 'btn btn-secondary';
        cancelButton.textContent = 'Отмена';
        cancelButton.onclick = () => document.getElementById('editModal').style.display = 'none';
        
        buttonsDiv.appendChild(saveButton);
        buttonsDiv.appendChild(cancelButton);
        form.appendChild(buttonsDiv);
        
        formContainer.appendChild(form);
    }

    createFormField(fieldName, fieldValue, fieldTypes, fieldLabels) {
        const fieldDiv = document.createElement('div');
        fieldDiv.className = 'form-group';
        
        const label = document.createElement('label');
        label.htmlFor = `edit-${fieldName}`;
        label.textContent = fieldLabels[fieldName] || fieldName;
        fieldDiv.appendChild(label);
        
        let inputElement;
        
        if (fieldTypes.checkbox && fieldTypes.checkbox.includes(fieldName)) {
            const container = document.createElement('div');
            container.className = 'checkbox-container';
            
            inputElement = document.createElement('input');
            inputElement.type = 'checkbox';
            inputElement.id = `edit-${fieldName}`;
            inputElement.name = fieldName;
            inputElement.checked = Boolean(fieldValue);
            inputElement.className = 'custom-checkbox';
            
            const checkboxLabel = document.createElement('label');
            checkboxLabel.htmlFor = `edit-${fieldName}`;
            checkboxLabel.textContent = fieldName;
            checkboxLabel.className = 'checkbox-label';
            
            container.appendChild(inputElement);
            container.appendChild(checkboxLabel);
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
            inputElement = document.createElement('input');
            inputElement.type = 'text';
            inputElement.id = `edit-${fieldName}`;
            inputElement.name = fieldName;
            inputElement.value = fieldValue !== null && fieldValue !== undefined ? fieldValue : '';
        }
        
        inputElement.className = 'form-control';
        fieldDiv.appendChild(inputElement);
        
        return fieldDiv;
    }

    async saveEditedProbe() {
        const probeId = document.getElementById('edit-probe-id').value;
        
        if (!probeId || !this.currentProbeData) {
            alert('Данные пробы не загружены');
            return;
        }
        
        const form = document.getElementById('edit-probe-form');
        const formData = new FormData(form);
        const updateData = {};
        
        for (let [key, value] of formData.entries()) {
            if (value === '' && this.currentProbeData[key] !== undefined) continue;
            
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
        
        updateData.id = parseInt(probeId);
        updateData.last_normalized = new Date().toISOString();
        
        const saveButton = document.querySelector('#edit-probe-form .btn-primary');
        const originalText = saveButton.textContent;
        saveButton.textContent = 'Сохранение...';
        saveButton.disabled = true;
        
        try {
            const response = await fetch(`/api/probes/${probeId}/update_probe`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(updateData)
            });
            
            const result = await response.json();
            
            if (result.success) {
                alert('Изменения успешно сохранены!');
                document.getElementById('editModal').style.display = 'none';
                
                // Обновляем данные
                await this.loadData();
                this.renderTable();
                this.renderStats();
            } else {
                alert(`Ошибка сохранения: ${result.error || result.message}`);
            }
        } catch (error) {
            console.error('Ошибка при сохранении:', error);
            alert('Ошибка при сохранении данных');
        } finally {
            saveButton.textContent = originalText;
            saveButton.disabled = false;
        }
    }

    // Удаление пробы
    deleteProbe(probeId) {
        if (confirm('Вы уверены, что хотите удалить эту пробу?')) {
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
                    this.loadData();
                    this.renderTable();
                    this.renderStats();
                } else {
                    alert(`Ошибка: ${result.error}`);
                }
            })
            .catch(error => alert('Ошибка сети: ' + error.message));
        }
    }

    // Вспомогательные методы
    getSafeValue(obj, property, defaultValue = '—') {
        return obj && obj[property] !== undefined ? obj[property] : defaultValue;
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    setupSeriesModal() {
        const seriesModal = document.getElementById('seriesModal');
        const addSeriesBtn = document.getElementById('addSeriesBtn');
        const cancelSeriesBtn = document.getElementById('cancelSeriesBtn');
        const saveSeriesBtn = document.getElementById('saveSeriesBtn');
        const seriesBaseName = document.getElementById('seriesBaseName');
        
        if (addSeriesBtn) {
            addSeriesBtn.addEventListener('click', () => {
                seriesModal.style.display = 'flex';
                this.updateSeriesPreview();
            });
        }
        
        if (cancelSeriesBtn) {
            cancelSeriesBtn.addEventListener('click', () => {
                seriesModal.style.display = 'none';
                this.clearSeriesForm();
            });
        }
        
        if (saveSeriesBtn) {
            saveSeriesBtn.addEventListener('click', async () => {
                await this.createSeries();
            });
        }
        
        if (seriesBaseName) {
            seriesBaseName.addEventListener('input', () => {
                this.updateSeriesPreview();
            });
        }
        
        // Закрытие по клику вне окна
        window.addEventListener('click', (e) => {
            if (e.target.id === 'seriesModal') {
                seriesModal.style.display = 'none';
                this.clearSeriesForm();
            }
        });
    }

    parseProbeName(baseName) {
        // Парсим название пробы типа "T2-4C1"
        const pattern = /^T2-(\d+)C(\d+)$/;
        const match = baseName.match(pattern);
        
        if (!match) {
            throw new Error('Неверный формат названия. Используйте: T2-{номер}C{повторность}');
        }
        
        return {
            fullName: baseName,
            methodNumber: match[1], // номер методики
            repeatNumber: match[2],  // номер повторности
            isValid: true
        };
    }

    generateSeriesProbes(baseName, methodNumber, repeatNumber, mass, volume) {
        // Генерируем 16 проб по заданному шаблону
        const templates = [
            'T2-{m}A{r}',
            'T2-{m}B{r}',
            'T2-L{m}C{r}',
            'T2-L{m}A{r}',
            'T2-L{m}B{r}',
            'T2-L{m}P{m}C{r}',
            'T2-L{m}P{m}A{r}',
            'T2-L{m}P{m}B{r}',
            'T2-L{m}P{m}F{m}C{r}',
            'T2-L{m}P{m}F{m}A{r}',
            'T2-L{m}P{m}F{m}B{r}',
            'T2-L{m}P{m}F{m}D{r}',
            'T2-L{m}P{m}F{m}N{m}C{r}',
            'T2-L{m}P{m}F{m}N{m}A{r}',
            'T2-L{m}P{m}F{m}N{m}B{r}',
            'T2-L{m}P{m}F{m}N{m}E{r}'
        ];
        
        return templates.map(template => {
            const name = template
                .replace(/{m}/g, methodNumber)
                .replace(/{r}/g, repeatNumber);
            
            return {
                name: name,
                sample_mass: parseFloat(mass) || 1.0,
                V_ml: parseFloat(volume) || 100.0,
                method_number: methodNumber,
                is_series: true,
                series_base: baseName,
                tags: [`методика_${methodNumber}`, `серия_${baseName}`],
                status_id: 1,
                priority: 1,
                Fe: 0,
                Ni: 0,
                Cu: 0,
                created_at: new Date().toISOString()
            };
        });
    }

    updateSeriesPreview() {
        const baseName = document.getElementById('seriesBaseName').value;
        const previewContainer = document.getElementById('seriesPreview');
        
        if (!previewContainer) return;
        
        if (!baseName.trim()) {
            previewContainer.innerHTML = '<div class="series-preview-item">Введите название основной пробы</div>';
            return;
        }
        
        try {
            const parsed = this.parseProbeName(baseName.trim());
            const mass = document.getElementById('seriesMass').value;
            const volume = document.getElementById('seriesVolume').value;
            
            const probes = this.generateSeriesProbes(
                parsed.fullName,
                parsed.methodNumber,
                parsed.repeatNumber,
                mass,
                volume
            );
            
            previewContainer.innerHTML = probes.map(probe => 
                `<div class="series-preview-item">${probe.name}</div>`
            ).join('');
            
        } catch (error) {
            previewContainer.innerHTML = `<div class="series-preview-item" style="color: #ff4444;">${error.message}</div>`;
        }
    }

    clearSeriesForm() {
        document.getElementById('seriesBaseName').value = '';
        document.getElementById('seriesMass').value = '1.00';
        document.getElementById('seriesVolume').value = '100.0';
        document.getElementById('seriesPreview').innerHTML = '';
    }

    async createSeries() {
        const baseName = document.getElementById('seriesBaseName').value.trim();
        const mass = document.getElementById('seriesMass').value;
        const volume = document.getElementById('seriesVolume').value;
        
        if (!baseName) {
            alert('Введите название основной пробы');
            return;
        }
        
        try {
            const parsed = this.parseProbeName(baseName);
            const probes = this.generateSeriesProbes(
                parsed.fullName,
                parsed.methodNumber,
                parsed.repeatNumber,
                mass,
                volume
            );
            
            // Отправляем запрос на сервер
            const response = await fetch('/api/add_series', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    base_name: parsed.fullName,
                    method_number: parsed.methodNumber,
                    repeat_number: parsed.repeatNumber,
                    mass: parseFloat(mass),
                    volume: parseFloat(volume),
                    probes: probes
                })
            });
            
            const result = await response.json();
            
            if (result.success) {
                alert(`Создано ${result.created_count} проб в серии "${parsed.fullName}"`);
                document.getElementById('seriesModal').style.display = 'none';
                this.clearSeriesForm();
                
                // Обновляем данные
                await this.loadData();
                this.renderTable();
                this.renderStats();
            } else {
                throw new Error(result.error || 'Ошибка при создании серии');
            }
            
        } catch (error) {
            console.error('Ошибка создания серии:', error);
            alert('Ошибка: ' + error.message);
        }
    }
    
}

// Инициализация приложения
let lab;
document.addEventListener('DOMContentLoaded', () => {
    lab = new ProbeLab();
});