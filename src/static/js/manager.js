// Глобальные переменные
let allProbes = [];
let selectedProbes = new Set();
let currentFilteredProbes = [];

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    loadProbes();
    loadStatistics();
    setupEventListeners();
});

// Настройка слушателей событий
function setupEventListeners() {
    // Переключение между методами поиска
    document.querySelectorAll('input[name="searchMethod"]').forEach(radio => {
        radio.addEventListener('change', function() {
            const nameSearch = document.getElementById('searchByName');
            const concSearch = document.getElementById('searchByConcentration');
            
            if (this.value === 'name') {
                nameSearch.style.display = 'block';
                concSearch.style.display = 'none';
            } else {
                nameSearch.style.display = 'none';
                concSearch.style.display = 'block';
            }
        });
    });
    
    // Поиск при нажатии Enter в фильтре тегов
    document.getElementById('tagFilter').addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            filterByTags();
        }
    });
}

// Загрузка всех проб
async function loadProbes() {
    showLoading(true);
    try {
        const response = await fetch('/api/probes');
        const data = await response.json();
        
        if (data.success) {
            allProbes = data.data;
            currentFilteredProbes = [...allProbes];
            renderProbesTable();
            updateSelectedInfo();
            showNotification('Пробы загружены', 'success');
        } else {
            showNotification('Ошибка загрузки проб: ' + data.error, 'error');
        }
    } catch (error) {
        showNotification('Ошибка сети: ' + error.message, 'error');
    } finally {
        showLoading(false);
    }
}

// Поиск проб по имени
async function searchProbes() {
    const substring = document.getElementById('nameSubstring').value;
    const caseSensitive = document.getElementById('caseSensitive').checked;
    
    if (!substring.trim()) {
        showNotification('Введите подстроку для поиска', 'warning');
        return;
    }
    
    showLoading(true);
    try {
        const response = await fetch('/api/probes/search', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name_substring: substring,
                case_sensitive: caseSensitive
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            currentFilteredProbes = data.data;
            renderProbesTable();
            showNotification(`Найдено проб: ${data.count}`, 'success');
        } else {
            showNotification('Ошибка поиска: ' + data.error, 'error');
        }
    } catch (error) {
        showNotification('Ошибка сети: ' + error.message, 'error');
    } finally {
        showLoading(false);
    }
}

// Поиск по концентрации
async function searchByConcentration() {
    const element = document.getElementById('concentrationElement').value;
    const min = document.getElementById('concentrationMin').value;
    const max = document.getElementById('concentrationMax').value;
    
    if (!min && !max) {
        showNotification('Введите минимум или максимум', 'warning');
        return;
    }
    
    showLoading(true);
    try {
        const response = await fetch('/api/probes/search', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                concentration_range: {
                    element: element,
                    min: min ? parseFloat(min) : null,
                    max: max ? parseFloat(max) : null
                }
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            currentFilteredProbes = data.data;
            renderProbesTable();
            showNotification(`Найдено проб: ${data.count}`, 'success');
        } else {
            showNotification('Ошибка поиска: ' + data.error, 'error');
        }
    } catch (error) {
        showNotification('Ошибка сети: ' + error.message, 'error');
    } finally {
        showLoading(false);
    }
}

// Рендеринг таблицы проб
function renderProbesTable() {
    const tbody = document.getElementById('probesTableBody');
    tbody.innerHTML = '';
    
    currentFilteredProbes.forEach(probe => {
        const row = document.createElement('tr');
        if (selectedProbes.has(probe.id)) {
            row.classList.add('selected');
        }
        
        row.innerHTML = `
            <td>
                <input type="checkbox" 
                       onchange="toggleProbeSelection(${probe.id}, this)"
                       ${selectedProbes.has(probe.id) ? 'checked' : ''}>
            </td>
            <td>${probe.id}</td>
            <td>
                <strong>${probe.name}</strong>
                <div class="small-text">${probe.Описание || ''}</div>
            </td>
            <td>${probe.Ca.toFixed(2)}</td>
            <td>${probe.Fe.toFixed(2)}</td>
            <td>${probe.Cu.toFixed(2)}</td>
            <td>
                ${probe.tags.map(tag => 
                    `<span class="tag ${getTagClass(tag)}">${tag}</span>`
                ).join('')}
            </td>
            <td>
                ${probe.is_solid ? 
                    '<span class="tag tag-solid">Твердая</span>' : ''}
                ${probe.is_solution ? 
                    '<span class="tag tag-liquid">Жидкая</span>' : ''}
            </td>
            <td>
                <button onclick="showProbeDetail(${probe.id})" 
                        class="btn-small btn-info">
                    <i class="fas fa-eye"></i>
                </button>
                <button onclick="editProbe(${probe.id})" 
                        class="btn-small btn-warning">
                    <i class="fas fa-edit"></i>
                </button>
            </td>
        `;
        
        tbody.appendChild(row);
    });
}

// Получение класса для тега
function getTagClass(tag) {
    if (tag.includes('твердая') || tag.includes('solid')) return 'tag-solid';
    if (tag.includes('жидкая') || tag.includes('liquid')) return 'tag-liquid';
    if (tag.includes('AOB')) return 'tag-aob';
    if (tag.includes('высокое')) return 'tag-high';
    if (tag.includes('низкое')) return 'tag-low';
    return '';
}

// Показать детали пробы
function showProbeDetail(probeId) {
    const probe = allProbes.find(p => p.id === probeId);
    if (!probe) return;
    
    const detailContent = document.getElementById('detailContent');
    
    let customFields = '';
    if (probe.custom_fields && Object.keys(probe.custom_fields).length > 0) {
        customFields = `
            <h4>Дополнительные поля:</h4>
            <div class="custom-fields">
                ${Object.entries(probe.custom_fields).map(([key, value]) => `
                    <div><strong>${key}:</strong> ${value}</div>
                `).join('')}
            </div>
        `;
    }
    
    detailContent.innerHTML = `
        <div class="detail-grid">
            <div><strong>ID:</strong> ${probe.id}</div>
            <div><strong>Название:</strong> ${probe.name}</div>
            <div><strong>Описание:</strong> ${probe.Описание}</div>
            <div><strong>Состояние:</strong> ${probe.is_solid ? 'Твердое' : 'Жидкое'}</div>
            <div><strong>Температура:</strong> ${probe.temperature || 'Не указана'}</div>
            <div><strong>Дата нормализации:</strong> ${new Date(probe.last_normalized).toLocaleString()}</div>
            
            <h4>Концентрации:</h4>
            <div class="concentrations">
                <div>Ca: ${probe.Ca.toFixed(2)} ± ${probe.dCa.toFixed(2)}</div>
                <div>Co: ${probe.Co.toFixed(2)} ± ${probe.dCo.toFixed(2)}</div>
                <div>Cu: ${probe.Cu.toFixed(2)} ± ${probe.dCu.toFixed(2)}</div>
                <div>Fe: ${probe.Fe.toFixed(2)} ± ${probe.dFe.toFixed(2)}</div>
                <div>Ni: ${probe.Ni.toFixed(2)} ± ${probe.dNi.toFixed(2)}</div>
            </div>
            
            ${customFields}
            
            <h4>Теги:</h4>
            <div class="tags-list">
                ${probe.tags.map(tag => 
                    `<span class="tag ${getTagClass(tag)}">${tag}</span>`
                ).join('')}
            </div>
        </div>
    `;
}

// Редактирование пробы
function editProbe(probeId) {
    // Здесь можно реализовать модальное окно редактирования
    showNotification('Функция редактирования в разработке', 'info');
}

// Управление выделением проб
function toggleProbeSelection(probeId, checkbox) {
    if (checkbox.checked) {
        selectedProbes.add(probeId);
    } else {
        selectedProbes.delete(probeId);
    }
    
    updateSelectedInfo();
    renderProbesTable();
}

function toggleSelectAll(checkbox) {
    const checkboxes = document.querySelectorAll('#probesTableBody input[type="checkbox"]');
    
    if (checkbox.checked) {
        currentFilteredProbes.forEach(probe => selectedProbes.add(probe.id));
    } else {
        currentFilteredProbes.forEach(probe => selectedProbes.delete(probe.id));
    }
    
    checkboxes.forEach(cb => cb.checked = checkbox.checked);
    updateSelectedInfo();
}

function clearSelection() {
    selectedProbes.clear();
    document.getElementById('selectAll').checked = false;
    updateSelectedInfo();
    renderProbesTable();
}

function updateSelectedInfo() {
    document.getElementById('selectedCount').textContent = selectedProbes.size;
}

// Управление тегами
async function addTagToSelected() {
    const tag = document.getElementById('newTag').value.trim();
    
    if (!tag) {
        showNotification('Введите тег', 'warning');
        return;
    }
    
    if (selectedProbes.size === 0) {
        showNotification('Выберите пробы', 'warning');
        return;
    }
    
    try {
        const response = await fetch('/api/probes/tags', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                action: 'add',
                tag: tag,
                probe_ids: Array.from(selectedProbes)
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification(data.message, 'success');
            loadProbes();
            document.getElementById('newTag').value = '';
        } else {
            showNotification('Ошибка: ' + data.error, 'error');
        }
    } catch (error) {
        showNotification('Ошибка сети: ' + error.message, 'error');
    }
}

async function removeTagFromSelected() {
    const tag = document.getElementById('newTag').value.trim();
    
    if (!tag) {
        showNotification('Введите тег', 'warning');
        return;
    }
    
    if (selectedProbes.size === 0) {
        showNotification('Выберите пробы', 'warning');
        return;
    }
    
    try {
        const response = await fetch('/api/probes/tags', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                action: 'remove',
                tag: tag,
                probe_ids: Array.from(selectedProbes)
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification(data.message, 'success');
            loadProbes();
            document.getElementById('newTag').value = '';
        } else {
            showNotification('Ошибка: ' + data.error, 'error');
        }
    } catch (error) {
        showNotification('Ошибка сети: ' + error.message, 'error');
    }
}

// Добавление тегов состояний
async function addStateTags() {
    try {
        const response = await fetch('/api/probes/state-tags', {
            method: 'POST'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification(data.message, 'success');
            loadProbes();
        } else {
            showNotification('Ошибка: ' + data.error, 'error');
        }
    } catch (error) {
        showNotification('Ошибка сети: ' + error.message, 'error');
    }
}

// Создание группы
async function createGroup() {
    const groupName = document.getElementById('groupName').value.trim();
    
    if (!groupName) {
        showNotification('Введите имя группы', 'warning');
        return;
    }
    
    if (selectedProbes.size === 0) {
        showNotification('Выберите пробы для группы', 'warning');
        return;
    }
    
    try {
        const response = await fetch('/api/probes/group', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name: groupName,
                probe_ids: Array.from(selectedProbes)
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification(data.message, 'success');
            document.getElementById('groupName').value = '';
            clearSelection();
            loadProbes();
        } else {
            showNotification('Ошибка: ' + data.error, 'error');
        }
    } catch (error) {
        showNotification('Ошибка сети: ' + error.message, 'error');
    }
}

// Фильтрация по тегам
async function filterByTags() {
    const tagFilter = document.getElementById('tagFilter').value;
    const matchAll = document.getElementById('matchAllTags').checked;
    
    if (!tagFilter.trim()) {
        currentFilteredProbes = [...allProbes];
        renderProbesTable();
        return;
    }
    
    const tags = tagFilter.split(',').map(tag => tag.trim()).filter(tag => tag);
    
    try {
        const response = await fetch('/api/probes/filter', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                tags: tags,
                match_all: matchAll
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            currentFilteredProbes = data.data;
            renderProbesTable();
            showNotification(`Найдено проб: ${data.count}`, 'success');
        } else {
            showNotification('Ошибка фильтрации: ' + data.error, 'error');
        }
    } catch (error) {
        showNotification('Ошибка сети: ' + error.message, 'error');
    }
}

// Добавление поля
function showFieldModal() {
    document.getElementById('fieldModal').style.display = 'block';
}

async function addField() {
    const fieldName = document.getElementById('fieldName').value.trim();
    const position = parseInt(document.getElementById('fieldPosition').value);
    const substring = document.getElementById('fieldSubstring').value.trim();
    const matchType = document.getElementById('fieldMatchType').value;
    const value = document.getElementById('fieldValue').value.trim();
    
    if (!fieldName || !substring || !value) {
        showNotification('Заполните все поля', 'warning');
        return;
    }
    
    try {
        const response = await fetch('/api/probes/add-field', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                field_name: fieldName,
                pattern: {
                    position: position,
                    substring: substring,
                    value: value,
                    match_type: matchType
                }
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification(data.message, 'success');
            closeModal('fieldModal');
            loadProbes();
            
            // Очистка формы
            document.getElementById('fieldName').value = '';
            document.getElementById('fieldSubstring').value = '';
            document.getElementById('fieldValue').value = '';
        } else {
            showNotification('Ошибка: ' + data.error, 'error');
        }
    } catch (error) {
        showNotification('Ошибка сети: ' + error.message, 'error');
    }
}

// Правила тегирования
function showRulesModal() {
    document.getElementById('rulesModal').style.display = 'block';
    renderRules();
}

function renderRules() {
    const container = document.getElementById('rulesContainer');
    container.innerHTML = '';
    
    // Пример правил
    const exampleRules = [
        {
            name: 'Высокое железо',
            condition: {
                type: 'concentration_range',
                element: 'Fe',
                min: 300
            },
            tag: 'высокое_Fe'
        },
        {
            name: 'Пробы AOB',
            condition: {
                type: 'name_substring',
                substring: 'AOB'
            },
            tag: 'AOB_группа'
        }
    ];
    
    exampleRules.forEach((rule, index) => {
        const ruleElement = document.createElement('div');
        ruleElement.className = 'rule-item';
        ruleElement.innerHTML = `
            <div class="rule-header">
                <strong>${rule.name}</strong>
                <button onclick="removeRule(${index})" class="btn-small btn-danger">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
            <div class="rule-body">
                <div><strong>Условие:</strong> ${JSON.stringify(rule.condition)}</div>
                <div><strong>Тег:</strong> ${rule.tag}</div>
            </div>
        `;
        container.appendChild(ruleElement);
    });
}

function addRule() {
    // Здесь можно реализовать добавление нового правила через форму
    showNotification('Функция добавления правил в разработке', 'info');
}

function removeRule(index) {
    // Здесь можно реализовать удаление правила
    showNotification('Функция удаления правил в разработке', 'info');
}

async function applyRules() {
    const rules = [
        {
            name: 'Высокое железо',
            condition: {
                type: 'concentration_range',
                element: 'Fe',
                min: 300
            },
            tag: 'высокое_Fe'
        }
    ];
    
    try {
        const response = await fetch('/api/probes/batch-tags', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ rules: rules })
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification(data.message, 'success');
            loadProbes();
        } else {
            showNotification('Ошибка: ' + data.error, 'error');
        }
    } catch (error) {
        showNotification('Ошибка сети: ' + error.message, 'error');
    }
}

// Статистика
async function loadStatistics() {
    try {
        const response = await fetch('/api/statistics');
        const data = await response.json();
        
        if (data.success) {
            updateStatsBar(data.data);
        }
    } catch (error) {
        console.error('Ошибка загрузки статистики:', error);
    }
}

function updateStatsBar(stats) {
    const statsBar = document.getElementById('statsBar');
    statsBar.innerHTML = `
        <span><i class="fas fa-vial"></i> Всего: ${stats.total_probes}</span>
        <span><i class="fas fa-cube"></i> Твердых: ${stats.solid_probes}</span>
        <span><i class="fas fa-tint"></i> Жидких: ${stats.solution_probes}</span>
        <span><i class="fas fa-tags"></i> Тегов: ${Object.keys(stats.tags_count).length}</span>
    `;
}

function showStatisticsModal() {
    document.getElementById('statisticsModal').style.display = 'block';
    
    // Здесь можно загрузить детальную статистику
    loadDetailedStatistics();
}

async function loadDetailedStatistics() {
    try {
        const response = await fetch('/api/statistics');
        const data = await response.json();
        
        if (data.success) {
            const stats = data.data;
            const content = document.getElementById('statisticsContent');
            
            let html = `
                <div class="stats-grid">
                    <div class="stat-card">
                        <h4>Общая информация</h4>
                        <div>Всего проб: ${stats.total_probes}</div>
                        <div>Твердых проб: ${stats.solid_probes}</div>
                        <div>Жидких проб: ${stats.solution_probes}</div>
                    </div>
            `;
            
            // Концентрации
            html += `<div class="stat-card">
                        <h4>Средние концентрации</h4>`;
            
            for (const [element, values] of Object.entries(stats.average_concentrations)) {
                html += `
                    <div>${element}: ${values.mean.toFixed(2)} 
                    (min: ${values.min.toFixed(2)}, max: ${values.max.toFixed(2)})</div>
                `;
            }
            
            html += `</div>`;
            
            // Теги
            if (Object.keys(stats.tags_count).length > 0) {
                html += `<div class="stat-card">
                            <h4>Теги</h4>`;
                
                for (const [tag, count] of Object.entries(stats.tags_count)) {
                    html += `<div>${tag}: ${count}</div>`;
                }
                
                html += `</div>`;
            }
            
            html += `</div>`;
            content.innerHTML = html;
        }
    } catch (error) {
        console.error('Ошибка загрузки статистики:', error);
    }
}

// Экспорт
async function exportToCSV() {
    try {
        const response = await fetch('/api/export/csv');
        
        if (response.ok) {
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `probes_export_${new Date().toISOString().slice(0, 10)}.csv`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(url);
            
            showNotification('Экспорт завершен', 'success');
        } else {
            const data = await response.json();
            showNotification('Ошибка экспорта: ' + data.error, 'error');
        }
    } catch (error) {
        showNotification('Ошибка сети: ' + error.message, 'error');
    }
}

// Вспомогательные функции
function closeModal(modalId) {
    document.getElementById(modalId).style.display = 'none';
}

function showNotification(message, type = 'info') {
    const notification = document.getElementById('notification');
    notification.textContent = message;
    notification.className = `notification show ${type}`;
    
    setTimeout(() => {
        notification.classList.remove('show');
    }, 3000);
}

function showLoading(show) {
    // Здесь можно реализовать индикатор загрузки
    if (show) {
        showNotification('Загрузка...', 'info');
    }
}

// Закрытие модальных окон при клике вне их
window.onclick = function(event) {
    const modals = document.getElementsByClassName('modal');
    for (let modal of modals) {
        if (event.target == modal) {
            modal.style.display = 'none';
        }
    }
};