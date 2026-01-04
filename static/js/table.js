class DataTableViewer {
    constructor() {
        this.data = null;
        this.filteredData = null;
        this.columnBlacklist = ['описание','last_normalized','is_solution','priority','is_solid','V (ml)','tags','status_id'];
        
        this.elements = {
            loading: document.getElementById('loading'),
            error: document.getElementById('error'),
            table: document.getElementById('dataTable'),
            tableHead: document.getElementById('tableHead'),
            tableBody: document.getElementById('tableBody'),
            searchInput: document.getElementById('searchInput'),
            totalRows: document.getElementById('totalRows'),
            visibleRows: document.getElementById('visibleRows')
        };
        
        this.init();
    }
    
    init() {
        this.loadData();
        this.elements.searchInput.addEventListener('input', (e) => {
            this.handleSearch(e.target.value);
        });
    }
    
    async loadData() {
        try {
            this.showLoading(true);
            const response = await fetch('/api/data');
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            this.data = await response.json();
            this.filteredData = this.data;
            
            this.renderTable();
            this.showLoading(false);
            this.elements.table.style.display = 'table';
            
            
        } catch (error) {
            console.error('Ошибка загрузки данных:', error);
            this.showError(true);
            this.showLoading(false);
        }
    }
    
    getColumns() {
        if (!this.data || !this.data.probes || this.data.probes.length === 0) {
            return [];
        }
        
        // Извлекаем все уникальные колонки из всех записей
        const allColumns = new Set();
        this.data.probes.forEach(probe => {
            Object.keys(probe).forEach(key => {
                if (!this.columnBlacklist.includes(key)) {
                    allColumns.add(key);
                }
            });
        });
        
        // Сортируем колонки: id первый, name второй, остальные в алфавитном порядке
        return Array.from(allColumns).sort((a, b) => {
            // id всегда первый
            if (a.toLowerCase() === 'id') return -1;
            if (b.toLowerCase() === 'id') return 1;
            
            // name всегда второй
            if (a.toLowerCase() === 'name') return -1;
            if (b.toLowerCase() === 'name') return 1;
            
            // Остальные в алфавитном порядке (без учета регистра)
            return a.toLowerCase().localeCompare(b.toLowerCase());
        });
    }
    
    renderTable() {
        if (!this.filteredData || !this.filteredData.probes) {
            return;
        }
        
        const columns = this.getColumns();
        
        // Render header - названия как в базе данных
        this.elements.tableHead.innerHTML = `
            <tr>
                ${columns.map(col => {
                    return `<th style="width: 150px; min-width: 150px; max-width: 150px;">${col}</th>`;
                }).join('')}
            </tr>
        `;
        
        // Render body
        if (this.filteredData.probes.length === 0) {
            this.elements.tableHead.innerHTML = `
                <tr>
                    ${columns.map(col => `<th>${col}</th>`).join('')}
                </tr>
            `;
        } else {
            this.elements.tableBody.innerHTML = this.filteredData.probes.map(probe => `
                <tr>
                    ${columns.map(col => {
                        const value = probe[col] ?? '';
                        return `<td title="${this.escapeHtml(String(value))}">
                                    ${this.escapeHtml(String(value))}
                                </td>`;
                    }).join('')}
                </tr>
            `).join('');
        }

        this.updateStats();
    }
    
    handleSearch(searchTerm) {
        if (!this.data || !this.data.probes) {
            return;
        }
        
        searchTerm = searchTerm.toLowerCase().trim();
        
        if (!searchTerm) {
            this.filteredData = this.data;
        } else {
            this.filteredData = {
                probes: this.data.probes.filter(probe => {
                    // Поиск по всем значениям в объекте
                    return Object.values(probe).some(value => {
                        return String(value).toLowerCase().includes(searchTerm);
                    });
                })
            };
        }
        
        this.renderTable();
    }
    
    updateStats() {
        const total = this.data ? (this.data.probes ? this.data.probes.length : 0) : 0;
        const visible = this.filteredData ? (this.filteredData.probes ? this.filteredData.probes.length : 0) : 0;
        
        this.elements.totalRows.textContent = `Всего записей: ${total}`;
        this.elements.visibleRows.textContent = `Отображено: ${visible}`;
    }
    
    showLoading(show) {
        this.elements.loading.style.display = show ? 'flex' : 'none';
    }
    
    showError(show) {
        this.elements.error.style.display = show ? 'flex' : 'none';
    }
    
    escapeHtml(text) {
        const map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        };
        return text.replace(/[&<>"']/g, m => map[m]);
    }
}


document.addEventListener('DOMContentLoaded', () => {
    const top = document.querySelector('.scroll-top');
    const topInner = document.querySelector('.scroll-top-inner');
    const bottom = document.querySelector('.data-table-wrapper');


    bottom.addEventListener('scroll', () => {
        top.scrollLeft = bottom.scrollLeft;
    });


    new DataTableViewer();
});
