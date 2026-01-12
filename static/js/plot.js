// Основной объект приложения
const App = {
    // Инициализация
    init: function() {
        this.loadAvailableColumns();
        this.bindEvents();
        this.updateDataCount();
    },
    
    // Загрузка доступных колонок
    loadAvailableColumns: async function() {
        try {
            const response = await fetch('/api/columns');
            const data = await response.json();
            
            if (data.columns && data.columns.length > 0) {
                this.populateSelectOptions(data.columns);
                this.updateFieldsList(data.columns);
            } else {
                this.showError('No numeric columns found in data');
            }
        } catch (error) {
            this.showError('Failed to load columns: ' + error.message);
        }
    },
    
    // Заполнение выпадающих списков
    populateSelectOptions: function(columns) {
        const xSelect = document.getElementById('x-axis');
        const ySelect = document.getElementById('y-axis');
        
        // Очистка существующих опций (кроме первой)
        while (xSelect.options.length > 1) xSelect.remove(1);
        while (ySelect.options.length > 1) ySelect.remove(1);
        
        // Добавление новых опций
        columns.forEach(column => {
            const xOption = new Option(column, column);
            const yOption = new Option(column, column);
            
            xSelect.add(xOption);
            ySelect.add(yOption.cloneNode(true));
        });
    },
    
    // Обновление списка полей в информационной панели
    updateFieldsList: function(columns) {
        const fieldsList = document.getElementById('fields-list');
        fieldsList.innerHTML = '';
        
        columns.forEach(column => {
            const span = document.createElement('span');
            span.className = 'field-item';
            span.textContent = column;
            fieldsList.appendChild(span);
        });
    },
    
    // Обновление счетчика данных
    updateDataCount: async function() {
        try {
            const response = await fetch('/api/data/sample');
            const data = await response.json();
            const count = data.sample ? data.sample.length : 0;
            document.getElementById('data-count').textContent = count;
        } catch (error) {
            console.error('Failed to load data count:', error);
        }
    },
    
    // Создание графика
    generatePlot: async function() {
        const xAxis = document.getElementById('x-axis').value;
        const yAxis = document.getElementById('y-axis').value;
        
        if (!xAxis || !yAxis) {
            this.showError('Please select both X and Y axes');
            return;
        }
        
        console.log('Generating plot with:', { x_axis: xAxis, y_axis: yAxis });
        
        // Показать индикатор загрузки
        this.showLoading(true);
        
        try {
            const response = await fetch('/api/plot', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    x_axis: xAxis,
                    y_axis: yAxis
                })
            });
            
            console.log('Response status:', response.status);
            
            const data = await response.json();
            console.log('Response data:', data);
            
            if (response.ok) {
                console.log('Plot data received, length:', JSON.stringify(data.plot).length);
                this.displayPlot(data.plot);
                this.updateStatistics(data.statistics);
            } else {
                this.showError(data.error || 'Failed to generate plot');
            }
        } catch (error) {
            console.error('Network error:', error);
            this.showError('Network error: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    },
    
    // Отображение графика Plotly
    displayPlot: function(plotJSON) {
        const plotDiv = document.getElementById('plot');
        const plotData = JSON.parse(plotJSON);
        
        // Очистка предыдущего графика
        plotDiv.innerHTML = '';
        
        // Отображение нового графика
        Plotly.newPlot('plot', plotData.data, plotData.layout, {
            responsive: true,
            displayModeBar: true,
            modeBarButtonsToRemove: ['lasso2d', 'select2d'],
            displaylogo: false,
            modeBarButtonsToAdd: [{
                name: 'Download as PNG',
                icon: Plotly.Icons.camera,
                click: function(gd) {
                    Plotly.downloadImage(gd, {
                        format: 'png',
                        width: 1200,
                        height: 600,
                        filename: 'scientific-plot'
                    });
                }
            }]
        });
        
        // Добавление обработчиков событий
        plotDiv.on('plotly_click', function(data) {
            if (data.points && data.points[0]) {
                const point = data.points[0];
                App.showPointDetails(point);
            }
        });
    },
    
    // Обновление статистики
    updateStatistics: function(stats) {
        document.getElementById('x-mean').textContent = stats.x_mean.toFixed(4);
        document.getElementById('y-mean').textContent = stats.y_mean.toFixed(4);
        document.getElementById('x-std').textContent = stats.x_std.toFixed(4);
        document.getElementById('y-std').textContent = stats.y_std.toFixed(4);
    },
    
    // Показать детали точки
    showPointDetails: function(point) {
        const message = `Probe: ${point.hovertext || 'Unknown'}\n` +
                       `X: ${point.x.toFixed(4)}\n` +
                       `Y: ${point.y.toFixed(4)}`;
        
        alert(message);
    },
    
    // Сброс графика
    resetPlot: function() {
        const plotDiv = document.getElementById('plot');
        plotDiv.innerHTML = '<div class="plot-placeholder"><p>Select axes and generate plot to visualize data</p></div>';
        
        // Сброс статистики
        ['x-mean', 'y-mean', 'x-std', 'y-std'].forEach(id => {
            document.getElementById(id).textContent = '-';
        });
        
        // Сброс выбора осей
        document.getElementById('x-axis').selectedIndex = 0;
        document.getElementById('y-axis').selectedIndex = 0;
    },
    
    // Управление индикатором загрузки
    showLoading: function(show) {
        const loading = document.getElementById('loading');
        if (show) {
            loading.classList.add('active');
        } else {
            loading.classList.remove('active');
        }
    },
    
    // Показать ошибку
    showError: function(message) {
        alert('Error: ' + message);
    },
    
    // Привязка событий
    bindEvents: function() {
        document.getElementById('generate-plot').addEventListener('click', () => {
            this.generatePlot();
        });
        
        document.getElementById('reset-view').addEventListener('click', () => {
            this.resetPlot();
        });
        
        // Автоматическая генерация при изменении выбора
        document.getElementById('x-axis').addEventListener('change', () => {
            if (document.getElementById('x-axis').value && document.getElementById('y-axis').value) {
                // Автогенерацию можно включить, если нужно
                // this.generatePlot();
            }
        });
        
        document.getElementById('y-axis').addEventListener('change', () => {
            if (document.getElementById('x-axis').value && document.getElementById('y-axis').value) {
                // Автогенерацию можно включить, если нужно
                // this.generatePlot();
            }
        });
        
        // Обработка нажатия Enter в селектах
        ['x-axis', 'y-axis'].forEach(id => {
            document.getElementById(id).addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.generatePlot();
                }
            });
        });
    }
};

// Инициализация приложения при загрузке страницы
document.addEventListener('DOMContentLoaded', () => {
    App.init();
    
    // Добавление стилей для placeholder
    const style = document.createElement('style');
    style.textContent = `
        .plot-placeholder {
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100%;
            color: #88aaff;
            font-size: 1.2rem;
            text-align: center;
            padding: 40px;
            opacity: 0.7;
        }
    `;
    document.head.appendChild(style);
});