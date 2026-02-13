// Основной объект приложения
const App = {
    // Инициализация
    init: function() {
        this.loadAvailableColumns();
        this.loadSeries();
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
    
    // Загрузка серий
    loadSeries: async function() {
        try {
            const response = await fetch('/api/series');
            const data = await response.json();
            
            if (data.series && data.series.length > 0) {
                this.populateSeriesOptions(data.series);
                this.updateSeriesList(data.series);
                document.getElementById('series-count').textContent = data.series.length;
            }
        } catch (error) {
            console.error('Failed to load series:', error);
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
    
    // Заполнение списка серий
    populateSeriesOptions: function(series) {
        const seriesSelect = document.getElementById('series-select');
        seriesSelect.innerHTML = '';
        
        series.forEach(s => {
            const option = document.createElement('option');
            option.value = s;
            option.textContent = s;
            seriesSelect.appendChild(option);
        });
    },
    
    // Обновление списка полей
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
    
    // Обновление списка серий
    updateSeriesList: function(series) {
        const seriesList = document.getElementById('series-list');
        seriesList.innerHTML = '';
        
        series.forEach(s => {
            const span = document.createElement('span');
            span.className = 'field-item';
            span.textContent = s;
            span.style.backgroundColor = this.getSeriesColor(s);
            seriesList.appendChild(span);
        });
    },
    
    // Генерация цвета для серии
    getSeriesColor: function(seriesName) {
        const colors = [
            'rgba(66, 153, 225, 0.2)',
            'rgba(72, 187, 120, 0.2)',
            'rgba(237, 137, 54, 0.2)',
            'rgba(245, 101, 101, 0.2)',
            'rgba(159, 122, 234, 0.2)',
            'rgba(246, 173, 85, 0.2)',
            'rgba(56, 178, 172, 0.2)',
            'rgba(240, 82, 82, 0.2)'
        ];
        
        // Хэш строки для получения индекса цвета
        let hash = 0;
        for (let i = 0; i < seriesName.length; i++) {
            hash = seriesName.charCodeAt(i) + ((hash << 5) - hash);
        }
        const index = Math.abs(hash) % colors.length;
        return colors[index];
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
        const analysisMode = document.getElementById('analysis-mode').value;
        const xAxis = document.getElementById('x-axis').value;
        const yAxis = document.getElementById('y-axis').value;
        const seriesSelect = document.getElementById('series-select');
        const selectedSeries = Array.from(seriesSelect.selectedOptions).map(opt => opt.value);
        
        if (!xAxis || !yAxis) {
            this.showError('Please select both X and Y axes');
            return;
        }
        
        if (analysisMode !== 'average' && selectedSeries.length === 0) {
            this.showError('Please select at least one series');
            return;
        }
        
        console.log('Generating plot with:', { 
            mode: analysisMode, 
            x_axis: xAxis, 
            y_axis: yAxis,
            series: selectedSeries 
        });
        
        // Показать индикатор загрузки
        this.showLoading(true);
        
        try {
            const requestData = {
                analysis_mode: analysisMode,
                x_axis: xAxis,
                y_axis: yAxis,
                series: selectedSeries,
                filters: {
                    hide_zero: document.getElementById('filter-zero').checked,
                    show_liquid: document.getElementById('filter-liquid').checked,
                    show_solid: document.getElementById('filter-solid').checked
                }
            };
            
            // Добавляем дополнительные данные для процентного анализа
            if (analysisMode === 'percentage') {
                requestData.reference_type = document.getElementById('reference-type').value;
                requestData.sample_type = document.getElementById('sample-type').value;
            }
            
            const response = await fetch('/api/plot', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(requestData)
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
                        filename: 'series-analysis-plot'
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
        document.getElementById('series-stats').textContent = stats.series_count || '-';
        document.getElementById('x-mean').textContent = stats.x_mean ? stats.x_mean.toFixed(4) : '-';
        document.getElementById('y-mean').textContent = stats.y_mean ? stats.y_mean.toFixed(4) : '-';
        document.getElementById('r-squared').textContent = stats.r_squared ? stats.r_squared.toFixed(4) : '-';
    },
    
    // Показать детали точки
    showPointDetails: function(point) {
        const message = `Probe: ${point.hovertext || 'Unknown'}\n` +
                       `Series: ${point.customdata?.series || 'N/A'}\n` +
                       `Type: ${point.customdata?.sample_type || 'N/A'}\n` +
                       `X: ${point.x.toFixed(4)}\n` +
                       `Y: ${point.y.toFixed(4)}`;
        
        alert(message);
    },
    
    // Сброс графика
    resetPlot: function() {
        const plotDiv = document.getElementById('plot');
        plotDiv.innerHTML = '<div class="plot-placeholder"><p>Select axes and generate plot to visualize data</p></div>';
        
        // Сброс статистики
        ['series-stats', 'x-mean', 'y-mean', 'r-squared'].forEach(id => {
            document.getElementById(id).textContent = '-';
        });
        
        // Сброс выбора серий
        const seriesSelect = document.getElementById('series-select');
        Array.from(seriesSelect.options).forEach(opt => opt.selected = false);
    },
    
    // Управление индикатором загрузки
    showLoading: function(show) {
        const loading = document.getElementById('loading');
        if (show) {
            loading.style.display = 'flex';
        } else {
            loading.style.display = 'none';
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
        
        // Выбор всех серий
        document.getElementById('select-all-series').addEventListener('click', () => {
            const seriesSelect = document.getElementById('series-select');
            Array.from(seriesSelect.options).forEach(opt => opt.selected = true);
        });
        
        // Очистка выбора серий
        document.getElementById('clear-series').addEventListener('click', () => {
            const seriesSelect = document.getElementById('series-select');
            Array.from(seriesSelect.options).forEach(opt => opt.selected = false);
        });
        
        // Переключение режима анализа
        document.getElementById('analysis-mode').addEventListener('change', (e) => {
            const percentageOptions = document.getElementById('percentage-options');
            const seriesSelector = document.getElementById('series-selector-group');
            
            if (e.target.value === 'percentage') {
                percentageOptions.style.display = 'block';
            } else {
                percentageOptions.style.display = 'none';
            }
            
            if (e.target.value === 'average') {
                seriesSelector.style.opacity = '0.5';
                seriesSelector.style.pointerEvents = 'none';
            } else {
                seriesSelector.style.opacity = '1';
                seriesSelector.style.pointerEvents = 'auto';
            }
        });
        
        // Автоматическая генерация при нажатии Enter
        ['x-axis', 'y-axis'].forEach(id => {
            document.getElementById(id).addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.generatePlot();
                }
            });
        });
    }
};

// Инициализация приложения
document.addEventListener('DOMContentLoaded', () => {
    App.init();
});