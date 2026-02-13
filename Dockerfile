# Используем легкий образ Python
FROM python:3.10-slim

# Устанавливаем рабочую директорию
WORKDIR /src

# Копируем зависимости и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код приложения
COPY src/ .

# Запускаем Gunicorn (внутри контейнера)
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "main:app"]