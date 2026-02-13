# gunicorn_config.py
bind = "0.0.0.0:8000"
workers = 4
timeout = 30
loglevel = "info"
accesslog = "-"