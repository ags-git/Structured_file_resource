# scanner/config.py
# config.py

import logging
import os

class DatabaseConfig:
    HOST = os.getenv('DB_HOST', 'localhost')
    PORT = int(os.getenv('DB_PORT', '5432'))
    NAME = os.getenv('DB_NAME', 'database')
    USER = os.getenv('DB_USER', 'user')
    PASSWORD = os.getenv('DB_PASSWORD', 'password')

class LoggingConfig:
    LEVEL = logging.INFO
    FILE_PATH = os.getenv('LOG_FILE', 'scanner.log')
