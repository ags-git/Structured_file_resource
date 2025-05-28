# main.py
import os
import logging
import argparse
import sys
from datetime import datetime
from typing import List

from scanner.models import InformationResource, ScanResult
from scanner.scanner import FilesystemScanner
from scanner.database import Database
from scanner.config import DatabaseConfig


def setup_logging():
    """Настройка логирования"""
    logging.basicConfig(
        level = logging.INFO,
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("scanner.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def parse_arguments():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='Filesystem scanner')
    parser.add_argument('--path', required=True, help='Path to scan')
    parser.add_argument('--name', required=True, help='Resource name')
    parser.add_argument('--db-host', default='localhost', help='Database host')
    parser.add_argument('--db-port', type=int, default=5432, help='Database port')
    parser.add_argument('--db-name', required=True, help='Database name')
    parser.add_argument('--db-user', required=True, help='Database user')
    parser.add_argument('--db-password', required=True, help='Database password')
    parser.add_argument('--batch-size', type=int, default=5000, help='Batch size for bulk operations')

    return parser.parse_args()


def get_resources_to_scan() -> List[InformationResource]:
    """Получение списка информационных ресурсов для сканирования"""
    # Здесь может быть логика получения ресурсов из БД или конфига
    return [
        InformationResource(
            information_resource_s=1,
            path="/home/papa/Изображения",
            name="ФОТО",
            description="ФОТО"
        )
    ]


def mark_inactive_items(db: Database, resource: InformationResource):
    db.mark_items_not_actual(resource.information_resource_s)


def scan_resource(db: Database, resource: InformationResource, logger) -> ScanResult:
    """Сканирование одного информационного ресурса"""
    start_time = datetime.now()
    errors = []

    try:
        # Создаем сканер с размером пакета 5000
        scanner = FilesystemScanner(db, batch_size=5000)

        # Помечаем существующие записи как неактуальные
        mark_inactive_items(db, resource)

        # Сканируем ресурс
        scanner.scan_resource(resource)

        # Получаем статистику
        stats = db.get_resource_stats(resource.information_resource_s)

    except Exception as e:
        error_msg = f"Error scanning resource {resource.information_resource_s}: {str(e)}"
        logger.error(error_msg)
        errors.append(error_msg)
        stats = (0, 0, 0)  # directories, files, total_size

    end_time = datetime.now()

    return ScanResult(
        total_directories=stats[0],
        total_files=stats[1],
        total_size=stats[2],
        start_time=start_time,
        end_time=end_time,
        errors=errors
    )


def main():
    """Основная функция"""
    logger = setup_logging()
    logger.info("Starting filesystem scanner")

    args = parse_arguments()

    # Проверяем существование указанного пути
    if not os.path.exists(args.path):
        logger.error(f"Path {args.path} does not exist")
        return

    try:
        # Инициализация подключения к БД
        db = Database(
            host = args.db_host,
            port = args.db_port,
            database = args.db_name,
            user = args.db_user,
            password = args.db_password
        )

        # Получение списка ресурсов для сканирования
        resources = get_resources_to_scan()

        print (f"resources={resources}")

        total_start_time = datetime.now()
        results = []

        # Сканирование каждого ресурса
        for resource in resources:
            logger.info(f"Starting scan of resource: {resource.name}")
            result = scan_resource(db, resource, logger)
            results.append(result)

            # Логирование результатов сканирования
            duration = (result.end_time - result.start_time).total_seconds()
            logger.info(
                f"Scan completed for {resource.name}:\n"
                f"  Directories: {result.total_directories}\n"
                f"  Files: {result.total_files}\n"
                f"  Total size: {result.total_size:,} bytes\n"
                f"  Duration: {duration:.2f} seconds"
            )

            if result.errors:
                logger.error(f"Errors during scan: {result.errors}")

        # Общая статистика
        total_duration = (datetime.now() - total_start_time).total_seconds()
        total_dirs = sum(r.total_directories for r in results)
        total_files = sum(r.total_files for r in results)
        total_size = sum(r.total_size for r in results)

        logger.info(
            f"All scans completed:\n"
            f"  Total directories: {total_dirs:,}\n"
            f"  Total files: {total_files:,}\n"
            f"  Total size: {total_size:,} bytes\n"
            f"  Total duration: {total_duration:.2f} seconds"
        )

    except Exception as e:
        logger.error(f"Critical error: {str(e)}")
        sys.exit(1)
    finally:
        if 'db' in locals():
            db.close()

    logger.info("Filesystem scanner finished")

if __name__ == "__main__":
    main()
