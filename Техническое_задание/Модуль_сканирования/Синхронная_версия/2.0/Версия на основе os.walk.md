# Задание

Перепиши полностью синхронную версию модуля сканирования файловой системы. Модуль должен быть построен на основе использования os.walk и os.stat, а также записи значений directory_s, которые назначаются каталогам при записи в таблицу directory в структуру хранения в оперативной памяти для дальнейшего использования для задания поля directory_s в таблице file. Кроме того, при записи данных в базу данных должны сохраняться значения nesting_level, как для каталогов, так и для файлов. Значение поля relative_path в таблицах directory и file не должно включать в себя наименование каталога или файла (значение поля name). Для каталога верхнего уровня (который задается в поле data_domain_path таблицы information_resource), значение relative_path должно быть равно '.', для остальных папок и файлов информационнного ресурса - путь относительно этой папки, начинающийся с '.'.

# Решение

I'll rewrite the synchronous version of the filesystem scanning module based on your requirements. Here's the full implementation:

 ```python
 # scanner/models.py

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class InformationResource:
    information_resource_s: int
    path: str
    name: str
    description: str

@dataclass
class DirectoryItem:
    information_resource_s: int
    directory_s: Optional[int] = None
    parent_directory_s: Optional[int] = None
    name: str = ''
    relative_path: str = ''
    nesting_level: int = 0
    first_discovered: datetime = None
    owner: str = ''
    is_actual: bool = True

@dataclass
class FileItem:
    information_resource_s: int
    file_s: Optional[int] = None
    directory_s: Optional[int] = None
    name: str = ''
    relative_path: str = ''
    extension: str = ''
    size_bytes: int = 0
    creation_time: datetime = None
    modification_time: datetime = None
    first_discovered: datetime = None
    owner: str = ''
    is_actual: bool = True
    nesting_level: int = 0

@dataclass
class ScanResult:
    total_directories: int
    total_files: int
    total_size: int
    start_time: datetime
    end_time: datetime
    errors: list
```
 
 ```python
# scanner/database.py

import logging
from typing import List, Tuple, Dict
import psycopg2
from psycopg2.extras import execute_values
from .models import DirectoryItem, FileItem

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        """Инициализация подключения к базе данных"""
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        self.conn.autocommit = False
        logger.info("Database connection established")

    def close(self):
        """Закрытие соединения с базой данных"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def save_directories_bulk(self, directories: List[DirectoryItem]) -> Dict[str, int]:
        """Пакетное сохранение директорий"""
        path_to_id = {}

        if not directories:
            return path_to_id

        try:
            with self.conn.cursor() as cur:
                values = [(
                    d.information_resource_s,
                    d.parent_directory_s,
                    d.name,
                    d.relative_path,
                    d.nesting_level,
                    d.first_discovered,
                    d.owner,
                    d.is_actual
                ) for d in directories]

                execute_values(cur, """
                    INSERT INTO directory (
                        information_resource_s,
                        parent_directory_s,
                        name,
                        relative_path,
                        nesting_level,
                        first_discovered,
                        owner,
                        is_actual
                    ) 
                    VALUES %s
                    ON CONFLICT (information_resource_s, relative_path, name) 
                    DO UPDATE SET
                        is_actual = EXCLUDED.is_actual,
                        owner = EXCLUDED.owner
                    RETURNING directory_s, relative_path, name
                """, values)

                results = cur.fetchall()
                for id, rel_path, name in results:
                    full_path = f"{rel_path}/{name}" if rel_path != '.' else name
                    path_to_id[full_path] = id
                    # Обновляем directory_s в объектах DirectoryItem
                    for directory in directories:
                        dir_full_path = f"{directory.relative_path}/{directory.name}" if directory.relative_path != '.' else directory.name
                        if dir_full_path == full_path:
                            directory.directory_s = id

            self.conn.commit()
            logger.debug(f"Saved {len(directories)} directories")
            return path_to_id

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error saving directories: {str(e)}")
            raise

    def save_files_bulk(self, files: List[FileItem]) -> None:
        """Пакетное сохранение файлов"""
        if not files:
            return

        try:
            with self.conn.cursor() as cur:
                values = [(
                    f.information_resource_s,
                    f.directory_s,
                    f.name,
                    f.relative_path,
                    f.extension,
                    f.size_bytes,
                    f.creation_time,
                    f.modification_time,
                    f.first_discovered,
                    f.owner,
                    f.is_actual,
                    f.nesting_level
                ) for f in files]

                execute_values(cur, """
                    INSERT INTO file (
                        information_resource_s,
                        directory_s,
                        name,
                        relative_path,
                        extension,
                        size_bytes,
                        creation_time,
                        modification_time,
                        first_discovered,
                        owner,
                        is_actual,
                        nesting_level
                    ) 
                    VALUES %s
                    ON CONFLICT (information_resource_s, relative_path, name) 
                    DO UPDATE SET
                        is_actual = EXCLUDED.is_actual,
                        size_bytes = EXCLUDED.size_bytes,
                        modification_time = EXCLUDED.modification_time,
                        owner = EXCLUDED.owner,
                        extension = EXCLUDED.extension
                    RETURNING file_s, relative_path, name
                """, values)

                results = cur.fetchall()
                path_to_id = {f"{rel_path}/{name}": id for id, rel_path, name in results}

                for file in files:
                    file_full_path = f"{file.relative_path}/{file.name}"
                    file.file_s = path_to_id.get(file_full_path)

            self.conn.commit()
            logger.debug(f"Saved {len(files)} files")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error saving files: {str(e)}")
            raise

    def get_resource_stats(self, resource_id: int) -> Tuple[int, int, int]:
        """
        Получение статистики по ресурсу:
        возвращает (количество директорий, количество файлов, общий размер файлов)
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    WITH dir_count AS (
                        SELECT COUNT(*) as dir_count
                        FROM directory
                        WHERE information_resource_s = %s
                        AND is_actual = TRUE
                    ),
                    file_stats AS (
                        SELECT COUNT(*) as file_count, COALESCE(SUM(size_bytes), 0) as total_size
                        FROM file
                        WHERE information_resource_s = %s
                        AND is_actual = TRUE
                    )
                    SELECT dir_count, file_count, total_size
                    FROM dir_count, file_stats
                """, (resource_id, resource_id))

                result = cur.fetchone()
                return result if result else (0, 0, 0)

        except Exception as e:
            logger.error(f"Error getting resource stats: {str(e)}")
            return (0, 0, 0)
```

```python
# scanner/scanner.py

import os
from datetime import datetime
from typing import List, Dict
import logging
from .models import InformationResource, DirectoryItem, FileItem, ScanResult
from .database import Database

logger = logging.getLogger(__name__)

class FilesystemScanner:
    def __init__(self, db: Database, batch_size: int = 5000):
        self.db = db
        self.batch_size = batch_size
        self.directories: List[DirectoryItem] = []
        self.files: List[FileItem] = []
        # Словарь для хранения пути к директории и её ID
        self.path_to_dir_id: Dict[str, int] = {}

    def _flush_directories(self):
        """Сохранение накопленных директорий в БД и получение их ID"""
        if self.directories:
            path_to_id = self.db.save_directories_bulk(self.directories)
            # Объединяем полученные ID с нашим словарем
            self.path_to_dir_id.update(path_to_id)
            self.directories = []

    def _flush_files(self):
        """Сохранение накопленных файлов в БД"""
        if self.files:
            self.db.save_files_bulk(self.files)
            self.files = []

    def _get_parent_directory_id(self, parent_path: str) -> int:
        """Получение ID родительской директории по пути"""
        return self.path_to_dir_id.get(parent_path)

    def _add_directory(self, resource: InformationResource, abs_path: str) -> None:
        """
        Добавление директории в список для последующего сохранения
        abs_path - абсолютный путь к директории
        """
        # Получаем относительный путь от корня информационного ресурса
        rel_path = os.path.relpath(abs_path, resource.path)

        # Определяем имя директории и её относительный путь (без имени)
        dir_name = os.path.basename(abs_path) or os.path.basename(resource.path)

        if rel_path == '.':
            # Корневая директория
            rel_path_without_name = ''
            nesting_level = 1
            parent_dir_s = None
        else:
            # Подкаталог
            parent_dir_path = os.path.dirname(abs_path)
            rel_parent_path = os.path.relpath(parent_dir_path, resource.path)
            if rel_parent_path == '.':
                rel_path_without_name = ''
            else:
                rel_path_without_name = rel_parent_path

            # Вычисляем уровень вложенности
            nesting_level = len(rel_path.split(os.sep)) if rel_path != '.' else 1

            # Получаем ID родительской директории
            if rel_parent_path == '.':
                parent_dir_key = os.path.basename(resource.path)
            else:
                parent_dir_key = rel_parent_path + '/' + os.path.basename(parent_dir_path)

            parent_dir_s = self.path_to_dir_id.get(parent_dir_key)

        dir_item = DirectoryItem(
            information_resource_s=resource.information_resource_s,
            parent_directory_s=parent_dir_s,
            name=dir_name,
            relative_path=rel_path_without_name,
            nesting_level=nesting_level,
            first_discovered=datetime.now(),
            owner=self._get_owner(abs_path),
            is_actual=True
        )

        self.directories.append(dir_item)
        if len(self.directories) >= self.batch_size:
            self._flush_directories()

    def _add_file(self, resource: InformationResource, abs_path: str) -> None:
        """
        Добавление файла в список для последующего сохранения
        abs_path - абсолютный путь к файлу
        """
        # Получаем относительный путь от корня информационного ресурса
        rel_path = os.path.relpath(abs_path, resource.path)

        # Определяем имя файла и его относительный путь (без имени)
        file_name = os.path.basename(abs_path)
        file_dir_path = os.path.dirname(abs_path)
        rel_dir_path = os.path.relpath(file_dir_path, resource.path)

        if rel_dir_path == '.':
            rel_path_without_name = ''
            dir_key = os.path.basename(resource.path)
        else:
            rel_path_without_name = rel_dir_path
            dir_key = rel_dir_path + '/' + os.path.basename(file_dir_path)

        # Получаем ID директории, в которой находится файл
        directory_s = self.path_to_dir_id.get(dir_key)

        # Вычисляем уровень вложенности
        nesting_level = len(rel_path.split(os.sep))

        file_stat = os.stat(abs_path)
        creation_time = datetime.fromtimestamp(file_stat.st_ctime)
        modification_time = datetime.fromtimestamp(file_stat.st_mtime)

        file_item = FileItem(
            information_resource_s=resource.information_resource_s,
            directory_s=directory_s,
            name=file_name,
            relative_path=rel_path_without_name,
            extension=os.path.splitext(file_name)[1].lower(),
            size_bytes=file_stat.st_size,
            creation_time=creation_time,
            modification_time=modification_time,
            first_discovered=datetime.now(),
            owner=self._get_owner(abs_path),
            is_actual=True,
            nesting_level=nesting_level
        )

        self.files.append(file_item)
        if len(self.files) >= self.batch_size:
            self._flush_files()

    def _get_owner(self, path: str) -> str:
        """Получение владельца файла/директории"""
        try:
            return str(os.stat(path).st_uid)
        except:
            return "unknown"

    def scan_resource(self, resource: InformationResource) -> ScanResult:
        """Сканирование информационного ресурса"""
        start_time = datetime.now()
        total_directories = 0
        total_files = 0
        total_size = 0
        errors = []

        try:
            # Сначала сканируем корневую директорию
            self._add_directory(resource, resource.path)
            self._flush_directories()
            total_directories += 1

            # Используем os.walk для обхода всей структуры каталогов
            for dirpath, dirnames, filenames in os.walk(resource.path):
                # Обработка поддиректорий
                for dirname in dirnames:
                    try:
                        full_dir_path = os.path.join(dirpath, dirname)
                        self._add_directory(resource, full_dir_path)
                        total_directories += 1
                    except Exception as e:
                        error_msg = f"Error processing directory {dirname}: {str(e)}"
                        logger.error(error_msg)
                        errors.append(error_msg)

                # Сохраняем директории перед обработкой файлов
                self._flush_directories()

                # Обработка файлов 
	            for filename in filenames:
	                try:
	                    full_file_path = os.path.join(dirpath, filename)
	                    self._add_file(resource, full_file_path)
	                    file_size = os.path.getsize(full_file_path)
	                    total_files += 1
	                    total_size += file_size
	                except Exception as e:
	                    error_msg = f"Error processing file {filename}: {str(e)}"
	                    logger.error(error_msg)
	                    errors.append(error_msg)

	            # Сохраняем файлы после обработки всех файлов в текущей директории
	            self._flush_files()

		    end_time = datetime.now()

	        return ScanResult(
	            total_directories=total_directories,
	            total_files=total_files,
	            total_size=total_size,
	            start_time=start_time,
	            end_time=end_time,
	            errors=errors
	        )

	    except Exception as e:
	        end_time = datetime.now()
	        error_msg = f"Error scanning resource: {str(e)}"
	        logger.error(error_msg)
	        errors.append(error_msg)

	        return ScanResult(
	            total_directories=total_directories,
	            total_files=total_files,
	            total_size=total_size,
	            start_time=start_time,
	            end_time=end_time,
	            errors=errors
	        )
```


Вот полный код файла main.py:

```python
# main.py

import os
import logging
import argparse
from datetime import datetime
from scanner.models import InformationResource
from scanner.scanner import FilesystemScanner
from scanner.database import Database

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scanner.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

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

def main():
    """Основная функция программы"""
    args = parse_arguments()

    # Проверяем существование указанного пути
    if not os.path.exists(args.path):
        logger.error(f"Path {args.path} does not exist")
        return

    # Инициализируем подключение к базе данных
    try:
        db = Database(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password
        )
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        return

    # Создаем или получаем информационный ресурс
    try:
        # Проверяем, существует ли ресурс с таким именем
        db.cursor.execute(
            """
            SELECT information_resource_s, name, path, 
                   total_directories, total_files, total_size,
                   last_scan_started_at, last_scan_completed_at
            FROM information_resources
            WHERE name = %s
            """,
            (args.name,)
        )

        result = db.cursor.fetchone()

        if result:
            resource_id, name, path, total_dirs, total_files, total_size, last_scan_start, last_scan_end = result
            logger.info(f"Found existing resource: {name} (ID: {resource_id})")

            # Обновляем путь, если он изменился
            if path != args.path:
                db.cursor.execute(
                    """
                    UPDATE information_resources
                    SET path = %s
                    WHERE information_resource_s = %s
                    """,
                    (args.path, resource_id)
                )
                db.conn.commit()
                logger.info(f"Updated path for resource {name} to {args.path}")

            resource = InformationResource(
                information_resource_s=resource_id,
                name=name,
                path=args.path,
                total_directories=total_dirs,
                total_files=total_files,
                total_size=total_size,
                last_scan_started_at=last_scan_start,
                last_scan_completed_at=last_scan_end
            )
        else:
            # Создаем новый ресурс
            db.cursor.execute(
                """
                INSERT INTO information_resources (name, path)
                VALUES (%s, %s)
                RETURNING information_resource_s
                """,
                (args.name, args.path)
            )
            resource_id = db.cursor.fetchone()[0]
            db.conn.commit()

            logger.info(f"Created new resource: {args.name} (ID: {resource_id})")

            resource = InformationResource(
                information_resource_s=resource_id,
                name=args.name,
                path=args.path,
                total_directories=0,
                total_files=0,
                total_size=0,
                last_scan_started_at=None,
                last_scan_completed_at=None
            )
    except Exception as e:
        logger.error(f"Failed to create resource: {str(e)}")
        db.close()
        return

    # Инициализируем сканер
    scanner = FilesystemScanner(db, batch_size=args.batch_size)

    # Выполняем сканирование
    logger.info(f"Starting scan of {args.path} ({args.name})")
    start_time = datetime.now()

    try:
        scan_result = scanner.scan_resource(resource)

        # Обновляем информацию о ресурсе
        db.cursor.execute(
            """
            UPDATE information_resources
            SET total_directories = %s,
                total_files = %s,
                total_size = %s,
                last_scan_started_at = %s,
                last_scan_completed_at = %s
            WHERE information_resource_s = %s
            """,
            (scan_result.total_directories, scan_result.total_files, scan_result.total_size, 
             scan_result.start_time, scan_result.end_time, resource.information_resource_s)
        )
        db.conn.commit()
        logger.info(f"Updated resource stats for ID: {resource.information_resource_s}")

        logger.info(f"Scan completed in {scan_result.end_time - scan_result.start_time}")
        logger.info(f"Total directories: {scan_result.total_directories}")
        logger.info(f"Total files: {scan_result.total_files}")
        logger.info(f"Total size: {scan_result.total_size} bytes")

        if scan_result.errors:
            logger.warning(f"Scan completed with {len(scan_result.errors)} errors")
            for error in scan_result.errors[:10]:  # Выводим первые 10 ошибок
                logger.warning(error)
            if len(scan_result.errors) > 10:
                logger.warning(f"... and {len(scan_result.errors) - 10} more errors")
    except Exception as e:
        logger.error(f"Scan failed: {str(e)}")
    finally:
        # Закрываем соединение с БД
        db.close()

if __name__ == "__main__":
    main()
```

## Как вызывать main.py

Файл `main.py` запускается из командной строки с указанием обязательных и необязательных аргументов. Вот как можно его вызвать:

### Базовый синтаксис

```bash
python main.py --path <путь_к_сканируемой_директории> --name <имя_ресурса> --db-name <имя_базы_данных> --db-user <имя_пользователя_бд> --db-password <пароль_бд>
```

### Обязательные аргументы

- `--path` - путь к директории, которую нужно сканировать
- `--name` - имя информационного ресурса (для идентификации в базе данных)
- `--db-name` - имя базы данных PostgreSQL
- `--db-user` - имя пользователя для подключения к базе данных
- `--db-password` - пароль для подключения к базе данных

### Необязательные аргументы

- `--db-host` - хост базы данных (по умолчанию 'localhost')
- `--db-port` - порт базы данных (по умолчанию 5432)
- `--batch-size` - размер пакета для операций массовой вставки в БД (по умолчанию 5000)

### Примеры использования

#### Пример 1: Базовое использование с локальной базой данных

```bash
python main.py --path /var/data/files --name "Production Files" --db-name filedb --db-user postgres --db-password secret123
```

#### Пример 2: Указание удаленной базы данных и настройка размера пакета

```bash
python main.py --path /home/user/documents --name "User Documents" --db-host db.example.com --db-port 5433 --db-name filedb --db-user scanner --db-password scanner_pass --batch-size 1000
```

#### Пример 3: Сканирование сетевого ресурса

```bash
python main.py --path /mnt/network_share --name "Network Share" --db-name filedb --db-user postgres --db-password postgres
```

После запуска скрипт начнет сканирование указанной директории, логируя прогресс в консоль и файл `scanner.log`. По завершении будет выведена сводная информация о результатах сканирования.