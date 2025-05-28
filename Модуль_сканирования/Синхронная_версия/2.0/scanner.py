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
        self.root_path = ''
        # Словарь для хранения пути к директории и её ID
        self.path_to_dir_id: Dict[str, int] = {}

    def _flush_directories(self):
        """Сохранение накопленных директорий в БД и получение их ID"""
        if self.directories:
            path_to_id = self.db.save_directories_bulk(self.directories)
            # Объединяем полученные ID с нашим словарем
            print(f"path_to_id={path_to_id}")
            self.path_to_dir_id.update(path_to_id)
            self.directories = []

    def _flush_files(self):
        print(f"self.files={self.files}")
        """Сохранение накопленных файлов в БД"""
        if self.files:
            self.db.save_files_bulk(self.files)
            self.files = []

    def _get_parent_directory_id(self, parent_path: str) -> int:
        """Получение ID родительской директории по пути"""
        return self.path_to_dir_id.get(parent_path)

    def _add_directory(self, resource: InformationResource, dir_path: str, dir_name: str) -> None:
        """
        Добавление директории в список для последующего сохранения
        abs_path - абсолютный путь к директории
        """
        # Получаем относительный путь от корня информационного ресурса
        abs_path = os.path.join(dir_path, dir_name)
        if abs_path == self.root_path:
            rel_path = '.'
        else:
            rel_path = os.path.join('.', os.path.relpath(dir_path, resource.path))

        if rel_path == '.':
            # Корневая директория
            nesting_level = 0
            parent_dir_s = None
        else:
            # Подкаталог
            nesting_level = len(rel_path.split(os.sep))-1
            # Получаем ID родительской директории
            parent_dir_s = self.path_to_dir_id.get(rel_path)
            # print (f"parent_dir_s={parent_dir_s}")

        dir_item = DirectoryItem(
            information_resource_s = resource.information_resource_s,
            parent_directory_s = parent_dir_s,
            name = dir_name,
            relative_path = rel_path, #rel_path_without_name,
            nesting_level = nesting_level,
            first_discovered = datetime.now(),
            owner = self._get_owner(abs_path),
            is_actual=True
        )

        self.directories.append(dir_item)
        if len(self.directories) >= self.batch_size:
            self._flush_directories()


    def _add_file(self, resource: InformationResource, dir_path: str, file_name: str) -> None:
        """
        Добавление файла в список для последующего сохранения
        abs_path - абсолютный путь к файлу
        """
        abs_path = os.path.join(dir_path, file_name)
        # Получаем относительный путь от корня информационного ресурса
        # rel_path = os.path.relpath(abs_path, resource.path)
        rel_path = os.path.join('.', os.path.relpath(dir_path, resource.path))
        print (f"rel_path={rel_path}")

        # Определяем имя файла и его относительный путь (без имени)
        # file_name = os.path.basename(abs_path)
        # file_dir_path = os.path.dirname(abs_path)
        # rel_dir_path = os.path.relpath(file_dir_path, resource.path)

        # if rel_dir_path == '.':
        #     rel_path_without_name = ''
        #     dir_key = os.path.basename(resource.path)
        # else:
        #     rel_path_without_name = rel_dir_path
        #     dir_key = rel_dir_path + '/' + os.path.basename(file_dir_path)

        # Получаем ID директории, в которой находится файл
        directory_s = self.path_to_dir_id.get(rel_path)
        print (f"directory_s={directory_s}")

        # Вычисляем уровень вложенности
        nesting_level = len(rel_path.split(os.sep))
        print(f"nesting_level={nesting_level}")

        file_stat = os.stat(abs_path)
        creation_time = datetime.fromtimestamp(file_stat.st_ctime)
        modification_time = datetime.fromtimestamp(file_stat.st_mtime)

        file_item = FileItem(
            information_resource_s = resource.information_resource_s,
            directory_s = directory_s,
            name = file_name,
            relative_path = rel_path,
            extension = os.path.splitext(file_name)[1].lower(),
            size_bytes = file_stat.st_size,
            creation_time = creation_time,
            modification_time = modification_time,
            first_discovered = datetime.now(),
            owner = self._get_owner(abs_path),
            is_actual = True,
            nesting_level = nesting_level
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

        # print("scan_resource")
        # print(f"resource.path={resource.path}")
        try:
            # Сначала сканируем корневую директорию
            self.root_path = os.path.join(resource.path, resource.name)
            self._add_directory(resource, resource.path, resource.name)
            self._flush_directories()
            total_directories += 1

            # Используем os.walk для обхода всей структуры каталогов
            for dirpath, dirnames, filenames in os.walk(self.root_path):
                # Обработка поддиректорий
                for dirname in dirnames:
                    try:
                        self._add_directory(resource, dirpath, dirname)
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
                        self._add_file(resource, dirpath, filename)
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
