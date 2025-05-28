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

    def mark_items_not_actual(self, resource_id: int) -> None:
        """Пометка записей как неактуальных"""
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE directory SET is_actual = FALSE WHERE information_resource_s = %s",
                (resource_id,)
            )
            cur.execute(
                "UPDATE file SET is_actual = FALSE WHERE information_resource_s = %s",
                (resource_id,)
            )

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
                    full_path = f"{rel_path}/{name}"
                    path_to_id[full_path] = id
                    # Обновляем directory_s в объектах DirectoryItem
                    # for directory in directories:
                    #     dir_full_path = f"{directory.relative_path}/{directory.name}" if directory.relative_path != '.' else directory.name
                    #     if dir_full_path == full_path:
                    #         directory.directory_s = id

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
