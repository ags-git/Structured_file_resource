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
