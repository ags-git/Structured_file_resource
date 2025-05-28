# scanner/hash_calculator.py
import hashlib
from pathlib import Path
from typing import Optional

class HashCalculator:
    def __init__(self, chunk_size: int = 8192):
        self.chunk_size = chunk_size

    def calculate_md5(self, file_path: Path) -> Optional[str]:
        """Вычисляет MD5-хеш файла"""
        try:
            md5 = hashlib.md5()
            with open(file_path, 'rb') as f:
                while chunk := f.read(self.chunk_size):
                    md5.update(chunk)
            return md5.hexdigest()
        except Exception:
            return None
