import sqlite3
import math
import mmh3
import pickle
import os
import zlib
from urllib.parse import urlparse


class BloomFilter:
    def __init__(self, size, hash_count, filepath="data/bloom.bin"):
        self.size = size
        self.hash_count = hash_count
        self.filepath = filepath
        self.bit_array = bytearray(math.ceil(size / 8))

    def add(self, string):
        for seed in range(self.hash_count):
            result = mmh3.hash(string, seed) % self.size
            byte_index = result // 8
            bit_index = result % 8
            self.bit_array[byte_index] |= (1 << bit_index)

    def lookup(self, string):
        for seed in range(self.hash_count):
            result = mmh3.hash(string, seed) % self.size
            byte_index = result // 8
            bit_index = result % 8
            if not (self.bit_array[byte_index] & (1 << bit_index)):
                return False
        return True

    def save(self):
        try:
            with open(self.filepath, 'wb') as f:
                pickle.dump(self.bit_array, f)
            return True
        except Exception as e:
            print(f"Bloom Save Error: {e}")
            return False

    def load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'rb') as f:
                    self.bit_array = pickle.load(f)
                return True
            except Exception:
                return False
        return False


def get_high_perf_connection(db_path):
    conn = sqlite3.connect(db_path, timeout=60, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;") 
    conn.execute("PRAGMA synchronous=NORMAL;") 
    conn.execute("PRAGMA cache_size=-64000;") 
    conn.execute("PRAGMA temp_store=MEMORY;") 
    conn.execute("PRAGMA mmap_size=30000000000;") 
    return conn


def compress_html(data):
    if not data: return None
    if isinstance(data, str): 
        data = data.encode('utf-8')
    return zlib.compress(data)


def decompress_html(blob_data):
    if not blob_data: return ""
    return zlib.decompress(blob_data).decode('utf-8', errors='replace')


def canonicalise(url):
    try:
        url = str(url).strip()
        if '#' in url: url = url.split('#')[0]
        
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"): return None
        
        netloc = parsed.hostname
        if not netloc: return None
        netloc = netloc.lower()
        
        if parsed.port:
            if (parsed.scheme == "http" and parsed.port != 80) or (parsed.scheme == "https" and parsed.port != 443):
                netloc += f":{parsed.port}"
        
        path = parsed.path.replace("//", "/")
        if not path: path = "/"
        
        if any(ext in path.lower() for ext in ['.png','.jpg','.jpeg','.gif','.css','.js','.ico','.svg','.pdf','.zip','.exe','.mp4']):
            return None

        clean_url = f"{parsed.scheme}://{netloc}{path}"
        if parsed.query:
            clean_url += f"?{parsed.query}"
            
        return clean_url
    except Exception:
        return None