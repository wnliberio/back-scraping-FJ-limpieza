import os, json
from datetime import datetime, timedelta
from ..config import OUTPUT_DIR, CACHE_HOURS

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def load_cache():
    path = os.path.join(OUTPUT_DIR, "cache.json")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(cache: dict):
    path = os.path.join(OUTPUT_DIR, "cache.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def cache_hit(cache: dict, key: str):
    item = cache.get(key)
    if not item:
        return None
    try:
        ts = datetime.fromisoformat(item["timestamp"])
        if datetime.now() - ts < timedelta(hours=CACHE_HOURS):
            return item["data"]
    except Exception:
        return None
    return None
