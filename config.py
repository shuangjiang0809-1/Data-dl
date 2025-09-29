# config.py

# === Dune API 配置 ===
API_KEY = "BDE49fOw7PeJRQEGRIOGLRmvKcUXC3bJ"   # ← 替换成你的 Dune API Key
BASE_URL = "https://api.dune.com/api/v1"
POLL_INTERVAL = 5   # 秒数，查询状态轮询间隔

# === Query 配置 ===
# 你可以放多个 Query ID
QUERY_IDS =[5859875, 5868046]

# === 文件命名方式 ===
# 可选:
#   "system"   -> 用系统当前时间 (UTC)
#   "last_day" -> 用 df 里最后一天的时间戳
FILENAME_TIMESTAMP_MODE = "last_month"

# === 输出目录 ===
OUTPUT_DIR = "./outputs"   # 会自动创建
import os
os.makedirs(OUTPUT_DIR, exist_ok=True)


# === 工具函数 ===
def dune_headers():
    return {"X-DUNE-API-KEY": API_KEY}
