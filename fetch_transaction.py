import time
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

from config import BASE_URL, POLL_INTERVAL, QUERY_IDS, dune_headers, FILENAME_TIMESTAMP_MODE, OUTPUT_DIR

def execute_query(query_id: int) -> str:
    url = f"{BASE_URL}/query/{query_id}/execute"
    res = requests.post(url, headers=dune_headers())
    res.raise_for_status()
    return res.json().get("execution_id")

def wait_for_completion(execution_id: str, query_id: int) -> None:
    url = f"{BASE_URL}/execution/{execution_id}/status"
    while True:
        res = requests.get(url, headers=dune_headers())
        res.raise_for_status()
        state = res.json().get("state")
        print(f"[{query_id}] Status:", state)
        if state in {"QUERY_STATE_COMPLETED", "QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"}:
            if state != "QUERY_STATE_COMPLETED":
                raise RuntimeError(f"[{query_id}] ended with {state}")
            return
        time.sleep(POLL_INTERVAL)

def fetch_results(execution_id: str) -> pd.DataFrame:
    url = f"{BASE_URL}/execution/{execution_id}/results"
    res = requests.get(url, headers=dune_headers())
    res.raise_for_status()
    rows = res.json().get("result", {}).get("rows", [])
    return pd.DataFrame(rows)

def to_iso8601(df: pd.DataFrame) -> pd.DataFrame:
    """把时间列转成 ISO 8601 格式，并确保时间列在第一列"""
    time_cols = [c for c in df.columns if any(k in c.lower() for k in ("time", "date", "timestamp"))]

    for col in time_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")

    # take "timestamp" as the first column
    if "timestamp" in df.columns:
        cols = ["timestamp"] + [c for c in df.columns if c != "timestamp"]
        df = df[cols]
    elif time_cols:  # 如果没有 timestamp，用找到的第一个时间列
        first_time_col = time_cols[0]
        cols = [first_time_col] + [c for c in df.columns if c != first_time_col]
        df = df[cols]

    return df

def get_filename_timestamp(df: pd.DataFrame) -> str:
    """
    生成文件名时间戳:
      - last_month: 上个月，格式 mm-yyyy
      - last_day  : df 内最新日期 YYYY-MM-DD
      - system    : 系统当前时间 YYYY-MM-DDTHH-MM-SS
    """
    if FILENAME_TIMESTAMP_MODE == "last_month":
        last_month = datetime.utcnow() - relativedelta(months=1)
        return last_month.strftime("%m-%Y")

    if FILENAME_TIMESTAMP_MODE == "last_day":
        time_cols = [c for c in df.columns if any(k in c.lower() for k in ("time", "date", "timestamp"))]
        if time_cols:
            last_ts = pd.to_datetime(df[time_cols[0]], errors="coerce").max()
            if not pd.isna(last_ts):
                return last_ts.strftime("%Y-%m-%d")

    return datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")


def main():
    for qid in QUERY_IDS:
        try:
            exec_id = execute_query(qid)
            wait_for_completion(exec_id, qid)
            df = fetch_results(exec_id)
            df = to_iso8601(df)

            ts_str = get_filename_timestamp(df)
            output_file = os.path.join(OUTPUT_DIR, f"dune_query_{qid}_{ts_str}.csv")

            df.to_csv(output_file, index=False, encoding="utf-8-sig")
            print(f"✅ [{qid}] Saved {len(df)} rows to {output_file}\n")
        except Exception as e:
            print(f"❌ [{qid}] Failed: {e}")

if __name__ == "__main__":
    main()
