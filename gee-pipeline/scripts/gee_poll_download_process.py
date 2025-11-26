# scripts/poll_download_convert.py
import ee, os, time, json, pandas as pd
from dotenv import load_dotenv
from google.cloud import storage
from pathlib import Path

load_dotenv("config/template_config.env")
SERVICE_ACCOUNT = os.getenv("SERVICE_ACCOUNT_EMAIL")
KEYPATH = os.getenv("SERVICE_ACCOUNT_KEYPATH")
BUCKET = os.getenv("GCS_BUCKET")

ee.Initialize(ee.ServiceAccountCredentials(SERVICE_ACCOUNT, KEYPATH))
client = storage.Client.from_service_account_json(KEYPATH)
bucket = client.bucket(BUCKET)

TASKS_META = 'outputs/raw_export_tasks.json'
DOWNLOAD_DIR = Path('outputs/raw_download')
PARQUET_DIR = Path('outputs/raw_parquet')

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

def wait_for_completion(task_ids, timeout=24*3600, poll_interval=20):
    start = time.time()
    completed = set()
    while time.time() - start < timeout:
        tasks = ee.batch.Task.list()
        status = {t.id: t.status().get('state') for t in tasks}
        for tid in task_ids:
            st = status.get(tid)
            if st == 'COMPLETED' and tid not in completed:
                completed.add(tid)
                print("Completed:", tid)
            elif st == 'FAILED':
                raise RuntimeError(f"Task failed: {tid}")
        if len(completed) == len(task_ids):
            return True
        time.sleep(poll_interval)
    raise TimeoutError("Timeout waiting for tasks to finish.")

def download_prefixes(prefixes):
    downloaded = []
    for prefix in prefixes:
        blobs = client.list_blobs(BUCKET, prefix=prefix)
        for blob in blobs:
            if blob.name.endswith('.csv'):
                local = DOWNLOAD_DIR / os.path.basename(blob.name)
                blob.download_to_filename(str(local))
                downloaded.append(local)
                print("Downloaded", local)
    return downloaded

def csv_to_parquet(csv_paths):
    out_paths = []
    for p in csv_paths:
        df = pd.read_csv(p)
        # sometimes GEE CSV contains .geo column with geometry -- keep or drop as needed
        # ensure columns: province/amphoe/tambon are present; if not, try to detect name fields
        # convert image date if present
        if 'system:time_start' in df.columns:
            df.rename(columns={'system:time_start':'image_date'}, inplace=True)
        # Try to coerce numeric columns
        for c in df.columns:
            try:
                df[c] = pd.to_numeric(df[c], errors='ignore')
            except:
                pass
        # write parquet by var/year/month structure extracted from filename (naive)
        fname = p.name
        # determine var from filename
        if 'NDVI' in fname.upper():
            var = 'NDVI'
        elif 'LST' in fname.upper():
            var = 'LST'
        elif 'RAIN' in fname.upper():
            var = 'RAIN'
        elif 'SMAP' in fname.upper():
            var = 'SMAP'
        elif 'FIRE' in fname.upper():
            var = 'FIRE'
        else:
            var = 'OTHER'
        # target path
        # try to get year/month from any date column
        year = df['image_date'].str.slice(0,4).iloc[0] if 'image_date' in df.columns and not df['image_date'].isna().all() else 'unknown'
        month = df['image_date'].str.slice(5,7).iloc[0] if 'image_date' in df.columns and not df['image_date'].isna().all() else '00'
        dest_dir = PARQUET_DIR / var / year / month
        dest_dir.mkdir(parents=True, exist_ok=True)
        outp = dest_dir / (p.stem + '.parquet')
        df.to_parquet(outp, index=False)
        out_paths.append(outp)
        print("Wrote parquet:", outp)
    return out_paths

if __name__ == "__main__":
    with open(TASKS_META) as f:
        tasks = json.load(f)
    task_ids = [t['task_id'] for t in tasks]
    print("Waiting for tasks (count):", len(task_ids))
    wait_for_completion(task_ids, timeout=48*3600)
    prefixes = list({t.get('prefix') for t in tasks if t.get('prefix')})
    print("Downloading prefixes:", prefixes)
    downloaded = download_prefixes(prefixes)
    csv_to_parquet(downloaded)
    print("All done.")
