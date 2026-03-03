import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from birthDeath import SOS
import threading

df = pd.read_csv("Birth_ids.csv", delimiter='|')
id_list = df.iloc[:, 0].astype(str).tolist()
sos=SOS()

total = len(id_list)
completed = 0
lock = threading.Lock()

results = []

def fetch_data(birth_id):
    try:
        data = sos.get_birth_data_by_id(birth_id, 'Birth')
        return birth_id, data
    except Exception as e:
        return birth_id, {"error": str(e)}

MAX_WORKERS = 3
SAVE_EVERY = 20

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(fetch_data, i): i for i in id_list}

    for future in as_completed(futures):
        birth_id, data = future.result()

        with lock:
            completed += 1
            left = total - completed
            print(f"✅ Done: {completed}/{total} | ⏳ Left: {left} | ID: {birth_id}")

            # 🔑 IMPORTANT: merge ID + returned dict
            row = {"id": birth_id}

            if isinstance(data, dict):
                row.update(data)
            else:
                row["error"] = str(data)

            results.append(row)

            # 💾 Save periodically
            if completed % SAVE_EVERY == 0:
                pd.DataFrame(results).to_csv("Birth_results.csv", index=False)
                print("💾 Partial save done")

# Final save
pd.DataFrame(results).to_csv("Birth_results.psv", index=False)
print("🎯 Final save completed")