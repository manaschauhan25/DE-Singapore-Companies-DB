import re
import json

with open("data/bronze/html/rendered_acra_gov.html", "r", encoding="utf-8") as f:
    html = f.read()

# Match escaped-quote pattern like:
# datasetId\":\"d_...\",\"name\":\"ACRA Information on Corporate Entities ('X')\"
pattern = (
    r'datasetId\\":\\\"(d_[a-f0-9]+)\\\",\\\"name\\\":\\\"ACRA Information on Corporate Entities \(\'([A-Z]|Others)\'\)'
)

matches = re.findall(pattern, html)

dataset_map = {letter: dataset_id for dataset_id, letter in matches}

if dataset_map:
    for letter, dataset_id in dataset_map.items():
        print(f"{letter}: {dataset_id}")
    print(f"\n✅ Extracted {len(dataset_map)} dataset IDs (A–Z + Others)")
else:
    print("❌ No dataset IDs found — try confirming the escape style again (but this one should match).")

# Save to file
with open("data/bronze/acra/json/acra_dataset_ids.json", "w", encoding="utf-8") as f:
    json.dump(dataset_map, f, indent=2)
