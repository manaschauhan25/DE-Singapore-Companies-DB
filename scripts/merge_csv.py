import pandas as pd
import glob
import os

# ✅ Your input folder
input_path = "data/temp/*.csv"   # adjust path

# ✅ Step 1: Read all CSV files
all_files = glob.glob(input_path)

dfs = []
for file in all_files:
    df = pd.read_csv(file)
    dfs.append(df)

# ✅ Step 2: Merge them into one DataFrame
merged_df = pd.concat(dfs, ignore_index=True)

# ✅ Step 3: Deduplicate using UEN
dedup_df = merged_df.drop_duplicates(subset=["uen"], keep="first")

# ✅ Step 4: Save output
dedup_df.to_csv("data/bronze/recordowld/recordowl.csv", index=False)

print("✅ Done! Merged & deduplicated CSV saved as acra_merged_dedup.csv")
