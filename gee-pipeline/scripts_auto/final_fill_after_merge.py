import pandas as pd

assert "scripts_auto" in __file__, "❌ Wrong script path"

MERGED = "gee-pipeline/outputs/merged/merged_dataset.parquet"
OUT = "gee-pipeline/outputs/merged/merged_dataset_FILLED.parquet"

def main():
    df = pd.read_parquet(MERGED)
    df["date"] = pd.to_datetime(df["date"])

    df = df.sort_values(["province", "date"])
    df = df.groupby("province").apply(
        lambda x: x.ffill().bfill()
    ).reset_index(drop=True)

    df.to_parquet(OUT)
    print("✅ Final fill complete")

if __name__ == "__main__":
    main()
