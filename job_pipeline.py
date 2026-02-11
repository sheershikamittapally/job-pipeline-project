import pandas as pd

# Project Notes:
# - Salary columns excluded due to high missingness.
# - original_listed_time kept in raw epoch form; derived datetime column created.
# - is_remote derived from remote_allowed indicator.
# - posting_age_days measures recency relative to newest posting in dataset.

# Step 1: Load the dataset
df = pd.read_csv("data/postings.csv")

# Step 2: Inspect the dataset
print(df.shape)       # rows, columns
print(df.columns)     # column names
print(df.head())      # first 5 rows

# Step 3: Select core columns (a smaller working dataset)
keep_cols = [
    "job_id",
    "company_name",
    "title",
    "description",
    "location",
    "work_type",
    "formatted_experience_level",
    "remote_allowed",
    "min_salary",
    "med_salary",
    "max_salary",
    "pay_period",
    "currency",
    "original_listed_time"
]

df = df[keep_cols].copy()

print(df.shape)
print(df.head())

# Step 4: Check missing values in core columns
core_missing = (
    df[keep_cols]
    .isna()
    .sum()
    .sort_values(ascending=False)
)

core_missing_pct = (
    df[keep_cols]
    .isna()
    .mean()
    .mul(100)
    .round(2)
    .sort_values(ascending=False)
)

missing_summary = pd.DataFrame({
    "missing_count": core_missing,
    "missing_pct": core_missing_pct
})

print("\nMissing values in core columns:")
print(missing_summary)

# Step 5: Drop columns with too much missing data

drop_cols = [
    "med_salary",
    "remote_allowed",
    "min_salary",
    "max_salary",
    "pay_period",
    "currency"
]

df_clean = df.drop(columns=drop_cols)

print(df_clean.shape)
print(df_clean.columns)

# Step 6: Drop rows missing critical fields

critical_cols = ["job_id", "title", "company_name", "location"]

df_clean = df_clean.dropna(subset=critical_cols)

print(df_clean.shape)

# Step 7: Standardize text fields

text_cols = ["company_name", "title", "location", "work_type"]

for col in text_cols:
    df_clean[col] = df_clean[col].astype(str).str.strip().str.lower()

print(df_clean[text_cols].head())

# Step 8: Convert listed_time to datetime

# df_clean["listed_time"] = pd.to_datetime(df_clean["listed_time"], errors="coerce")

# print(df_clean["listed_time"].dtype)
# print(df_clean[["listed_time"]].head())

# Step 8: Convert listed_time to datetime (initial attempt)

# This produced 1970-01-01 because pandas assumed nanoseconds
# df_clean["listed_time"] = pd.to_datetime(df_clean["listed_time"], errors="coerce")

# Step 8 (fixed): Convert listed_time from timestamp (ms) to datetime

# df_clean["listed_time"] = pd.to_datetime(
    # df_clean["listed_time"],
    # unit="ns",
    # errors="coerce"
# )

# print(df_clean["listed_time"].dtype)
# print(df_clean[["listed_time"]].head())

# print(df_clean["listed_time"].head(10))
# print(df_clean["listed_time"].describe())

# NOTE (learning): listed_time converted to 1970, so it’s not a true posting timestamp.
# Using original_listed_time instead.

# Step 8 (updated): Convert original_listed_time (epoch milliseconds) to datetime

df_clean["original_listed_time"] = pd.to_datetime(
    df_clean["original_listed_time"],
    unit="ms",
    errors="coerce"
)

# quick check
print(df_clean["original_listed_time"].head())
print(df_clean["original_listed_time"].min(), "→", df_clean["original_listed_time"].max())

# Step 9: Feature Engineering – Derived Columns

df["is_remote"] = df["work_type"].str.contains("remote", na=False)

print(df["is_remote"].value_counts())

# quick check
print(df["work_type"].value_counts().head(10))

# Step 9A: Inspect columns related to remote / location

possible_cols = [
    "remote_allowed",
    "formatted_work_type",
    "location",
    "job_location",
    "workplace_type"
]

for col in possible_cols:
    if col in df.columns:
        print(f"\nColumn: {col}")
        print(df[col].value_counts().head(10))

# Step 9B: Feature Engineering - Define is_remote

df["is_remote"] = df["remote_allowed"] == 1

print(df["is_remote"].value_counts())

# Step 10: Feature Engineering – Define posting_age_days

# 1) Convert epoch milliseconds -> datetime
df["original_listed_time_dt"] = pd.to_datetime(
    df["original_listed_time"],
    unit="ms",
    errors="coerce"
)

# 2) Find the most recent posting date
max_posting_date = df["original_listed_time_dt"].max()

# 3) Compute age as a timedelta (max - each row)
posting_age_timedelta = max_posting_date - df["original_listed_time_dt"]

# 4) Convert timedelta -> integer days
df["posting_age_days"] = posting_age_timedelta.dt.days

# 5) Quick checks
print("dtype original_listed_time_dt:", df["original_listed_time_dt"].dtype)
print(df["posting_age_days"].describe())

# Step 11A: Analysis – Remote share

# Total number of job postings
total_postings = len(df)

# Number of remote postings (True values count as 1)
remote_postings = df["is_remote"].sum()

# Percentage of remote postings
percent_remote = (remote_postings / total_postings) * 100

print("Total postings:", total_postings)
print("Remote postings:", remote_postings)
print("Percent remote:", round(percent_remote, 2), "%")

# Step 11B: Analysis – Posting age by remote status

avg_posting_age = (
    df
    .groupby("is_remote")["posting_age_days"]
    .mean()
)

print(avg_posting_age)

# Step 11C: Analysis - Remote status by work type

avg_remote_jobs = (
    df
    .groupby("work_type")["is_remote"]
    .mean()
    .sort_values(ascending=False)
)

print(avg_remote_jobs)

# Step 12: Outputs — Write artifacts to disk (GitHub-friendly)

from pathlib import Path

# Create output directory for pipeline artifacts (idempotent)
output_dir = Path("output")
output_dir.mkdir(exist_ok=True)

# Save a small, shareable sample of the cleaned dataset (avoid committing huge files)
df.head(5000).to_csv(output_dir / "clean_postings_sample.csv", index=False)

# Export remote share by work type (mean of boolean is_remote = proportion remote)
remote_share_by_work_type = (
    avg_remote_jobs
    .rename("remote_share")
    .reset_index()
    .sort_values("remote_share", ascending=False)
)
remote_share_by_work_type.to_csv(output_dir / "remote_share_by_work_type.csv", index=False)

# Export average posting age (days) by remote status
if "avg_posting_age" in globals():
    posting_age_by_remote = (
        avg_posting_age
        .rename("avg_posting_age_days")
        .reset_index()
        .sort_values("avg_posting_age_days")
    )
    posting_age_by_remote.to_csv(output_dir / "posting_age_by_remote.csv", index=False)

print("Step 12 complete ✅ Outputs written to ./output/")