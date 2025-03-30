# scripts/test_load_data.py

from centrepoint.utils.load import load_data

# Change these if you want to test another subject or category
data_category = "temperature"
subject_identifier = "PIL_JAM_LW"
file_format = "parquet" # or "avro"

print(f"📂 Loading {file_format.upper()} data for {subject_identifier} in '{data_category}'...")

df = load_data(data_category, subject_identifier, file_format=file_format)

print("✅ Data loaded successfully")
print("📐 Schema:")
print(df.schema)
print("📏 Shape:", df.shape)
print("🔍 Preview:")
print(df.head(5))
