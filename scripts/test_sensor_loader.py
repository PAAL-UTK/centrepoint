# scripts/test_sensor_loader.py

from pathlib import Path
from centrepoint.utils.sensor_loader import SensorLoader

SUBJECT_ID = "PIL_JAM_LW"
DATA_DIR = Path("data")
SENSOR = "imu"  # Try also with "imu", "temperature"

# Instantiate the loader
loader = SensorLoader(subject_id=SUBJECT_ID, sensor=SENSOR, data_dir=DATA_DIR)
print(loader)

print(f"\n--- Testing load_lazy() for {SENSOR} ---")
lazy_df = loader.load_lazy()
df = lazy_df.collect()
print("Collected LazyFrame:")
print(df.shape)
print(df.schema)
print(df.head())

print(f"\n--- Testing iter_days() for {SENSOR} ---")
for i, chunk in enumerate(loader.iter_days()):
    print(f"Chunk {i + 1}: {chunk.shape}")
    print(chunk.schema)
    print(chunk.head())
    if i == 1:  # Only show first 2 chunks for brevity
        break
