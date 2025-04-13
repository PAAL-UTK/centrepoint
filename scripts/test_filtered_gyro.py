import duckdb
import polars as pl
import matplotlib.pyplot as plt
from scipy.signal import butter, filtfilt

def butter_lowpass_filter(data, cutoff=35.0, fs=128.0, order=4):
    b, a = butter(order, cutoff / (0.5 * fs), btype='low', analog=False)
    return filtfilt(b, a, data)

# Connect to DuckDB and get sample data
con = duckdb.connect("dwh/imu.duckdb")
df = con.execute("""
    SELECT ts, gyroscope_x
    FROM imu
    WHERE subject_id = (SELECT subject_id FROM imu LIMIT 1)
    ORDER BY ts
    LIMIT 5000
""").pl()
con.close()

# Convert to numpy
ts = df["ts"].to_numpy()
raw = df["gyroscope_x"].to_numpy()
filtered = butter_lowpass_filter(raw)

# Plot
plt.figure(figsize=(12, 5))
plt.plot(ts, raw, label="Raw", alpha=0.6)
plt.plot(ts, filtered, label="Filtered", linewidth=2)
plt.xlabel("Timestamp")
plt.ylabel("Gyroscope X")
plt.title("Gyroscope X Axis: Raw vs Filtered")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("gyro_x_filtered_vs_raw.png", dpi=300)
print("✅ Plot saved as gyro_x_filtered_vs_raw.png")


