import matplotlib.pyplot as plt
import polars as pl
import duckdb

con = duckdb.connect("dwh/imu.duckdb")
result = con.execute("SELECT subject_id FROM imu LIMIT 1").fetchone()
if result is None:
    raise ValueError("No subjects found in imu table — is the DB populated?")
subject_id = result[0]

# First 10 minutes @ 128 Hz = 7680 samples
raw_df = con.execute(f"""
    SELECT ts, gyroscope_x FROM imu
    WHERE subject_id = {subject_id}
    ORDER BY ts
    LIMIT 7680
""").pl()

filtered_df = con.execute(f"""
    SELECT ts, gyroscope_x FROM filtered_imu
    WHERE subject_id = {subject_id}
    ORDER BY ts
    LIMIT 7680
""").pl()

con.close()

# Plot
plt.figure(figsize=(12, 5))
plt.plot(raw_df["ts"], raw_df["gyroscope_x"], label="Raw", alpha=0.6)
plt.plot(filtered_df["ts"], filtered_df["gyroscope_x"], label="Filtered", linewidth=2)
plt.xlabel("Timestamp (ts)")
plt.ylabel("Gyroscope X")
plt.title("First Chunk - Gyroscope X Axis: Raw vs Filtered")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("first_chunk_raw_vs_filtered.png", dpi=300)
print("✅ Saved: first_chunk_raw_vs_filtered.png")

