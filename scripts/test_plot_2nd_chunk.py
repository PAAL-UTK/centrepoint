import matplotlib.pyplot as plt
import polars as pl
import duckdb

# Connect and pull the subject
con = duckdb.connect("dwh/imu.duckdb")
result = con.execute("SELECT subject_id FROM imu LIMIT 1").fetchone()
if result is None:
    raise ValueError("No subjects found in imu table — is the DB populated?")
subject_id = result[0]

# Define chunking
chunk_size = 7680  # 10 min @ 128 Hz
overlap = 128      # 1 sec overlap
offset = 2 * (chunk_size - overlap)

# Load 2nd chunk
raw_df = con.execute(f"""
    SELECT ts, gyroscope_x FROM imu
    WHERE subject_id = {subject_id}
    ORDER BY ts
    LIMIT {chunk_size} OFFSET {offset}
""").pl()

filtered_df = con.execute(f"""
    SELECT ts, gyroscope_x FROM filtered_imu
    WHERE subject_id = {subject_id}
    ORDER BY ts
    LIMIT {chunk_size} OFFSET {offset}
""").pl()

con.close()

# Plot
plt.figure(figsize=(12, 5))
plt.plot(raw_df["ts"], raw_df["gyroscope_x"], label="Raw", alpha=0.6)
plt.plot(filtered_df["ts"], filtered_df["gyroscope_x"], label="Filtered", linewidth=2)

# Highlight overlap region
overlap_ts_start = raw_df["ts"][0]
overlap_ts_end = raw_df["ts"][overlap]
plt.axvspan(overlap_ts_start, overlap_ts_end, color='grey', alpha=0.2, label="Overlap")

plt.xlabel("Timestamp (ts)")
plt.ylabel("Gyroscope X")
plt.title("Third Chunk - Gyroscope X Axis: Raw vs Filtered")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("third_chunk_raw_vs_filtered.png", dpi=300)
print("✅ Saved: second_chunk_raw_vs_filtered.png")

