import io
import zipfile
import requests
import pandas as pd

from datetime import date

from pathlib import Path
Path("data").mkdir(exist_ok=True)

base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
pkg = requests.get(
    base_url + "/api/3/action/package_show",
    params={"id": "merged-gtfs-ttc-routes-and-schedules"},
    timeout=60
).json()

resources = pkg["result"]["resources"]

# Heuristic: find the first GTFS zip
gtfs_res = None
for r in resources:
    url = (r.get("url") or "").lower()
    name = (r.get("name") or "").lower()
    if url.endswith(".zip") and ("gtfs" in url or "gtfs" in name):
        gtfs_res = r
        break

if not gtfs_res:
    raise RuntimeError("Couldn't find a GTFS zip resource in the package resources.")

gtfs_url = gtfs_res["url"]
print("Downloading:", gtfs_url)

zbytes = requests.get(gtfs_url, timeout=120).content
zf = zipfile.ZipFile(io.BytesIO(zbytes))

def read_txt(filename):
    with zf.open(filename) as f:
        return pd.read_csv(f, low_memory=False)

routes = read_txt("routes.txt")
trips = read_txt("trips.txt")
stop_times = read_txt("stop_times.txt")
calendar = read_txt("calendar.txt")

weekday_service_ids = set(
    calendar[
        (calendar["monday"] == 1) &
        (calendar["tuesday"] == 1) &
        (calendar["wednesday"] == 1) &
        (calendar["thursday"] == 1) &
        (calendar["friday"] == 1) &
        (calendar["saturday"] == 0) &
        (calendar["sunday"] == 0)
    ]["service_id"].astype(str)
)

# Bus only: in GTFS, route_type 3 = bus
bus_routes = routes[routes["route_type"] == 3][["route_id", "route_short_name", "route_long_name"]]

bus_trips = trips.merge(bus_routes, on="route_id", how="inner")

# Convert HH:MM:SS (can exceed 24h) to seconds
def hms_to_seconds(s):
    h, m, sec = s.split(":")
    return int(h)*3600 + int(m)*60 + int(sec)

def time_band_from_sec(sec: float) -> str:
    s = int(sec) % 86400
    hour = s // 3600

    if 6 <= hour < 9:
        return "AM Peak (6–9)"
    if 9 <= hour < 15:
        return "Midday (9–15)"
    if 15 <= hour < 19:
        return "PM Peak (15–19)"
    if 19 <= hour < 24:
        return "Evening (19–24)"
    return "Overnight (0–6)"

for col in ["arrival_time", "departure_time"]:
    stop_times[col + "_sec"] = stop_times[col].astype(str).map(hms_to_seconds)

# Compute per-trip runtime using first departure and last arrival
trip_first = stop_times.sort_values(["trip_id", "stop_sequence"]).groupby("trip_id").first()
trip_last  = stop_times.sort_values(["trip_id", "stop_sequence"]).groupby("trip_id").last()

runtime = (
    pd.DataFrame({
        "trip_id": trip_first.index,
        "start_sec": trip_first["departure_time_sec"].values,
        "end_sec": trip_last["arrival_time_sec"].values,
        "first_stop_id": trip_first["stop_id"].values,
        "last_stop_id": trip_last["stop_id"].values
    })
)

runtime["runtime_min"] = (runtime["end_sec"] - runtime["start_sec"]) / 60.0

bus_trip_runtime = bus_trips.merge(runtime, on="trip_id", how="inner")

bus_trip_runtime["service_id"] = bus_trip_runtime["service_id"].astype(str)
bus_trip_runtime = bus_trip_runtime[
    bus_trip_runtime["service_id"].isin(weekday_service_ids)
].copy()


bus_trip_runtime["time_band"] = bus_trip_runtime["start_sec"].map(time_band_from_sec)

# Quick sanity + sample ranking by median runtime (not your final metric)
summary = (bus_trip_runtime
           .groupby(["route_id","route_short_name","direction_id","time_band"])
           .agg(trips=("trip_id","count"),
                median_runtime_min=("runtime_min","median"),
                p10_runtime_min=("runtime_min", lambda x: x.quantile(0.10)),
                p90_runtime_min=("runtime_min", lambda x: x.quantile(0.90)))
           .reset_index()
           .sort_values(["trips"], ascending=False)
)

summary["runtime_spread_min"] = (
    summary["p90_runtime_min"] - summary["p10_runtime_min"]
)

# Compute layovers between consecutive trips in the same vehicle block
block_trips = bus_trip_runtime.copy()

# Sort trips in the order each vehicle runs them
block_trips = block_trips.sort_values(
    ["service_id", "block_id", "start_sec"]
)

# Start time of the next trip in the same vehicle block
block_trips["next_start_sec"] = (
    block_trips
    .groupby(["service_id", "block_id"])["start_sec"]
    .shift(-1)
)

block_trips["next_first_stop_id"] = (
    block_trips
    .groupby(["service_id", "block_id"])["first_stop_id"]
    .shift(-1)
)

# Layover = gap between end of this trip and start of next
block_trips["layover_min"] = (
    block_trips["next_start_sec"] - block_trips["end_sec"]
) / 60.0

block_trips["time_band"] = block_trips["end_sec"].map(time_band_from_sec)

block_trips = block_trips.dropna(subset=["next_start_sec", "next_first_stop_id"])

# Keep only reasonable layovers (ignore long breaks)
layovers = block_trips[
    (block_trips["layover_min"] >= 0) &
    (block_trips["layover_min"] <= 120) &
    (block_trips["last_stop_id"] == block_trips["next_first_stop_id"])
]

# Route-level layover summary
layover_summary = (
    layovers
    .groupby(["route_id", "route_short_name", "direction_id","time_band"])
    .agg(
        layovers=("layover_min", "count"),
        median_layover_min=("layover_min", "median"),
        p10_layover_min=("layover_min", lambda x: x.quantile(0.10)),
        p90_layover_min=("layover_min", lambda x: x.quantile(0.90)),
    )
    .reset_index()
    .sort_values("median_layover_min")
)

fragility_base = summary.merge(
    layover_summary,
    on=["route_id","route_short_name","direction_id","time_band"],
    how="left"
)

# Fill missing layovers with a "safe" value (means not fragile on this axis)
fragility_base["median_layover_min"] = fragility_base["median_layover_min"].fillna(10)

# --- Robust normalizer for variability ---
# Use a robust scale: median + 2*IQR (stable, not super sensitive)
spread = fragility_base["runtime_spread_min"].clip(lower=0)

q50 = spread.quantile(0.50)
iqr = spread.quantile(0.75) - spread.quantile(0.25)
k = float(q50 + 2 * iqr) if iqr > 0 else float(spread.quantile(0.90))
k = max(k, 1.0)

# --- Component scores (both in [0,1), never exactly 1) ---
lay = fragility_base["median_layover_min"].clip(lower=0)

# Layover: 0 -> ~0.95, then decays (prevents instant max)
fragility_base["layover_score"] = 0.95 / (1 + lay)

# Variability: bounded growth, never 1
fragility_base["runtime_score"] = spread / (spread + k)

# --- Composite score ---
# Weights sum to 95 so the top is <= 95%
fragility_base["fragility_score"] = (
    35 * fragility_base["layover_score"] +
    60 * fragility_base["runtime_score"]
).round(2)

def explain(row):
    parts = []
    if row["median_layover_min"] <= 1:
        parts.append("no recovery time")
    elif row["median_layover_min"] <= 3:
        parts.append("very low recovery time")

    if row["runtime_spread_min"] >= fragility_base["runtime_spread_min"].quantile(0.90):
        parts.append("high schedule variability")

    return ", ".join(parts) if parts else "moderate"

fragility_base["why"] = fragility_base.apply(explain, axis=1)

MIN_TRIPS = 50
ranked = fragility_base[fragility_base["trips"] >= MIN_TRIPS] \
    .sort_values("fragility_score", ascending=False)

print("\nMost fragile bus routes (Is My Bus Lying?):")
print(
    ranked[
        [
            "route_short_name",
            "direction_id",
            "fragility_score",
            "median_layover_min",
            "runtime_spread_min",
            "trips",
            "why"
        ]
    ]
    .head(15)
    .to_string(
        index=False,
        col_space={
            "route_short_name": 6,
            "direction_id": 3,
            "fragility_score": 6,
            "median_layover_min": 6,
            "runtime_spread_min": 6,
            "trips": 5,
            "why": 35,
        }
    )
)

# =========================
# FINAL OUTPUT
# =========================

today = date.today().isoformat()  # e.g. 2026-01-14

out = fragility_base.sort_values(
    ["route_id", "direction_id", "time_band"],
    ascending=True
)

out["data_updated_date"] = today

dated_path = f"data/fragility_by_timeband_{today}.csv"
latest_path = "data/fragility_by_timeband_latest.csv"

out.to_csv(dated_path, index=False)
out.to_csv(latest_path, index=False)

print(f"Saved {dated_path}")
print(f"Saved {latest_path}")
