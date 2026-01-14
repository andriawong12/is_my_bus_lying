import pandas as pd
import streamlit as st

st.set_page_config(page_title="Is My Bus Lying? (TTC)", layout="wide")

CSV_URL = "https://raw.githubusercontent.com/andriawong12/is_my_bus_lying/main/data/fragility_by_timeband_latest.csv"

# Time band ordering (must match exactly what your GTFS script outputs)
BAND_ORDER = [
    "Overnight (0–6)",
    "AM Peak (6–9)",
    "Midday (9–15)",
    "PM Peak (15–19)",
    "Evening (19–24)",
]
# If your CSV uses hyphens instead of en-dashes, swap to:
# BAND_ORDER = ["Overnight (0-6)", "AM Peak (6-9)", "Midday (9-15)", "PM Peak (15-19)", "Evening (19-24)"]

@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    df = pd.read_csv(CSV_URL)

    # Ensure numeric columns are numeric
    for c in ["route_id", "direction_id", "trips", "fragility_score", "median_layover_min", "runtime_spread_min"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Order time bands chronologically
    if "time_band" in df.columns:
        df["time_band"] = pd.Categorical(df["time_band"], categories=BAND_ORDER, ordered=True)

    # Clamp score to 0..100
    if "fragility_score" in df.columns:
        df["fragility_score"] = df["fragility_score"].clip(0, 100)

    return df

st.title("Is My Bus Lying? (TTC Bus Schedule Fragility)")
st.caption("Data auto-updates via scheduled GitHub Actions. Toggle the table to view.")

df = load_data()

# -------------------------
# UI Controls (start empty + toggle)
# -------------------------
st.sidebar.header("Table")
show_table = st.sidebar.toggle("Show table (all rows)", value=False)

route_search = st.sidebar.text_input("Route ID contains (e.g., 74)", "").strip()

if not show_table:
    st.info("Table is hidden. Toggle **Show table (all rows)** to display it.")
    st.stop()

# Optional filter by route_id substring
f = df.copy()
if route_search:
    rid = pd.to_numeric(f["route_id"], errors="coerce").fillna(-1).astype(int).astype(str)
    f = f[rid.str.contains(route_search, na=False)].copy()

# Sort: route_id asc, direction_id asc (0 then 1), time_band in your specified order
sort_cols = [c for c in ["route_id", "direction_id", "time_band"] if c in f.columns]
if sort_cols:
    f = f.sort_values(sort_cols, ascending=True)

# Columns (route_short_name removed)
cols = [
    "route_id",
    "direction_id",
    "time_band",
    "fragility_score",
    "median_layover_min",
    "runtime_spread_min",
    "trips",
    "why",
]
cols = [c for c in cols if c in f.columns]
display = f[cols].copy()

# Whole numbers stay whole
for c in ["route_id", "direction_id", "trips"]:
    if c in display.columns:
        display[c] = pd.to_numeric(display[c], errors="coerce").round(0).astype("Int64")

# Round numeric metrics to 2 decimals
for c in ["median_layover_min", "runtime_spread_min"]:
    if c in display.columns:
        display[c] = pd.to_numeric(display[c], errors="coerce").round(2)

# Score rounded to 2 decimals, clamp to <= 100
if "fragility_score" in display.columns:
    display["fragility_score"] = (
        pd.to_numeric(display["fragility_score"], errors="coerce")
        .clip(0, 100)
        .round(2)
    )

st.write(f"Rows shown: {len(display):,}")

# Display formatting: score shown as percent with 2 decimals
fmt = {}
if "fragility_score" in display.columns:
    fmt["fragility_score"] = lambda x: "" if pd.isna(x) else f"{float(x):.2f}%"
for c in ["median_layover_min", "runtime_spread_min"]:
    if c in display.columns:
        fmt[c] = "{:.2f}"

st.dataframe(display.style.format(fmt, na_rep=""), use_container_width=True, height=650)

# Download CSV (pretty: score is percent string)
download_df = display.copy()
if "fragility_score" in download_df.columns:
    download_df["fragility_score"] = download_df["fragility_score"].map(
        lambda x: "" if pd.isna(x) else f"{float(x):.2f}%"
    )

st.download_button(
    "Download current table CSV",
    data=download_df.to_csv(index=False).encode("utf-8"),
    file_name="bus_fragility_table.csv",
    mime="text/csv",
)
