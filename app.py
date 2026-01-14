# app.py
import os
import json
import time
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Is My Bus Lying? (TTC)", layout="wide")

# -------------------------
# Config
# -------------------------
CSV_PATH = Path("fragility_by_timeband.csv")
META_PATH = Path(".gtfs_meta.json")

CKAN_BASE_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
PACKAGE_ID = "merged-gtfs-ttc-routes-and-schedules"

# Time band ordering (must match exactly what your GTFS script outputs)
BAND_ORDER = [
    "Overnight (0–6)",
    "AM Peak (6–9)",
    "Midday (9–15)",
    "PM Peak (15–19)",
    "Evening (19–24)",
]

# If your CSV uses hyphens "-" instead of en-dashes "–", use this instead:
# BAND_ORDER = [
#     "Overnight (0-6)",
#     "AM Peak (6-9)",
#     "Midday (9-15)",
#     "PM Peak (15-19)",
#     "Evening (19-24)",
# ]

# -------------------------
# Helpers: GTFS freshness check
# -------------------------
def _load_meta() -> dict:
    if META_PATH.exists():
        try:
            return json.loads(META_PATH.read_text())
        except Exception:
            return {}
    return {}

def _save_meta(meta: dict) -> None:
    META_PATH.write_text(json.dumps(meta, indent=2, sort_keys=True))

@st.cache_data(ttl=3600)
def get_gtfs_zip_url() -> str:
    """Find the GTFS ZIP URL from the CKAN package metadata."""
    resp = requests.get(
        f"{CKAN_BASE_URL}/api/3/action/package_show",
        params={"id": PACKAGE_ID},
        timeout=30,
    )
    resp.raise_for_status()
    pkg = resp.json()
    resources = pkg["result"]["resources"]

    # Prefer a zip that looks like GTFS
    for r in resources:
        url = (r.get("url") or "").lower()
        name = (r.get("name") or "").lower()
        if url.endswith(".zip") and ("gtfs" in url or "gtfs" in name):
            return r["url"]

    # Fallback: first zip
    for r in resources:
        url = (r.get("url") or "").lower()
        if url.endswith(".zip"):
            return r["url"]

    raise RuntimeError("Could not find a GTFS zip resource in the CKAN package.")

@st.cache_data(ttl=3600)
def get_remote_signature(gtfs_url: str) -> dict:
    """Return a lightweight signature of remote file freshness using HEAD headers."""
    r = requests.head(gtfs_url, timeout=30, allow_redirects=True)
    r.raise_for_status()

    # Common freshness hints
    etag = r.headers.get("ETag") or r.headers.get("Etag")
    last_modified = r.headers.get("Last-Modified")
    content_length = r.headers.get("Content-Length")

    # Sometimes CDNs don’t provide ETag/Last-Modified; length at least helps.
    return {
        "gtfs_url": gtfs_url,
        "etag": etag,
        "last_modified": last_modified,
        "content_length": content_length,
    }

def signature_changed(old: dict, new: dict) -> bool:
    """Decide whether remote file changed since last build."""
    # If we never recorded anything, treat as changed.
    if not old:
        return True

    # Compare strongest fields first
    for k in ("etag", "last_modified", "content_length", "gtfs_url"):
        if (old.get(k) or "") != (new.get(k) or ""):
            return True
    return False

def rebuild_csv() -> None:
    """Run your GTFS pipeline to regenerate the CSV."""
    # This runs the heavy compute script. It must produce fragility_by_timeband.csv.
    subprocess.run([sys.executable, "gtfs_bus_schedule_fragility.py"], check=True)

# -------------------------
# Streamlit: auto-update logic (Option 2)
# -------------------------
st.title("Is My Bus Lying? (TTC Bus Schedule Fragility)")
st.caption("Auto-rebuilds the local CSV only if the TTC GTFS ZIP appears to have changed.")

with st.sidebar:
    st.header("Data")
    force_rebuild = st.button("Force rebuild now")
    show_debug = st.toggle("Show update debug", value=False)

# Try to decide whether we should rebuild
needs_rebuild = force_rebuild or (not CSV_PATH.exists())

meta = _load_meta()
remote_sig = {}
gtfs_url = None

try:
    gtfs_url = get_gtfs_zip_url()
    remote_sig = get_remote_signature(gtfs_url)

    if signature_changed(meta.get("remote_signature", {}), remote_sig):
        # Remote changed → rebuild
        needs_rebuild = True

except Exception as e:
    # If we can't check freshness, we don't want to crash the app.
    # If CSV exists, keep going. If it doesn't, we must stop.
    if not CSV_PATH.exists():
        st.error(f"Could not check GTFS freshness and no CSV exists yet.\n\nError: {e}")
        st.stop()
    else:
        if show_debug:
            st.warning(f"Freshness check failed; using existing CSV.\n\nError: {e}")

if needs_rebuild:
    with st.spinner("Updating data: downloading GTFS + rebuilding CSV..."):
        try:
            rebuild_csv()
            # Record metadata after successful rebuild
            meta_out = {
                "rebuilt_at_epoch": int(time.time()),
                "remote_signature": remote_sig or {},
            }
            _save_meta(meta_out)
            st.sidebar.success("Data updated.")
        except subprocess.CalledProcessError as e:
            st.sidebar.error(f"Rebuild failed. Using existing CSV if available.\n\n{e}")
            if not CSV_PATH.exists():
                st.stop()

if show_debug:
    st.sidebar.subheader("Update debug")
    st.sidebar.write("CSV exists:", CSV_PATH.exists())
    st.sidebar.write("GTFS URL:", gtfs_url)
    st.sidebar.write("Remote signature:", remote_sig)
    st.sidebar.write("Stored meta:", meta)

# -------------------------
# Load CSV
# -------------------------
@st.cache_data
def load_data() -> pd.DataFrame:
    df = pd.read_csv(CSV_PATH)

    # Normalize types for sorting/filtering and for your requested integer columns
    for c in ["route_id", "direction_id", "trips"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Ensure time band order is chronological, not alphabetical
    if "time_band" in df.columns:
        df["time_band"] = pd.Categorical(df["time_band"], categories=BAND_ORDER, ordered=True)

    # Keep score in 0..100
    if "fragility_score" in df.columns:
        df["fragility_score"] = pd.to_numeric(df["fragility_score"], errors="coerce").clip(0, 100)

    return df

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

# Filter (optional search)
f = df.copy()
if route_search:
    # Search route_id by substring for convenience, without needing route_short_name
    # Convert to int-safe string (no .0)
    rid = pd.to_numeric(f["route_id"], errors="coerce").fillna(-1).astype(int).astype(str)
    f = f[rid.str.contains(route_search, na=False)].copy()

# Sort: route_id asc, direction_id asc (0 then 1), time_band in your specified order
sort_cols = [c for c in ["route_id", "direction_id", "time_band"] if c in f.columns]
if sort_cols:
    f = f.sort_values(sort_cols, ascending=True)

# -------------------------
# Columns: remove route_short_name as requested
# -------------------------
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

# Score as percent string (2 decimals) for display and download
if "fragility_score" in display.columns:
    display["fragility_score"] = pd.to_numeric(display["fragility_score"], errors="coerce").clip(0, 100).round(2)

st.write(f"Rows shown: {len(display):,}")

# Display with formatting (percent sign for score)
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