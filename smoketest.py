#!/usr/bin/env python3
"""
Smoke test: query live FixMyStreet Open311 API for Cambridgeshire reports.
Covers Cambridge City, Cambridgeshire County, and South Cambridgeshire
District (which includes Great Shelford, ~52.134°N 0.121°E).

Findings from probing the live API:
  - Responses are wrapped: {"service_requests":[...]} and {"services":[...]}
  - agency_responsible is {"recipient": ["Council Name"]} (not MaPit IDs)
  - Reports are sorted newest-first; paginate via rolling start_date/end_date
"""

import json
import sys
import urllib.request
import urllib.parse
from collections import Counter
from datetime import datetime, timezone, timedelta

BASE_URL = "https://www.fixmystreet.com/open311/v2"
JURISDICTION = "fixmystreet"

# Council name substrings to match in agency_responsible.recipient
CAMBRIDGE_COUNCILS = [
    "Cambridgeshire",
    "Cambridge City",
    "South Cambridgeshire",
]

# Bounding box: north Cambridge down to (and including) Great Shelford
BBOX = {
    "north": 52.260,
    "south": 52.120,   # Great Shelford lat ~52.134
    "west":  -0.060,
    "east":   0.300,
}


# ------------------------------------------------------------------ #
# HTTP helpers                                                         #
# ------------------------------------------------------------------ #

def fetch_json(url: str) -> dict | list:
    print(f"  GET {url}")
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def build_url(path: str, params: dict) -> str:
    return f"{BASE_URL}/{path}?" + urllib.parse.urlencode(params)


# ------------------------------------------------------------------ #
# API calls (handle the service_requests / services wrappers)         #
# ------------------------------------------------------------------ #

def get_services(lat: float, lon: float) -> list[dict]:
    url = build_url("services.json", {
        "jurisdiction_id": JURISDICTION,
        "lat": lat,
        "long": lon,
    })
    data = fetch_json(url)
    if isinstance(data, dict):
        return data.get("services", [])
    return data or []


def get_requests(status: str, start: str, end: str | None = None) -> list[dict]:
    params = {
        "jurisdiction_id": JURISDICTION,
        "status": status,
        "start_date": start,
    }
    if end:
        params["end_date"] = end
    url = build_url("requests.json", params)
    data = fetch_json(url)
    if isinstance(data, dict):
        return data.get("service_requests", [])
    return data or []


# ------------------------------------------------------------------ #
# Filtering helpers                                                    #
# ------------------------------------------------------------------ #

def is_cambridge_report(report: dict) -> bool:
    ag = report.get("agency_responsible", {})
    recipients = ag.get("recipient", []) if isinstance(ag, dict) else [str(ag)]
    return any(
        any(name in r for name in CAMBRIDGE_COUNCILS)
        for r in recipients
    )


def in_bbox(report: dict) -> bool:
    try:
        lat = float(report.get("lat") or 0)
        lon = float(report.get("long") or 0)
        return (
            BBOX["south"] <= lat <= BBOX["north"]
            and BBOX["west"] <= lon <= BBOX["east"]
        )
    except (TypeError, ValueError):
        return False


def council_name(report: dict) -> str:
    ag = report.get("agency_responsible", {})
    if isinstance(ag, dict):
        recipients = ag.get("recipient", [])
        return ", ".join(recipients) if recipients else "Unknown"
    return str(ag) or "Unknown"


# ------------------------------------------------------------------ #
# Pagination: fetch all Cambridge reports across a date window        #
# ------------------------------------------------------------------ #

def fetch_cambridge_reports(days_back: int = 180) -> tuple[list, list]:
    """
    Page through the API in 30-day windows to collect Cambridge reports.
    Returns (cambridge_reports, all_fetched_reports).
    The global feed is UK-wide; we filter client-side.
    """
    cambridge = []
    all_reports = []

    now = datetime.now(timezone.utc)
    window_end = now
    window_days = 30
    windows_fetched = 0
    max_windows = days_back // window_days

    print(f"\n  Paginating in {window_days}-day windows (up to {max_windows} pages):")

    while windows_fetched < max_windows:
        window_start = window_end - timedelta(days=window_days)
        start_str = window_start.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        end_str = window_end.strftime("%Y-%m-%dT%H:%M:%S+00:00")

        batch = get_requests("open", start_str, end_str)
        all_reports.extend(batch)

        cambs_batch = [r for r in batch if is_cambridge_report(r)]
        cambridge.extend(cambs_batch)

        print(f"    {start_str[:10]} → {end_str[:10]}  "
              f"page={len(batch):4}  cambridge={len(cambs_batch):3}  "
              f"running_total={len(cambridge)}")

        windows_fetched += 1
        window_end = window_start  # slide backwards

        if len(batch) == 0:
            print("    (empty page — stopping early)")
            break

    return cambridge, all_reports


# ------------------------------------------------------------------ #
# Display helpers                                                      #
# ------------------------------------------------------------------ #

def section(title: str):
    print(f"\n{'='*62}")
    print(f"  {title}")
    print(f"{'='*62}")


def bar(label: str, count: int, total: int, width: int = 28):
    filled = int(width * count / total) if total else 0
    pct = 100 * count / total if total else 0
    print(f"  {label:<36} {'█'*filled:<{width}} {count:>4}  ({pct:.1f}%)")


# ------------------------------------------------------------------ #
# Main                                                                 #
# ------------------------------------------------------------------ #

def main():
    print("\nFixMyStreet Cambridgeshire — Live API Smoke Test")
    print(f"Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # ---------------------------------------------------------------- #
    # 1. Service categories at Great Shelford                           #
    # ---------------------------------------------------------------- #
    section("1. Report categories available at Great Shelford (52.134, 0.121)")
    try:
        services = get_services(52.134, 0.121)
        print(f"\n  {len(services)} categories:\n")
        for svc in sorted(services, key=lambda s: s.get("service_name", "")):
            print(f"  {svc.get('service_name','?')}")
    except Exception as e:
        print(f"  ERROR: {e}")
        services = []

    # ---------------------------------------------------------------- #
    # 2. Fetch Cambridge reports (paginated, last 180 days)             #
    # ---------------------------------------------------------------- #
    section("2. Fetching open Cambridgeshire reports (last 180 days)")
    try:
        cambridge_reports, all_reports = fetch_cambridge_reports(days_back=180)
    except Exception as e:
        print(f"\n  ERROR: {e}")
        sys.exit(1)

    print(f"\n  Total UK reports scanned:  {len(all_reports):,}")
    print(f"  Cambridgeshire reports:    {len(cambridge_reports):,}")

    bbox_reports = [r for r in cambridge_reports if in_bbox(r)]
    print(f"  Within bbox (to Gt Shelford): {len(bbox_reports):,}")

    if not cambridge_reports:
        print("\n  No Cambridgeshire reports found — try increasing days_back.")
        sys.exit(0)

    total = len(cambridge_reports)

    # ---------------------------------------------------------------- #
    # 3. By council                                                     #
    # ---------------------------------------------------------------- #
    section("3. Reports by council")
    council_counts: Counter = Counter(council_name(r) for r in cambridge_reports)
    for name, count in council_counts.most_common():
        bar(name[:36], count, total)

    # ---------------------------------------------------------------- #
    # 4. By category                                                    #
    # ---------------------------------------------------------------- #
    section("4. Reports by category")
    cat_counts: Counter = Counter(
        r.get("service_name") or "Unknown" for r in cambridge_reports
    )
    for name, count in cat_counts.most_common(25):
        bar(name[:36], count, total)
    if len(cat_counts) > 25:
        print(f"  ... and {len(cat_counts) - 25} more categories")

    # ---------------------------------------------------------------- #
    # 5. Weekly volume                                                  #
    # ---------------------------------------------------------------- #
    section("5. Report volume by week")
    week_counts: Counter = Counter()
    for r in cambridge_reports:
        try:
            dt = datetime.fromisoformat(r.get("requested_datetime", ""))
            week_counts[dt.strftime("%Y-W%W")] += 1
        except (ValueError, TypeError):
            pass

    if week_counts:
        max_wk = max(week_counts.values())
        for week in sorted(week_counts):
            bar(week, week_counts[week], max_wk)

    # ---------------------------------------------------------------- #
    # 6. Photo/media prevalence                                         #
    # ---------------------------------------------------------------- #
    section("6. Reports with photos")
    with_photo = sum(1 for r in cambridge_reports if r.get("media_url"))
    bar("Has photo", with_photo, total)
    bar("No photo", total - with_photo, total)

    # ---------------------------------------------------------------- #
    # 7. Interface breakdown (how people reported)                      #
    # ---------------------------------------------------------------- #
    section("7. Reporting interface (mobile / web / etc.)")
    iface_counts: Counter = Counter(
        r.get("interface_used") or "Unknown" for r in cambridge_reports
    )
    for name, count in iface_counts.most_common():
        bar(name[:36], count, total)

    # ---------------------------------------------------------------- #
    # 8. Sample reports in bbox                                         #
    # ---------------------------------------------------------------- #
    section("8. Sample reports within bounding box (first 8)")
    samples = bbox_reports[:8] if bbox_reports else cambridge_reports[:8]
    label = "bbox" if bbox_reports else "all Cambridge (no bbox hits)"
    print(f"\n  Showing {label}:\n")
    for r in samples:
        print(f"  ID:       {r.get('service_request_id')}")
        print(f"  Category: {r.get('service_name')}")
        print(f"  Council:  {council_name(r)}")
        print(f"  Location: {r.get('lat')}, {r.get('long')}")
        print(f"  Address:  {r.get('address','—')}")
        print(f"  Reported: {r.get('requested_datetime')}")
        desc = (r.get("description") or "").strip().replace("\n", " ")
        print(f"  Desc:     {desc[:110]}{'...' if len(desc)>110 else ''}")
        print()

    # ---------------------------------------------------------------- #
    # 9. Field population audit                                         #
    # ---------------------------------------------------------------- #
    section("9. Field population audit")
    all_keys = sorted(set().union(*(r.keys() for r in cambridge_reports)))
    print(f"\n  {'Field':<35} {'Populated':>10}  {'%':>6}")
    print(f"  {'-'*35} {'-'*10}  {'-'*6}")
    for key in all_keys:
        populated = sum(
            1 for r in cambridge_reports
            if r.get(key) not in (None, "", [], {})
        )
        pct = 100 * populated / total
        flag = "  ← sparse" if pct < 50 else ""
        print(f"  {key:<35} {populated:>10}  {pct:>5.1f}%{flag}")

    print(f"\n{'='*62}")
    print("  Smoke test complete.")
    print(f"{'='*62}\n")


if __name__ == "__main__":
    main()
