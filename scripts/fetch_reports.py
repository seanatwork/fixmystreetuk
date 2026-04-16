#!/usr/bin/env python3
"""
Fetch street issue reports from FixMyStreet for the
Cambridgeshire/Great Shelford bounding box and write:

  data/reports.json  — open (current) reports, categorised and colour-coded
  data/fixed.json    — resolved reports, lat/lon/id/title only (loaded on demand)

Bounding box: north Cambridge → Great Shelford (~52.134°N)
  lon_min, lat_min, lon_max, lat_max  (GeoJSON order)
"""

import json
import re
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

BBOX = "0.050,52.120,0.300,52.260"
FMS_AJAX = "https://www.fixmystreet.com/ajax"
DATA_DIR  = Path(__file__).parent.parent / "data"
OUT_OPEN  = DATA_DIR / "reports.json"
OUT_FIXED = DATA_DIR / "fixed.json"

CATEGORIES = [
    "Potholes",
    "Roads/highways",
    "Street lighting",
    "Pavements/footpaths",
    "Flytipping",
    "Trees",
    "Street cleaning",
    "Dog fouling",
    "Graffiti",
    "Road traffic signs",
    "Street nameplates",
    "Car parking",
    "Abandoned vehicles",
    "Bus stops",
    "Traffic lights",
    "Parks/landscapes",
    "Public toilets",
    "Other",
]

# Colour shown in the app per category
CATEGORY_COLOUR = {
    "Potholes":              "#e74c3c",
    "Roads/highways":        "#c0392b",
    "Street lighting":       "#f39c12",
    "Pavements/footpaths":   "#e67e22",
    "Flytipping":            "#27ae60",
    "Trees":                 "#2ecc71",
    "Street cleaning":       "#16a085",
    "Dog fouling":           "#8e44ad",
    "Graffiti":              "#9b59b6",
    "Road traffic signs":    "#2980b9",
    "Street nameplates":     "#3498db",
    "Car parking":           "#1abc9c",
    "Abandoned vehicles":    "#95a5a6",
    "Bus stops":             "#7f8c8d",
    "Traffic lights":        "#d35400",
    "Parks/landscapes":      "#229954",
    "Public toilets":        "#5d6d7e",
    "Other":                 "#808b96",
}


def fetch(params: dict) -> dict:
    url = FMS_AJAX + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "fms-cambridgeshire/1.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def parse_total(pagination_html: str) -> tuple[int, int]:
    """Return (end_item, total) from '1 to 66 of 66' text."""
    m = re.search(r"(\d+) to (\d+) of (\d+)", pagination_html)
    if m:
        return int(m.group(2)), int(m.group(3))
    return 0, 0


def fetch_category(category: str) -> list[dict]:
    """Fetch all open reports for one category, paginating as needed."""
    reports = []
    page = 1
    while True:
        params = {"bbox": BBOX, "filter_category": category}
        if page > 1:
            params["p"] = page

        data = fetch(params)
        pins = data.get("pins", [])
        if not pins:
            break

        for p in pins:
            reports.append({
                "id":       p[3],
                "lat":      p[0],
                "lon":      p[1],
                "title":    p[4],
                "category": category,
                "colour":   CATEGORY_COLOUR.get(category, "#808b96"),
            })

        end, total = parse_total(data.get("pagination", ""))
        if end >= total or end == 0:
            break
        page += 1
        time.sleep(0.3)

    return reports


def fetch_all_bbox(show_old: bool = False) -> list[dict]:
    """Fetch every page of pins from the bbox (no category filter)."""
    all_pins = []
    page = 1
    params: dict = {"bbox": BBOX}
    if show_old:
        params["show_old_reports"] = 1

    while True:
        if page > 1:
            params["p"] = page
        data = fetch(params)
        pins = data.get("pins", [])
        if not pins:
            break
        all_pins.extend(pins)
        end, total = parse_total(data.get("pagination", ""))
        print(f"    page {page:>3}  pins so far: {len(all_pins):>5} / {total}", end="\r")
        if end >= total or end == 0:
            break
        page += 1
        time.sleep(0.3)

    print()  # newline after \r progress
    return all_pins


def main() -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    DATA_DIR.mkdir(exist_ok=True)

    # ── Step 1: open reports, per category (gives us category + colour) ──
    print(f"Step 1 — open reports by category (bbox {BBOX})")
    open_reports: dict[int, dict] = {}

    for cat in CATEGORIES:
        reports = fetch_category(cat)
        new = 0
        for r in reports:
            if r["id"] not in open_reports:
                open_reports[r["id"]] = r
                new += 1
        print(f"  {cat:<30} {len(reports):>3}  ({new} new)")
        time.sleep(0.2)

    open_list = sorted(open_reports.values(), key=lambda r: r["id"], reverse=True)
    OUT_OPEN.write_text(json.dumps({
        "fetched_at": now,
        "bbox":  [0.050, 52.120, 0.300, 52.260],
        "count": len(open_list),
        "reports": open_list,
    }, indent=2))
    print(f"\n✓ {len(open_list)} open reports → {OUT_OPEN}")

    # ── Step 2: all reports incl. resolved (bbox only, no category filter) ──
    print(f"\nStep 2 — all reports including resolved …")
    all_pins = fetch_all_bbox(show_old=True)

    open_ids = set(open_reports.keys())
    fixed_list = [
        {"id": p[3], "lat": p[0], "lon": p[1], "title": p[4]}
        for p in all_pins
        if p[3] not in open_ids
    ]
    # Deduplicate (same ID can appear across pages)
    seen: set[int] = set()
    fixed_deduped = []
    for r in fixed_list:
        if r["id"] not in seen:
            seen.add(r["id"])
            fixed_deduped.append(r)

    fixed_deduped.sort(key=lambda r: r["id"], reverse=True)
    OUT_FIXED.write_text(json.dumps({
        "fetched_at": now,
        "bbox":  [0.050, 52.120, 0.300, 52.260],
        "count": len(fixed_deduped),
        "reports": fixed_deduped,
    }, indent=2))
    print(f"✓ {len(fixed_deduped)} resolved reports → {OUT_FIXED}")


if __name__ == "__main__":
    main()
