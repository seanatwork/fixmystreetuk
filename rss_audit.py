#!/usr/bin/env python3
"""
Focused RSS data audit — three questions before scaffolding the app:
  1. How many of the 5,491 CCC reports fall inside the bbox?
  2. How deep does RSS pagination actually go?
  3. Do feeds overlap / duplicate?

Bounding box: north Cambridge down to Great Shelford (~52.134°N)
"""

import time
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime

BBOX = dict(north=52.260, south=52.120, west=-0.060, east=0.300)
GEORSS = "http://www.georss.org/georss"

FEEDS = {
    "CCC":  "https://www.fixmystreet.com/rss/reports/Cambridgeshire",
    "SCDC": "https://www.fixmystreet.com/rss/reports/South+Cambridgeshire+District+Council",
    "NH":   "https://report.nationalhighways.co.uk/rss/reports/National+Highways/South+Cambridgeshire",
}

MAX_PAGES = 300   # hard ceiling — 300 × 20 = 6,000 items max per feed


@dataclass
class Report:
    id: str
    title: str
    category: str
    lat: float | None
    lon: float | None
    date: str
    source: str

    @property
    def in_bbox(self) -> bool:
        if self.lat is None or self.lon is None:
            return False
        return (
            BBOX["south"] <= self.lat <= BBOX["north"]
            and BBOX["west"] <= self.lon <= BBOX["east"]
        )


def fetch_page(url: str) -> list[Report] | None:
    """Fetch one RSS page; return None if empty or error."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "fms-audit/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            root = ET.fromstring(resp.read().decode())
    except Exception as e:
        print(f"    ERROR {e}")
        return None

    items = root.findall(".//item")
    if not items:
        return None

    reports = []
    for item in items:
        guid = item.findtext("guid") or item.findtext("link") or ""
        point = item.findtext(f"{{{GEORSS}}}point")
        lat = lon = None
        if point:
            parts = point.strip().split()
            if len(parts) == 2:
                lat, lon = float(parts[0]), float(parts[1])
        reports.append(Report(
            id=guid,
            title=(item.findtext("title") or "").strip(),
            category=item.findtext("category") or "Unknown",
            lat=lat,
            lon=lon,
            date=item.findtext("pubDate") or "",
            source="",
        ))
    return reports


def crawl_feed(name: str, base_url: str) -> list[Report]:
    all_reports: list[Report] = []
    print(f"\n  {name}: {base_url}")

    for page in range(1, MAX_PAGES + 1):
        url = f"{base_url}?p={page}" if page > 1 else base_url
        reports = fetch_page(url)

        if not reports:
            print(f"    page {page:>3}  → empty — stopping")
            break

        for r in reports:
            r.source = name
        all_reports.extend(reports)

        in_box = sum(1 for r in reports if r.in_bbox)
        no_geo = sum(1 for r in reports if r.lat is None)
        print(f"    page {page:>3}  fetched={len(reports):>2}  "
              f"in_bbox={in_box:>2}  no_geo={no_geo:>2}  "
              f"running={len(all_reports):>4}")

        # Stop early if whole page is outside bbox AND we've seen ≥5 pages
        # (feeds are newest-first; once we're well past the area, keep going
        #  a few more pages to confirm it's not just a geographic gap)
        time.sleep(0.3)   # polite crawling

    return all_reports


def section(title: str):
    print(f"\n{'='*62}\n  {title}\n{'='*62}")


def main():
    print("\nFMS RSS Data Audit")
    print(f"Bbox: {BBOX['south']}°N–{BBOX['north']}°N, {BBOX['west']}°E–{BBOX['east']}°E")
    print(f"Run:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    section("Crawling feeds (this will take a few minutes)")
    all_by_feed: dict[str, list[Report]] = {}
    for name, url in FEEDS.items():
        reports = crawl_feed(name, url)
        all_by_feed[name] = reports

    # ------------------------------------------------------------------ #
    # Q1: bbox yield per feed                                             #
    # ------------------------------------------------------------------ #
    section("Q1 — How many reports fall inside the bbox?")
    all_reports: list[Report] = []
    for name, reports in all_by_feed.items():
        total = len(reports)
        in_box = [r for r in reports if r.in_bbox]
        no_geo = sum(1 for r in reports if r.lat is None)
        pct = 100 * len(in_box) / total if total else 0
        print(f"\n  {name}")
        print(f"    Total fetched  : {total:>4}")
        print(f"    No coordinates : {no_geo:>4}")
        print(f"    In bbox        : {len(in_box):>4}  ({pct:.1f}%)")
        for r in in_box[:3]:
            print(f"      · {r.lat:.4f},{r.lon:.4f}  [{r.category}]  {r.title[:60]}")
        all_reports.extend(reports)

    # ------------------------------------------------------------------ #
    # Q2: pagination depth                                                #
    # ------------------------------------------------------------------ #
    section("Q2 — How deep does pagination go?")
    for name, reports in all_by_feed.items():
        pages = (len(reports) + 19) // 20
        print(f"  {name:<6}  {len(reports):>4} reports across ~{pages} pages")

    # ------------------------------------------------------------------ #
    # Q3: duplicates / overlap across feeds                               #
    # ------------------------------------------------------------------ #
    section("Q3 — Overlap and duplicates across feeds")
    # Normalise IDs (strip trailing slash, lowercase)
    id_sets: dict[str, set[str]] = {
        name: {r.id.rstrip("/").lower() for r in reports}
        for name, reports in all_by_feed.items()
    }
    names = list(id_sets.keys())
    for i, a in enumerate(names):
        for b in names[i+1:]:
            overlap = id_sets[a] & id_sets[b]
            print(f"  {a} ∩ {b}  :  {len(overlap)} shared IDs")
            if overlap:
                for oid in list(overlap)[:3]:
                    print(f"    {oid}")

    # Within-feed duplicates
    for name, reports in all_by_feed.items():
        ids = [r.id for r in reports]
        dupes = len(ids) - len(set(ids))
        print(f"  {name} internal duplicates: {dupes}")

    # ------------------------------------------------------------------ #
    # Bonus: categories in bbox                                           #
    # ------------------------------------------------------------------ #
    section("Bonus — categories represented inside bbox")
    from collections import Counter
    bbox_all = [r for r in all_reports if r.in_bbox]
    cat_counts = Counter(r.category for r in bbox_all)
    print(f"\n  Total in-bbox reports: {len(bbox_all)}")
    for cat, n in cat_counts.most_common():
        print(f"    {cat:<40} {n}")

    # ------------------------------------------------------------------ #
    # Bonus: date range of in-bbox reports                                #
    # ------------------------------------------------------------------ #
    section("Bonus — date range of in-bbox reports")
    dates = [r.date for r in bbox_all if r.date]
    if dates:
        print(f"  Oldest : {dates[-1]}")
        print(f"  Newest : {dates[0]}")

    print(f"\n{'='*62}\n  Audit complete.\n{'='*62}\n")


if __name__ == "__main__":
    main()
