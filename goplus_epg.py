#!/usr/bin/env python3
"""
GO+ (goplus.mn) — 7 хоногийн EPG татагч.

Гаралт:
  * goplus_epg.xml   — XMLTV формат (Kodi, TVHeadend, Jellyfin, IPTV player-д шууд ашиглана)
  * goplus_epg.json  — бүх сувгийн хөтөлбөр JSON-оор
  * goplus_epg.csv   — Excel-д нээхэд зориулсан хүснэгт

Ашиглах:
  pip install requests
  python goplus_epg.py                 # өнөөдрөөс эхлэн 7 хоног
  python goplus_epg.py --days 7 --past 1 --out ./epg
  python goplus_epg.py --channels 002,003,006

Тайлбар:
  * events/schedule API нь нэвтрэлт шаардахгүй.
  * channels/get API нь зөвхөн IRIS-DEVICE-* header шаарддаг.
  * Сервер ойролцоогоор 5-6 хоногийн ирээдүйн хөтөлбөр л өгдөг тул
    7 дахь өдөр хоосон байж болно.
"""

import argparse
import csv
import json
import os
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from xml.sax.saxutils import escape

import requests

BASE = "https://www.goplus.mn"
CHANNELS_URL = BASE + "/managetv/tvinfo/channels/get"
SCHEDULE_URL = BASE + "/managetv/tvinfo/events/schedule"
UB_TZ = timezone(timedelta(hours=8))  # Asia/Ulaanbaatar

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/130.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": BASE + "/webclient/",
    "IRIS-DEVICE-CLASS": "PC",
    "IRIS-DEVICE-TYPE": "WEB",
    "IRIS-HW-DEVICE-ID": uuid.uuid4().hex,
    "IRIS-APP-VERSION": "1.0",
}

session = requests.Session()
session.headers.update(HEADERS)


def get_json(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if data.get("err", 0) != 0:
                raise RuntimeError(data.get("mes"))
            return data
        except Exception as e:  # noqa: BLE001
            if attempt == retries - 1:
                raise
            time.sleep(1.5 * (attempt + 1))
    return None


def pick_loc(loc_list, key, prefer=("MON", "ENG")):
    """loc жагсаалтаас хэлний дагуу утга сонгоно."""
    if not loc_list:
        return None
    for lan in prefer:
        for l in loc_list:
            if l.get("lan") == lan and l.get(key):
                return l[key]
    for l in loc_list:
        if l.get(key):
            return l[key]
    return None


def fetch_channels():
    data = get_json(CHANNELS_URL)
    chans = []
    for ch in data.get("chs", []):
        if ch.get("sty") != "TV_CHANNEL":
            continue
        loc = ch.get("loc", [])
        logo = None
        for l in loc:
            lnk = (l.get("img_lnk") or {}).get("log")
            if lnk and lnk.get("sizes"):
                size = lnk["sizes"].get("m") or next(iter(lnk["sizes"].values()))
                logo = BASE + lnk["url"].replace("{image-size}", size)
                break
        chans.append({
            "id": ch["sid"],
            "name": pick_loc(loc, "nam") or ch["sid"],
            "number": int(ch.get("ord") or 0),
            "logo": logo,
            "hd": ch.get("hd", False),
        })
    chans.sort(key=lambda c: c["number"])
    return chans


def fetch_schedule(sid, start, end):
    """Нэг сувгийн хөтөлбөрийг өдөр өдрөөр татна (давхардлыг eid-ээр арилгана)."""
    events, seen = [], set()
    day = start
    while day < end:
        nxt = min(day + timedelta(days=1), end)
        params = {
            "start": day.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": nxt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "language": "ENG",
            "serviceId": sid,
            "view": "cd-events-grid-view",
        }
        data = get_json(SCHEDULE_URL, params)
        for ev in data.get("evs", []):
            if ev["eid"] in seen:
                continue
            seen.add(ev["eid"])
            con = ev.get("con", {})
            events.append({
                "channel_id": sid,
                "event_id": ev["eid"],
                "start": ev["sta"],
                "stop": ev["end"],
                "title": pick_loc(con.get("loc"), "tit") or con.get("oti") or "",
                "original_title": con.get("oti", ""),
                "categories": con.get("categories", []),
                "year": con.get("oda", ""),
                "duration_sec": con.get("dur"),
                "parental": con.get("par"),
                "season": con.get("ses"),
                "live": ev.get("liv", False),
                "hd": ev.get("hd", False),
                "language": con.get("lan", ""),
            })
        day = nxt
    events.sort(key=lambda e: e["start"])
    return events


def iso_to_xmltv(s):
    dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return dt.astimezone(UB_TZ).strftime("%Y%m%d%H%M%S %z")


def iso_to_local(s):
    dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return dt.astimezone(UB_TZ).strftime("%Y-%m-%d %H:%M")


def write_xmltv(path, channels, programmes):
    with open(path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<tv generator-info-name="goplus_epg.py" source-info-url="https://www.goplus.mn">\n')
        for c in channels:
            f.write(f'  <channel id="{escape(c["id"])}">\n')
            f.write(f'    <display-name lang="mn">{escape(c["name"])}</display-name>\n')
            f.write(f'    <display-name>{c["number"]}</display-name>\n')
            if c["logo"]:
                f.write(f'    <icon src="{escape(c["logo"])}"/>\n')
            f.write("  </channel>\n")
        for p in programmes:
            f.write(f'  <programme start="{iso_to_xmltv(p["start"])}" '
                    f'stop="{iso_to_xmltv(p["stop"])}" channel="{escape(p["channel_id"])}">\n')
            f.write(f'    <title lang="mn">{escape(p["title"])}</title>\n')
            for cat in p["categories"]:
                f.write(f'    <category lang="en">{escape(cat)}</category>\n')
            if p["year"]:
                f.write(f'    <date>{escape(str(p["year"]))}</date>\n')
            if p["live"]:
                f.write("    <live/>\n")
            f.write("  </programme>\n")
        f.write("</tv>\n")


def write_csv(path, channels, programmes):
    names = {c["id"]: c["name"] for c in channels}
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["Суваг ID", "Суваг", "Эхлэх (UB)", "Дуусах (UB)", "Нэр", "Ангилал"])
        for p in programmes:
            w.writerow([p["channel_id"], names.get(p["channel_id"], ""),
                        iso_to_local(p["start"]), iso_to_local(p["stop"]),
                        p["title"], "; ".join(p["categories"])])


def main():
    ap = argparse.ArgumentParser(description="GO+ 7 хоногийн EPG татагч")
    ap.add_argument("--days", type=int, default=7, help="Ирээдүйн хоногийн тоо (default 7)")
    ap.add_argument("--past", type=int, default=0, help="Өнгөрсөн хоногийн тоо (catch-up)")
    ap.add_argument("--channels", help="Зөвхөн эдгээр сувгийн ID, таслалаар: 002,003")
    ap.add_argument("--workers", type=int, default=8, help="Зэрэг хүсэлтийн тоо")
    ap.add_argument("--out", default=".", help="Гаралтын хавтас")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)

    # Улаанбаатарын цагаар өнөөдрийн 00:00-оос эхэлнэ
    today_ub = datetime.now(UB_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    start = (today_ub - timedelta(days=args.past)).astimezone(timezone.utc)
    end = (today_ub + timedelta(days=args.days)).astimezone(timezone.utc)

    print("Сувгийн жагсаалт татаж байна...")
    channels = fetch_channels()
    if args.channels:
        wanted = {s.strip() for s in args.channels.split(",")}
        channels = [c for c in channels if c["id"] in wanted]
    print(f"  {len(channels)} суваг. Хугацаа: {start:%Y-%m-%d %H:%M}Z → {end:%Y-%m-%d %H:%M}Z")

    programmes, failed = [], []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(fetch_schedule, c["id"], start, end): c for c in channels}
        for i, fut in enumerate(as_completed(futs), 1):
            c = futs[fut]
            try:
                evs = fut.result()
                programmes.extend(evs)
                print(f"  [{i}/{len(channels)}] {c['id']} {c['name']}: {len(evs)} хөтөлбөр")
            except Exception as e:  # noqa: BLE001
                failed.append(c["id"])
                print(f"  [{i}/{len(channels)}] {c['id']} {c['name']}: АЛДАА {e}", file=sys.stderr)

    order = {c["id"]: c["number"] for c in channels}
    programmes.sort(key=lambda p: (order.get(p["channel_id"], 0), p["start"]))

    xml_path = os.path.join(args.out, "goplus_epg.xml")
    json_path = os.path.join(args.out, "goplus_epg.json")
    csv_path = os.path.join(args.out, "goplus_epg.csv")

    write_xmltv(xml_path, channels, programmes)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"generated": datetime.now(UB_TZ).isoformat(),
                   "channels": channels, "programmes": programmes},
                  f, ensure_ascii=False, indent=2)
    write_csv(csv_path, channels, programmes)

    print(f"\nНийт {len(programmes)} хөтөлбөр → {xml_path}, {json_path}, {csv_path}")
    if failed:
        print(f"Амжилтгүй сувгууд: {', '.join(failed)}", file=sys.stderr)


if __name__ == "__main__":
    main()
