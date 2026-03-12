import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

TZ = timezone(timedelta(hours=8))

CHANNELS = [
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1001", "МҮОНТ", "МҮОНТ"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1020", "Монголын Мэдээ", "Монголын Мэдээ"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1007", "Central TV", "Central TV"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1017", "TV8", "TV8"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1010", "Боловсрол ТВ", "Боловсрол ТВ"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1005", "TV5", "TV5"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1006", "UBS-1", "UBS-1"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1024", "Bloomberg TV MGL", "Bloomberg TV MGL"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1008", "NTV", "NTV"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1356", "TV9", "TV9"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1003", "MN25", "MN25"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1014", "SBN", "SBN"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1018", "ETV HD", "ETV HD"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1011", "TenGerTV", "TenGerTV"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1015", "C1", "C1"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1030", "TV7", "TV7"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1032", "Channel 11", "Channel 11"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1019", "Сүлд ТВ", "Сүлд ТВ"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1044", "Парламент телевиз", "Парламент телевиз"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1012", "Star TV", "Star TV"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1004", "Eagle", "Eagle"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1013", "Малчин HD", "Малчин HD"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1016", "TM", "TM"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1070", "Бидний цөөхөн Монголчууд", "Бидний цөөхөн Монголчууд"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1100", "History", "History"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1101", "Lifetime", "Lifetime"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1102", "Crime Investigation", "Crime Investigation"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1169", "Trace Urban", "Trace Urban"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1116", "AFN", "AFN"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1118", "KBS World", "KBS World"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1350", "Global Trekker", "Global Trekker"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1352", "Love Nature", "Love Nature"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1392", "Discovery Channel", "Discovery Channel"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1097", "SCI", "SCI"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1149", "Premier Sports 1", "Premier Sports 1"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1214", "Premier Sports 2", "Premier Sports 2"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1074", "Premier Sports 3", "Premier Sports 3"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1224", "Premier Sports 4", "Premier Sports 4"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1225", "Premier Sports 5", "Premier Sports 5"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1212", "EuroSport", "EuroSport"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1229", "NickJR", "NickJR"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1227", "CartoonNetwork", "CartoonNetwork"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1231", "Cbeebies", "Cbeebies"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1349", "MoonBug", "MoonBug"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1351", "DuckTV", "DuckTV"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1230", "Dreambox", "Dreambox"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1243", "AsianBox", "AsianBox"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1241", "MovieBox", "MovieBox"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1237", "HBO", "HBO"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1240", "HBO Hits", "HBO Hits"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1239", "HBO SIGNATURE", "HBO SIGNATURE"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1238", "HBO Family", "HBO Family"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1244", "Cinemax", "Cinemax"),
    ("https://looktv.mn/RTEFacade/GetProgramLists?channel_external_ids=CH1250", "Playboy", "Playboy"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://looktv.mn/",
    "Origin": "https://looktv.mn",
}

def xmltv_time_from_ms(ms_str: str) -> str:
    ms = int(str(ms_str).strip())
    dt = datetime.fromtimestamp(ms / 1000, tz=TZ)
    return dt.strftime("%Y%m%d%H%M%S %z")

def clean_text(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip()

def indent(elem, level=0):
    i = "\n" + level * "  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        last_child = None
        for child in elem:
            indent(child, level + 1)
            last_child = child
        if last_child is not None and (not last_child.tail or not last_child.tail.strip()):
            last_child.tail = i
    if level and (not elem.tail or not elem.tail.strip()):
        elem.tail = i

def fetch_channel_xml(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def parse_programs(xml_text: str):
    root = ET.fromstring(xml_text)
    return root.findall(".//Program")

def add_channel(tv_root, channel_id: str, display_name: str):
    channel = ET.SubElement(tv_root, "channel", id=channel_id)
    dn = ET.SubElement(channel, "display-name", lang="mn")
    dn.text = display_name

def is_today_or_future(start_val: str) -> bool:
    if not start_val:
        return False
    start_ms = int(start_val)
    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=TZ)
    today_start = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    return start_dt >= today_start

def add_programme(tv_root, channel_id: str, p):
    title = clean_text(p.attrib.get("name"))
    start_val = clean_text(p.attrib.get("startVal"))
    end_val = clean_text(p.attrib.get("endVal"))
    desc = clean_text(p.attrib.get("description"))
    pr_name = clean_text(p.attrib.get("prName"))
    pr_level = clean_text(p.attrib.get("prLevel"))

    if not title or not start_val:
        return False
    if not is_today_or_future(start_val):
        return False

    attrs = {
        "channel": channel_id,
        "start": xmltv_time_from_ms(start_val),
    }
    if end_val:
        attrs["stop"] = xmltv_time_from_ms(end_val)

    programme = ET.SubElement(tv_root, "programme", attrs)

    title_el = ET.SubElement(programme, "title", lang="mn")
    title_el.text = title

    if desc:
        desc_el = ET.SubElement(programme, "desc", lang="mn")
        desc_el.text = desc

    if pr_name:
        rating = ET.SubElement(programme, "rating", system="LookTV")
        value = ET.SubElement(rating, "value")
        value.text = pr_name

    if pr_level:
        category = ET.SubElement(programme, "category", lang="mn")
        category.text = f"Level {pr_level}"

    return True

def build_epg(output_file="epg_today.xml"):
    tv = ET.Element("tv", attrib={"generator-info-name": "looktv-merged-epg-today-forward"})

    total_channels = 0
    total_programmes = 0
    failed = []

    for url, channel_id, display_name in CHANNELS:
        try:
            xml_text = fetch_channel_xml(url)
            programs = parse_programs(xml_text)

            add_channel(tv, channel_id, display_name)
            total_channels += 1

            added = 0
            for p in programs:
                if add_programme(tv, channel_id, p):
                    added += 1

            total_programmes += added
            print(f"[OK] {display_name}: {added} programme")
        except Exception as e:
            failed.append((display_name, url, str(e)))
            print(f"[FAIL] {display_name}: {e}")

    indent(tv)
    tree = ET.ElementTree(tv)
    tree.write(output_file, encoding="utf-8", xml_declaration=True)

    print(f"Done: {output_file}")
    print(f"Channels: {total_channels}")
    print(f"Programmes: {total_programmes}")

    if failed:
        print("Failed channels:")
        for name, url, err in failed:
            print(f" - {name}: {err}")
            print(f"   {url}")

if __name__ == "__main__":
    build_epg("epg_today.xml")
