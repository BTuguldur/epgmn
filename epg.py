
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import re
import xml.dom.minidom
import pytz

# Цагийн бүсийн тохиргоо
mn_tz = pytz.timezone("Asia/Ulaanbaatar")

# Суурь URL
BASE_URL = "https://www.zuragt.mn/"
headers = {"User-Agent": "Mozilla/5.0"}

# XMLTV үндсэн элемент
current_date = datetime.now(mn_tz).strftime("%Y%m%d%H%M%S")
tv = ET.Element("tv", {
    "date": current_date,
    "generator-info-name": "Tugldr",
    "generator-info-url": "https://epg.pw",
    "source-info-name": "FREE EPG",
    "source-info-url": "https://www.zuragt.mn/"
})

# 7 хоногийн хөтөлбөрийг татах
for day_offset in range(7):
    date = datetime.now(mn_tz) + timedelta(days=day_offset)
    date_str = date.strftime("%Y-%m-%d")

    url = f"{BASE_URL}?date={date_str}" if day_offset > 0 else BASE_URL
    print(f"Өгөгдөл татаж байна: {url}")

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Алдаа гарлаа {response.status_code}, {url}")
        continue

    soup = BeautifulSoup(response.text, "html.parser")
    tv_boxes = soup.find_all("div", class_="tv-box")

    for tv_box in tv_boxes:
        channel_tag = tv_box.find("div", class_="tv-header").find("h1")
        if channel_tag:
            channel_name = channel_tag.text.strip()
            channel_elem = ET.SubElement(tv, "channel", id=channel_name)
            ET.SubElement(channel_elem, "display-name", lang="mn").text = channel_name

            program_items = tv_box.find_all("li", class_=re.compile("addBookmark tv-(passed|active|future)"))
            programme_list = []

            for item in program_items:
                time_tag = item.find("div", class_="time")
                title_tag = item.find("div", class_="program")
                if time_tag and title_tag:
                    start_time = time_tag.get_text(strip=True)
                    title = title_tag.text.strip()
                    try:
                        start_time_cleaned = re.search(r"\d{1,2}:\d{2}", start_time)
                        if start_time_cleaned:
                            time_str = start_time_cleaned.group()
                            hour, minute = map(int, time_str.split(":"))
                            start_dt = date.replace(hour=hour, minute=minute, second=0, microsecond=0)
                            start_xmltv = start_dt.strftime("%Y%m%d%H%M%S") + " +0800"

                            programme_list.append({
                                "start": start_dt,
                                "start_str": start_xmltv,
                                "title": title,
                                "channel": channel_name
                            })
                        else:
                            print(f"⚠️ {channel_name} сувгийн '{start_time}' формат танигдсангүй.")
                    except Exception as e:
                        print(f"⚠️ {channel_name} сувгийн '{start_time}' цагийг уншихад алдаа гарлаа: {e}")

            for i in range(len(programme_list)):
                if i + 1 < len(programme_list):
                    stop_dt = programme_list[i + 1]["start"]
                else:
                    stop_dt = datetime.combine((date + timedelta(days=1)).date(), datetime.min.time()).replace(tzinfo=mn_tz)

                stop_xmltv = stop_dt.strftime("%Y%m%d%H%M%S") + " +0800"

                programme_elem = ET.SubElement(tv, "programme",
                                               start=programme_list[i]["start_str"],
                                               stop=stop_xmltv,
                                               channel=programme_list[i]["channel"])
                ET.SubElement(programme_elem, "title", lang="mn").text = programme_list[i]["title"]

# XMLTV хадгалах
output_file = "weekly_epg_updated.xml"
tree = ET.ElementTree(tv)
xml_str = ET.tostring(tv, encoding='utf-8')
parsed_xml = xml.dom.minidom.parseString(xml_str)
formatted_xml = parsed_xml.toprettyxml(indent="  ")

with open(output_file, "w", encoding="utf-8") as f:
    f.write(formatted_xml)

print(f"{output_file} амжилттай үүсгэгдлээ!")
