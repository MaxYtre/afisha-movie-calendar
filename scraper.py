import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from icalendar import Calendar, Event

BASE_URL = "https://www.afisha.ru/data-vyhoda/"
HEADERS = {"User-Agent": "Mozilla/5.0"}
OUTPUT_FILE = "calendar.ics"

def fetch_month(offset_months):
    target = (datetime.now() + timedelta(days=30*offset_months)).strftime("%Y-%m")
    url = BASE_URL + "?month=" + target
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.text

def parse_films(html):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    current_date = None
    for elem in soup.select("[data-test='GROUP-DATE'], div.V68Cw"):
        if elem.name == "h2":
            current_date = datetime.strptime(
                elem.text.strip() + f" {datetime.now().year}",
                "%d %B %Y"
            ).date()
        else:
            title_tag = elem.select_one("a[data-test='LINK ITEM-NAME']")
            if not title_tag or not current_date:
                continue
            title = title_tag.text.strip()
            link = "https://www.afisha.ru" + title_tag["href"]
            items.append({"title": title, "date": current_date, "link": link})
    return items

def filter_non_russian(films):
    result = []
    for film in films:
        resp = requests.get(film["link"], headers=HEADERS)
        soup = BeautifulSoup(resp.text, "html.parser")
        country_text = ""
        for fact in soup.select(".facts__item"):
            if "Страна" in fact.text:
                country_text = fact.text
                break
        if "Россия" in country_text:
            continue
        result.append(film)
    return result

def build_calendar(films):
    cal = Calendar()
    cal.add("prodid", "-//Afisha Movie Calendar//")
    cal.add("version", "2.0")
    for film in films:
        ev = Event()
        ev.add("summary", film["title"])
        ev.add("dtstart", film["date"])
        ev.add("dtend", film["date"] + timedelta(days=1))
        ev.add("description", film["link"])
        cal.add_component(ev)
    with open(OUTPUT_FILE, "wb") as f:
        f.write(cal.to_ical())

def main():
    all_films = []
    for m in (0, 1):
        html = fetch_month(m)
        all_films.extend(parse_films(html))
    films = filter_non_russian(all_films)
    print(f"Найдено фильмов: {len(films)}")
    for f in films:
        print(f)
    build_calendar(films)

if __name__ == "__main__":
    main()
