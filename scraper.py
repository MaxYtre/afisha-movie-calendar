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
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for card in soup.select(".data-vyhoda__card"):
        date_tag = card.select_one(".data-vyhoda__date")
        if not date_tag: continue
        date_str = date_tag.text.strip()
        try:
            date = datetime.strptime(date_str + f".{datetime.now().year}", "%d.%m.%Y")
        except:
            continue
        link = card.select_one("a")['href']
        title = card.select_one(".data-vyhoda__title").text.strip()
        items.append({"title": title, "date": date, "link": link})
    return items

def filter_non_russian(films):
    result = []
    for film in films:
        resp = requests.get(film["link"], headers=HEADERS)
        soup = BeautifulSoup(resp.text, "html.parser")
        country = soup.select_one(".fact__country")
        if country and "Россия" in country.text:
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
        ev.add("dtstart", film["date"].date())
        ev.add("dtend", film["date"].date() + timedelta(days=1))
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
    build_calendar(films)

if __name__ == "__main__":
    main()
