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
    # Проходим по блокам даты и карточкам
    for section in soup.select("section[data-test='DATA-SLIDER']"):
        # Группа с датой
        date_tag = section.select_one("h2[data-test='GROUP-DATE']")
        if not date_tag:
            continue
        # Преобразуем «9 октября» в дату
        group_date = datetime.strptime(
            date_tag.text.strip() + f" {datetime.now().year}",
            "%d %B %Y"
        ).date()
        # Все карточки в этой группе
        for card in section.select("div.V68Cw"):
            title_link = card.select_one("a[data-test='LINK ITEM-NAME']")
            if not title_link:
                continue
            title = title_link.text.strip()
            link = "https://www.afisha.ru" + title_link["href"]
            items.append({"title": title, "date": group_date, "link": link})
    return items

def filter_non_russian(films):
    result = []
    for film in films:
        resp = requests.get(film["link"], headers=HEADERS)
        soup = BeautifulSoup(resp.text, "html.parser")
        # Ищем факт «Страна» в списке facts__item
        country = ""
        fact_items = soup.select(".facts__item")
        for fact in fact_items:
            if "Страна" in fact.text:
                country = fact.text
                break
        if "Россия" in country:
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
