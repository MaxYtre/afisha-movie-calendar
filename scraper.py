#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Парсер афиши кинотеатров Перми с afisha.ru
Создаёт all-day события без времени и уведомлений с красивыми описаниями
С хлопушками 👏 перед названием, баннерами, рейтингами и описаниями фильмов
"""

import argparse
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from icalendar import Calendar, Event

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Константы
BASE_URL = 'https://www.afisha.ru/prm/schedule_cinema/'
SCHEDULE_URL = BASE_URL  # Страница 1
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
HEADERS = {
    'User-Agent': USER_AGENT,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1'
}

# Задержки для избежания HTTP 429
DELAYS = {
    'default': 5,    # Обычные запросы
    'page': 8,       # Страницы списков
    'detail': 12,    # Детальные страницы фильмов
    'retry': 10      # Повторные попытки
}

def smart_delay(delay_type: str = 'default', multiplier: int = 1):
    """Умная задержка с случайной вариацией"""
    base_delay = DELAYS.get(delay_type, DELAYS['default'])
    delay = base_delay * multiplier + random.uniform(0, 3)
    time.sleep(delay)
    logger.debug(f"Задержка {delay_type}: {delay:.2f} сек")

def make_request(session: requests.Session, url: str, delay_type: str = 'default') -> Optional[requests.Response]:
    """Выполняет HTTP-запрос с обработкой ошибок и retry"""
    smart_delay(delay_type)
    try:
        response = session.get(url, headers=HEADERS, timeout=30)
        if response.status_code == 429:
            logger.warning(f"HTTP 429 для {url}. Увеличиваем задержку.")
            time.sleep(60)  # Длинная пауза при rate limit
            return make_request(session, url, 'retry')
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logger.error(f"Ошибка запроса {url}: {e}")
        return None

def parse_schedule_calendar(soup: BeautifulSoup) -> Optional[datetime]:
    """Парсит календарь виджета для ближайшей даты сеанса (на основе предоставленного HTML)"""
    calendar_div = soup.find('div', {'aria-label': 'Календарь'})
    if not calendar_div:
        logger.warning("Календарь не найден")
        return None

    # Ищем активные даты (кликабельные <a class="pdT6c">)
    active_days = calendar_div.find_all('a', class_=re.compile(r'pdT6c'))
    if not active_days:
        logger.warning("Активные даты не найдены")
        return None

    # Ближайшая дата - первая активная
    first_day = active_days[0]
    aria_label = first_day.get('aria-label', '')
    # Парсим из aria-label, напр. "8 октября"
    date_match = re.search(r'(\d+)\s+([а-я]+)', aria_label.lower())
    if date_match:
        day = int(date_match.group(1))
        month_name = date_match.group(2)
        month_map = {
            'октября': 10, 'ноября': 11, 'декабря': 12,
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5,
            'июня': 6, 'июля': 7, 'августа': 8, 'сентября': 9
        }
        if month_name in month_map:
            current_year = datetime.now().year
            try:
                dt = datetime(current_year, month_map[month_name], day)
                logger.info(f"Ближайшая дата сеанса: {dt.strftime('%d.%m.%Y')}")
                return dt.date()
            except ValueError:
                pass

    # Fallback: завтра
    tomorrow = (datetime.now() + timedelta(days=1)).date()
    logger.info(f"Fallback дата: завтра {tomorrow}")
    return tomorrow

def extract_movie_detail(session: requests.Session, movie_url: str) -> Dict[str, Any]:
    """Извлекает детали фильма: баннер, описание, возраст, расписание, страна"""
    response = make_request(session, movie_url, 'detail')
    if not response:
        return {}

    soup = BeautifulSoup(response.text, 'html.parser')
    details = {}

    # Баннер (img src или background-image)
    banner_img = soup.find('img', class_=re.compile(r'(poster|banner|hero-image)')) or soup.find('img', alt=re.compile(r'.*фильм.*', re.I))
    if banner_img:
        src = banner_img.get('src') or banner_img.get('data-src')
        banner_url = urljoin(BASE_URL, src) if src else ''
        if not banner_url:
            # Если background-image
            style = banner_img.get('style', '')
            bg_match = re.search(r'url\(["\']?(https?://[^"\']+)["\']?\)', style)
            if bg_match:
                banner_url = bg_match.group(1)
        if banner_url:
            details['banner'] = banner_url
            logger.debug(f"Баннер: {banner_url}")

    # Возраст (12+, 16+)
    age_elem = soup.find('span', class_=re.compile(r'age|rating')) or soup.find(string=re.compile(r'\d+\+'))
    if age_elem:
        age_text = re.search(r'(\d+)\+', str(age_elem))
        if age_text:
            details['age'] = age_text.group(1) + '+'
            logger.debug(f"Возраст: {details['age']}")

    # Описание под "О фильме"
    desc_section = soup.find('h2', string=re.compile(r'о фильме', re.I))
    if desc_section:
        desc_elem = desc_section.find_next_sibling('div', class_=re.compile(r'description|about')) or desc_section.find_next('p')
        if desc_elem:
            description = desc_elem.get_text(strip=True)[:300]
            if len(description) > 200:
                description += '...'
            details['description'] = description
            logger.debug(f"Описание: {description[:50]}...")

    # Расписание (ближайшая дата)
    schedule_date = parse_schedule_calendar(soup)
    if schedule_date:
        details['date'] = schedule_date

    # Страна (для исключения России)
    country_elem = soup.find(string=re.compile(r'(росси|usa|uk|france|germany)', re.I))
    if country_elem:
        details['country'] = country_elem.strip()[:50]  # Коротко

    return details

def extract_movie_from_list(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Извлекает фильмы со страницы списка"""
    movies = []
    movie_elements = soup.find_all('div', class_=re.compile(r'movie|film|item|card'))
    for elem in movie_elements:
        title_elem = elem.find('a', class_=re.compile(r'title|name|h3')) or elem.find('h3')
        if title_elem:
            title = title_elem.get_text(strip=True)
            url = urljoin(BASE_URL, title_elem.get('href', ''))
            if title and url:
                movie = {
                    'title': title,
                    'url': url,
                    'date': None,
                    'time': None  # Не используется
                }
                movies.append(movie)
                logger.debug(f"Найден фильм: {title}")

    logger.info(f"На странице найдено {len(movies)} фильмов")
    return movies

def parse_all_schedule_pages(session: requests.Session, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
    """Парсит все страницы пагинации"""
    all_movies = []
    existing_titles = set()
    current_page = 1
    page_count = 0

    while True:
        if max_pages and page_count >= max_pages:
            logger.info(f"Достигнут лимит страниц: {max_pages}")
            break

        page_url = SCHEDULE_URL if current_page == 1 else f"{BASE_URL}page{current_page}/"
        logger.info(f"Парсинг страницы {current_page}: {page_url}")

        response = make_request(session, page_url, 'page')
        if not response:
            logger.warning(f"Страница {current_page} недоступна - завершаем")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        page_movies = extract_movie_from_list(soup)

        if not page_movies:
            logger.info(f"Пустая страница {current_page} - завершаем")
            break

        for movie in page_movies:
            if movie['title'] not in existing_titles:
                existing_titles.add(movie['title'])
                all_movies.append(movie)

        page_count += 1
        current_page += 1

    logger.info(f"Итого уникальных фильмов: {len(all_movies)} на {page_count} страницах")
    return all_movies

def create_event(movie: Dict[str, Any], details: Dict[str, Any], exclude_country: str = 'Россия') -> Optional[Event]:
    """Создаёт all-day событие без времени и уведомлений с красивым описанием"""
    # Проверка страны
    if 'country' in details and exclude_country.lower() in details['country'].lower():
        logger.info(f"Пропуск {exclude_country} фильма: {movie['title']}")
        return None

    # Дата
    event_date = details.get('date', (datetime.now() + timedelta(days=1)).date())
    event_date_str = event_date.strftime('%Y%m%d')

    # Заголовок с хлопушкой
    summary = f"👏 {movie['title']}"

    # Красивое описание
    description_parts = [f"🎭 Рейтинг: {details.get('age', 'Не указан')}"]
    if 'banner' in details:
        description_parts.append(f"\n[![Баннер фильма]]({details['banner']})")
    else:
        description_parts.append("\n[Изображение недоступно]")
    if 'description' in details:
        description_parts.append(f"\n📜 О фильме:\n{details['description']}")
    else:
        description_parts.append("\n📜 О фильме:\nИнформация недоступна")
    description_parts.append("\n🗓️ Событие на весь день: фильм в кинотеатрах Перми. Проверьте актуальное расписание на afisha.ru.")
    description_parts.append(f"\n📍 Источник: {movie['url']}")
    description = '\n'.join(description_parts)

    # All-day событие без уведомлений
    event = Event()
    event.add('uid', f"afisha-movie-{hash(movie['title'])}@maxytre.github.io")
    event.add('summary', summary)
    event.add('dtstart', datetime.strptime(event_date_str, '%Y%m%d').date())  # All-day
    event.add('dtend', datetime.strptime(event_date_str, '%Y%m%d').date() + timedelta(days=1))
    event.add('description', description)
    event.add('location', 'Кинотеатры Перми')
    # Без VALARM

    logger.info(f"Создано событие: {summary} на {event_date}")
    return event

def main():
    parser = argparse.ArgumentParser(description="Парсер афиши кинотеатров Перми")
    parser.add_argument('--exclude-country', default='Россия', help='Исключить фильмы страны')
    parser.add_argument('--delay', type=int, default=5, help='Базовая задержка (сек)')
    parser.add_argument('--skip-details', action='store_true', help='Пропустить детали')
    parser.add_argument('--max-movies', type=int, default=None, help='Макс. фильмов')
    parser.add_argument('--max-pages', type=int, default=None, help='Макс. страниц')

    args = parser.parse_args()

    # Масштабирование задержек
    scale = args.delay / 5.0
    for key in DELAYS:
        DELAYS[key] *= scale

    logger.info("🚀 Запуск парсера: all-day события без уведомлений с хлопушками 👏")
    logger.info(f"Режим: {'--skip-details' if args.skip_details else 'Полный'}")
    if args.max_movies:
        logger.info(f"Лимит фильмов: {args.max_movies}")
    if args.max_pages:
        logger.info(f"Лимит страниц: {args.max_pages}")

    session = requests.Session()
    session.headers.update(HEADERS)

    all_movies = parse_all_schedule_pages(session, args.max_pages)
    if not all_movies:
        logger.error("Нет фильмов")
        return

    if args.max_movies:
        all_movies = all_movies[:args.max_movies]
        logger.info(f"Ограничено до {len(all_movies)} фильмов")

    events = []
    processed = 0
    for movie in all_movies:
        details = {}
        if not args.skip_details:
            details = extract_movie_detail(session, movie['url'])
        event = create_event(movie, details, args.exclude_country)
        if event:
            events.append(event)
        processed += 1
        if processed % 10 == 0:
            logger.info(f"Обработано {processed}/{len(all_movies)} фильмов, {len(events)} событий")
        if not args.skip_details:
            smart_delay('detail', 0.5)

    # Календарь
    cal = Calendar()
    cal.add('prodid', '-//Afisha Calendar//MaxYtre//RU')
    cal.add('version', '2.0')
    cal.add('calscale', 'GREGORIAN')
    cal.add('method', 'PUBLISH')
    for event in events:
        cal.add_component(event)

    with open('calendar.ics', 'wb') as f:
        f.write(cal.to_ical())

    logger.info(f"✅ Готово: {len(events)} событий в calendar.ics (all-day без уведомлений)")

if __name__ == '__main__':
    main()
