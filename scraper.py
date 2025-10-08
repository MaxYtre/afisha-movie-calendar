#!/usr/bin/env python3
# coding: utf-8

import requests
from bs4 import BeautifulSoup
from ics import Calendar, Event
from datetime import datetime, timedelta, date
import re
import json
import os
import time
import random
from urllib.parse import urljoin, quote, urlparse, parse_qs
import argparse

# Конфигурация и параметры
MAX_MOVIES = 1           # None или целое число — лимит фильмов
MAX_RETRIES = 8
BACKOFF_FACTOR = 2
BASE_DELAY = 3              # seconds
RANDOM_DELAY = 2            # seconds
PAGE_DELAY = 4              # seconds

# Страны, фильмы которых НЕ включать в календарь
EXCLUDE_COUNTRIES = ['Россия']

# Аргументы командной строки
parser = argparse.ArgumentParser(description='Парсер афиши и генерация iCal-календаря')
parser.add_argument(
    '--exclude-country',
    action='append',
    default=['Россия'],
    help='Страна, которую не включать в календарь (можно указать несколько)'
)
parser.add_argument(
    '--max-movies',
    type=int,
    default=None,
    help='Максимальное число фильмов для обработки'
)
args = parser.parse_args()
EXCLUDE_COUNTRIES = args.exclude_country
MAX_MOVIES = args.max_movies

def safe_delay(delay=BASE_DELAY):
    """
    Задержка между запросами с рандомизацией
    """
    time.sleep(delay + random.uniform(0, RANDOM_DELAY))

def get_soup(url):
    """
    Получить объект BeautifulSoup по URL с retry при HTTP 429
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; CalendarBot/1.0; +https://example.com/bot)'
    }
    delay = BASE_DELAY
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, headers=headers)
        if resp.status_code == 429:
            # слишком много запросов — ждем и повторяем
            time.sleep(delay + random.uniform(0, RANDOM_DELAY))
            delay *= BACKOFF_FACTOR
            continue
        resp.raise_for_status()
        return BeautifulSoup(resp.text, 'html.parser')
    # Если не удалось после попыток — исключение
    resp.raise_for_status()

def parse_movie_page(soup, url):
    """
    Разобрать страницу фильма и вернуть Event или None, если фильм из исключённых стран
    """
    # Название фильма
    title_el = soup.find(class_='oOY35', attrs={'data-test': 'ITEM-NAME'})
    if not title_el:
        return None
    title = title_el.get_text(strip=True)

    # Сбор метаданных: страна и год
    country_links = soup.select('span[data-test="ITEM-META"] a[data-test="LINK"]')
    countries = [a.get_text(strip=True) for a in country_links]

    # Пропуск фильмов из исключённых стран
    if any(country in EXCLUDE_COUNTRIES for country in countries):
        return None

    # Парсинг дат сеансов
    showtimes = []
    for scr in soup.select('.kZjgU'):  # пример селектора для блоков с датами
        date_text = scr.get('datetime') or scr.get_text(strip=True)
        try:
            dt = datetime.fromisoformat(date_text)
            showtimes.append(dt)
        except Exception:
            continue

    if not showtimes:
        return None

    # Создание события
    event = Event()
    event.name = title
    event.begin = min(showtimes)
    event.end = max(showtimes) + timedelta(hours=2)
    event.description = (
        f"Сеансы: {', '.join(dt.strftime('%Y-%m-%d %H:%M') for dt in showtimes)}\n"
        f"Страна: {', '.join(countries)}\n"
        f"Источник: {url}"
    )
    event.url = url
    return event

def main():
    """
    Основной цикл парсинга и генерации календаря
    """
    base_list_url = 'https://www.afisha.ru/movie/schedule/perm/'
    movie_urls = []

    # Получить ссылки на страницы фильмов
    page = 1
    while True:
        list_url = f"{base_list_url}?page={page}"
        try:
            soup = get_soup(list_url)
        except requests.exceptions.HTTPError as e:
            print(f"Превышен лимит запросов на {list_url}: {e}")
            break
        links = soup.select('a[data-test="LINK"]')
        urls = [urljoin(base_list_url, a['href']) for a in links if '/movie/' in a['href']]
        if not urls or (MAX_MOVIES and len(movie_urls) >= MAX_MOVIES):
            break
        movie_urls.extend(urls)
        page += 1
        safe_delay(PAGE_DELAY)

    if MAX_MOVIES:
        movie_urls = movie_urls[:MAX_MOVIES]

    # Инициализация календаря
    cal = Calendar()

    # Обработка каждой страницы фильма
    for idx, url in enumerate(movie_urls, 1):
        try:
            soup = get_soup(url)
            event = parse_movie_page(soup, url)
            if event:
                cal.events.add(event)
        except Exception as e:
            print(f"Ошибка при обработке {url}: {e}")
        safe_delay()

    # Сохранить результат
    with open('calendar.ics', 'w', encoding='utf-8') as f:
        f.writelines(cal)
    print(f"Готово: сохранён calendar.ics ({len(cal.events)} событий)")

if __name__ == '__main__':
    main()
