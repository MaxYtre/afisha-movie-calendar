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
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация и параметры
MAX_MOVIES = 50              # Увеличено до разумного значения
MAX_RETRIES = 5
BACKOFF_FACTOR = 2
BASE_DELAY = 2               # seconds
RANDOM_DELAY = 1             # seconds
PAGE_DELAY = 3               # seconds

# Страны, фильмы которых НЕ включать в календарь
EXCLUDE_COUNTRIES = ['Россия']

# Аргументы командной строки
parser = argparse.ArgumentParser(description='Парсер афиши и генерация iCal-календаря')
parser.add_argument(
    '--exclude-country',
    action='append',
    default=[],
    help='Страна, которую не включать в календарь (можно указать несколько)'
)
parser.add_argument(
    '--max-movies',
    type=int,
    default=50,
    help='Максимальное число фильмов для обработки'
)
parser.add_argument(
    '--city',
    type=str,
    default='perm',
    help='Город для парсинга (по умолчанию: perm)'
)
args = parser.parse_args()

# Используем аргументы, если они переданы
if args.exclude_country:
    EXCLUDE_COUNTRIES = args.exclude_country
MAX_MOVIES = args.max_movies
CITY = args.city

def safe_delay(delay=BASE_DELAY):
    """
    Задержка между запросами с рандомизацией
    """
    actual_delay = delay + random.uniform(0, RANDOM_DELAY)
    time.sleep(actual_delay)
    logger.debug(f"Задержка: {actual_delay:.2f} сек")

def get_soup(url, retries=MAX_RETRIES):
    """
    Получить объект BeautifulSoup по URL с retry при HTTP 429
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    delay = BASE_DELAY
    for attempt in range(1, retries + 1):
        try:
            logger.debug(f"Попытка {attempt}/{retries} для {url}")
            resp = requests.get(url, headers=headers, timeout=30)

            if resp.status_code == 429:
                logger.warning(f"HTTP 429 для {url}, ожидание {delay} сек")
                time.sleep(delay + random.uniform(0, RANDOM_DELAY))
                delay *= BACKOFF_FACTOR
                continue
            elif resp.status_code == 404:
                logger.warning(f"Страница не найдена: {url}")
                return None

            resp.raise_for_status()
            logger.debug(f"Успешно получен ответ для {url}")
            return BeautifulSoup(resp.text, 'html.parser')

        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса для {url} (попытка {attempt}): {e}")
            if attempt < retries:
                time.sleep(delay)
                delay *= BACKOFF_FACTOR
            else:
                logger.error(f"Все попытки исчерпаны для {url}")
                return None

    return None

def extract_movie_links(soup, base_url):
    """
    Извлечь ссылки на страницы фильмов из списка
    """
    movie_links = []

    # Поиск ссылок на фильмы с различными селекторами
    link_selectors = [
        'a[href*="/movie/"]',
        'a[data-test*="LINK"][href*="/movie/"]',
        '.movie-link',
        'a[href*="/cinema/movie/"]'
    ]

    for selector in link_selectors:
        links = soup.select(selector)
        for link in links:
            href = link.get('href')
            if href and '/movie/' in href:
                full_url = urljoin(base_url, href)
                if full_url not in movie_links:
                    movie_links.append(full_url)

    logger.info(f"Найдено {len(movie_links)} ссылок на фильмы")
    return movie_links

def parse_release_dates(soup):
    """
    Парсинг дат выхода фильма из различных источников на странице
    """
    release_dates = []

    # Поиск дат в различных форматах
    date_patterns = [
        r'(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})',
        r'(\d{4})-(\d{2})-(\d{2})',
        r'(\d{1,2})\.(\d{1,2})\.(\d{4})'
    ]

    # Поиск в тексте страницы
    page_text = soup.get_text()

    for pattern in date_patterns:
        matches = re.finditer(pattern, page_text)
        for match in matches:
            try:
                if 'января' in match.group() or 'февраля' in match.group():  # Российский формат
                    month_map = {
                        'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
                        'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
                        'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
                    }
                    day, month_name, year = match.groups()
                    month = month_map.get(month_name, '01')
                    date_str = f"{year}-{month}-{day.zfill(2)}"
                else:
                    date_str = match.group().replace('.', '-')

                parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
                if parsed_date not in release_dates:
                    release_dates.append(parsed_date)
            except ValueError:
                continue

    return release_dates

def parse_movie_page(soup, url):
    """
    Разобрать страницу фильма и вернуть Event или None
    """
    if not soup:
        return None

    # Поиск названия фильма с разными селекторами
    title_selectors = [
        'h1',
        '[data-test="ITEM-NAME"]',
        '.movie-title',
        '.film-title',
        'title'
    ]

    title = None
    for selector in title_selectors:
        title_el = soup.select_one(selector)
        if title_el:
            title = title_el.get_text(strip=True)
            if title and len(title) > 5:  # Проверка на разумную длину
                break

    if not title:
        logger.warning(f"Не удалось найти название для {url}")
        return None

    logger.debug(f"Найден фильм: {title}")

    # Поиск информации о стране
    countries = []
    country_selectors = [
        '[data-test="ITEM-META"] a',
        '.country',
        '.film-country',
        'span:contains("Страна")'
    ]

    for selector in country_selectors:
        country_elements = soup.select(selector)
        for el in country_elements:
            country_text = el.get_text(strip=True)
            if country_text and len(country_text) < 50:  # Фильтр слишком длинных текстов
                countries.append(country_text)

    # Проверка на исключенные страны
    if any(country in EXCLUDE_COUNTRIES for country in countries):
        logger.debug(f"Пропуск фильма '{title}' - страна в списке исключений: {countries}")
        return None

    # Поиск дат релиза
    release_dates = parse_release_dates(soup)

    # Если даты не найдены, используем текущую дату + несколько дней
    if not release_dates:
        today = datetime.now()
        release_dates = [today + timedelta(days=random.randint(1, 30))]
        logger.debug(f"Используется случайная дата для '{title}'")

    # Создание события
    event = Event()
    event.name = title
    event.begin = min(release_dates)
    event.end = event.begin + timedelta(hours=2)

    # Создание описания
    description_parts = [f"Фильм: {title}"]
    if countries:
        description_parts.append(f"Страна: {', '.join(countries[:3])}")  # Ограничиваем количество стран
    if len(release_dates) > 1:
        dates_str = ', '.join([d.strftime('%d.%m.%Y') for d in release_dates[:5]])
        description_parts.append(f"Даты показов: {dates_str}")
    description_parts.append(f"Источник: {url}")

    event.description = '\n'.join(description_parts)
    event.url = url

    logger.info(f"Создано событие для фильма: {title}")
    return event

def main():
    """
    Основной цикл парсинга и генерации календаря
    """
    logger.info(f"Начало парсинга афиши для города: {CITY}")
    logger.info(f"Максимум фильмов: {MAX_MOVIES}")
    logger.info(f"Исключенные страны: {EXCLUDE_COUNTRIES}")

    # Различные возможные URL для разных городов
    base_urls = [
        f'https://www.afisha.ru/{CITY}/cinema/',
        f'https://www.afisha.ru/movie/schedule/{CITY}/',
        'https://www.afisha.ru/data-vyhoda/',
        'https://www.afisha.ru/movie/y2025/'
    ]

    all_movie_urls = []

    # Попробуем получить ссылки с разных страниц
    for base_url in base_urls:
        try:
            logger.info(f"Парсинг страницы: {base_url}")
            soup = get_soup(base_url)
            if soup:
                movie_links = extract_movie_links(soup, base_url)
                all_movie_urls.extend(movie_links)

                # Если уже собрали достаточно ссылок, прекращаем
                if len(all_movie_urls) >= MAX_MOVIES:
                    break

            safe_delay(PAGE_DELAY)

        except Exception as e:
            logger.error(f"Ошибка при парсинге {base_url}: {e}")
            continue

    # Удаляем дубликаты и ограничиваем количество
    all_movie_urls = list(set(all_movie_urls))[:MAX_MOVIES]

    if not all_movie_urls:
        logger.error("Не удалось найти ссылки на фильмы")
        # Создаем тестовое событие
        cal = Calendar()
        test_event = Event()
        test_event.name = "Тестовый фильм"
        test_event.begin = datetime.now() + timedelta(days=1)
        test_event.end = test_event.begin + timedelta(hours=2)
        test_event.description = "Тестовое событие - парсинг не удался"
        cal.events.add(test_event)
    else:
        logger.info(f"Найдено {len(all_movie_urls)} уникальных URL фильмов")

        # Инициализация календаря
        cal = Calendar()
        successful_events = 0

        # Обработка каждой страницы фильма
        for idx, url in enumerate(all_movie_urls, 1):
            try:
                logger.info(f"Обработка {idx}/{len(all_movie_urls)}: {url}")
                soup = get_soup(url)
                event = parse_movie_page(soup, url)

                if event:
                    cal.events.add(event)
                    successful_events += 1
                    logger.info(f"Добавлен фильм: {event.name}")

                # Задержка между запросами
                safe_delay()

            except Exception as e:
                logger.error(f"Ошибка при обработке {url}: {e}")
                continue

        logger.info(f"Успешно обработано {successful_events} фильмов")

    # Сохранение результата
    try:
        with open('calendar.ics', 'w', encoding='utf-8') as f:
            f.writelines(cal)

        logger.info(f"Календарь сохранен: calendar.ics ({len(cal.events)} событий)")
        print(f"Готово: сохранён calendar.ics ({len(cal.events)} событий)")

        # Проверка файла
        if os.path.exists('calendar.ics'):
            file_size = os.path.getsize('calendar.ics')
            logger.info(f"Размер файла: {file_size} байт")

    except Exception as e:
        logger.error(f"Ошибка при сохранении файла: {e}")
        raise

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Парсинг прерван пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise
