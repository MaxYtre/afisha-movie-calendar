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

# ОПТИМИЗИРОВАННЫЕ параметры БЕЗ лимитов по умолчанию
MAX_RETRIES = 5              
BACKOFF_FACTOR = 5           
BASE_DELAY = 5               
RANDOM_DELAY = 3             
PAGE_DELAY = 8               
DETAIL_DELAY = 12            

# Страны, фильмы которых НЕ включать в календарь
EXCLUDE_COUNTRIES = ['Россия']

# Аргументы командной строки - ВОССТАНОВЛЕНЫ для совместимости
parser = argparse.ArgumentParser(description='Безлимитный парсер афиши с расписанием сеансов')
parser.add_argument(
    '--exclude-country',
    action='append',
    default=[],
    help='Страна, которую не включать в календарь (можно указать несколько)'
)
parser.add_argument(
    '--max-movies',
    type=int,
    default=None,
    help='Максимальное число фильмов для обработки (по умолчанию - БЕЗ ЛИМИТА, обрабатываются ВСЕ)'
)
parser.add_argument(
    '--max-pages',
    type=int,
    default=None,
    help='Максимальное число страниц для парсинга (по умолчанию - БЕЗ ЛИМИТА, парсятся ВСЕ)'
)
parser.add_argument(
    '--delay',
    type=int,
    default=5,
    help='Базовая задержка между запросами в секундах'
)
parser.add_argument(
    '--skip-details',
    action='store_true',
    help='Пропустить получение детальной информации о фильмах (быстрее, но без стран)'
)
args = parser.parse_args()

# Используем аргументы, если они переданы
if args.exclude_country:
    EXCLUDE_COUNTRIES = args.exclude_country

# ВАЖНО: Лимиты используются ТОЛЬКО если явно заданы
MAX_MOVIES = args.max_movies  # None по умолчанию
MAX_PAGES = args.max_pages    # None по умолчанию

if args.delay:
    BASE_DELAY = args.delay
SKIP_DETAILS = args.skip_details

# Логирование настроек
if MAX_MOVIES:
    logger.info(f"Установлен лимит фильмов: {MAX_MOVIES}")
else:
    logger.info("❌ Лимит фильмов ОТКЛЮЧЕН - обрабатываются ВСЕ найденные")

if MAX_PAGES:
    logger.info(f"Установлен лимит страниц: {MAX_PAGES}")
else:
    logger.info("❌ Лимит страниц ОТКЛЮЧЕН - парсятся ВСЕ существующие")

def smart_delay(request_type='default'):
    """
    Умная задержка с разными параметрами для разных типов запросов
    """
    delays = {
        'default': BASE_DELAY,
        'detail': DETAIL_DELAY,
        'page': PAGE_DELAY,
        'retry': BASE_DELAY * 2
    }

    base_delay = delays.get(request_type, BASE_DELAY)
    actual_delay = base_delay + random.uniform(1, RANDOM_DELAY)

    time.sleep(actual_delay)
    logger.debug(f"Задержка {request_type}: {actual_delay:.2f} сек")

def get_soup(url, retries=MAX_RETRIES, request_type='default'):
    """
    Получить объект BeautifulSoup по URL с улучшенной обработкой HTTP 429
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0'
    }

    session = requests.Session()
    session.headers.update(headers)

    delay = BASE_DELAY
    for attempt in range(1, retries + 1):
        try:
            logger.debug(f"Запрос {attempt}/{retries} для {url[:60]}...")

            if attempt > 1:
                smart_delay('retry')

            resp = session.get(url, timeout=45)

            if resp.status_code == 429:
                wait_time = delay * BACKOFF_FACTOR
                logger.warning(f"HTTP 429 для {url[:60]}... Ожидание {wait_time} сек (попытка {attempt})")
                time.sleep(wait_time)
                delay *= BACKOFF_FACTOR
                continue
            elif resp.status_code == 404:
                logger.warning(f"Страница не найдена: {url[:60]}...")
                return None
            elif resp.status_code == 403:
                logger.warning(f"Доступ запрещен (403): {url[:60]}...")
                time.sleep(delay * 2)
                delay *= 2
                continue

            resp.raise_for_status()
            logger.debug(f"Успешный ответ для {url[:60]}... (статус: {resp.status_code})")

            smart_delay(request_type)

            return BeautifulSoup(resp.text, 'html.parser')

        except requests.exceptions.Timeout:
            logger.warning(f"Таймаут для {url[:60]}... (попытка {attempt})")
            if attempt < retries:
                time.sleep(delay)
                delay *= BACKOFF_FACTOR
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса для {url[:60]}... (попытка {attempt}): {e}")
            if attempt < retries:
                time.sleep(delay)
                delay *= BACKOFF_FACTOR
            else:
                logger.error(f"Все попытки исчерпаны для {url[:60]}...")
                return None

    return None

def parse_schedule_calendar(soup):
    """
    Парсить календарь расписания сеансов и найти ближайшую доступную дату
    """
    available_dates = []

    # Поиск календарного виджета
    calendar_selectors = [
        '.EyErB',  # основной класс календаря из примера
        '[aria-label="Календарь"]',
        '.calendar',
        '.schedule-calendar'
    ]

    calendar_widget = None
    for selector in calendar_selectors:
        calendar_widget = soup.select_one(selector)
        if calendar_widget:
            logger.debug(f"Найден календарь с селектором: {selector}")
            break

    if not calendar_widget:
        logger.debug("Календарь сеансов не найден")
        return None

    # Поиск активных дат (ссылки, не кнопки disabled)
    date_links = calendar_widget.find_all('a', class_='pdT6c')

    for link in date_links:
        try:
            aria_label = link.get('aria-label', '')
            day_elem = link.select_one('.YCVqY')
            if day_elem:
                day_number = day_elem.get_text(strip=True)

                # Определяем месяц и год из aria-label
                if 'октября' in aria_label:
                    month = 10
                    year = 2025
                elif 'ноября' in aria_label:
                    month = 11
                    year = 2025
                elif 'декабря' in aria_label:
                    month = 12
                    year = 2025
                else:
                    now = datetime.now()
                    month = now.month
                    year = now.year

                try:
                    show_date = date(year, month, int(day_number))
                    available_dates.append(show_date)
                    logger.debug(f"Найдена доступная дата: {show_date}")
                except ValueError as e:
                    logger.debug(f"Ошибка парсинга даты {day_number}.{month}.{year}: {e}")
                    continue

        except Exception as e:
            logger.debug(f"Ошибка при обработке элемента даты: {e}")
            continue

    if available_dates:
        available_dates.sort()
        nearest_date = available_dates[0]
        logger.debug(f"Ближайшая доступная дата: {nearest_date}")
        return nearest_date

    logger.debug("Не найдено доступных дат в календаре")
    return None

def parse_showtimes_from_page(soup):
    """
    Парсить время сеансов со страницы расписания фильма
    """
    showtimes = []

    time_selectors = [
        '.showtime',
        '.session-time', 
        '.time',
        '[data-time]',
        '.screening-time'
    ]

    for selector in time_selectors:
        time_elements = soup.select(selector)
        for elem in time_elements:
            time_text = elem.get_text(strip=True)
            time_match = re.search(r'(\d{1,2}[:.]\d{2})', time_text)
            if time_match:
                time_str = time_match.group(1).replace('.', ':')
                try:
                    parsed_time = datetime.strptime(time_str, '%H:%M')
                    if time_str not in showtimes:
                        showtimes.append(time_str)
                except ValueError:
                    continue

    if not showtimes:
        page_text = soup.get_text()
        time_patterns = [
            r'(\d{1,2}:\d{2})',
            r'(\d{1,2}\.\d{2})',
            r'(\d{1,2}[:.]\d{2})'
        ]

        for pattern in time_patterns:
            matches = re.findall(pattern, page_text)
            for match in matches:
                time_str = match.replace('.', ':')
                try:
                    parsed_time = datetime.strptime(time_str, '%H:%M')
                    hour = parsed_time.hour
                    if 6 <= hour <= 23:
                        if time_str not in showtimes:
                            showtimes.append(time_str)
                except ValueError:
                    continue

    return showtimes

def parse_movie_banner(soup):
    """
    Найти баннер/постер фильма
    """
    banner_selectors = [
        'img[src*="mediastorage"]',  # Основной селектор для баннеров afisha.ru
        '.poster img',
        '.movie-poster img',
        '.film-poster img',
        'img[alt*="постер"]',
        'img[alt*="poster"]',
        '.main-image img',
        '.hero-image img',
        'img[data-src*="mediastorage"]'
    ]

    for selector in banner_selectors:
        banner_elem = soup.select_one(selector)
        if banner_elem:
            # Получаем src или data-src
            banner_url = banner_elem.get('src') or banner_elem.get('data-src')
            if banner_url:
                # Делаем полный URL если нужно
                if banner_url.startswith('//'):
                    banner_url = 'https:' + banner_url
                elif banner_url.startswith('/'):
                    banner_url = 'https://www.afisha.ru' + banner_url

                logger.debug(f"Найден баннер: {banner_url}")
                return banner_url

    logger.debug("Баннер не найден")
    return None

def parse_movie_description(soup):
    """
    Найти описание фильма под заголовком "О фильме"
    """
    description_selectors = [
        # Поиск заголовка "О фильме" и следующего за ним текста
        'h2:contains("О фильме") + div',
        'h3:contains("О фильме") + div',
        'h2:contains("О фильме") + p',
        'h3:contains("О фильме") + p',
        '.about-movie',
        '.movie-description',
        '.film-description',
        '.description',
        '.synopsis',
        '.plot',
        '[data-test="ITEM-DESCRIPTION"]'
    ]

    # Сначала ищем по заголовку "О фильме"
    about_headers = soup.find_all(['h1', 'h2', 'h3', 'h4'], string=re.compile(r'О фильме', re.I))
    for header in about_headers:
        # Ищем следующий элемент с текстом
        next_elem = header.find_next_sibling(['div', 'p', 'section'])
        if next_elem:
            description = next_elem.get_text(strip=True)
            if description and len(description) > 20:
                logger.debug(f"Найдено описание через заголовок: {description[:100]}...")
                return description

    # Альтернативный поиск по селекторам
    for selector in description_selectors:
        if ':contains(' in selector:
            # Пропускаем CSS-селекторы с :contains, так как BeautifulSoup их не поддерживает
            continue

        desc_elem = soup.select_one(selector)
        if desc_elem:
            description = desc_elem.get_text(strip=True)
            if description and len(description) > 20:
                logger.debug(f"Найдено описание через селектор {selector}: {description[:100]}...")
                return description

    logger.debug("Описание фильма не найдено")
    return None

def parse_age_rating(soup):
    """
    Найти возрастной рейтинг фильма (например: 12+, 16+, 18+)
    """
    # Паттерны для поиска возрастного рейтинга
    age_patterns = [
        r'(\d+\+)',  # 12+, 16+, 18+
        r'(\d+ лет\+)',  # 12 лет+
        r'(без ограничений)',
        r'(0\+)',
        r'(6\+)',
        r'(12\+)',
        r'(16\+)',
        r'(18\+)'
    ]

    # Селекторы для поиска возрастного рейтинга
    age_selectors = [
        '.age-rating',
        '.rating',
        '.age',
        '[data-test="AGE-RATING"]',
        '.movie-rating',
        '.film-rating',
        '.restriction',
        '.mpaa'
    ]

    # Поиск по селекторам
    for selector in age_selectors:
        age_elem = soup.select_one(selector)
        if age_elem:
            age_text = age_elem.get_text(strip=True)
            for pattern in age_patterns:
                match = re.search(pattern, age_text, re.I)
                if match:
                    rating = match.group(1)
                    logger.debug(f"Найден возрастной рейтинг через селектор: {rating}")
                    return rating

    # Поиск по всему тексту страницы
    page_text = soup.get_text()
    for pattern in age_patterns:
        matches = re.findall(pattern, page_text, re.I)
        for match in matches:
            # Проверяем, что это действительно возрастной рейтинг
            if any(age in match for age in ['0+', '6+', '12+', '16+', '18+', 'без ограничений']):
                logger.debug(f"Найден возрастной рейтинг в тексте: {match}")
                return match

    logger.debug("Возрастной рейтинг не найден")
    return None

def extract_movie_data_from_schedule(soup):
    """
    Извлечь данные о фильмах ТОЛЬКО из карточек фильмов (ИСПРАВЛЕННАЯ ВЕРСИЯ)
    """
    movies_data = []

    # Специфичные селекторы для карточек фильмов на afisha.ru
    movie_card_selectors = [
        'div.oP17O[role="listitem"]',  # Основной селектор из примера
        'div[data-test="ITEM"]',      # Альтернативный селектор из data-test
        '.oP17O',                     # Упрощенный селектор класса
    ]

    movie_elements = []
    for selector in movie_card_selectors:
        elements = soup.select(selector)
        if elements:
            movie_elements = elements
            logger.debug(f"Найдены элементы карточек с селектором: {selector} ({len(elements)} шт.)")
            break

    if not movie_elements:
        logger.warning("❌ Карточки фильмов не найдены! Возможно, изменилась структура сайта.")
        return movies_data

    logger.debug(f"🎬 Обрабатываем {len(movie_elements)} карточек фильмов")

    for idx, card in enumerate(movie_elements, 1):
        try:
            # Поиск названия фильма
            title_selectors = [
                'a[data-test="LINK ITEM-NAME ITEM-URL"]',     # Основной селектор названия
                'a.CjnHd.y8A5E.nbCNS.yknrM',                 # Полный класс из примера
                'a[data-test*="ITEM-NAME"]',                  # Частичное совпадение
                '.QWR1k a',                                   # Ссылка в информационном блоке
                'a[href*="/movie/"]'                          # Ссылка на страницу фильма
            ]

            title = None
            movie_url = None

            for sel in title_selectors:
                title_elem = card.select_one(sel)
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    # Получаем URL фильма
                    movie_url = title_elem.get('href')
                    if movie_url and not movie_url.startswith('http'):
                        movie_url = 'https://www.afisha.ru' + movie_url
                    break

            if not title or len(title) < 2:
                continue

            # Поиск метаданных (год, жанр)
            meta_info = []
            meta_selectors = [
                'div[data-test="ITEM-META"]',    # Основной селектор метаданных
                '.S_wwn',                        # Класс из примера
                '.QWR1k .S_wwn',                # Полный путь к метаданным
            ]

            for sel in meta_selectors:
                meta_elem = card.select_one(sel)
                if meta_elem:
                    meta_text = meta_elem.get_text(strip=True)
                    if meta_text:
                        meta_info.append(meta_text)
                    break

            # Поиск рейтинга
            rating = None
            rating_selectors = [
                'div[data-test="RATING"]',       # Селектор рейтинга
                '.IrSqF.zPI3b.BNjPz.k96pX',     # Классы рейтинга из примера
            ]

            for sel in rating_selectors:
                rating_elem = card.select_one(sel)
                if rating_elem:
                    rating_text = rating_elem.get_text(strip=True)
                    try:
                        rating = float(rating_text)
                    except:
                        rating = rating_text
                    break

            # Поиск изображения/постера
            image_url = None
            img_selectors = [
                'img[data-test="IMAGE ITEM-IMAGE"]',  # Основной селектор изображения
                'picture img',                        # Изображение в picture элементе
                'img[src*="mediastorage"]',           # Изображения с mediastorage
            ]

            for sel in img_selectors:
                img_elem = card.select_one(sel)
                if img_elem:
                    image_url = img_elem.get('src')
                    if not image_url:
                        image_url = img_elem.get('data-src')
                    break

            # Создание объекта данных о фильме
            movie_data = {
                'title': title,
                'url': movie_url,
                'times': [],                # Заполняется позже при детальном парсинге
                'countries': [],            # Заполняется позже при детальном парсинге
                'nearest_show_date': None,
                'banner_url': image_url,    # Используем найденное изображение
                'description': None,
                'age_rating': None,
                'meta_info': meta_info,     # Дополнительная информация (год, жанр)
                'rating': rating            # Рейтинг фильма
            }

            movies_data.append(movie_data)
            logger.debug(f"✅ Добавлен фильм {idx}: {title}")
            if meta_info:
                logger.debug(f"   📋 Мета: {', '.join(meta_info)}")
            if rating:
                logger.debug(f"   ⭐ Рейтинг: {rating}")

        except Exception as e:
            logger.error(f"❌ Ошибка при обработке карточки {idx}: {e}")
            continue

    logger.debug(f"🎭 Извлечено {len(movies_data)} фильмов из карточек")
    return movies_data

def parse_all_schedule_pages(base_url):
    """
    Парсить все страницы расписания с учетом лимитов
    """
    all_movies_data = []
    current_page = 1

    if MAX_PAGES:
        logger.info(f"Парсинг с лимитом страниц: {MAX_PAGES}")
    else:
        logger.info(f"🔥 БЕЗЛИМИТНЫЙ парсинг всех существующих страниц")

    # Цикл с учетом лимита страниц
    while True:
        # Проверка лимита страниц
        if MAX_PAGES and current_page > MAX_PAGES:
            logger.info(f"🛑 Достигнут лимит страниц: {MAX_PAGES}")
            break

        if current_page == 1:
            page_url = base_url
        else:
            page_url = f"{base_url}page{current_page}/"

        logger.info(f"📄 Парсинг страницы {current_page}: {page_url}")

        soup = get_soup(page_url, request_type='page')

        if not soup:
            logger.info(f"❌ Страница {current_page} недоступна (404) - завершаем парсинг")
            break

        page_movies = extract_movie_data_from_schedule(soup)

        if not page_movies:
            logger.info(f"❌ На странице {current_page} не найдено фильмов - завершаем парсинг")
            break

        logger.info(f"✅ На странице {current_page} найдено {len(page_movies)} фильмов")

        # Добавляем фильмы, избегая дубликатов
        new_movies_count = 0
        existing_titles = {movie['title'] for movie in all_movies_data}

        for movie in page_movies:
            if movie['title'] not in existing_titles:
                all_movies_data.append(movie)
                existing_titles.add(movie['title'])
                new_movies_count += 1

                # Проверка лимита фильмов
                if MAX_MOVIES and len(all_movies_data) >= MAX_MOVIES:
                    logger.info(f"🛑 Достигнут лимит фильмов: {MAX_MOVIES}")
                    all_movies_data = all_movies_data[:MAX_MOVIES]
                    return all_movies_data

        logger.info(f"➕ Добавлено {new_movies_count} новых фильмов (всего: {len(all_movies_data)})")

        current_page += 1
        smart_delay('page')

    logger.info(f"🎬 ИТОГО найдено {len(all_movies_data)} уникальных фильмов на {current_page - 1} страницах")
    return all_movies_data

def parse_movie_details_and_schedule(movie_url):
    """
    Получить РАСШИРЕННЫЕ данные о фильме: страны, баннер, описание, возраст, расписание
    """
    if not movie_url:
        return [], None, [], None, None, None

    logger.debug(f"Получение расширенных деталей: {movie_url[:60]}...")
    soup = get_soup(movie_url, request_type='detail')

    if not soup:
        return [], None, [], None, None, None

    # Парсим страны
    countries = []
    country_selectors = [
        '[data-test="ITEM-META"] a',
        '.country',
        '.film-country',
        '.movie-country',
        'span:contains("Страна")',
        '.meta-info',
        '.film-meta'
    ]

    for selector in country_selectors:
        country_elements = soup.select(selector)
        for el in country_elements:
            country_text = el.get_text(strip=True)
            if country_text and len(country_text) < 50 and country_text not in countries:
                if not any(word in country_text.lower() for word in ['жанр', 'режиссер', 'актер', 'год', 'время']):
                    countries.append(country_text)

    # Парсим ближайшую дату сеансов из календаря
    nearest_show_date = parse_schedule_calendar(soup)

    # Парсим время сеансов
    showtimes = parse_showtimes_from_page(soup)

    # Парсим баннер фильма
    banner_url = parse_movie_banner(soup)

    # Парсим описание фильма
    description = parse_movie_description(soup)

    # Парсим возрастной рейтинг
    age_rating = parse_age_rating(soup)

    logger.debug(f"Парсинг завершен. Баннер: {'✅' if banner_url else '❌'}, Описание: {'✅' if description else '❌'}, Возраст: {'✅' if age_rating else '❌'}")

    return countries, nearest_show_date, showtimes, banner_url, description, age_rating

def create_calendar_event(movie_data):
    """
    Создать КРАСИВОЕ событие календаря с эмоджи и расширенной информацией
    """
    title = movie_data['title']
    times = movie_data['times']
    countries = movie_data['countries']
    movie_url = movie_data['url']
    nearest_show_date = movie_data.get('nearest_show_date')
    banner_url = movie_data.get('banner_url')
    description = movie_data.get('description')
    age_rating = movie_data.get('age_rating')
    meta_info = movie_data.get('meta_info', [])
    rating = movie_data.get('rating')

    # Проверка на исключенные страны
    if any(country in EXCLUDE_COUNTRIES for country in countries):
        logger.debug(f"Пропуск фильма '{title}' - страна в списке исключений: {countries}")
        return None

    # Определение даты и времени события
    if nearest_show_date:
        event_date = nearest_show_date
        logger.debug(f"Используется дата из расписания: {event_date}")
    else:
        event_date = datetime.now().date() + timedelta(days=1)
        logger.debug(f"Используется дата по умолчанию: {event_date}")

    # Определение времени
    if times:
        try:
            time_str = times[0]
            show_time = datetime.strptime(time_str, '%H:%M').time()
            event_datetime = datetime.combine(event_date, show_time)
        except ValueError:
            event_datetime = datetime.combine(event_date, datetime.min.time().replace(hour=19))
    else:
        event_datetime = datetime.combine(event_date, datetime.min.time().replace(hour=19))

    # Создание события
    event = Event()
    # Добавляем эмоджи хлопушки перед названием
    event.name = f"🎬 {title}"
    event.begin = event_datetime
    event.end = event_datetime + timedelta(hours=2)

    # Создание КРАСИВОГО описания с эмоджи
    description_parts = []

    # Заголовок с эмоджи
    description_parts.append(f"🎬 {title}")
    description_parts.append("=" * 50)

    # Основная информация
    if rating:
        description_parts.append(f"⭐ Рейтинг: {rating}")

    if age_rating:
        description_parts.append(f"🔞 Возраст: {age_rating}")

    if meta_info:
        description_parts.append(f"📋 Информация: {', '.join(meta_info)}")

    if countries:
        country_emoji = "🌍"
        description_parts.append(f"{country_emoji} Страна: {', '.join(countries[:3])}")

    # Описание фильма
    if description:
        description_parts.append("")
        description_parts.append("📖 О фильме:")
        description_parts.append(description[:500] + ("..." if len(description) > 500 else ""))

    # Баннер
    if banner_url:
        description_parts.append("")
        description_parts.append(f"🖼️ Постер: {banner_url}")

    # Информация о сеансах
    description_parts.append("")
    description_parts.append("🎭 Расписание:")
    if times:
        description_parts.append(f"⏰ Сеансы: {', '.join(times[:5])}")

    if nearest_show_date:
        description_parts.append(f"📅 Ближайший показ: {nearest_show_date.strftime('%d.%m.%Y')}")

    description_parts.append(f"📅 Дата события: {event_datetime.strftime('%d.%m.%Y %H:%M')}")

    # Источник
    if movie_url:
        description_parts.append("")
        description_parts.append(f"🔗 Подробности: {movie_url}")

    event.description = '\n'.join(description_parts)
    if movie_url:
        event.url = movie_url

    logger.info(f"Создано красивое событие: 🎬 {title} на {event_datetime.strftime('%d.%m.%Y %H:%M')}")
    return event

def main():
    """
    Парсинг и генерация календаря с поддержкой лимитов и РАСШИРЕННОЙ информацией
    """
    logger.info("🎬 Парсинг расписания кинотеатров Перми с РАСШИРЕННОЙ информацией о фильмах")
    logger.info("🔧 ИСПРАВЛЕНА ЛОГИКА ПАРСИНГА - теперь собираются ТОЛЬКО фильмы из карточек!")

    if MAX_MOVIES:
        logger.info(f"Установлен лимит фильмов: {MAX_MOVIES}")
    else:
        logger.info("❌ Лимит фильмов ОТКЛЮЧЕН - обрабатываются ВСЕ найденные")

    if MAX_PAGES:
        logger.info(f"Установлен лимит страниц: {MAX_PAGES}")
    else:
        logger.info("❌ Лимит страниц ОТКЛЮЧЕН - парсятся ВСЕ существующие")

    logger.info(f"Пропуск деталей: {'ДА (только основная информация)' if SKIP_DETAILS else 'НЕТ (ПОЛНАЯ информация: страны, баннер, описание, возраст)'}")
    logger.info(f"Базовая задержка: {BASE_DELAY} сек")
    logger.info(f"Исключенные страны: {EXCLUDE_COUNTRIES}")

    base_schedule_url = 'https://www.afisha.ru/prm/schedule_cinema/'

    try:
        # Парсим все страницы расписания
        all_movies_data = parse_all_schedule_pages(base_schedule_url)

        if not all_movies_data:
            logger.error("Не найдено фильмов ни на одной странице")
            cal = Calendar()
            test_event = Event()
            test_event.name = "🎬 Фильмы не найдены"
            test_event.begin = datetime.now() + timedelta(days=1)
            test_event.end = test_event.begin + timedelta(hours=2)
            test_event.description = "Не удалось найти фильмы в расписании кинотеатров"
            cal.events.add(test_event)
        else:
            cal = Calendar()
            successful_events = 0

            total_movies = len(all_movies_data)
            logger.info(f"🎯 Начинаем обработку {total_movies} найденных фильмов с РАСШИРЕННЫМИ деталями")

            # Обработка каждого фильма с расширенной информацией
            for idx, movie_data in enumerate(all_movies_data, 1):
                try:
                    logger.info(f"Обработка {idx}/{total_movies}: {movie_data['title']}")

                    # Получаем РАСШИРЕННУЮ информацию
                    if not SKIP_DETAILS and movie_data['url']:
                        logger.debug(f"Получение расширенных деталей для фильма {idx}")
                        countries, nearest_date, detailed_times, banner_url, description, age_rating = parse_movie_details_and_schedule(movie_data['url'])

                        movie_data['countries'] = countries
                        movie_data['nearest_show_date'] = nearest_date
                        if not movie_data['banner_url'] and banner_url:
                            movie_data['banner_url'] = banner_url
                        movie_data['description'] = description
                        movie_data['age_rating'] = age_rating

                        # Дополняем время сеансов
                        if detailed_times:
                            all_times = list(set(movie_data['times'] + detailed_times))
                            movie_data['times'] = sorted(all_times)
                    else:
                        if SKIP_DETAILS:
                            logger.debug(f"Пропуск деталей для фильма {idx} (флаг --skip-details)")
                        movie_data['countries'] = []
                        movie_data['nearest_show_date'] = None
                        movie_data['description'] = None
                        movie_data['age_rating'] = None

                    # Создаем КРАСИВОЕ событие календаря
                    event = create_calendar_event(movie_data)

                    if event:
                        cal.events.add(event)
                        successful_events += 1

                    # Прогресс каждые 10 фильмов
                    if idx % 10 == 0:
                        logger.info(f"📊 Обработано {idx}/{total_movies} фильмов, создано {successful_events} событий")

                    # Дополнительная задержка между фильмами
                    if idx < total_movies:
                        smart_delay('default')

                except Exception as e:
                    logger.error(f"Ошибка при обработке фильма {movie_data['title']}: {e}")
                    continue

            logger.info(f"✅ ЗАВЕРШЕНО: обработано {total_movies} фильмов, создано {successful_events} красивых событий")

        # Сохранение результата
        with open('calendar.ics', 'w', encoding='utf-8') as f:
            f.writelines(cal)

        logger.info(f"📅 Календарь сохранен: calendar.ics ({len(cal.events)} событий)")
        print(f"✅ Готово: сохранён calendar.ics ({len(cal.events)} событий)")

        if os.path.exists('calendar.ics'):
            file_size = os.path.getsize('calendar.ics')
            logger.info(f"📁 Размер файла: {file_size} байт")

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        cal = Calendar()
        error_event = Event()
        error_event.name = "🎬 Ошибка парсинга"
        error_event.begin = datetime.now() + timedelta(days=1)
        error_event.end = error_event.begin + timedelta(hours=2)
        error_event.description = f"Произошла ошибка при парсинге: {str(e)}"
        cal.events.add(error_event)

        with open('calendar.ics', 'w', encoding='utf-8') as f:
            f.writelines(cal)

        raise

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Парсинг прерван пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise
