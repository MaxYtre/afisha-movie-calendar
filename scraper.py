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

# ОПТИМИЗИРОВАННЫЕ параметры БЕЗ КАКИХ-ЛИБО лимитов
MAX_RETRIES = 3              
BACKOFF_FACTOR = 3           
BASE_DELAY = 5               
RANDOM_DELAY = 3             
PAGE_DELAY = 8               
DETAIL_DELAY = 12            

# Страны, фильмы которых НЕ включать в календарь
EXCLUDE_COUNTRIES = ['Россия']

# Аргументы командной строки
parser = argparse.ArgumentParser(description='Полностью безлимитный парсер афиши с расписанием сеансов')
parser.add_argument(
    '--exclude-country',
    action='append',
    default=[],
    help='Страна, которую не включать в календарь (можно указать несколько)'
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
# УБРАНЫ аргументы --max-movies и --max-pages

args = parser.parse_args()

# Используем аргументы, если они переданы
if args.exclude_country:
    EXCLUDE_COUNTRIES = args.exclude_country

if args.delay:
    BASE_DELAY = args.delay
SKIP_DETAILS = args.skip_details

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
            time_match = re.search(r'(\d{1,2}[:.:]\d{2})', time_text)
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
            r'(\d{1,2}[:.:]\d{2})'
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

def extract_movie_data_from_schedule(soup):
    """
    Извлечь ВСЕ данные о фильмах из расписания кинотеатров - БЕЗ КАКИХ-ЛИБО ЛИМИТОВ
    """
    movies_data = []

    movie_selectors = [
        '.movie-item',
        '.film-item', 
        '.schedule-item',
        '[data-movie]',
        '.movie',
        '.film',
        'article',
        '.content-item',
        '.list-item',
        '.cinema-movie',
        '.schedule-movie',
        '.event-item',
        '.item'
    ]

    movie_elements = []
    for selector in movie_selectors:
        elements = soup.select(selector)
        if elements:
            movie_elements = elements
            logger.debug(f"Найдены элементы с селектором: {selector} ({len(elements)} шт.)")
            break

    if not movie_elements:
        # Поиск по ссылкам - обрабатываем ВСЕ найденные ссылки
        links = soup.find_all('a', href=True)
        movie_links = [link for link in links if 'movie' in link['href'] or 'film' in link['href']]

        logger.debug(f"Найдено {len(movie_links)} ссылок на фильмы - обрабатываем ВСЕ без исключений")

        # УБРАНО ограничение [:MAX_MOVIES] - обрабатываем ВСЕ
        for idx, link in enumerate(movie_links, 1):
            title = link.get_text(strip=True)
            if title and len(title) > 3:
                movie_data = {
                    'title': title,
                    'url': urljoin('https://www.afisha.ru', link['href']),
                    'times': [],
                    'countries': [],
                    'nearest_show_date': None
                }
                movies_data.append(movie_data)

        logger.debug(f"Добавлено {len(movies_data)} фильмов через ссылки")
        return movies_data

    # Обработка найденных элементов - ВСЕ БЕЗ ИСКЛЮЧЕНИЯ
    logger.debug(f"Обрабатываем ВСЕ {len(movie_elements)} найденных элементов фильмов без ограничений")

    # УБРАНО ограничение [:MAX_MOVIES] - обрабатываем ВСЕ элементы
    for idx, element in enumerate(movie_elements, 1):
        try:
            # Поиск названия фильма
            title_selectors = ['h1', 'h2', 'h3', '.title', '.name', 'a', 'strong']
            title = None

            for sel in title_selectors:
                title_elem = element.select_one(sel)
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    if title and len(title) > 3:
                        break

            if not title:
                continue

            # Поиск времени сеансов в элементе
            times = []
            time_patterns = [
                r'(\d{1,2}[:.:]\d{2})',
                r'(\d{1,2}:\d{2})',
                r'(\d{1,2}\.\d{2})'
            ]

            element_text = element.get_text()
            for pattern in time_patterns:
                matches = re.findall(pattern, element_text)
                for match in matches:
                    try:
                        time_str = match.replace('.', ':')
                        parsed_time = datetime.strptime(time_str, '%H:%M')
                        if time_str not in times:
                            times.append(time_str)
                    except ValueError:
                        continue

            # Поиск ссылки на фильм
            movie_url = None
            link_elem = element.find('a', href=True)
            if link_elem:
                movie_url = urljoin('https://www.afisha.ru', link_elem['href'])

            movie_data = {
                'title': title,
                'url': movie_url,
                'times': times,
                'countries': [],
                'nearest_show_date': None
            }

            movies_data.append(movie_data)
            logger.debug(f"Добавлен фильм {idx}: {title} ({len(times)} сеансов)")

        except Exception as e:
            logger.error(f"Ошибка при обработке элемента фильма {idx}: {e}")
            continue

    logger.debug(f"Извлечено {len(movies_data)} фильмов из текущей страницы")
    return movies_data

def parse_all_schedule_pages(base_url):
    """
    Парсить ВСЕ СУЩЕСТВУЮЩИЕ страницы расписания БЕЗ КАКИХ-ЛИБО ЛИМИТОВ
    Останавливается ТОЛЬКО при достижении несуществующей страницы
    """
    all_movies_data = []
    current_page = 1

    logger.info(f"🔥 АБСОЛЮТНО БЕЗЛИМИТНЫЙ парсинг ВСЕХ существующих страниц расписания")
    logger.info("Остановка ТОЛЬКО при достижении несуществующей страницы (404)")

    # УБРАН лимит MAX_PAGES - парсим до тех пор, пока страницы существуют
    while True:  # БЕСКОНЕЧНЫЙ цикл - останавливается только при 404 или пустой странице
        if current_page == 1:
            page_url = base_url
        else:
            page_url = f"{base_url}page{current_page}/"

        logger.info(f"📄 Парсинг страницы {current_page}: {page_url}")

        soup = get_soup(page_url, request_type='page')

        if not soup:
            logger.info(f"❌ Страница {current_page} недоступна (404) - ЗАВЕРШАЕМ парсинг")
            break

        page_movies = extract_movie_data_from_schedule(soup)

        if not page_movies:
            logger.info(f"❌ На странице {current_page} не найдено фильмов - ЗАВЕРШАЕМ парсинг")
            break

        logger.info(f"✅ На странице {current_page} найдено {len(page_movies)} фильмов")

        # Добавляем ВСЕ фильмы, избегая дубликатов только по названию
        new_movies_count = 0
        existing_titles = {movie['title'] for movie in all_movies_data}

        for movie in page_movies:
            if movie['title'] not in existing_titles:
                all_movies_data.append(movie)
                existing_titles.add(movie['title'])
                new_movies_count += 1

        logger.info(f"➕ Добавлено {new_movies_count} новых фильмов (всего: {len(all_movies_data)})")

        # УБРАНА полностью проверка лимита фильмов - продолжаем до конца

        current_page += 1

        # Проверяем следующую страницу только для логирования
        next_page_url = f"{base_url}page{current_page}/"
        logger.debug(f"🔍 Проверяем наличие страницы {current_page}")

        smart_delay('page')

    logger.info(f"🎬 ИТОГО найдено {len(all_movies_data)} уникальных фильмов на {current_page - 1} страницах")
    return all_movies_data

def parse_movie_details_and_schedule(movie_url):
    """
    Получить дополнительные данные о фильме и расписание сеансов
    """
    if not movie_url:
        return [], None, []

    logger.debug(f"Получение деталей и расписания: {movie_url[:60]}...")
    soup = get_soup(movie_url, request_type='detail')

    if not soup:
        return [], None, []

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

    return countries, nearest_show_date, showtimes

def create_calendar_event(movie_data):
    """
    Создать событие календаря для фильма с учетом реального расписания
    """
    title = movie_data['title']
    times = movie_data['times']
    countries = movie_data['countries']
    movie_url = movie_data['url']
    nearest_show_date = movie_data.get('nearest_show_date')

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
    event.name = title
    event.begin = event_datetime
    event.end = event_datetime + timedelta(hours=2)

    # Создание описания
    description_parts = [f"Фильм: {title}"]
    if countries:
        description_parts.append(f"Страна: {', '.join(countries[:3])}")
    if times:
        description_parts.append(f"Сеансы: {', '.join(times[:5])}")
    if nearest_show_date:
        description_parts.append(f"Ближайший показ: {nearest_show_date.strftime('%d.%m.%Y')}")
    description_parts.append(f"Дата события: {event_datetime.strftime('%d.%m.%Y %H:%M')}")
    if movie_url:
        description_parts.append(f"Источник: {movie_url}")

    event.description = '\n'.join(description_parts)
    if movie_url:
        event.url = movie_url

    logger.info(f"Создано событие для фильма: {title} на {event_datetime.strftime('%d.%m.%Y %H:%M')}")
    return event

def main():
    """
    ПОЛНОСТЬЮ БЕЗЛИМИТНЫЙ парсинг и генерация календаря
    """
    logger.info("🔥 АБСОЛЮТНО БЕЗЛИМИТНЫЙ парсинг расписания кинотеатров Перми")
    logger.info("❌ НЕТ ЛИМИТОВ: ни на страницы, ни на фильмы")
    logger.info("🛑 Остановка ТОЛЬКО при: 404 страницы или отсутствии фильмов")
    logger.info(f"Пропуск деталей: {'ДА (только основная информация)' if SKIP_DETAILS else 'НЕТ (полная информация + расписание)'}")
    logger.info(f"Базовая задержка: {BASE_DELAY} сек")
    logger.info(f"Исключенные страны: {EXCLUDE_COUNTRIES}")

    base_schedule_url = 'https://www.afisha.ru/prm/schedule_cinema/'

    try:
        # Парсим АБСОЛЮТНО ВСЕ страницы расписания
        all_movies_data = parse_all_schedule_pages(base_schedule_url)

        if not all_movies_data:
            logger.error("Не найдено фильмов ни на одной странице")
            cal = Calendar()
            test_event = Event()
            test_event.name = "Фильмы не найдены"
            test_event.begin = datetime.now() + timedelta(days=1)
            test_event.end = test_event.begin + timedelta(hours=2)
            test_event.description = "Не удалось найти фильмы в расписании кинотеатров"
            cal.events.add(test_event)
        else:
            cal = Calendar()
            successful_events = 0

            total_movies = len(all_movies_data)
            logger.info(f"🎯 Начинаем ПОЛНОСТЬЮ БЕЗЛИМИТНУЮ обработку ВСЕХ {total_movies} найденных фильмов")

            # Обработка АБСОЛЮТНО КАЖДОГО фильма БЕЗ КАКИХ-ЛИБО ОГРАНИЧЕНИЙ
            for idx, movie_data in enumerate(all_movies_data, 1):
                try:
                    logger.info(f"Обработка {idx}/{total_movies}: {movie_data['title']}")

                    # Получаем детальную информацию и расписание
                    if not SKIP_DETAILS and movie_data['url']:
                        logger.debug(f"Получение деталей и расписания для фильма {idx}")
                        countries, nearest_date, detailed_times = parse_movie_details_and_schedule(movie_data['url'])

                        movie_data['countries'] = countries
                        movie_data['nearest_show_date'] = nearest_date

                        # Дополняем время сеансов
                        if detailed_times:
                            all_times = list(set(movie_data['times'] + detailed_times))
                            movie_data['times'] = sorted(all_times)
                    else:
                        if SKIP_DETAILS:
                            logger.debug(f"Пропуск деталей для фильма {idx} (флаг --skip-details)")
                        movie_data['countries'] = []
                        movie_data['nearest_show_date'] = None

                    # Создаем событие календаря
                    event = create_calendar_event(movie_data)

                    if event:
                        cal.events.add(event)
                        successful_events += 1

                    # Прогресс каждые 50 фильмов (увеличен интервал для больших объемов)
                    if idx % 50 == 0:
                        logger.info(f"📊 Обработано {idx}/{total_movies} фильмов, создано {successful_events} событий")

                    # Дополнительная задержка между фильмами
                    if idx < total_movies:
                        smart_delay('default')

                except Exception as e:
                    logger.error(f"Ошибка при обработке фильма {movie_data['title']}: {e}")
                    continue

            logger.info(f"✅ ЗАВЕРШЕНО: обработано {total_movies} фильмов, создано {successful_events} событий")

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
        error_event.name = "Ошибка парсинга"
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
