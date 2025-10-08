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

# ОПТИМИЗИРОВАННЫЕ параметры для минимизации HTTP 429
MAX_MOVIES = 3              # Уменьшено для меньшего количества запросов
MAX_RETRIES = 3              # Уменьшено количество повторов
BACKOFF_FACTOR = 3           # Увеличен фактор отката
BASE_DELAY = 5               # Увеличена базовая задержка (было 2)
RANDOM_DELAY = 3             # Увеличена случайная задержка (было 1)
PAGE_DELAY = 8               # Увеличена задержка между страницами (было 3)
DETAIL_DELAY = 12            # Специальная задержка для детальных страниц

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
    default=3,
    help='Максимальное число фильмов для обработки'
)
parser.add_argument(
    '--delay',
    type=int,
    default=5,
    help='Базовая задержка между запросами в секундах'
)
args = parser.parse_args()

# Используем аргументы, если они переданы
if args.exclude_country:
    EXCLUDE_COUNTRIES = args.exclude_country
MAX_MOVIES = args.max_movies
if args.delay:
    BASE_DELAY = args.delay

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
            logger.debug(f"Запрос {attempt}/{retries} для {url[:50]}...")

            # Предварительная задержка перед каждым запросом
            if attempt > 1:
                smart_delay('retry')

            resp = session.get(url, timeout=45)

            if resp.status_code == 429:
                wait_time = delay * BACKOFF_FACTOR
                logger.warning(f"HTTP 429 для {url[:50]}... Ожидание {wait_time} сек (попытка {attempt})")
                time.sleep(wait_time)
                delay *= BACKOFF_FACTOR
                continue
            elif resp.status_code == 404:
                logger.warning(f"Страница не найдена: {url[:50]}...")
                return None
            elif resp.status_code == 403:
                logger.warning(f"Доступ запрещен (403): {url[:50]}...")
                time.sleep(delay * 2)
                delay *= 2
                continue

            resp.raise_for_status()
            logger.debug(f"Успешный ответ для {url[:50]}... (статус: {resp.status_code})")

            # Задержка после успешного запроса
            smart_delay(request_type)

            return BeautifulSoup(resp.text, 'html.parser')

        except requests.exceptions.Timeout:
            logger.warning(f"Таймаут для {url[:50]}... (попытка {attempt})")
            if attempt < retries:
                time.sleep(delay)
                delay *= BACKOFF_FACTOR
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса для {url[:50]}... (попытка {attempt}): {e}")
            if attempt < retries:
                time.sleep(delay)
                delay *= BACKOFF_FACTOR
            else:
                logger.error(f"Все попытки исчерпаны для {url[:50]}...")
                return None

    return None

def extract_movie_data_from_schedule(soup):
    """
    Извлечь данные о фильмах из расписания кинотеатров
    """
    movies_data = []

    # Поиск блоков с фильмами в расписании
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
        '.cinema-movie'
    ]

    movie_elements = []
    for selector in movie_selectors:
        elements = soup.select(selector)
        if elements:
            movie_elements = elements
            logger.info(f"Найдены элементы с селектором: {selector} ({len(elements)} шт.)")
            break

    if not movie_elements:
        # Если стандартные селекторы не работают, ищем по ссылкам
        links = soup.find_all('a', href=True)
        movie_links = [link for link in links if 'movie' in link['href'] or 'film' in link['href']]

        for link in movie_links[:MAX_MOVIES]:
            title = link.get_text(strip=True)
            if title and len(title) > 3:
                movie_data = {
                    'title': title,
                    'url': urljoin('https://www.afisha.ru', link['href']),
                    'times': [],
                    'countries': []
                }
                movies_data.append(movie_data)

        logger.info(f"Найдено {len(movies_data)} фильмов через ссылки")
        return movies_data

    # Обработка найденных элементов фильмов
    for element in movie_elements[:MAX_MOVIES]:
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

            # Поиск времени сеансов
            time_patterns = [
                r'(\d{1,2}[:.:]\d{2})',
                r'(\d{1,2}:\d{2})',
                r'(\d{1,2}\.\d{2})'
            ]

            times = []
            element_text = element.get_text()
            for pattern in time_patterns:
                matches = re.findall(pattern, element_text)
                for match in matches:
                    try:
                        # Приводим к стандартному формату
                        time_str = match.replace('.', ':')
                        parsed_time = datetime.strptime(time_str, '%H:%M')
                        if time_str not in times:  # Избегаем дубликатов
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
                'countries': []  # Будет заполнено позже при парсинге страницы фильма
            }

            movies_data.append(movie_data)
            logger.debug(f"Добавлен фильм: {title} ({len(times)} сеансов)")

        except Exception as e:
            logger.error(f"Ошибка при обработке элемента фильма: {e}")
            continue

    logger.info(f"Извлечено {len(movies_data)} фильмов из расписания")
    return movies_data

def parse_movie_details(movie_url):
    """
    Получить дополнительные данные о фильме со страницы фильма
    ОПТИМИЗИРОВАНО: с увеличенными задержками
    """
    if not movie_url:
        return []

    # Увеличенная задержка для детальных страниц
    logger.debug(f"Получение деталей фильма: {movie_url[:50]}...")
    soup = get_soup(movie_url, request_type='detail')

    if not soup:
        return []

    countries = []

    # Поиск информации о стране
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
                # Фильтрация нерелевантных данных
                if not any(word in country_text.lower() for word in ['жанр', 'режиссер', 'актер', 'год', 'время']):
                    countries.append(country_text)

    return countries

def create_calendar_event(movie_data):
    """
    Создать событие календаря для фильма
    """
    title = movie_data['title']
    times = movie_data['times']
    countries = movie_data['countries']
    movie_url = movie_data['url']

    # Проверка на исключенные страны
    if any(country in EXCLUDE_COUNTRIES for country in countries):
        logger.debug(f"Пропуск фильма '{title}' - страна в списке исключений: {countries}")
        return None

    # Определение времени события
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)

    if times:
        # Используем первое время сеанса
        try:
            time_str = times[0]
            show_time = datetime.strptime(time_str, '%H:%M').time()
            # Если время уже прошло сегодня, планируем на завтра
            event_datetime = datetime.combine(today, show_time)
            if event_datetime < datetime.now():
                event_datetime = datetime.combine(tomorrow, show_time)
        except ValueError:
            event_datetime = datetime.combine(tomorrow, datetime.min.time().replace(hour=19))
    else:
        # Если время не найдено, планируем на завтра в 19:00
        event_datetime = datetime.combine(tomorrow, datetime.min.time().replace(hour=19))

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
    description_parts.append(f"Дата: {event_datetime.strftime('%d.%m.%Y %H:%M')}")
    if movie_url:
        description_parts.append(f"Источник: {movie_url}")

    event.description = '\n'.join(description_parts)
    if movie_url:
        event.url = movie_url

    logger.info(f"Создано событие для фильма: {title}")
    return event

def main():
    """
    Основной цикл парсинга и генерации календаря
    ОПТИМИЗИРОВАНО: с контролем количества запросов
    """
    logger.info("Начало парсинга расписания кинотеатров Перми")
    logger.info(f"Максимум фильмов: {MAX_MOVIES}")
    logger.info(f"Базовая задержка: {BASE_DELAY} сек")
    logger.info(f"Задержка для деталей: {DETAIL_DELAY} сек")
    logger.info(f"Исключенные страны: {EXCLUDE_COUNTRIES}")

    # URL для парсинга расписания кинотеатров
    schedule_url = 'https://www.afisha.ru/prm/schedule_cinema/'

    try:
        logger.info(f"Парсинг расписания: {schedule_url}")
        schedule_soup = get_soup(schedule_url, request_type='page')

        if not schedule_soup:
            logger.error("Не удалось получить страницу расписания")
            # Создаем тестовое событие
            cal = Calendar()
            test_event = Event()
            test_event.name = "Расписание недоступно"
            test_event.begin = datetime.now() + timedelta(days=1)
            test_event.end = test_event.begin + timedelta(hours=2)
            test_event.description = "Не удалось получить расписание с сайта afisha.ru"
            cal.events.add(test_event)
        else:
            # Извлекаем данные о фильмах из расписания
            movies_data = extract_movie_data_from_schedule(schedule_soup)

            if not movies_data:
                logger.warning("Не найдено фильмов в расписании")
                # Создаем тестовое событие
                cal = Calendar()
                test_event = Event()
                test_event.name = "Нет фильмов в расписании"
                test_event.begin = datetime.now() + timedelta(days=1)
                test_event.end = test_event.begin + timedelta(hours=2)
                test_event.description = "В расписании кинотеатров не найдено фильмов"
                cal.events.add(test_event)
            else:
                # Инициализация календаря
                cal = Calendar()
                successful_events = 0

                # ЛИМИТИРОВАННАЯ обработка фильмов для минимизации запросов
                limited_movies = movies_data[:min(MAX_MOVIES, 15)]  # Не более 15 фильмов
                logger.info(f"Обработка ограничена {len(limited_movies)} фильмами для минимизации HTTP 429")

                # Обработка каждого фильма
                for idx, movie_data in enumerate(limited_movies, 1):
                    try:
                        logger.info(f"Обработка {idx}/{len(limited_movies)}: {movie_data['title']}")

                        # Получаем дополнительную информацию о фильме ТОЛЬКО для первых 10 фильмов
                        if movie_data['url'] and idx <= 10:
                            logger.debug(f"Получение деталей для фильма {idx}")
                            countries = parse_movie_details(movie_data['url'])
                            movie_data['countries'] = countries
                        else:
                            logger.debug(f"Пропуск деталей для фильма {idx} (экономия запросов)")
                            movie_data['countries'] = []

                        # Создаем событие календаря
                        event = create_calendar_event(movie_data)

                        if event:
                            cal.events.add(event)
                            successful_events += 1

                        # Дополнительная задержка между фильмами
                        if idx < len(limited_movies):
                            logger.debug(f"Пауза между фильмами...")
                            smart_delay('default')

                    except Exception as e:
                        logger.error(f"Ошибка при обработке фильма {movie_data['title']}: {e}")
                        continue

                logger.info(f"Успешно обработано {successful_events} фильмов")

        # Сохранение результата
        with open('calendar.ics', 'w', encoding='utf-8') as f:
            f.writelines(cal)

        logger.info(f"Календарь сохранен: calendar.ics ({len(cal.events)} событий)")
        print(f"Готово: сохранён calendar.ics ({len(cal.events)} событий)")

        # Проверка файла
        if os.path.exists('calendar.ics'):
            file_size = os.path.getsize('calendar.ics')
            logger.info(f"Размер файла: {file_size} байт")

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        # Создаем аварийное событие
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
