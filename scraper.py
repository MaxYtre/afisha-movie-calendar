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
MAX_MOVIES = 50              # Разумное значение по умолчанию
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
args = parser.parse_args()

# Используем аргументы, если они переданы
if args.exclude_country:
    EXCLUDE_COUNTRIES = args.exclude_country
MAX_MOVIES = args.max_movies

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
        '.content-item'
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
    """
    if not movie_url:
        return []

    soup = get_soup(movie_url)
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
        '.meta-info'
    ]

    for selector in country_selectors:
        country_elements = soup.select(selector)
        for el in country_elements:
            country_text = el.get_text(strip=True)
            if country_text and len(country_text) < 50 and country_text not in countries:
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
    if times:
        # Используем первое время сеанса
        try:
            time_str = times[0]
            show_time = datetime.strptime(time_str, '%H:%M').time()
            event_datetime = datetime.combine(today, show_time)
        except ValueError:
            event_datetime = datetime.now() + timedelta(hours=1)
    else:
        # Если время не найдено, планируем на завтра
        event_datetime = datetime.combine(today + timedelta(days=1), datetime.min.time().replace(hour=19))

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
    """
    logger.info("Начало парсинга расписания кинотеатров Перми")
    logger.info(f"Максимум фильмов: {MAX_MOVIES}")
    logger.info(f"Исключенные страны: {EXCLUDE_COUNTRIES}")

    # URL для парсинга расписания кинотеатров
    schedule_url = 'https://www.afisha.ru/prm/schedule_cinema/'

    try:
        logger.info(f"Парсинг расписания: {schedule_url}")
        schedule_soup = get_soup(schedule_url)

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

                # Обработка каждого фильма
                for idx, movie_data in enumerate(movies_data[:MAX_MOVIES], 1):
                    try:
                        logger.info(f"Обработка {idx}/{len(movies_data)}: {movie_data['title']}")

                        # Получаем дополнительную информацию о фильме
                        if movie_data['url']:
                            countries = parse_movie_details(movie_data['url'])
                            movie_data['countries'] = countries
                            safe_delay()

                        # Создаем событие календаря
                        event = create_calendar_event(movie_data)

                        if event:
                            cal.events.add(event)
                            successful_events += 1

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
