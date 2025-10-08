#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Упрощенный парсер Afisha.ru без внешних рейтингов
- Только рейтинг с Афиши.ru
- Ускоренные задержки
- Реальные даты сеансов
- Без российских фильмов
"""

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

# Настройки для защиты от бана
MAX_MOVIES     = 3  # None for unlimited or int limit
MAX_RETRIES    = 8
BACKOFF_FACTOR = 2
BASE_DELAY     = 3  # seconds  # seconds
RANDOM_DELAY   = 2  # seconds  # seconds
PAGE_DELAY     = 4  # seconds
MAX_RETRIES    = 8
BASE_DELAY     = 3  # seconds  # базовая задержка в секундах
RANDOM_DELAY   = 2  # seconds  # случайная добавка к задержке
PAGE_DELAY = 2  # задержка между страницами

def safe_delay():
    """Безопасная задержка с случайным компонентом"""
    delay = BASE_DELAY + random.uniform(0, RANDOM_DELAY)
    time.sleep(delay)

def parse_date_from_movie_page(soup):
    """Парсит первую дату сеанса со страницы фильма"""
    try:
        # Месяц: class="v7qKY"
        month_element = soup.find(class_="v7qKY")
        # Дата: class="YCVqY"  
        date_element = soup.find(class_="YCVqY")

        if month_element and date_element:
            month_text = month_element.get_text(strip=True).lower()
            date_text = date_element.get_text(strip=True)

            # Словарь месяцев
            months_map = {
                'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
                'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
                'янв': 1, 'фев': 2, 'мар': 3, 'апр': 4, 'май': 5, 'июн': 6,
                'июл': 7, 'авг': 8, 'сен': 9, 'окт': 10, 'ноя': 11, 'дек': 12
            }

            # Извлекаем день из текста даты
            day_match = re.search(r'\d+', date_text)
            if day_match:
                day = int(day_match.group())

                # Ищем месяц
                for month_name, month_num in months_map.items():
                    if month_name in month_text:
                        try:
                            current_year = date.today().year
                            event_date = date(current_year, month_num, day)

                            # Если дата в прошлом, берем следующий год
                            if event_date < date.today():
                                event_date = date(current_year + 1, month_num, day)

                            return event_date
                        except ValueError:
                            continue

        # Fallback - завтра
        return date.today() + timedelta(days=1)

    except Exception as e:
        print(f"   ⚠️ Ошибка парсинга даты: {e}")
        return date.today() + timedelta(days=1)

def safe_request(session, url, max_retries=MAX_RETRIES):
    """Безопасный запрос с повторами"""
    for attempt in range(max_retries):
        try:
            print(f"   📡 Попытка {attempt + 1}/{max_retries}: {url}")

            response = session.get(url, timeout=30)
            response.raise_for_status()

            print(f"   ✅ Успешно загружено: {len(response.text)} символов")
            return response

        except Exception as e:
            print(f"   ❌ Попытка {attempt + 1} неудачна: {e}")

            if attempt < max_retries - 1:
                retry_delay = (attempt + 1) * 3  # Увеличиваем задержку
                print(f"   ⏰ Ждем {retry_delay}с перед повтором...")
                time.sleep(retry_delay)
            else:
                print(f"   💔 Все {max_retries} попыток исчерпаны")

    return None

def parse_movie_details_simplified(movie_url, session, movie_title):
    """Упрощенный парсинг деталей фильма БЕЗ внешних рейтингов"""
    try:
        print(f"   🔍 Парсим детали фильма: {movie_title}")

        response = safe_request(session, movie_url)
        if not response:
            return {}

        soup = BeautifulSoup(response.text, 'html.parser')
        details = {}

        # Дата первого сеанса
        event_date = parse_date_from_movie_page(soup)
        details['event_date'] = event_date
        print(f"   📅 Дата сеанса: {event_date.strftime('%d.%m.%Y')}")

        # Страна производства: class="GwglV"
        country_element = soup.find(class_="GwglV")
        if country_element:
            country = country_element.get_text(strip=True)
            details['country'] = country
            print(f"   🌍 Страна: {country}")

            # Проверяем российские фильмы
            if country.lower() in ['россия', 'russia']:
                print(f"   🚫 ИГНОРИРУЕМ: российский фильм")
                details['ignore_russian'] = True
                return details

        # Жанр: class="CjnHd y8A5E"
        genre_elements = soup.find_all(class_="CjnHd y8A5E")
        if genre_elements:
            details['genre'] = genre_elements[0].get_text(strip=True)
            print(f"   🎭 Жанр: {details['genre']}")

        # О фильме: class="aEVDY WEIGb t1V2l"
        description_element = soup.find(class_="aEVDY WEIGb t1V2l")
        if description_element:
            details['description'] = description_element.get_text(strip=True)
            print(f"   📖 Описание найдено: {len(details['description'])} символов")
        else:
            print("   ⚠️ Описание не найдено")

        # Постер: class="PwMBX rmwkz"
        poster_element = soup.find(class_="PwMBX rmwkz")
        if poster_element:
            if poster_element.name == 'img' and poster_element.get('src'):
                details['poster'] = urljoin(movie_url, poster_element['src'])
            else:
                img = poster_element.find('img', src=True)
                if img:
                    details['poster'] = urljoin(movie_url, img['src'])

            if details.get('poster'):
                print(f"   🖼️ Постер найден: {details['poster']}")
            else:
                print("   ⚠️ Постер не найден")
        else:
            print("   ⚠️ Элемент постера не найден")

        # УБРАНО: Поиск внешних рейтингов IMDB и Кинопоиск
        # Оставляем только рейтинг Афиши, который парсится на главной странице

        # Пауза после парсинга деталей
        safe_delay()

        return details

    except Exception as e:
        print(f"   ❌ Ошибка парсинга деталей: {e}")
        return {}

def parse_afisha_page_simplified(page_url, session):
    """Упрощенный парсинг страницы афиши"""
    movies = []
    ignored_count = 0

    try:
        print(f"\n📡 Парсим страницу: {page_url}")

        response = safe_request(session, page_url)
        if not response:
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        print(f"📄 Размер страницы: {len(response.text)} символов")

        # Ищем фильмы
        movie_elements = soup.find_all(class_="CjnHd y8A5E nbCNS yknrM")
        print(f"🎬 Найдено {len(movie_elements)} фильмов на странице")

        for i, movie_element in enumerate(movie_elements):
            try:
                print(f"\n   🎬 Обрабатываем фильм {i+1}/{len(movie_elements)}")

                # Название и ссылка
                title = movie_element.get_text(strip=True)
                movie_url = None

                if movie_element.name == 'a' and movie_element.get('href'):
                    movie_url = urljoin(page_url, movie_element['href'])
                else:
                    parent = movie_element.find_parent('a', href=True)
                    if parent:
                        movie_url = urljoin(page_url, parent['href'])

                if not title or not movie_url:
                    print(f"   ⚠️ Пропускаем: нет названия или ссылки")
                    continue

                print(f"   📝 Название: {title}")
                print(f"   🔗 Ссылка: {movie_url}")

                # Рейтинг Афиши и год из списка
                afisha_rating = None
                year = None
                container = movie_element.find_parent()

                if container:
                    rating_element = container.find(class_="IrSqF zPI3b BNjPz k96pX")
                    if rating_element:
                        afisha_rating = rating_element.get_text(strip=True)
                        print(f"   ⭐ Рейтинг Афиши: {afisha_rating}")

                    year_element = container.find(class_="S_wwn")
                    if year_element:
                        year = year_element.get_text(strip=True)
                        print(f"   📅 Год: {year}")

                # Детальная информация (БЕЗ внешних рейтингов)
                details = parse_movie_details_simplified(movie_url, session, title)

                # Проверяем российские фильмы
                if details.get('ignore_russian'):
                    ignored_count += 1
                    print(f"   🚫 ПРОПУЩЕН: российский фильм")
                    continue

                # Создаем объект фильма
                movie_data = {
                    'title': title,
                    'date': details.get('event_date', date.today() + timedelta(days=len(movies) + 1)),
                    'source': 'Afisha.ru',
                    'confidence': 1.0,
                    'direct_url': movie_url,
                    'found_text': title,
                    'afisha_rating': afisha_rating,  # Только рейтинг Афиши
                    'year': year,
                    'country': details.get('country'),
                    'genre': details.get('genre'),
                    'description': details.get('description', f"Фильм '{title}' в кинотеатрах Перми."),
                    'poster': details.get('poster')
                    # УБРАНО: imdb_rating, kinopoisk_rating
                }

                movies.append(movie_data)
                print(f"   ✅ ДОБАВЛЕН в календарь")

                # Пауза между фильмами
                print(f"   ⏰ Пауза перед следующим фильмом...")
                safe_delay()

            except Exception as e:
                print(f"   ❌ Ошибка обработки фильма {i+1}: {e}")
                continue

        print(f"\n📊 Страница завершена: добавлено {len(movies)}, пропущено {ignored_count}")
        return movies

    except Exception as e:
        print(f"❌ Ошибка парсинга страницы: {e}")
        return []

def get_all_afisha_pages_simplified():
    """Упрощенный сбор всех фильмов БЕЗ внешних рейтингов"""
    print("🎬 УПРОЩЕННЫЙ ПАРСЕР AFISHA.RU")
    print("⚡ Быстрые задержки: 1-2с между запросами")
    print("⭐ ТОЛЬКО рейтинг Афиши (БЕЗ IMDB/Кинопоиск)")
    print("📅 Реальные даты сеансов")
    print("🚫 Фильтрация российских фильмов")

    all_movies = []

    # Настройка сессии
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'DNT': '1',
        'Upgrade-Insecure-Requests': '1'
    })

    base_url = "https://www.afisha.ru/prm/schedule_cinema"

    try:
        # Парсим первую страницу
        page_1_movies = parse_afisha_page_simplified(base_url, session)
        all_movies.extend(page_1_movies)

        print(f"\n📊 Страница 1: найдено {len(page_1_movies)} фильмов")

        # Пауза между страницами
        print(f"⏰ Пауза {PAGE_DELAY}с между страницами...")
        time.sleep(PAGE_DELAY)

        # Дополнительные страницы
        page_num = 2
        max_pages = 5

        while page_num <= max_pages:
            page_url = f"{base_url}/page{page_num}/"

            print(f"\n🔍 Проверяем страницу {page_num}...")

            # Проверяем существование
            try:
                response = session.head(page_url, timeout=30)
                if response.status_code != 200:
                    print(f"⚠️ Страница {page_num} недоступна")
                    break
            except:
                print(f"⚠️ Ошибка проверки страницы {page_num}")
                break

            # Парсим страницу
            page_movies = parse_afisha_page_simplified(page_url, session)

            if not page_movies:
                print(f"⚠️ На странице {page_num} нет фильмов")
                break

            all_movies.extend(page_movies)
            print(f"📊 Страница {page_num}: найдено {len(page_movies)} фильмов")

            page_num += 1

            # Пауза между страницами
            if page_num <= max_pages:
                print(f"⏰ Пауза {PAGE_DELAY}с между страницами...")
                time.sleep(PAGE_DELAY)

        print(f"\n🎉 ПАРСИНГ ЗАВЕРШЕН!")
        print(f"📊 Всего найдено: {len(all_movies)} фильмов")

        # Упрощенная статистика
        with_description = len([m for m in all_movies if m.get('description') and len(m['description']) > 50])
        with_poster = len([m for m in all_movies if m.get('poster')])
        with_afisha_rating = len([m for m in all_movies if m.get('afisha_rating')])

        print("\n📈 Качество данных:")
        print(f"   📖 С описанием: {with_description}/{len(all_movies)}")
        print(f"   🖼️ С постером: {with_poster}/{len(all_movies)}")
        print(f"   ⭐ С рейтингом Афиши: {with_afisha_rating}/{len(all_movies)}")

    except Exception as e:
        print(f"❌ Общая ошибка: {e}")

    return all_movies

def create_simplified_calendar(movies_list):
    """Создает календарь с упрощенной информацией"""
    calendar = Calendar()
    calendar.creator = "Календарь кинотеатров Перми"

    print(f"\n📅 Создаем календарь из {len(movies_list)} фильмов...")

    for movie in movies_list:
        event = Event()
        event.name = f"🎬 {movie['title']}"
        event.begin = movie['date']
        event.make_all_day()

        description_lines = []

        # Основная информация (БЕЗ внешних рейтингов)
        info_parts = []
        if movie.get('afisha_rating'):
            info_parts.append(f"Рейтинг: {movie['afisha_rating']}")
        if movie.get('year'):
            info_parts.append(f"Год: {movie['year']}")
        if movie.get('country'):
            info_parts.append(f"Страна: {movie['country']}")
        if movie.get('genre'):
            info_parts.append(f"Жанр: {movie['genre']}")

        if info_parts:
            description_lines.extend([
                "ℹ️ ИНФОРМАЦИЯ:",
                f"{' | '.join(info_parts)}",
                ""
            ])

        # Постер
        if movie.get('poster'):
            description_lines.extend([
                "🖼️ ПОСТЕР:",
                f"   {movie['poster']}",
                ""
            ])

        # Описание
        if movie.get('description'):
            description_lines.extend([
                "📖 ОПИСАНИЕ:",
                f"{movie['description']}",
                ""
            ])

        # Ссылка
        if movie.get('direct_url'):
            description_lines.extend([
                "🎫 СТРАНИЦА ФИЛЬМА:",
                f"👉 {movie['direct_url']}"
            ])

        event.description = "\n".join(description_lines)
        calendar.events.add(event)

    return calendar

def remove_duplicates(movies_list):
    """Удаляет дубликаты"""
    if not movies_list:
        return []

    print(f"\n🔄 Удаляем дубликаты из {len(movies_list)} фильмов...")

    seen_titles = set()
    unique_movies = []

    for movie in movies_list:
        title = movie['title'].lower().strip()
        if title not in seen_titles:
            seen_titles.add(title)
            unique_movies.append(movie)
        else:
            print(f"   🗑️ Дубликат: {movie['title']}")

    return unique_movies

def save_calendar(calendar, filename='perm-cinema.ics'):
    """Сохраняет календарь"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.writelines(calendar.serialize())
        return True
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")
        return False

def main():
    print("🎬 УПРОЩЕННЫЙ ПАРСЕР AFISHA.RU")
    print("⚡ Быстрые задержки для оптимальной работы")
    print("⭐ Только рейтинг Афиши (надежно и безопасно)")
    print("📅 Реальные даты сеансов")
    print("🚫 Без российских фильмов")
    print("🚀 Без внешних API - максимальная стабильность")

    start_time = time.time()

    # Собираем фильмы
    all_movies = get_all_afisha_pages_simplified()

    if not all_movies:
        print("⚠️ Фильмы не найдены")
        return

    # Удаляем дубликаты
    unique_movies = remove_duplicates(all_movies)

    if not unique_movies:
        print("⚠️ Нет уникальных фильмов")
        return

    # Создаем календарь
    calendar = create_simplified_calendar(unique_movies)

    # Сохраняем
    if save_calendar(calendar):
        elapsed = int(time.time() - start_time)

        print(f"\n✅ ПАРСИНГ УСПЕШНО ЗАВЕРШЕН!")
        print(f"⏰ Время выполнения: {elapsed//60}м {elapsed%60}с")
        print(f"📅 Календарь создан с {len(calendar.events)} событиями")
        print(f"🎬 Уникальных фильмов: {len(unique_movies)}")
        print(f"⚡ Упрощенный и надежный подход")

    else:
        print("❌ Ошибка сохранения календаря")

if __name__ == "__main__":
    main()
