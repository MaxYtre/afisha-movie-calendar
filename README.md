# Afisha Movie Calendar

Скрипт парсит афишу фильмов на сайте afisha.ru, фильтрует российские фильмы и генерирует файл `calendar.ics` для Google Calendar.

## Установка
1. Клонировать репозиторий:
   ```
   git clone https://github.com/<ваш-юзернейм>/afisha-movie-calendar.git
   ```
2. Перейти в папку проекта и установить зависимости:
   ```
   pip install -r requirements.txt
   ```

## Использование
```
python scraper.py
```

## Обновление
Еженедельно календарь обновляется автоматически с помощью GitHub Actions.
