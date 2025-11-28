# news_parser.py (обновленная версия с инкрементальным парсингом и логичным обновлением кэша)
import asyncio
import json
import os
from datetime import datetime, timedelta
import pytz
import feedparser
import aiohttp
import re
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config import *
from bs4 import BeautifulSoup
from telethon import TelegramClient
from telethon.errors import FloodWaitError, UnauthorizedError
from playwright.async_api import async_playwright
from urllib.parse import quote
import hashlib
import random
from email.utils import parsedate_to_datetime
import sqlite3
from utils import *
from news_analyzer import *

init_db()  # Инициализация БД при запуске модуля

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/news_parser.log", mode="a", encoding="utf-8")
    ]
)

# Глобальные блокировки
SESSION_POOL_LOCK = asyncio.Lock()
DUPLICATE_CACHE_LOCK = asyncio.Lock()

# Пул сессий: первые 4 — для мониторинга, остальные — для ручного парсинга
SESSION_POOL = []
for i in range(len(ACCOUNTS)):
    is_monitoring_session = i < 2
    SESSION_POOL.append({
        "name": f"account_{i}",
        "semaphore": asyncio.Semaphore(3), 
        "available": True,
        "current_task": None,
        "tasks_processed": 0,
        "is_monitoring": is_monitoring_session
    })

# Очередь задач и события для отслеживания завершения
TASK_QUEUE = asyncio.Queue()
TASK_EVENTS = {}

def generate_aliases(bank_name):
    """Генерация алиасов для поиска в тексте"""
    aliases = [bank_name]
    if bank_name in BANKS:
        aliases.extend(BANKS[bank_name].get("aliases", []))
    return aliases

def is_bank_name_match(text, aliases):
    """Проверка на соответствие названию банка — ищет ВСЕ слова из алиаса в тексте"""
    if not text:
        return False
    normalized_text = normalize_text_for_aliases(text)
    for alias in aliases:
        normalized_alias = normalize_text_for_aliases(alias)
        alias_words = normalized_alias.split()
        all_words_found = True
        for word in alias_words:
            if word not in normalized_text:
                all_words_found = False
                break
        if all_words_found:
            return True
    return False

def normalize_text_for_aliases(text):
    """Нормализация текста для сравнения алиасов"""
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^a-zа-яё0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

async def get_session_for_task(is_monitoring=False):
    """Получение подходящей сессии для задачи."""
    async with SESSION_POOL_LOCK:
        target_type = is_monitoring
        for session_info in SESSION_POOL:
            if session_info["available"] and session_info["is_monitoring"] == target_type:
                session_info["available"] = False
                logging.info(f"Сессия {session_info['name']} (тип: {'мониторинг' if is_monitoring else 'ручной'}) выделена для задачи")
                return session_info
        logging.warning(f"Нет доступных сессий типа {'мониторинг' if is_monitoring else 'ручной'}")
        return None

def release_session(session_info):
    """Освобождение сессии после выполнения задачи"""
    async def _release():
        async with SESSION_POOL_LOCK:
            session_info["available"] = True
            session_info["current_task"] = None
            logging.info(f"Сессия {session_info['name']} освобождена")
    asyncio.create_task(_release())


async def fetch_news_from_apis(bank_name, date_from, date_to, topic=None, is_monitoring=False):
    """Асинхронное получение новостей из API"""
    reg_number = BANKS.get(bank_name, {}).get("reg_number", bank_name)
    aliases = generate_aliases(bank_name)
    news_articles = []
    timeout = aiohttp.ClientTimeout(total=20)
    MAX_QUERY_LENGTH = 500
    current_batch = []
    current_length = 0
    batches = []
    for alias in aliases:
        normalized_alias = normalize_text_for_aliases(alias)
        if current_length + len(normalized_alias) + 1 > MAX_QUERY_LENGTH:
            batches.append(current_batch)
            current_batch = []
            current_length = 0
        current_batch.append(alias)
        current_length += len(normalized_alias) + 1
    if current_batch:
        batches.append(current_batch)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        for batch in batches:
            if "newsapi" in NEWSAPI_KEY:
                tasks.append(fetch_newsapi_news(session, bank_name, batch, date_from, date_to, topic))
            if "gnews" in GNEWS_API_KEY:
                tasks.append(fetch_gnews_news(session, bank_name, batch, date_from, date_to, topic))
            if "mediastack" in MEDIASTACK_API_KEY:
                tasks.append(fetch_mediastack_news(session, bank_name, batch, date_from, date_to, topic))
            if "currents" in CURRENTS_API_KEY:
                tasks.append(fetch_currents_news(session, bank_name, batch, date_from, date_to, topic))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Ошибка при получении новостей из API: {result}")
            elif result:
                news_articles.extend(result)
    seen_texts = set()
    unique_articles = []
    for article in news_articles:
        text = article["text"]
        if text not in seen_texts:
            seen_texts.add(text)
            unique_articles.append(article)
    if not unique_articles:
        logging.warning(f"Ни один API не нашел новостей для {bank_name}")
    logging.info(f"После удаления дубликатов: {len(unique_articles)} новостей для {bank_name}")
    for item in unique_articles:
        item["topic"] = topic or ""
        item["is_monitoring"] = is_monitoring
    await save_to_db_async(unique_articles, "parsed_news")
    return unique_articles

    """Асинхронное получение новостей из API"""
    reg_number = BANKS.get(bank_name, {}).get("reg_number", bank_name)
    cached_data = get_parsed_news_from_db(["newsapi", "gnews", "mediastack", "currents"], bank_name, reg_number, date_from, date_to, topic)
    if cached_data is not None:
        return cached_data
    aliases = generate_aliases(bank_name)
    news_articles = []
    timeout = aiohttp.ClientTimeout(total=20)
    MAX_QUERY_LENGTH = 500
    current_batch = []
    current_length = 0
    batches = []
    for alias in aliases:
        normalized_alias = normalize_text_for_aliases(alias)
        if current_length + len(normalized_alias) + 1 > MAX_QUERY_LENGTH:
            batches.append(current_batch)
            current_batch = []
            current_length = 0
        current_batch.append(alias)
        current_length += len(normalized_alias) + 1
    if current_batch:
        batches.append(current_batch)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        for batch in batches:
            if "newsapi" in NEWSAPI_KEY:
                tasks.append(fetch_newsapi_news(session, bank_name, batch, date_from, date_to, topic))
            if "gnews" in GNEWS_API_KEY:
                tasks.append(fetch_gnews_news(session, bank_name, batch, date_from, date_to, topic))
            if "mediastack" in MEDIASTACK_API_KEY:
                tasks.append(fetch_mediastack_news(session, bank_name, batch, date_from, date_to, topic))
            if "currents" in CURRENTS_API_KEY:
                tasks.append(fetch_currents_news(session, bank_name, batch, date_from, date_to, topic))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Ошибка при получении новостей из API: {result}")
            elif result:
                news_articles.extend(result)
    seen_texts = set()
    unique_articles = []
    for article in news_articles:
        text = article["text"]
        if text not in seen_texts:
            seen_texts.add(text)
            unique_articles.append(article)
    if not unique_articles:
        logging.warning(f"Ни один API не нашел новостей для {bank_name}")
    logging.info(f"После удаления дубликатов: {len(unique_articles)} новостей для {bank_name}")
    for item in unique_articles:
        item["topic"] = topic or ""
        item["is_monitoring"] = is_monitoring
    await save_to_db_async(unique_articles, "parsed_news")
    return unique_articles

async def fetch_newsapi_news(session, bank_name, aliases, date_from, date_to, topic=None):
    """Получение новостей из NewsAPI"""
    reg_number = BANKS.get(bank_name, {}).get("reg_number", bank_name)
    query = " OR ".join([f'"{alias}"' for alias in aliases])
    if topic:
        query += f' AND "{topic}"'
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "from": date_from,
        "to": date_to,
        "language": "ru",
        "sortBy": "publishedAt",
        "apiKey": NEWSAPI_KEY
    }
    try:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                news_articles = []
                for article in data.get("articles", []):
                    title = article.get("title", "")
                    description = article.get("description", "")
                    text = f"{title} {description}".strip()
                    published = article.get("publishedAt", "")
                    try:
                        published_date = datetime.strptime(published[:10], "%Y-%m-%d")
                        date_str = published_date.strftime("%Y-%m-%d")
                    except (ValueError, TypeError):
                        logging.warning(f"Некорректный формат даты в NewsAPI: {published}")
                        date_str = published
                    if not text:
                        continue
                    news_articles.append({
                        "bank": bank_name,
                        "reg_number": reg_number,
                        "text": text,
                        "date": date_str,
                        "link": article.get("url", "newsapi"),
                        "source": "newsapi"
                    })
                logging.info(f"Найдено {len(news_articles)} подходящих новостей из NewsAPI для {bank_name}")
                return news_articles
            else:
                logging.error(f"Ошибка в ответе NewsAPI: {data}")
    except aiohttp.ClientError as e:
        logging.error(f"Ошибка при запросе к NewsAPI: {e}")
    return []

async def fetch_gnews_news(session, bank_name, aliases, date_from, date_to, topic=None):
    """Получение новостей из GNews"""
    reg_number = BANKS.get(bank_name, {}).get("reg_number", bank_name)
    query = " OR ".join(aliases)
    if topic:
        query += f' AND "{topic}"'
    url = "https://gnews.io/api/v4/search"
    params = {
        "q": query,
        "lang": "ru",
        "from": date_from,
        "to": date_to,
        "token": GNEWS_API_KEY
    }
    for attempt in range(3):
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    news_articles = []
                    for article in data.get("articles", []):
                        title = article.get("title", "")
                        description = article.get("description", "")
                        text = f"{title} {description}".strip()
                        published = article.get("publishedAt", "")
                        try:
                            published_date = datetime.strptime(published[:10], "%Y-%m-%d")
                            date_str = published_date.strftime("%Y-%m-%d")
                        except (ValueError, TypeError):
                            logging.warning(f"Некорректный формат даты в GNews: {published}")
                            date_str = published
                        if not text:
                            continue
                        news_articles.append({
                            "bank": bank_name,
                            "reg_number": reg_number,
                            "text": text,
                            "date": date_str,
                            "link": article.get("url", "gnews"),
                            "source": "gnews"
                        })
                    logging.info(f"Найдено {len(news_articles)} подходящих новостей из GNews для {bank_name}")
                    return news_articles
                else:
                    logging.error(f"Ошибка HTTP {response.status} в GNews")
                    if response.status == 429:
                        await asyncio.sleep(2 * (attempt + 1))
                    else:
                        return []
        except aiohttp.ClientError as e:
            logging.error(f"Ошибка при запросе к GNews: {e}")
    return []

async def fetch_mediastack_news(session, bank_name, aliases, date_from, date_to, topic=None):
    """Получение новостей из Mediastack"""
    reg_number = BANKS.get(bank_name, {}).get("reg_number", bank_name)
    query = " OR ".join(aliases)
    if topic:
        query += f' AND "{topic}"'
    url = "http://api.mediastack.com/v1/news"
    params = {
        "access_key": MEDIASTACK_API_KEY,
        "keywords": query,
        "date": f"{date_from},{date_to}",
        "languages": "ru",
        "sort": "published_desc"
    }
    try:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                news_articles = []
                for article in data.get("data", []):
                    title = article.get("title", "")
                    description = article.get("description", "")
                    text = f"{title} {description}".strip()
                    published = article.get("published_at", "")
                    try:
                        published_date = datetime.strptime(published[:10], "%Y-%m-%d")
                        date_str = published_date.strftime("%Y-%m-%d")
                    except (ValueError, TypeError):
                        logging.warning(f"Некорректный формат даты в Mediastack: {published}")
                        date_str = published
                    if not text:
                        continue
                    news_articles.append({
                        "bank": bank_name,
                        "reg_number": reg_number,
                        "text": text,
                        "date": date_str,
                        "link": article.get("url", "mediastack"),
                        "source": "mediastack"
                    })
                logging.info(f"Найдено {len(news_articles)} подходящих новостей из Mediastack для {bank_name}")
                return news_articles
            else:
                logging.error(f"Ошибка HTTP {response.status} в Mediastack")
    except aiohttp.ClientError as e:
        logging.error(f"Ошибка при запросе к Mediastack: {e}")
    return []

async def fetch_currents_news(session, bank_name, aliases, date_from, date_to, topic=None):
    """Получение новостей из Currents"""
    reg_number = BANKS.get(bank_name, {}).get("reg_number", bank_name)
    query = " OR ".join(aliases)
    if topic:
        query += f' AND "{topic}"'
    url = "https://api.currentsapi.services/v1/search"
    params = {
        "apiKey": CURRENTS_API_KEY,
        "keywords": query,
        "start_date": date_from,
        "end_date": date_to,
        "language": "ru"
    }
    try:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                news_articles = []
                for article in data.get("news", []):
                    title = article.get("title", "")
                    description = article.get("description", "")
                    text = f"{title} {description}".strip()
                    published = article.get("published", "")
                    try:
                        published_date = datetime.strptime(published[:10], "%Y-%m-%d")
                        date_str = published_date.strftime("%Y-%m-%d")
                    except (ValueError, TypeError):
                        logging.warning(f"Некорректный формат даты в Currents: {published}")
                        date_str = published
                    if not text:
                        continue
                    news_articles.append({
                        "bank": bank_name,
                        "reg_number": reg_number,
                        "text": text,
                        "date": date_str,
                        "link": article.get("url", "currents"),
                        "source": "currents"
                    })
                logging.info(f"Найдено {len(news_articles)} подходящих новостей из Currents для {bank_name}")
                return news_articles
            else:
                logging.error(f"Ошибка HTTP {response.status} в Currents")
    except aiohttp.ClientError as e:
        logging.error(f"Ошибка при запросе к Currents: {e}")
    return []

async def fetch_rss_news(bank_name, date_from, date_to, topic=None, is_monitoring=False):
    """Асинхронное получение новостей из RSS-лент и inkazan.ru"""
    reg_number = BANKS.get(bank_name, {}).get("reg_number", bank_name)
    aliases = generate_aliases(bank_name)
    all_articles = []
    async with aiohttp.ClientSession() as session:
        inkazan_task = scrape_inkazan_news(session, bank_name, aliases, date_from, date_to, topic)
        rss_tasks = [
            parse_single_rss_feed(session, rss_feed, bank_name, reg_number, aliases, date_from, date_to, is_monitoring)
            for rss_feed in RSS_FEEDS
        ]
        results = await asyncio.gather(inkazan_task, *rss_tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Ошибка при парсинге RSS или inkazan: {result}")
                continue
            elif result:
                for item in result:
                    item["topic"] = topic or ""
                    item["is_monitoring"] = is_monitoring
                if result:
                    await save_to_db_async(result, "parsed_news")
                    all_articles.extend(result)
    logging.info(f"Найдено {len(all_articles)} RSS-новостей (включая inkazan.ru) для {bank_name}")
    return all_articles

async def parse_single_rss_feed(session, rss_feed, bank_name, reg_number, aliases, date_from, date_to, is_monitoring):
    """Асинхронная функция для парсинга одной RSS-ленты."""
    logging.info(f"Проверка RSS-ленты: {rss_feed}")
    articles = []
    try:
        async with session.get(rss_feed) as response:
            if response.status != 200:
                logging.warning(f"RSS-лента {rss_feed} недоступна: HTTP {response.status}")
                return articles
            feed_text = await response.text()
            feed = feedparser.parse(feed_text)
            if not feed.entries:
                logging.warning(f"Нет записей в RSS-ленте: {rss_feed}")
                return articles
            for entry in feed.entries:
                try:
                    # Определение даты публикации
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        entry_date = datetime(*entry.published_parsed[:6])
                    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                        entry_date = datetime(*entry.updated_parsed[:6])
                    else:
                        entry_date = None
                    date_str = entry.get('published', entry.get('updated', 'Неизвестно'))
                    if entry_date:
                        date_str = entry_date.strftime("%Y-%m-%d")
                    # Проверка, попадает ли дата в нужный диапазон
                    try:
                        if entry_date:
                            entry_date_dt = entry_date.date()
                            date_from_dt = datetime.strptime(date_from, "%Y-%m-%d").date()
                            date_to_dt = datetime.strptime(date_to, "%Y-%m-%d").date()
                            if not (date_from_dt <= entry_date_dt <= date_to_dt):
                                continue
                    except Exception as e:
                        logging.warning(f"Ошибка при проверке даты RSS {rss_feed}: {e}")
                        continue
                    # Формирование текста новости
                    title = entry.get('title', '')
                    summary = entry.get('summary', '')
                    content = entry.get('content', [{}])
                    if content and isinstance(content, list) and 'value' in content[0]:
                        content_text = content[0]['value']
                    else:
                        content_text = ''
                    text = f"{title} {summary} {content_text}".strip()
                    if not text:
                        continue
                    # Проверка на упоминание банка
                    if is_bank_name_match(text, aliases):
                        articles.append({
                            "bank": bank_name,
                            "reg_number": reg_number,
                            "text": text,
                            "date": date_str,
                            "link": entry.link,
                            "source": rss_feed,
                            "is_monitoring": is_monitoring
                        })
                except Exception as e:
                    logging.error(f"Ошибка обработки записи RSS в ленте {rss_feed}: {e}")
                    continue
    except Exception as e:
        logging.error(f"Ошибка при запросе к RSS-ленте {rss_feed}: {e}")
    return articles

async def fetch_1000bankov_news(bank_name, date_from, date_to, topic=None, is_monitoring=False):
    """Асинхронный парсинг новостей с 1000bankov.ru"""
    reg_number = BANKS.get(bank_name, {}).get("reg_number", bank_name)
    aliases = generate_aliases(bank_name)
    news_data = []
    try:
        try:
            date_from_dt = datetime.strptime(date_from, "%Y-%m-%d").date()
            date_to_dt = datetime.strptime(date_to, "%Y-%m-%d").date()
        except ValueError:
            logging.error(f"Неверный формат дат: {date_from}, {date_to}")
            return []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            page = await context.new_page()
            url = f"https://1000bankov.ru/news/bank/{reg_number}/"
            await page.goto(url, wait_until="domcontentloaded")
            await page.wait_for_timeout(100)
            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')
            news_cards = soup.find_all('div', class_='newsCard')
            for card in news_cards:
                try:
                    title_elem = card.find('h3', class_='newsCard__header')
                    link_elem = card.find('a', class_='newsCard__headerLink')
                    date_elem = card.find('span', class_='newsCard__date')
                    if not (title_elem and link_elem and date_elem):
                        continue
                    title = title_elem.text.strip()
                    link = link_elem['href']
                    if link.startswith('http'):
                        full_link = link
                    else:
                        full_link = f"https://1000bankov.ru{link}"
                    date_str = date_elem.text.strip()
                    try:
                        date_obj = datetime.strptime(date_str, "%d.%m.%Y")
                        news_date = date_obj.strftime("%Y-%m-%d")
                    except ValueError:
                        logging.warning(f"Некорректный формат даты: {date_str}")
                        continue
                    if not (date_from_dt <= date_obj.date() <= date_to_dt):
                        continue
                    if is_bank_name_match(title, aliases):
                        news_data.append({
                            "bank": bank_name,
                            "reg_number": reg_number,
                            "text": title,
                            "date": news_date,
                            "link": full_link,
                            "source": "1000bankov.ru",
                            "is_monitoring": is_monitoring
                        })
                except Exception as e:
                    logging.error(f"Ошибка обработки карточки новости: {e}")
            await browser.close()
    except Exception as e:
        logging.error(f"Ошибка парсинга с сайта 1000bankov: {e}")
    logging.info(f"Найдено {len(news_data)} новостей с 1000bankov для {bank_name}")
    for item in news_data:
        item["topic"] = topic or ""
        item["is_monitoring"] = is_monitoring
    if news_data:
        await save_to_db_async(news_data, "parsed_news")
    return news_data

async def scrape_inkazan_news(session, bank_name, aliases, date_from, date_to, topic=None):
    """Асинхронный парсинг новостей с inkazan.ru"""
    url = "https://inkazan.ru/news"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    reg_number = BANKS.get(bank_name, {}).get("reg_number", bank_name)
    articles = []
    try:
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                logging.warning(f"inkazan.ru недоступен: HTTP {response.status}")
                return []
            html_content = await response.text()
            soup = BeautifulSoup(html_content, 'html.parser')
            news_items = soup.select('div.news-list__item')
            for item in news_items:
                try:
                    title_elem = item.select_one('a.news-list__title')
                    if not title_elem:
                        continue
                    title = title_elem.get_text(strip=True)
                    link = title_elem['href']
                    if not link.startswith('http'):
                        link = f"https://inkazan.ru{link}"
                    date_elem = item.select_one('div.news-list__date')
                    date_str = date_elem.get_text(strip=True) if date_elem else "Неизвестно"
                    try:
                        date_match = re.search(r'(\d{1,2})\s+(\w+)\s*(\d{4})', date_str)
                        if date_match:
                            day = int(date_match.group(1))
                            month_name = date_match.group(2).lower()
                            year = int(date_match.group(3))
                            months = {
                                'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
                                'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
                                'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
                            }
                            month = months.get(month_name)
                            if month:
                                news_date = datetime(year, month, day).strftime("%Y-%m-%d")
                            else:
                                news_date = date_str
                        else:
                            news_date = date_str
                    except Exception as e:
                        logging.warning(f"Ошибка парсинга даты: {date_str}, сохраняю как есть")
                        news_date = date_str
                    try:
                        if isinstance(news_date, str) and len(news_date) >= 10:
                            news_date_dt = datetime.strptime(news_date[:10], "%Y-%m-%d").date()
                            date_from_dt = datetime.strptime(date_from, "%Y-%m-%d").date()
                            date_to_dt = datetime.strptime(date_to, "%Y-%m-%d").date()
                            if not (date_from_dt <= news_date_dt <= date_to_dt):
                                continue
                    except ValueError as e:
                        logging.error(f"Неверный формат даты для проверки периода: {news_date}, ошибка: {e}")
                        continue
                    async with session.get(link, headers=headers) as article_response:
                        if article_response.status != 200:
                            continue
                        article_html = await article_response.text()
                        article_soup = BeautifulSoup(article_html, 'html.parser')
                        content_elem = article_soup.select_one('div.article__content')
                        if content_elem:
                            text = content_elem.get_text(separator=" ", strip=True)
                        else:
                            text = title
                        if text and is_bank_name_match(text, aliases):
                            articles.append({
                                "bank": bank_name,
                                "reg_number": reg_number,
                                "text": text,
                                "date": news_date,
                                "link": link,
                                "source": "inkazan.ru"
                            })
                except Exception as e:
                    logging.error(f"Ошибка при обработке новости с inkazan.ru: {e}")
    except Exception as e:
        logging.error(f"Ошибка при запросе к inkazan.ru: {e}")
    return articles

async def parse_channel(client, channel, bank_name, date_from, date_to, topic, aliases, reg_number):
    """Парсинг одного канала с проверкой даты и содержания"""
    all_messages = []
    for attempt in range(3):
        try:
            async for message in client.iter_messages(channel, limit=150):
                if message.text and message.date:
                    message_date = message.date.replace(tzinfo=None).date()
                    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d").date()
                    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d").date()
                    if date_from_dt <= message_date <= date_to_dt:
                        if is_bank_name_match(message.text, aliases):
                            all_messages.append({
                                "bank": bank_name,
                                "reg_number": reg_number,
                                "text": message.text,
                                "date": message_date.strftime("%Y-%m-%d"),
                                "link": f"https://t.me/{channel}/{message.id}",
                                "source": f"telegram_{channel}",
                                "topic": topic or ""
                            })
            break
        except sqlite3.OperationalError as e:
            if 'database is locked' in str(e).lower():
                logging.warning(f"База данных сессии Telegram заблокирована в канале {channel}; попытка {attempt + 1}/3, ждем 2 секунды")
                await asyncio.sleep(2)
            else:
                logging.error(f"Другая ошибка SQLite в канале {channel}: {e}")
                raise
        except FloodWaitError as e:
            logging.warning(f"FloodWaitError в канале {channel}: ждем {e.seconds} секунд")
            await asyncio.sleep(e.seconds + random.uniform(0, 2))
        except UnauthorizedError:
            logging.error(f"Неавторизованный доступ к каналу {channel}")
            break
        except Exception as e:
            logging.error(f"Ошибка парсинга канала {channel}: {e}")
            break
    return all_messages

async def fetch_telegram_news(bank_name, date_from, date_to, topic=None, task_id=None, is_monitoring=False):
    """Парсинг Telegram-каналов"""
    reg_number = BANKS.get(bank_name, {}).get("reg_number", bank_name)
    aliases = generate_aliases(bank_name)
    all_messages = []
    session_info = await get_session_for_task(is_monitoring)
    if not session_info:
        session_type = "мониторинга" if is_monitoring else "ручного парсинга"
        logging.warning(f"Нет доступных сессий {session_type} для парсинга Telegram для {bank_name}, task_id={task_id}")
        return []
    client = None
    try:
        account_idx = int(session_info["name"].split("_")[1])
        account = ACCOUNTS[account_idx]
        client = TelegramClient(
            f"sessions/{session_info['name']}", 
            account["api_id"], 
            account["api_hash"]
        )
        await client.connect()
        client.session._execute('PRAGMA busy_timeout = 5000')
        if not await client.is_user_authorized():
            logging.error(f"Сессия {session_info['name']} недействительна.")
            try:
                os.remove(f"sessions/{session_info['name']}.session")
                logging.info(f"Удалена недействительная сессия {session_info['name']}")
            except Exception as e:
                logging.warning(f"Ошибка удаления сессии {session_info['name']}: {e}")
            return []
        logging.info(f"Клиент Telegram {session_info['name']} запущен для задачи {task_id}")
        session_info["current_task"] = task_id
        for channel in NEWS_CHANNELS:
            messages = await parse_channel(
                client, 
                channel, 
                bank_name, 
                date_from, 
                date_to, 
                topic,
                aliases,
                reg_number
            )
            all_messages.extend(messages)
        logging.info(f"Telegram: найдено {len(all_messages)} сообщений для {bank_name} (task_id={task_id})")
        for item in all_messages:
            item["is_monitoring"] = is_monitoring
        if all_messages:
            await save_to_db_async(all_messages, "parsed_news")
    except Exception as e:
        logging.error(f"Ошибка при парсинге Telegram для {bank_name} (task_id={task_id}): {e}")
    finally:
        release_session(session_info)
        if client and client.is_connected():
            await client.disconnect()
            logging.info(f"Клиент Telegram {session_info['name']} отключен")
    return all_messages

async def start_queue_processors():
    """Запуск обработчиков очередей"""
    logging.info("Запуск обработчиков очереди")
    asyncio.create_task(check_task_queue())

async def check_task_queue():
    """Проверка очереди задач"""
    while True:
        try:
            task, is_monitoring = await TASK_QUEUE.get()
            session_info = await get_session_for_task(is_monitoring)
            if session_info:
                session_info["available"] = False
                session_info["current_task"] = task[4]
                asyncio.create_task(process_telegram_task(session_info, task, is_monitoring))
            else:
                # Если сессии нет, возвращаем задачу в очередь
                await TASK_QUEUE.put((task, is_monitoring))
                await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Ошибка в обработчике очереди: {e}")
            await asyncio.sleep(2)

async def process_telegram_task(session_info, task, is_monitoring=False):
    """Обработка задачи парсинга Telegram"""
    selected_bank, date_from, date_to, topic, task_id = task
    client = None
    try:
        account_idx = int(session_info["name"].split("_")[1])
        account = ACCOUNTS[account_idx]
        async with session_info["semaphore"]:
            client = TelegramClient(
                f"sessions/{session_info['name']}", 
                account["api_id"], 
                account["api_hash"]
            )
            await client.connect()
            client.session._execute('PRAGMA busy_timeout = 5000')
            if not await client.is_user_authorized():
                logging.error(f"Сессия {session_info['name']} недействительна.")
                try:
                    os.remove(f"sessions/{session_info['name']}.session")
                    logging.info(f"Удалена недействительная сессия {session_info['name']}")
                except Exception as e:
                    logging.warning(f"Ошибка удаления сессии {session_info['name']}: {e}")
                return
            all_messages = []
            for channel in NEWS_CHANNELS:
                messages = await parse_channel(
                    client, 
                    channel, 
                    selected_bank, 
                    date_from, 
                    date_to, 
                    topic,
                    generate_aliases(selected_bank),
                    BANKS.get(selected_bank, {}).get("reg_number", selected_bank)
                )
                all_messages.extend(messages)
            task_id_str = f"{task_id}_monitoring" if is_monitoring else f"{task_id}_main"
            for item in all_messages:
                item["topic"] = topic or ""
                item["is_monitoring"] = is_monitoring
            await save_to_db_async(all_messages, "parsed_news")
            logging.info(f"Сохранено {len(all_messages)} сообщений из Telegram для {selected_bank} (task_id={task_id_str})")
    except Exception as e:
        logging.error(f"Ошибка обработки задачи {task_id}: {e}", exc_info=True)
    finally:
        release_session(session_info)
        if client and client.is_connected():
            await client.disconnect()
        if task_id in TASK_EVENTS:
            TASK_EVENTS[task_id].set()
            del TASK_EVENTS[task_id]

# --- ИСПРАВЛЕННЫЕ ФУНКЦИИ ЧТЕНИЯ ИЗ БД ---

def get_analyzed_news_for_period(bank_name, date_from, date_to, topic=None):
    """Чтение новостей из analyzed_news, если период покрыт СВЕЖИМ парсингом (менее 1 часа)"""
    try:
        conn = sqlite3.connect('news.db', timeout=30)
        cursor = conn.cursor()
        topic = topic or ""
        cursor.execute('''
            SELECT last_parse_time, last_from, last_to FROM parse_history WHERE bank = ?
        ''', (bank_name,))
        result = cursor.fetchone()
        if result and result[1] is not None and result[2] is not None:
            last_parse_str, last_from, last_to = result
            last_parse = datetime.strptime(last_parse_str, "%Y-%m-%d %H:%M:%S")
            if (datetime.now() - last_parse) < timedelta(hours=1) and last_from <= date_from and date_to <= last_to:
                cursor.execute('''
                    SELECT bank, reg_number, text, summary, event_type, event_date, entities, date, link, source, category, sentiment, informativeness
                    FROM analyzed_news
                    WHERE bank = ? 
                    AND date >= ?
                    AND date <= ?
                    AND (topic = ? OR ? = '')
                ''', (bank_name, date_from, date_to, topic, topic))
                rows = cursor.fetchall()
                logging.info(f"Найдено {len(rows)} новостей в кэше analyzed для {bank_name} за {date_from}-{date_to} (период покрыт)")
                return [{
                    "bank": row[0], "reg_number": row[1], "text": row[2], "summary": row[3],
                    "event_type": row[4], "event_date": row[5], "entities": row[6].split(",") if row[6] else [],
                    "date": row[7], "link": row[8], "source": row[9], "category": row[10],
                    "sentiment": row[11], "informativeness": row[12]
                } for row in rows]
            else:
                logging.info(f"Кэш analyzed не покрывает период {date_from}-{date_to} для {bank_name}")
        return None
    except sqlite3.Error as e:
        logging.error(f"Ошибка чтения из БД analyzed_news: {e}")
        return None
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def get_parsed_news_for_period(bank_name, date_from, date_to, topic=None):
    """
    Чтение ВСЕХ сырых новостей из parsed_news за период.
    Убран фильтр по last_fetch_time, так как parsed_news - это хранилище, а не кэш.
    """
    try:
        conn = sqlite3.connect('news.db', timeout=30)
        cursor = conn.cursor()
        topic = topic or ""
        cursor.execute('''
            SELECT bank, reg_number, text, date, link, source, topic
            FROM parsed_news
            WHERE bank = ? 
            AND date >= ?
            AND date <= ?
            AND (topic = ? OR ? = '')
        ''', (bank_name, date_from, date_to, topic, topic))
        rows = cursor.fetchall()
        logging.info(f"Найдено {len(rows)} сырых новостей в parsed_news для {bank_name} за {date_from}-{date_to}")
        return [{
            "bank": row[0], "reg_number": row[1], "text": row[2], "date": row[3],
            "link": row[4], "source": row[5], "topic": row[6]
        } for row in rows]
    except sqlite3.Error as e:
        logging.error(f"Ошибка чтения из БД parsed_news: {e}")
        return []
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def get_parse_history(bank_name):
    """Получение last_parse_time, last_from, last_to из parse_history"""
    try:
        conn = sqlite3.connect('news.db', timeout=30)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT last_parse_time, last_from, last_to FROM parse_history WHERE bank = ?
        ''', (bank_name,))
        result = cursor.fetchone()
        return result
    except sqlite3.Error as e:
        logging.error(f"Ошибка чтения parse_history: {e}")
        return None
    finally:
        conn.close()

# --- ГЛАВНАЯ ИСПРАВЛЕННАЯ ФУНКЦИЯ ---

async def fetch_all_news(selected_bank, date_from, date_to, topic=None, chat_id=None, is_monitoring=False):
    """Сбор всех новостей с корректным объединением данных."""
    logging.info(f"Начало сбора всех новостей для {selected_bank}, chat_id={chat_id}, даты: {date_from} - {date_to}, тема: {topic}, monitoring={is_monitoring}")
    
    if is_monitoring:
        logging.info("Запрос в режиме мониторинга, кэш игнорируется.")
        all_news = await _perform_full_parsing(selected_bank, date_from, date_to, topic, chat_id, is_monitoring)
        analyzed_news = await analyze_all_news(all_news, topic=topic, is_monitoring=True)
        await save_to_db_async(analyzed_news, "analyzed_news")
        await update_parse_time(selected_bank, date_from, date_to)
        return analyzed_news

    # 1. Проверяем кэш проанализированных новостей (analyzed_news)
    analyzed_news = get_analyzed_news_for_period(selected_bank, date_from, date_to, topic)
    if analyzed_news is not None:
        logging.info(f"Возвращаем новости из analyzed_news для {selected_bank} за {date_from}-{date_to} (len: {len(analyzed_news)})")
        return analyzed_news

    # 2. Получаем историю парсинга
    parse_info = get_parse_history(selected_bank)
    if parse_info:
        last_parse_time, last_from, last_to = parse_info
        last_parse_dt = datetime.strptime(last_parse_time, "%Y-%m-%d %H:%M:%S")
        fresh = (datetime.now() - last_parse_dt) < timedelta(hours=4)
    else:
        fresh = False
        last_from = None
        last_to = None

    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d").date()
    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d").date()
    if last_from:
        last_from_dt = datetime.strptime(last_from, "%Y-%m-%d").date()
        last_to_dt = datetime.strptime(last_to, "%Y-%m-%d").date()
    else:
        last_from_dt = None
        last_to_dt = None

    # 3. Определяем, какие периоды нужно допарсить
    periods_to_parse = []
    if last_from_dt is None or last_to_dt is None:
        # Никогда не парсили — парсим весь период
        periods_to_parse.append((date_from, date_to))
    else:
        # Допарсинг прошлого
        if date_from_dt < last_from_dt:
            end = (last_from_dt - timedelta(days=1)).strftime("%Y-%m-%d")
            periods_to_parse.append((date_from, end))
        # Допарсинг будущего/настоящего
        if date_to_dt > last_to_dt:
            start = (last_to_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            periods_to_parse.append((start, date_to))

    # 4. Парсим недостающие периоды и сохраняем СЫРЫЕ данные
    for start, end in periods_to_parse:
        logging.info(f"Допарсинг недостающего периода для {selected_bank}: {start} по {end}")
        missing_news = await _perform_full_parsing(selected_bank, start, end, topic, chat_id, is_monitoring)
        await save_to_db_async(missing_news, "parsed_news")

    # 5. Обновляем историю парсинга
    new_last_from = min(date_from_dt, last_from_dt) if last_from_dt else date_from_dt
    new_last_to = max(date_to_dt, last_to_dt) if last_to_dt else date_to_dt
    await update_parse_time(selected_bank, new_last_from.strftime("%Y-%m-%d"), new_last_to.strftime("%Y-%m-%d"))

    # 6. Главное изменение: Получаем ВСЕ сырые новости за запрошенный период из БД
    all_raw_news = get_parsed_news_for_period(selected_bank, date_from, date_to, topic)

    # 7. Анализируем ВСЕ собранные сырые данные
    logging.info(f"Анализ всех ({len(all_raw_news)}) сырых новостей для {selected_bank} за {date_from}-{date_to}")
    analyzed_news = await analyze_all_news(all_raw_news, topic=topic, is_monitoring=False)
    if analyzed_news:
        await save_to_db_async(analyzed_news, "analyzed_news")

    logging.info(f"Объединенные и проанализированные новости для {selected_bank} {date_from}-{date_to}: {len(analyzed_news)}")
    return analyzed_news

async def _perform_full_parsing(selected_bank, date_from, date_to, topic, chat_id, is_monitoring):
    """Выполняет полный парсинг и возвращает сырые новости."""
    task_id = f"{chat_id}_{selected_bank}_{int(datetime.now().timestamp())}_{'monitoring' if is_monitoring else 'main'}"
    all_news = []
    seen_links = set()
    tasks = [
        fetch_news_from_apis(selected_bank, date_from, date_to, topic, is_monitoring),
        fetch_rss_news(selected_bank, date_from, date_to, topic, is_monitoring),
        fetch_1000bankov_news(selected_bank, date_from, date_to, topic, is_monitoring),
        fetch_telegram_news(selected_bank, date_from, date_to, topic, task_id, is_monitoring)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            logging.error(f"Ошибка при сборе новостей: {result}")
            continue
        elif result:
            for news in result:
                if news["link"] not in seen_links:
                    all_news.append(news)
                    seen_links.add(news["link"])
    logging.info(f"Собрано {len(all_news)} сырых новостей для {selected_bank} из всех источников после фильтрации дубликатов")
    return all_news

async def update_parse_time(bank_name, date_from, date_to):
    """Обновляет время последнего парсинга для банка с периодом"""
    try:
        conn = sqlite3.connect('news.db', timeout=30)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO parse_history (bank, last_parse_time, last_from, last_to) 
            VALUES (?, CURRENT_TIMESTAMP, ?, ?)
        ''', (bank_name, date_from, date_to))
        conn.commit()
        logging.info(f"Обновлено время последнего парсинга для {bank_name} с периодом {date_from}-{date_to}")
    except sqlite3.Error as e:
        logging.error(f"Ошибка обновления parse_history для {bank_name}: {e}")
    finally:
        conn.close()
async def fetch_all_news_parallel(bank_list, date_from, date_to, topic=None, chat_id=None, is_monitoring=False):
    """Параллельный сбор новостей для нескольких банков."""
    if not bank_list:
        return []
    logging.info(f"Начало параллельного сбора новостей для {len(bank_list)} банков: {bank_list}")
    # Создаем задачи для каждого банка
    tasks = []
    for bank_name in bank_list:
        task = asyncio.create_task(
            fetch_all_news(bank_name, date_from, date_to, topic, chat_id, is_monitoring)
        )
        tasks.append(task)
    # Ждем завершения всех задач с обработкой исключений для отказоустойчивости
    results = await asyncio.gather(*tasks, return_exceptions=True)
    # Собираем все новости в один список
    all_news_combined = []
    seen_links = set()
    for result in results:
        if isinstance(result, Exception):
            logging.error(f"Ошибка при параллельном сборе новостей: {result}")
            continue  
        if isinstance(result, list):
            for news_item in result:
                link = news_item.get("link", "")
                if link not in seen_links:
                    seen_links.add(link)
                    all_news_combined.append(news_item)
    logging.info(f"Параллельный сбор завершен. Всего уникальных новостей: {len(all_news_combined)}")
    return all_news_combined