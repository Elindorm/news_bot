# monitoring.py (оптимизированная версия для 350+ банков на 4 vCPU / 8GB RAM)
import asyncio
import sqlite3
import logging
from datetime import datetime, timedelta
import pytz
from config import *
from news_analyzer import analyze_all_news, deduplicate_in_parallel, is_duplicate, calculate_informativeness
from utils import normalize_text_for_aliases, DB_WRITE_LOCK
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import aiohttp
import random
import os
import json
import hashlib
import feedparser
from bs4 import BeautifulSoup
from telethon import TelegramClient
from telethon.errors import FloodWaitError, UnauthorizedError
from playwright.async_api import async_playwright
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import re
from collections import OrderedDict, defaultdict

# Импорт пула сессий из news_parser.py
from news_parser import SESSION_POOL_LOCK, get_session_for_task, release_session

# Хранилище "горячих" новостей для уведомлений
hot_news_cache = {}

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/monitoring.log", mode="a", encoding="utf-8")
    ]
)

# === ОПТИМИЗИРОВАННЫЕ НАСТРОЙКИ ПОД 4 vCPU / 8GB RAM ===
BANK_SEM = asyncio.Semaphore(2)          # До 2 банков одновременно
RSS_SEM = asyncio.Semaphore(5)           # До 5 RSS-лент параллельно
PLAYWRIGHT_SEM = asyncio.Semaphore(1)    # Playwright остаётся 1 (тяжёлый)
HTTP_LIMIT = 50
BATCH_SIZE = 10
DELAY_BETWEEN_BANKS = 2
DELAY_BETWEEN_BATCHES = 15
ACTIVE_SUBSCRIPTION_DAYS = 30

# Инициализация базы данных
def init_monitoring_db():
    try:
        conn = sqlite3.connect('monitoring.db')
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS subscriptions (
                chat_id INTEGER,
                bank_name TEXT,
                reg_number TEXT,
                last_notification TIMESTAMP DEFAULT '1970-01-01 00:00:00',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chat_id, bank_name)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS monitored_news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bank_name TEXT,
                reg_number TEXT,
                text TEXT,
                date TEXT,
                link TEXT,
                source TEXT,
                topic TEXT DEFAULT '',
                is_monitoring BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (link, bank_name)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analyzed_monitored_news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bank_name TEXT,
                reg_number TEXT,
                text TEXT,
                summary TEXT,
                event_type TEXT,
                event_date TEXT,
                entities TEXT,
                date TEXT,
                link TEXT,
                source TEXT,
                category TEXT,
                sentiment TEXT,
                informativeness INTEGER,
                summary_hash TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (link, bank_name)
            )
        ''')
        conn.commit()
        logging.info("База данных monitoring.db инициализирована.")
    except sqlite3.Error as e:
        logging.error(f"Ошибка инициализации monitoring.db: {e}")
    finally:
        conn.close()

# Асинхронное сохранение
async def save_to_monitoring_db_async(data, table_name="monitored_news"):
    if not data:
        return
    async with DB_WRITE_LOCK:
        try:
            conn = sqlite3.connect('monitoring.db')
            cursor = conn.cursor()
            inserted_count = 0
            if table_name == "monitored_news":
                for item in data:
                    cursor.execute('''
                        INSERT OR IGNORE INTO monitored_news (bank_name, reg_number, text, date, link, source, topic, is_monitoring)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        item.get("bank", ""),
                        item.get("reg_number", ""),
                        item.get("text", ""),
                        item.get("date", ""),
                        item.get("link", ""),
                        item.get("source", ""),
                        item.get("topic", ""),
                        1
                    ))
                    if cursor.rowcount > 0:
                        inserted_count += 1
            elif table_name == "analyzed_monitored_news":
                for item in data:
                    summary_hash = hashlib.md5(item.get("summary", "").encode('utf-8')).hexdigest()
                    cursor.execute('''
                        INSERT OR IGNORE INTO analyzed_monitored_news (
                            bank_name, reg_number, text, summary, event_type, event_date,
                            entities, date, link, source, category, sentiment, informativeness, summary_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        item.get("bank", ""),
                        item.get("reg_number", ""),
                        item.get("text", ""),
                        item.get("summary", ""),
                        item.get("event_type", ""),
                        item.get("event_date", "").strftime("%Y-%m-%d") if isinstance(item.get("event_date"), datetime) else item.get("event_date", ""),
                        json.dumps(item.get("entities", [])),
                        item.get("date", ""),
                        item.get("link", ""),
                        item.get("source", ""),
                        item.get("category", ""),
                        item.get("sentiment", ""),
                        item.get("informativeness", 0),
                        summary_hash
                    ))
                    if cursor.rowcount > 0:
                        inserted_count += 1
            conn.commit()
            logging.info(f"Сохранено {inserted_count} новых уникальных записей в {table_name}")
        except sqlite3.Error as e:
            logging.error(f"Ошибка сохранения в {table_name}: {e}")
        finally:
            conn.close()

# Управление подписками
def add_subscription(chat_id, bank_name):
    try:
        conn = sqlite3.connect('monitoring.db')
        cursor = conn.cursor()
        reg_number = BANKS.get(bank_name, {}).get("reg_number", bank_name)
        cursor.execute('''
            INSERT OR IGNORE INTO subscriptions (chat_id, bank_name, reg_number, last_notification)
            VALUES (?, ?, ?, '1970-01-01 00:00:00')
        ''', (chat_id, bank_name, reg_number))
        conn.commit()
        logging.info(f"Подписка добавлена: chat_id={chat_id}, bank={bank_name}.")
    except sqlite3.Error as e:
        logging.error(f"Ошибка добавления подписки: {e}")
    finally:
        conn.close()

def remove_subscription(chat_id, bank_name):
    try:
        conn = sqlite3.connect('monitoring.db')
        cursor = conn.cursor()
        cursor.execute('DELETE FROM subscriptions WHERE chat_id = ? AND bank_name = ?', (chat_id, bank_name))
        conn.commit()
        logging.info(f"Подписка удалена: chat_id={chat_id}, bank={bank_name}")
    except sqlite3.Error as e:
        logging.error(f"Ошибка удаления подписки: {e}")
    finally:
        conn.close()

def get_user_subscriptions(chat_id):
    try:
        conn = sqlite3.connect('monitoring.db')
        cursor = conn.cursor()
        cursor.execute('SELECT bank_name, reg_number FROM subscriptions WHERE chat_id = ?', (chat_id,))
        return cursor.fetchall()
    except sqlite3.Error as e:
        logging.error(f"Ошибка чтения подписок для chat_id {chat_id}: {e}")
        return []
    finally:
        conn.close()

def get_user_subscriptions_by_bank(bank_name):
    try:
        conn = sqlite3.connect('monitoring.db')
        cursor = conn.cursor()
        cursor.execute('SELECT chat_id FROM subscriptions WHERE bank_name = ?', (bank_name,))
        rows = cursor.fetchall()
        return [row[0] for row in rows]
    except sqlite3.Error as e:
        logging.error(f"Ошибка чтения подписок для {bank_name}: {e}")
        return []
    finally:
        conn.close()

def get_all_subscriptions():
    try:
        conn = sqlite3.connect('monitoring.db')
        cursor = conn.cursor()
        cursor.execute('SELECT chat_id, bank_name FROM subscriptions')
        return cursor.fetchall()
    except sqlite3.Error as e:
        logging.error(f"Ошибка чтения всех подписок: {e}")
        return []
    finally:
        conn.close()

def get_active_banks(days=ACTIVE_SUBSCRIPTION_DAYS):
    try:
        conn = sqlite3.connect('monitoring.db')
        cursor = conn.cursor()
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''
            SELECT DISTINCT bank_name 
            FROM subscriptions 
            WHERE created_at >= ? OR last_notification >= ?
        ''', (cutoff_date, cutoff_date))
        banks = [row[0] for row in cursor.fetchall()]
        logging.info(f"Найдено {len(banks)} активных банков для мониторинга (за последние {days} дней)")
        return banks
    except sqlite3.Error as e:
        logging.error(f"Ошибка получения активных банков: {e}")
        return []
    finally:
        conn.close()

def get_new_analyzed_news(chat_id):
    conn = None
    try:
        conn = sqlite3.connect('monitoring.db')
        cursor = conn.cursor()
        cursor.execute('SELECT bank_name FROM subscriptions WHERE chat_id = ?', (chat_id,))
        banks = [row[0] for row in cursor.fetchall()]
        if not banks:
            return []
        cursor.execute('SELECT last_notification FROM subscriptions WHERE chat_id = ? LIMIT 1', (chat_id,))
        last_notif_row = cursor.fetchone()
        last_notification = last_notif_row[0] if last_notif_row else '1970-01-01 00:00:00'
        placeholders = ','.join(['?'] * len(banks))
        cursor.execute(f'''
            SELECT bank_name, reg_number, text, summary, event_type, event_date, entities, 
                   date, link, source, category, sentiment, informativeness
            FROM analyzed_monitored_news
            WHERE bank_name IN ({placeholders}) AND created_at > ?
            ORDER BY created_at DESC
        ''', banks + [last_notification])
        rows = cursor.fetchall()
        news_list = []
        for row in rows:
            news_item = {
                "bank": row[0],
                "reg_number": row[1],
                "text": row[2],
                "summary": row[3],
                "event_type": row[4],
                "event_date": row[5],
                "entities": json.loads(row[6]) if row[6] else [],
                "date": row[7],
                "link": row[8],
                "source": row[9],
                "category": row[10],
                "sentiment": row[11],
                "informativeness": row[12]
            }
            news_list.append(news_item)
        logging.info(f"Для chat_id={chat_id} найдено {len(news_list)} новых analyzed новостей")
        return news_list
    except sqlite3.Error as e:
        logging.error(f"Ошибка при получении новых новостей для chat_id {chat_id}: {e}")
        return []
    finally:
        if conn:
            conn.close()

def update_last_notification(chat_id):
    try:
        conn = sqlite3.connect('monitoring.db')
        cursor = conn.cursor()
        cursor.execute('UPDATE subscriptions SET last_notification = CURRENT_TIMESTAMP WHERE chat_id = ?', (chat_id,))
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Ошибка обновления last_notification для {chat_id}: {e}")
    finally:
        conn.close()

# === ФУНКЦИИ ПАРСИНГА ===
async def fetch_1000bankov_news_monitoring(bank_name, date_from, date_to):
    reg_number = BANKS.get(bank_name, {}).get("reg_number", bank_name)
    aliases = generate_aliases(bank_name)
    news_data = []
    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d").date()
    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d").date()
    async with PLAYWRIGHT_SEM:
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--disable-extensions',
                        '--disable-plugins',
                        '--disable-images',
                        '--blink-settings=imagesEnabled=false'
                    ]
                )
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={'width': 1920, 'height': 1080},
                    java_script_enabled=True,
                    ignore_https_errors=True
                )
                page = await context.new_page()
                url = f"https://1000bankov.ru/news/bank/{reg_number}/"
                logging.info(f"Playwright: переход на {url} для {bank_name}")
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(1000)
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                news_cards = soup.find_all('div', class_='newsCard')
                logging.info(f"Найдено {len(news_cards)} карточек новостей для {bank_name}")
                for card in news_cards:
                    try:
                        title_elem = card.find('h3', class_='newsCard__header')
                        link_elem = card.find('a', class_='newsCard__headerLink')
                        date_elem = card.find('span', class_='newsCard__date')
                        if not (title_elem and link_elem and date_elem):
                            continue
                        title = title_elem.text.strip()
                        link = link_elem['href']
                        full_link = link if link.startswith('http') else f"https://1000bankov.ru{link}"
                        date_str = date_elem.text.strip()
                        try:
                            date_obj = datetime.strptime(date_str, "%d.%m.%Y")
                            news_date = date_obj.strftime("%Y-%m-%d")
                            news_date_dt = date_obj.date()
                        except ValueError:
                            continue
                        if not (date_from_dt <= news_date_dt <= date_to_dt):
                            continue
                        if is_bank_name_match(title, aliases):
                            news_data.append({
                                "bank": bank_name,
                                "reg_number": reg_number,
                                "text": title,
                                "date": news_date,
                                "link": full_link,
                                "source": "1000bankov.ru"
                            })
                    except Exception as e:
                        logging.error(f"Ошибка обработки карточки новости для {bank_name}: {e}")
                await browser.close()
        except Exception as e:
            logging.error(f"Критическая ошибка Playwright для {bank_name}: {e}")
        finally:
            await asyncio.sleep(3)
    logging.info(f"Найдено {len(news_data)} новостей с 1000bankov для {bank_name}")
    return news_data

async def fetch_telegram_news_monitoring(bank_name, date_from, date_to):
    reg_number = BANKS.get(bank_name, {}).get("reg_number", bank_name)
    aliases = generate_aliases(bank_name)
    all_messages = []
    session_info = await get_session_for_task(is_monitoring=True)
    if not session_info:
        logging.warning(f"Нет доступных сессий для мониторинга Telegram для {bank_name}")
        return []
    client = None
    try:
        account_idx = int(session_info["name"].split("_")[1])
        account = ACCOUNTS[account_idx]
        client = TelegramClient(f"sessions/{session_info['name']}", account["api_id"], account["api_hash"])
        await client.connect()
        client.session._execute('PRAGMA busy_timeout = 5000')
        if not await client.is_user_authorized():
            logging.error(f"Сессия {session_info['name']} недействительна.")
            try:
                os.remove(f"sessions/{session_info['name']}.session")
            except Exception:
                pass
            return []
        
        for channel in NEWS_CHANNELS:
            for _attempt in range(3):  # Переименовано для устранения предупреждения
                try:
                    async for message in client.iter_messages(channel.lower(), limit=25):
                        if message.text and message.date:
                            message_date = message.date.replace(tzinfo=None).date()
                            date_from_dt = datetime.strptime(date_from, "%Y-%m-%d").date()
                            date_to_dt = datetime.strptime(date_to, "%Y-%m-%d").date()
                            if date_from_dt <= message_date <= date_to_dt and is_bank_name_match(message.text, aliases):
                                all_messages.append({
                                    "bank": bank_name,
                                    "reg_number": reg_number,
                                    "text": message.text,
                                    "date": message_date.strftime("%Y-%m-%d"),
                                    "link": f"https://t.me/{channel}/{message.id}",  # Убрали пробел
                                    "source": f"telegram_{channel}"
                                })
                    break
                except sqlite3.OperationalError as e:
                    if 'database is locked' in str(e).lower():
                        await asyncio.sleep(2)
                    else:
                        raise
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds + random.uniform(0, 2))
                except UnauthorizedError:
                    break
                except Exception as e:
                    logging.error(f"Ошибка парсинга канала {channel}: {e}")
                    break
        logging.info(f"Telegram: найдено {len(all_messages)} сообщений для {bank_name}")
    except Exception as e:
        logging.error(f"Ошибка при парсинге Telegram для {bank_name}: {e}")
    finally:
        release_session(session_info)
        if client and client.is_connected():
            await client.disconnect()
    return all_messages

async def scrape_inkazan_news_monitoring(session, bank_name, aliases, date_from, date_to):
    url = "https://inkazan.ru/news"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    reg_number = BANKS.get(bank_name, {}).get("reg_number", bank_name)
    articles = []
    try:
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                return []
            html_content = await response.text()
            soup = BeautifulSoup(html_content, 'html.parser')
            news_items = soup.select('div.news-list__item')
            for item in news_items:
                title_elem = item.select_one('a.news-list__title')
                if not title_elem:
                    continue
                title = title_elem.get_text(strip=True)
                link = title_elem['href']
                link = link if link.startswith('http') else f"https://inkazan.ru{link}"
                date_elem = item.select_one('div.news-list__date')
                date_str = date_elem.get_text(strip=True) if date_elem else "Неизвестно"
                try:
                    date_match = re.search(r'(\d{1,2})\s+(\w+)\s*(\d{4})', date_str)
                    if date_match:
                        day = int(date_match.group(1))
                        month_name = date_match.group(2).lower()
                        year = int(date_match.group(3))
                        months = {'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12}
                        month = months.get(month_name)
                        if month:
                            news_date = datetime(year, month, day).strftime("%Y-%m-%d")
                        else:
                            continue
                    else:
                        continue
                    news_date_dt = datetime.strptime(news_date, "%Y-%m-%d").date()
                    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d").date()
                    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d").date()
                    if not (date_from_dt <= news_date_dt <= date_to_dt):
                        continue
                except Exception:
                    continue
                async with session.get(link, headers=headers) as article_response:
                    if article_response.status != 200:
                        continue
                    article_html = await article_response.text()
                    article_soup = BeautifulSoup(article_html, 'html.parser')
                    content_elem = article_soup.select_one('div.article__content')
                    text = content_elem.get_text(separator=" ", strip=True) if content_elem else title
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
        logging.error(f"Ошибка при запросе к inkazan.ru: {e}")
    return articles

async def fetch_rss_news_monitoring(bank_name, date_from, date_to):
    reg_number = BANKS.get(bank_name, {}).get("reg_number", bank_name)
    aliases = generate_aliases(bank_name)
    all_articles = []
    connector = aiohttp.TCPConnector(limit=HTTP_LIMIT)
    async with aiohttp.ClientSession(connector=connector) as session:
        inkazan_task = scrape_inkazan_news_monitoring(session, bank_name, aliases, date_from, date_to)
        rss_tasks = []
        for rss_feed in RSS_FEEDS:
            async def limited_rss_parse(feed=rss_feed):
                async with RSS_SEM:
                    return await parse_single_rss_feed_monitoring(session, feed, bank_name, reg_number, aliases, date_from, date_to)
            rss_tasks.append(asyncio.create_task(limited_rss_parse()))
        results = await asyncio.gather(inkazan_task, *rss_tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Ошибка при парсинге RSS или inkazan: {result}")
                continue
            elif result:
                all_articles.extend(result)
    logging.info(f"Найдено {len(all_articles)} RSS-новостей (включая inkazan.ru) для {bank_name}")
    return all_articles

async def parse_single_rss_feed_monitoring(session, rss_feed, bank_name, reg_number, aliases, date_from, date_to):
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
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        entry_date = datetime(*entry.published_parsed[:6])
                    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                        entry_date = datetime(*entry.updated_parsed[:6])
                    else:
                        continue
                    date_str = entry_date.strftime("%Y-%m-%d")
                    entry_date_dt = entry_date.date()
                    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d").date()
                    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d").date()
                    if not (date_from_dt <= entry_date_dt <= date_to_dt):
                        continue
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
                    if is_bank_name_match(text, aliases):
                        articles.append({
                            "bank": bank_name,
                            "reg_number": reg_number,
                            "text": text,
                            "date": date_str,
                            "link": entry.link,
                            "source": rss_feed
                        })
                except Exception as e:
                    logging.error(f"Ошибка обработки записи RSS в ленте {rss_feed}: {e}")
                    continue
    except Exception as e:
        logging.error(f"Ошибка при запросе к RSS-ленте {rss_feed}: {e}")
    return articles

def generate_aliases(bank_name):
    aliases = [bank_name]
    if bank_name in BANKS:
        aliases.extend(BANKS[bank_name].get("aliases", []))
    return aliases

def is_bank_name_match(text, aliases):
    if not text:
        return False
    normalized_text = normalize_text_for_aliases(text)
    for alias in aliases:
        normalized_alias = normalize_text_for_aliases(alias)
        alias_words = normalized_alias.split()
        all_words_found = all(word in normalized_text for word in alias_words)
        if all_words_found:
            return True
    return False

def is_news_already_in_db(link, bank_name, table_name="analyzed_monitored_news"):
    try:
        conn = sqlite3.connect('monitoring.db')
        cursor = conn.cursor()
        cursor.execute(f'SELECT 1 FROM {table_name} WHERE link = ? AND bank_name = ? LIMIT 1', (link, bank_name))
        return cursor.fetchone() is not None
    except sqlite3.Error as e:
        logging.error(f"Ошибка проверки дубликата для link={link}, bank={bank_name}: {e}")
        return False
    finally:
        conn.close()

def get_existing_analyzed_summaries(bank_name, days=30):
    last_date = datetime.now() - timedelta(days=days)
    try:
        conn = sqlite3.connect('monitoring.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT summary, date, event_type, entities, category FROM analyzed_monitored_news 
            WHERE bank_name = ? AND created_at > ?
        ''', (bank_name, last_date.strftime("%Y-%m-%d %H:%M:%S")))
        rows = cursor.fetchall()
        return [(row[0], row[1], row[2], row[3], row[4]) for row in rows]
    except sqlite3.Error as e:
        logging.error(f"Ошибка чтения существующих summaries для {bank_name}: {e}")
        return []
    finally:
        conn.close()

# --- ОСНОВНАЯ ФУНКЦИЯ ОБРАБОТКИ БАНКА ---
async def process_bank_monitoring(bank_name, date_from, date_to):
    all_news = []
    rss_news = await fetch_rss_news_monitoring(bank_name, date_from, date_to)
    all_news.extend(rss_news)
    await asyncio.sleep(1)
    bankov_news = await fetch_1000bankov_news_monitoring(bank_name, date_from, date_to)
    all_news.extend(bankov_news)
    await asyncio.sleep(1)
    telegram_news = await fetch_telegram_news_monitoring(bank_name, date_from, date_to)
    all_news.extend(telegram_news)
    await asyncio.sleep(1)
    filtered_news = [item for item in all_news if not is_news_already_in_db(item.get("link", ""), bank_name, "monitored_news")]
    if not filtered_news:
        logging.info(f"Нет новых raw новостей для {bank_name}")
        return []
    await save_to_monitoring_db_async(filtered_news, "monitored_news")
    analyzed_news = await analyze_all_news(filtered_news, topic=None, is_monitoring=True)
    if not analyzed_news:
        logging.info(f"После анализа нет релевантных новостей для {bank_name}")
        return []
    existing_summaries = get_existing_analyzed_summaries(bank_name)
    if not existing_summaries:
        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(5)
            unique_news = await deduplicate_in_parallel(analyzed_news, session, semaphore, similarity_threshold=0.85)
    else:
        combined_news = []
        for summary, date_str, event_type, entities_str, category in existing_summaries:
            combined_news.append({
                "summary": summary,
                "date": date_str,
                "event_type": event_type,
                "entities": [e.strip() for e in entities_str.split(',')] if entities_str else [],
                "source": "database",
                "informativeness": calculate_informativeness(summary),
                "category": category or "Обычная",
                "is_from_db": True
            })
        for news in analyzed_news:
            combined_news.append({**news, "is_from_db": False})
        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(5)
            unique_combined = await deduplicate_in_parallel(combined_news, session, semaphore, similarity_threshold=0.85)
        unique_news = [news for news in unique_combined if not news.get("is_from_db", False)]
    if not unique_news:
        logging.info(f"Нет новых analyzed новостей для {bank_name}")
        return []
    await save_to_monitoring_db_async(unique_news, "analyzed_monitored_news")
    return unique_news

async def monitoring_loop(bot):
    init_monitoring_db()
    scheduled_hours = [7, 11, 15, 19]
    moscow_tz = pytz.timezone('Europe/Moscow')

    while True:
        try:
            now = datetime.now(moscow_tz)
            # Определяем ближайший запуск
            next_run = None
            for hour in scheduled_hours:
                candidate = now.replace(hour=hour, minute=0, second=0, microsecond=0)
                if candidate > now:
                    if next_run is None or candidate < next_run:
                        next_run = candidate
            if next_run is None:
                next_run = now.replace(hour=scheduled_hours[0], minute=0, second=0, microsecond=0) + timedelta(days=1)

            sleep_seconds = (next_run - now).total_seconds()
            if sleep_seconds > 0:
                logging.info(f"Ожидание следующего цикла до {next_run.strftime('%Y-%m-%d %H:%M:%S')} ({int(sleep_seconds)} сек)")
                await asyncio.sleep(sleep_seconds)

            # === ЗАПУСК МОНИТОРИНГА ===
            logging.info("✅ Запуск цикла мониторинга")
            run_time = datetime.now(moscow_tz)
            date_to = run_time.strftime("%Y-%m-%d")
            date_from = (run_time - timedelta(hours=12)).strftime("%Y-%m-%d")
            banks = get_active_banks()
            if not banks:
                logging.info("Нет активных банков — пропускаем цикл.")
                continue

            user_notifications = defaultdict(lambda: defaultdict(list))
            for i in range(0, len(banks), BATCH_SIZE):
                batch = banks[i:i + BATCH_SIZE]
                logging.info(f"Обработка батча {i // BATCH_SIZE + 1}: {len(batch)} банков")

                tasks = []
                for bank in batch:
                    async def process_with_semaphore(b_name):
                        async with BANK_SEM:
                            result = await process_bank_monitoring(b_name, date_from, date_to)
                            await asyncio.sleep(DELAY_BETWEEN_BANKS)
                            return result
                    tasks.append(asyncio.create_task(process_with_semaphore(bank)))

                results = await asyncio.gather(*tasks, return_exceptions=True)
                for j, result in enumerate(results):
                    bank_name = batch[j]
                    if isinstance(result, Exception):
                        logging.error(f"Ошибка обработки банка {bank_name}: {result}")
                        continue
                    if result:
                        subs = get_user_subscriptions_by_bank(bank_name)
                        for chat_id in subs:
                            user_notifications[chat_id][bank_name].extend(result)

                await asyncio.sleep(DELAY_BETWEEN_BATCHES)

            # === ОТПРАВКА УВЕДОМЛЕНИЙ ===
            all_subscriptions = get_all_subscriptions()
            unique_chats = {chat_id for chat_id, _ in all_subscriptions}

            for chat_id in unique_chats:
                bank_news = user_notifications[chat_id]
                total = sum(len(news) for news in bank_news.values())
                
                # Генерируем уникальный идентификатор для этой итерации мониторинга
                monitoring_iteration_id = f"{int(datetime.now().timestamp())}_{chat_id}"
                
                if total > 0:
                    message = "📬 <b>Найдены новости по вашим подпискам!</b>\n"
                    for bank, news_list in bank_news.items():
                        neg = sum(1 for n in news_list if n.get("sentiment") == "Негативная")
                        message += f"• {bank}: {len(news_list)} последних (🔴 {neg} негативных)\n"
                    message += f"\nВсего: {total}\nНажмите кнопку ниже, чтобы просмотреть."
                    
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="📰 Просмотреть все новости", callback_data=f"view_monitoring_iteration_{monitoring_iteration_id}")]
                    ])
                    
                    # Сохраняем новости с привязкой к итерации мониторинга
                    if chat_id not in hot_news_cache:
                        hot_news_cache[chat_id] = {}
                    
                    # Сохраняем новости этой итерации
                    iteration_news = []
                    for news_list in bank_news.values():
                        iteration_news.extend(news_list)
                    
                    hot_news_cache[chat_id][monitoring_iteration_id] = {
                        "news": iteration_news,
                        "timestamp": datetime.now().timestamp()
                    }
                    
                    # Ограничиваем размер кеша - храним последние 10 итераций
                    if len(hot_news_cache[chat_id]) > 10:
                        # Удаляем самые старые итерации
                        oldest_iterations = sorted(
                            hot_news_cache[chat_id].items(), 
                            key=lambda x: x[1]["timestamp"]
                        )[:len(hot_news_cache[chat_id]) - 10]
                        for iter_id, _ in oldest_iterations:
                            del hot_news_cache[chat_id][iter_id]
                    
                else:
                    message = "📭 <b>За последние 4 часа новостей по вашим подпискам не найдено.</b>\nМы продолжаем мониторинг."
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🔍 Просмотреть архив новостей", callback_data="view_monitoring_archive")],
                        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                    ])

                try:
                    await bot.send_message(chat_id, message, parse_mode="HTML", reply_markup=keyboard, disable_web_page_preview=True)
                    if total > 0:
                        update_last_notification(chat_id)
                except Exception as e:
                    logging.error(f"Не удалось отправить уведомление chat_id={chat_id}: {e}")

        except Exception as e:
            logging.error(f"Критическая ошибка в monitoring_loop: {e}", exc_info=True)
            await asyncio.sleep(60)