# utils.py (с фиксом для длинного имени файла в заметке)

import csv
import logging
from datetime import datetime, timedelta
import sqlite3
import re
import asyncio

# --- Глобальная асинхронная блокировка для базы данных ---
DB_WRITE_LOCK = asyncio.Lock()
# ------------------------------------------------------


def init_db():
    """Инициализация базы данных SQLite"""
    try:
        conn = sqlite3.connect('news.db')
        cursor = conn.cursor()

        # Таблица для хранения спарсированных новостей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS parsed_news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bank TEXT,
                reg_number TEXT,
                text TEXT,
                date TEXT,
                link TEXT,
                source TEXT,
                topic TEXT DEFAULT '',
                is_monitoring BOOLEAN DEFAULT 0,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_fetch_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (link)
            )
        ''')

        # Добавляем столбец last_fetch_time, если его нет
        cursor.execute("PRAGMA table_info(parsed_news)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'last_fetch_time' not in columns:
            cursor.execute('''
                ALTER TABLE parsed_news ADD COLUMN last_fetch_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ''')
            logging.info("Добавлен столбец last_fetch_time в таблицу parsed_news")

        # Таблица для хранения проанализированных новостей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analyzed_news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bank TEXT,
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Добавляем summary_hash, если его нет
        cursor.execute("PRAGMA table_info(analyzed_news)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'summary_hash' not in columns:
            cursor.execute('''
                ALTER TABLE analyzed_news ADD COLUMN summary_hash TEXT
            ''')
            logging.info("Добавлен столбец summary_hash в таблицу analyzed_news")

        # Таблица кэша для запросов к Gemini API
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gemini_cache (
                cache_key TEXT PRIMARY KEY,
                response TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Таблица кэша для дедупликации (сравнение пар)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS duplicate_cache (
                cache_key TEXT PRIMARY KEY,
                value INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Жесткий кэш дубликатов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS hard_duplicate_cache (
                cache_key TEXT PRIMARY KEY,
                summary_hash TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Мягкий кэш похожих событий
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS soft_duplicate_cache (
                cache_key TEXT PRIMARY KEY,
                similarity REAL,
                timestamp TEXT
            )
        ''')

        # История парсинга
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS parse_history (
                bank TEXT PRIMARY KEY,
                last_parse_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_from TEXT,
                last_to TEXT
            )
        ''')

        # Добавляем last_from и last_to при необходимости
        cursor.execute("PRAGMA table_info(parse_history)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'last_from' not in columns:
            cursor.execute('ALTER TABLE parse_history ADD COLUMN last_from TEXT')
        if 'last_to' not in columns:
            cursor.execute('ALTER TABLE parse_history ADD COLUMN last_to TEXT')
        logging.info("Добавлены столбцы last_from и last_to в parse_history")

        # Индексы
        cursor.execute("PRAGMA table_info(analyzed_news)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'summary_hash' in columns:
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_event_date_hash 
                ON analyzed_news (event_type, event_date, summary_hash)
            """)
            logging.info("Создан индекс idx_event_date_hash")

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_parsed_date ON parsed_news (date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_analyzed_date ON analyzed_news (date)")

        conn.commit()
        logging.info("База данных news.db инициализирована.")
    except sqlite3.Error as e:
        logging.error(f"Ошибка инициализации БД: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()


async def save_to_db_async(data, table_name):
    """Асинхронное сохранение данных в базу с блокировкой для предотвращения конфликтов."""
    if not data:
        return

    async with DB_WRITE_LOCK:
        try:
            conn = sqlite3.connect('news.db', timeout=30)
            cursor = conn.cursor()

            if table_name == "parsed_news":
                for item in data:
                    cursor.execute('''
                        INSERT OR IGNORE INTO parsed_news (
                            bank, reg_number, text, date, link, source, topic, is_monitoring, last_fetch_time
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ''', (
                        item.get("bank", ""),
                        item.get("reg_number", ""),
                        item.get("text", ""),
                        item.get("date", ""),
                        item.get("link", ""),
                        item.get("source", ""),
                        item.get("topic", ""),
                        item.get("is_monitoring", False)
                    ))
            elif table_name == "analyzed_news":
                for item in data:
                    event_date_str = item.get("event_date", "")
                    if isinstance(event_date_str, datetime):
                        event_date_str = event_date_str.strftime("%Y-%m-%d")
                    entities_str = ",".join(item.get("entities", []))
                    summary_hash = item.get("summary_hash", "")
                    cursor.execute('''
                        INSERT OR REPLACE INTO analyzed_news (
                            bank, reg_number, text, summary, event_type, event_date,
                            entities, date, link, source, category, sentiment, informativeness, summary_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        item.get("bank", ""),
                        item.get("reg_number", ""),
                        item.get("text", ""),
                        item.get("summary", ""),
                        item.get("event_type", ""),
                        event_date_str,
                        entities_str,
                        item.get("date", ""),
                        item.get("link", ""),
                        item.get("source", ""),
                        item.get("category", ""),
                        item.get("sentiment", ""),
                        item.get("informativeness", 0),
                        summary_hash
                    ))

            conn.commit()
            logging.info(f"Данные сохранены в таблицу {table_name}, {len(data)} записей")
        except sqlite3.Error as e:
            logging.error(f"Ошибка сохранения данных в таблицу {table_name}: {e}")
        finally:
            if 'conn' in locals() and conn:
                conn.close()
    await asyncio.sleep(0.01)


def save_to_db(data, table_name):
    """Синхронное сохранение данных в базу (для обратной совместимости)."""
    if not data:
        return

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(save_to_db_async(data, table_name))
        loop.close()
    else:
        future = asyncio.run_coroutine_threadsafe(save_to_db_async(data, table_name), loop)
        future.result()


def save_to_csv(data, filename="news.csv"):
    """Сохранение данных в CSV-файл с сортировкой по тональности и дате внутри тональности"""
    fieldnames = ["bank", "reg_number", "text", "date", "link", "sentiment", "summary", "category"]
    cleaned_data = [{k: v for k, v in item.items() if k in fieldnames} for item in data]

    sentiment_groups = {
        "Негативная": [],
        "Нейтральная": [],
        "Позитивная": []
    }

    for item in cleaned_data:
        sentiment = item.get("sentiment", "Нейтральная")
        if sentiment in sentiment_groups:
            sentiment_groups[sentiment].append(item)
        else:
            sentiment_groups["Нейтральная"].append(item)

    for sentiment in sentiment_groups:
        sentiment_groups[sentiment].sort(
            key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d") if x["date"] else datetime.now(),
            reverse=True
        )

    sorted_data = (
        sentiment_groups["Негативная"] +
        sentiment_groups["Нейтральная"] +
        sentiment_groups["Позитивная"]
    )

    with open(filename, mode="w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sorted_data)
    logging.info(f"Данные сохранены в {filename}")


def normalize_text_for_aliases(text):
    """Нормализация текста для проверки алиасов"""
    if not text:
        return ""
    text = re.sub(r'[^\w\s]', ' ', text.lower())
    text = re.sub(r'\s+', ' ', text.strip())
    return text
