# news_analyzer.py (улучшенная версия с оптимизированным обнаружением дубликатов и параллельной проверкой по парам)
import asyncio
import aiohttp
import re
import hashlib
import json
import random
from collections import OrderedDict, defaultdict, deque
from datetime import datetime, timedelta
import logging
from config import *
import sqlite3
from utils import *

# Для TF-IDF
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Глобальные кэши для news.db
gemini_cache = OrderedDict()  # key -> (response, timestamp)
duplicate_cache = OrderedDict()  # key -> (is_duplicate, timestamp)
hard_duplicate_cache = OrderedDict()  # key -> (hash, timestamp)
soft_duplicate_cache = OrderedDict()  # key -> (similarity, timestamp)

# Размеры кэшей
MAX_CACHE_SIZE = 1000000
MAX_DUPLICATE_CACHE_SIZE = 20000
MAX_HARD_CACHE_SIZE = 1000
MAX_SOFT_CACHE_SIZE = 5000

# TTL в часах
CACHE_TTL_HOURS = 24

# Доверенные источники
TRUSTED_SOURCES = {"tass.ru", "interfax.ru", "kommersant.ru", "vedomosti.ru"}

# Предопределенные типы событий для нормализации
EVENT_TYPE_MAPPING = {
    # === Санкции и судебные разбирательства ===
    "санкции": "санкции",
    "судебное разбирательство": "санкции",
    "обжалование": "санкции",
    "апелляция": "санкции",
    "оспаривание санкций": "санкции",
    "жалоба в суд": "санкции",

    # === Ипотека ===
    "ипотека": "ипотека",
    "семейная ипотека": "ипотека",
    "it ипотека": "ипотека",
    "цифровая ипотека": "ипотека",
    "льготная ипотека": "ипотека",
    "рефинансирование ипотеки": "ипотека",
    "ипотечное кредитование": "ипотека",

    # === Кредитование ===
    "кредитование": "кредитование",
    "выдача кредита": "кредитование",
    "дополнительное кредитование": "кредитование",
    "реструктуризация кредита": "кредитование",
    "кредитный лимит": "кредитование",
    "кредитные лимиты": "кредитование",

    # === Продукты ===
    "запуск продукта": "запуск продукта",
    "новый продукт": "запуск продукта",
    "вывод на рынок": "запуск продукта",
    "релиз продукта": "запуск продукта",
    "инвестиционный продукт": "запуск продукта",
    "биржевой фонд": "запуск продукта",
    "конкурс": "запуск продукта",
    "розыгрыш": "запуск продукта",
    "акция": "запуск продукта",

    # === IT и технологии ===
    "трансформация it архитектуры": "трансформация it-архитектуры",
    "реинжиниринг it ландшафта": "трансформация it-архитектуры",
    "цифровая трансформация": "трансформация it-архитектуры",
    "миграция на новую платформу": "трансформация it-архитектуры",
    "обновление core системы": "трансформация it-архитектуры",

    # === Прогнозы и аналитика ===
    "прогноз ключевой ставки": "прогноз ключевой ставки",
    "прогноз по ставке": "прогноз ключевой ставки",
    "аналитика ключевой ставки": "прогноз ключевой ставки",
    "ключевая ставка": "прогноз ключевой ставки",
    "ставка цб": "прогноз ключевой ставки",
    "монетарная политика": "прогноз ключевой ставки",

    # === Поддержка бизнеса ===
    "поддержка предпринимателей": "поддержка предпринимателей",
    "кредитование малого бизнеса": "поддержка предпринимателей",
    "мсп": "поддержка предпринимателей",
    "ип": "поддержка предпринимателей",

    # === Регуляторика ===
    "обследование операций": "обследование операций",
    "проверка цб": "обследование операций",
    "аудит банка": "обследование операций",
    "надзорное мероприятие": "обследование операций",

    # === Корпоративное управление ===
    "смена руководства": "смена руководства",
    "кадровые изменения": "смена руководства",
    "назначение": "смена руководства",
    "уход ceo": "смена руководства",
    "новый ceo": "смена руководства",

    # === Финансы ===
    "инвестиции": "инвестиции",
    "допэмиссия": "инвестиции",
    "привлечение капитала": "инвестиции",
    "размещение акций": "инвестиции",
    "спо": "инвестиции",

    # === Налоги ===
    "налоги": "налоги",
    "налоговое регулирование": "налоги",
    "материальная выгода": "налоги",
    "налогообложение физлиц": "налоги",

    # === Прочие ===
    "регистрация компании": "регистрация компании",
    "жалоба клиента": "жалоба клиента",
    "штраф": "штраф",
    "недвижимость": "недвижимость",
    "денежно кредитная политика": "денежно-кредитная политика",
    "рейтинг": "рейтинг",
    "рейтинговое действие": "рейтинг",
}

# Список ключевых слов для исключения нерелевантных новостей
IRRELEVANT_KEYWORDS = []

# Глобальный TF-IDF векторизатор
_tfidf_vectorizer = TfidfVectorizer(
    ngram_range=(1, 3),
    lowercase=True,
    max_features=5000
)

# --- ФУНКЦИИ ЗАГРУЗКИ/СОХРАНЕНИЯ КЭША ДЛЯ news.db ---
def load_cache():
    try:
        conn = sqlite3.connect('news.db')
        cursor = conn.cursor()
        cursor.execute("SELECT cache_key, response, timestamp FROM gemini_cache ORDER BY timestamp ASC")
        rows = cursor.fetchall()
        cache = OrderedDict()
        for row in rows:
            try:
                ts = datetime.fromisoformat(row[2])
            except:
                ts = datetime.now()
            cache[row[0]] = (row[1], ts)
        return cache
    except sqlite3.Error as e:
        logging.error(f"Ошибка загрузки кэша Gemini: {e}")
        return OrderedDict()
    finally:
        conn.close()

def save_cache():
    cleanup_cache(gemini_cache)
    try:
        conn = sqlite3.connect('news.db', timeout=30)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM gemini_cache")
        for key, (value, ts) in gemini_cache.items():
            cursor.execute("INSERT INTO gemini_cache (cache_key, response, timestamp) VALUES (?, ?, ?)",
                           (key, value, ts.isoformat()))
        conn.commit()
        logging.info("Кэш Gemini сохранен в БД")
    except sqlite3.Error as e:
        logging.error(f"Ошибка сохранения кэша Gemini: {e}")
    finally:
        conn.close()

def load_duplicate_cache():
    try:
        conn = sqlite3.connect('news.db')
        cursor = conn.cursor()
        cursor.execute("SELECT cache_key, value, timestamp FROM duplicate_cache ORDER BY timestamp ASC")
        rows = cursor.fetchall()
        cache = OrderedDict()
        for row in rows:
            try:
                ts = datetime.fromisoformat(row[2])
            except:
                ts = datetime.now()
            cache[row[0]] = (bool(row[1]), ts)
        return cache
    except sqlite3.Error as e:
        logging.error(f"Ошибка загрузки кэша дубликатов: {e}")
        return OrderedDict()
    finally:
        conn.close()

def save_duplicate_cache():
    cleanup_cache(duplicate_cache)
    try:
        conn = sqlite3.connect('news.db', timeout=30)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM duplicate_cache")
        for key, (value, ts) in duplicate_cache.items():
            cursor.execute("INSERT INTO duplicate_cache (cache_key, value, timestamp) VALUES (?, ?, ?)",
                           (key, int(value), ts.isoformat()))
        conn.commit()
        logging.info("Кэш дубликатов сохранен в БД")
    except sqlite3.Error as e:
        logging.error(f"Ошибка сохранения кэша дубликатов: {e}")
    finally:
        conn.close()

def load_hard_duplicate_cache():
    try:
        conn = sqlite3.connect('news.db')
        cursor = conn.cursor()
        cursor.execute("SELECT cache_key, summary_hash, timestamp FROM hard_duplicate_cache ORDER BY timestamp ASC")
        rows = cursor.fetchall()
        cache = OrderedDict()
        for row in rows:
            try:
                ts = datetime.fromisoformat(row[2])
            except:
                ts = datetime.now()
            cache[row[0]] = (row[1], ts)
        return cache
    except sqlite3.Error as e:
        logging.error(f"Ошибка загрузки жесткого кэша дубликатов: {e}")
        return OrderedDict()
    finally:
        conn.close()

def save_hard_duplicate_cache():
    cleanup_cache(hard_duplicate_cache)
    try:
        conn = sqlite3.connect('news.db', timeout=30)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM hard_duplicate_cache")
        for key, (value, ts) in hard_duplicate_cache.items():
            cursor.execute("INSERT INTO hard_duplicate_cache (cache_key, summary_hash, timestamp) VALUES (?, ?, ?)",
                           (key, value, ts.isoformat()))
        conn.commit()
        logging.info("Жесткий кэш дубликатов сохранен в БД")
    except sqlite3.Error as e:
        logging.error(f"Ошибка сохранения жесткого кэша дубликатов: {e}")
    finally:
        conn.close()

def load_soft_duplicate_cache():
    try:
        conn = sqlite3.connect('news.db')
        cursor = conn.cursor()
        cursor.execute("SELECT cache_key, similarity, timestamp FROM soft_duplicate_cache ORDER BY timestamp ASC")
        rows = cursor.fetchall()
        cache = OrderedDict()
        for row in rows:
            try:
                ts = datetime.fromisoformat(row[2])
            except:
                ts = datetime.now()
            cache[row[0]] = (float(row[1]), ts)
        return cache
    except sqlite3.Error as e:
        logging.error(f"Ошибка загрузки мягкого кэша: {e}")
        return OrderedDict()
    finally:
        conn.close()

def save_soft_duplicate_cache():
    cleanup_cache(soft_duplicate_cache)
    try:
        conn = sqlite3.connect('news.db', timeout=30)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM soft_duplicate_cache")
        for key, (value, ts) in soft_duplicate_cache.items():
            cursor.execute("INSERT INTO soft_duplicate_cache (cache_key, similarity, timestamp) VALUES (?, ?, ?)",
                           (key, value, ts.isoformat()))
        conn.commit()
        logging.info("Мягкий кэш похожих событий сохранен в БД")
    except sqlite3.Error as e:
        logging.error(f"Ошибка сохранения мягкого кэша: {e}")
    finally:
        conn.close()

def cleanup_cache(cache, max_age_hours=CACHE_TTL_HOURS):
    now = datetime.now()
    keys_to_delete = [
        key for key, (_, timestamp) in cache.items()
        if (now - timestamp).total_seconds() > max_age_hours * 3600
    ]
    for key in keys_to_delete:
        del cache[key]
    if keys_to_delete:
        logging.info(f"Очищено {len(keys_to_delete)} устаревших записей из кэша")

# Инициализация БД и кэшей
def init_db_extended():
    from utils import init_db
    init_db()
    try:
        conn = sqlite3.connect('news.db')
        cursor = conn.cursor()
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_event_date_hash 
            ON analyzed_news (event_type, event_date, summary_hash)
        """)
        conn.commit()
        logging.info("Индекс для дубликатов создан")
    except sqlite3.Error as e:
        logging.error(f"Ошибка создания индекса: {e}")
    finally:
        conn.close()

init_db_extended()
gemini_cache = load_cache()
duplicate_cache = load_duplicate_cache()
hard_duplicate_cache = load_hard_duplicate_cache()
soft_duplicate_cache = load_soft_duplicate_cache()

# --- ✅ ГЛОБАЛЬНЫЙ АДАПТИВНЫЙ СЕМАФОР ДЛЯ LLM ---
class AdaptiveSemaphore:
    def __init__(self, initial_value=10):
        self._semaphore = asyncio.Semaphore(initial_value)
        self._current_value = initial_value
        self._lock = asyncio.Lock()
        self._last_429_time = None

    async def acquire(self):
        await self._semaphore.acquire()

    async def release(self):
        self._semaphore.release()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.release()

    async def reduce_limit(self, new_limit):
        async with self._lock:
            if new_limit < self._current_value:
                new_sem = asyncio.Semaphore(new_limit)
                for _ in range(self._current_value - new_limit):
                    if self._semaphore._value > 0:
                        self._semaphore.release()
                        await new_sem.acquire()
                self._semaphore = new_sem
                self._current_value = new_limit
                logging.warning(f"Адаптивный семафор: лимит снижен до {new_limit}")

    async def maybe_increase_limit(self):
        async with self._lock:
            if self._last_429_time and (datetime.now() - self._last_429_time).total_seconds() > 60:
                new_limit = min(self._current_value + 1, 15)
                if new_limit > self._current_value:
                    await self.reduce_limit(new_limit)
                    self._last_429_time = None

    def record_429(self):
        self._last_429_time = datetime.now()

LLM_SEMAPHORE = AdaptiveSemaphore(initial_value=10)

# --------------------------------------------------
async def send_gemini_request(session, prompt, retries=10, semaphore=None):
    async with LLM_SEMAPHORE:
        normalized_prompt = re.sub(r'\s+', ' ', prompt.strip())
        cache_key = hashlib.md5(normalized_prompt.encode('utf-8')).hexdigest()
        now = datetime.now()
        if cache_key in gemini_cache:
            response, ts = gemini_cache[cache_key]
            if (now - ts).total_seconds() < CACHE_TTL_HOURS * 3600:
                return response
            else:
                del gemini_cache[cache_key]

        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {PROXY_API_KEY}"}
        data = {"contents": [{"role": "user", "parts": [{"text": prompt}]}]}
        for attempt in range(retries):
            try:
                async with session.post(GEMINI_API_URL, headers=headers, json=data, timeout=aiohttp.ClientTimeout(total=60)) as response:
                    if response.status == 200:
                        result = await response.json()
                        text = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "Ошибка")
                        if text != "Ошибка":
                            gemini_cache[cache_key] = (text, now)
                            if len(gemini_cache) > MAX_CACHE_SIZE:
                                gemini_cache.popitem(last=False)
                            await LLM_SEMAPHORE.maybe_increase_limit()
                            return text
                        else:
                            logging.warning(f"LLM вернул 'Ошибка' для промпта: {prompt[:50]}...")
                            return "Ошибка"
                    elif response.status == 429:
                        LLM_SEMAPHORE.record_429()
                        delay = min(60, (2 ** attempt) + random.uniform(2, 5))
                        logging.warning(f"HTTP 429. Попытка {attempt + 1}/{retries}. Ждем {delay:.1f} сек.")
                        await asyncio.sleep(delay)
                        await LLM_SEMAPHORE.reduce_limit(max(3, LLM_SEMAPHORE._current_value - 1))
                    elif response.status in (500, 502, 503, 504):
                        delay = min(30, (attempt + 1) * 3 + random.uniform(1, 3))
                        logging.warning(f"HTTP {response.status}. Попытка {attempt + 1}/{retries}. Ждем {delay:.1f} сек.")
                        await asyncio.sleep(delay)
                    else:
                        logging.error(f"Неожиданный HTTP {response.status} для промпта: {prompt[:50]}...")
                        await asyncio.sleep(5)
            except asyncio.TimeoutError:
                logging.warning(f"Таймаут запроса к LLM (попытка {attempt + 1}/{retries})")
                await asyncio.sleep(10 + attempt * 2)
            except aiohttp.ClientError as e:
                logging.warning(f"Ошибка сети при запросе к LLM: {e}. Попытка {attempt + 1}/{retries}")
                await asyncio.sleep(5 + attempt * 2)
            except Exception as e:
                logging.error(f"Необработанное исключение при запросе к LLM: {e}")
                await asyncio.sleep(10)
        logging.error(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось выполнить запрос к LLM после {retries} попыток. Промпт: {prompt[:100]}...")
        return "Ошибка"

def normalize_text(text):
    if not text:
        return ""
    text = re.sub(r'[^\w\s]', ' ', text.lower().strip())
    return re.sub(r'\s+', ' ', text)

def calculate_informativeness(text):
    words = normalize_text(text).split()
    return len(set(words)) * 5

def normalize_date(date_input):
    if isinstance(date_input, datetime):
        return date_input.replace(tzinfo=None)
    if not isinstance(date_input, str):
        return datetime.now().replace(tzinfo=None)
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%a, %d %b %Y %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(date_input, fmt).replace(tzinfo=None)
        except ValueError:
            continue
    return datetime.now().replace(tzinfo=None)

def normalize_event_type(event_type):
    if not isinstance(event_type, str):
        event_type = str(event_type)
   
    event_type = re.sub(r'[^\w\s]', '', event_type.lower().strip())
    
    mapped = EVENT_TYPE_MAPPING.get(event_type)
    if mapped:
        return mapped
  
    return event_type

def extract_keywords(text, top_n=5):
    if not text:
        return []
    normalized_text = normalize_text(text)
    if not normalized_text:
        return []
    try:
        tfidf_matrix = _tfidf_vectorizer.fit_transform([normalized_text])
        feature_names = _tfidf_vectorizer.get_feature_names_out()
        tfidf_scores = tfidf_matrix.toarray()[0]
        top_indices = tfidf_scores.argsort()[-top_n:][::-1]
        return [feature_names[i] for i in top_indices if tfidf_scores[i] > 0.1]
    except Exception as e:
        logging.warning(f"Ошибка при извлечении ключевых слов: {e}")
        return []

def is_topic_relevant(text, topic):
    if not topic:
        return True
    normalized_text = normalize_text(text)
    normalized_topic = normalize_text(topic)
    if normalized_topic in normalized_text:
        return True
    topic_keywords = {
        "ипотека": ["ипотек", "ипотечн", "жилье", "недвижимость", "кредит на жилье"],
        "кредит": ["кредит", "заем", "ссуд", "потребительский кредит", "автокредит"],
        "санкции": ["санкци", "ограничен", "блокиров", "запрет"],
        "технологии": ["технолог", "it", "айти", "инновац", "цифров", "онлайн", "мобильн", "приложен"],
        "финансы": ["финанс", "капитал", "актив", "прибыль", "убыток", "рентабельность"],
        "открытие офисов": ["офис", "отделен", "филиал", "точка", "банкомат", "атм"],
        "штраф": ["штраф", "взыскан", "нарушен", "санкци", "пени", "неустойка"],
        "жалоба клиента": ["жалоб", "претензи", "недовольств", "обман", "мошенничеств", "суд", "исковое"],
    }
    keywords = topic_keywords.get(normalized_topic, [normalized_topic])
    for keyword in keywords:
        if keyword in normalized_text:
            return True
    return False

# --- ✅ УЛУЧШЕННАЯ ФУНКЦИЯ is_duplicate С ОБЯЗАТЕЛЬНОЙ LLM-ПРОВЕРКОЙ ---
async def is_duplicate(session, new_summary, existing_summary, new_date, existing_date, new_event_type, existing_event_type, new_entities, existing_entities, threshold=0.7):
    hash1 = hashlib.md5(new_summary.encode('utf-8')).hexdigest()
    hash2 = hashlib.md5(existing_summary.encode('utf-8')).hexdigest()
    pair_id = hashlib.md5(f"{hash1}:{hash2}".encode()).hexdigest()[:8]
    logging.debug(f"[{pair_id}] Начало проверки дубликата")

    try:
        new_date = normalize_date(new_date)
        existing_date = normalize_date(existing_date)
    except ValueError:
        logging.debug(f"[{pair_id}] Ошибка нормализации даты")
        return False

    new_event_type = normalize_event_type(new_event_type)
    existing_event_type = normalize_event_type(existing_event_type)

    if abs((new_date - existing_date).days) > 5:
        logging.debug(f"[{pair_id}] Разница в датах > 5 дней: {abs((new_date - existing_date).days)}")
        return False

    new_entities_lower = [e.lower().strip() for e in new_entities]
    existing_entities_lower = [e.lower().strip() for e in existing_entities]
    common_entities = set(new_entities_lower) & set(existing_entities_lower)
    if not common_entities and new_event_type not in ["реклама", "обычная"]:
        logging.debug(f"[{pair_id}] Нет общих сущностей для важного события: {new_entities} vs {existing_entities}")
        return False

    sorted_hashes = sorted([hash1, hash2])
    combined_key = hashlib.md5(":".join(sorted_hashes).encode('utf-8')).hexdigest()
    now = datetime.now()

    if combined_key in duplicate_cache:
        is_dupe, ts = duplicate_cache[combined_key]
        if (now - ts).total_seconds() < CACHE_TTL_HOURS * 3600:
            logging.debug(f"[{pair_id}] Кэш дубликатов: {is_dupe}")
            return is_dupe
        else:
            del duplicate_cache[combined_key]

    prompt = (
        "Ты — эксперт по анализу финансовых новостей. Определи, являются ли две выжимки ОДНИМ И ТЕМ ЖЕ СОБЫТИЕМ. "
        "Даже если типы событий немного отличаются, но описывается одно и то же событие — считай дубликатом.\n"
        "КРИТЕРИИ ДЛЯ ДУБЛИКАТОВ:\n"
        "1. Одно и то же конкретное событие (например, один и тот же штраф ЦБ, одна и та же дата IPO, одна и та же сделка, обжалование одного и того же решения суда).\n"
        "2. Совпадают ключевые сущности и дата события (даже если типы событий разные, но содержание идентично).\n"
        "3. Схожесть по смыслу превышает 70% (учитывай синонимы, перефразировки, но фокус на фактах).\n"
        "КРИТЕРИИ ДЛЯ НЕ ДУБЛИКАТОВ:\n"
        "1. Разные события, даже если они по одной теме.\n"
        "2. Отличаются ключевые сущности или факты.\n"
        "3. Разница в датах превышает 3 дня.\n"
        "ВАЖНО: Если есть неуверенность, считай НЕ дубликатом. Будь строг: дубликат только если это абсолютно одно событие.\n"
        f"Выжимка 1: '{new_summary}'\n"
        f"Выжимка 2: '{existing_summary}'\n"
        "Ключевые сущности 1: " + ", ".join(new_entities) + "\n"
        "Ключевые сущности 2: " + ", ".join(existing_entities) + "\n"
        "Дата события 1: " + new_date.strftime("%Y-%m-%d") + "\n"
        "Дата события 2: " + existing_date.strftime("%Y-%m-%d") + "\n"
        "Тип события 1: " + new_event_type + "\n"
        "Тип события 2: " + existing_event_type + "\n"
        "Дай ответ в формате:\n"
        "Решение: [Дубликат/Не дубликат]\n"
        "Доверие: [0-100]\n"
        "Обоснование: [краткое объяснение]"
    )

    response = await send_gemini_request(session, prompt)
    logging.debug(f"[{pair_id}] LLM ответ: {response}")

    is_dupe = False
    trust_score = 0

    if "дубликат" in response.lower() and "не дубликат" not in response.lower():
        is_dupe = True
    elif "не дубликат" in response.lower():
        is_dupe = False

    match_trust = re.search(r"Доверие:\s*(\d+)", response)
    if match_trust:
        trust_score = int(match_trust.group(1))

    if "решение" not in response.lower() and "дубликат" not in response.lower():
        texts = [normalize_text(new_summary), normalize_text(existing_summary)]
        try:
            tfidf_matrix = _tfidf_vectorizer.fit_transform(texts)
            cosine_sim = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
            is_dupe = cosine_sim >= threshold
            trust_score = int(cosine_sim * 100)
        except Exception as e:
            logging.warning(f"[{pair_id}] Ошибка TF-IDF: {e}")
            is_dupe = False

    duplicate_cache[combined_key] = (is_dupe, now)
    if len(duplicate_cache) > MAX_DUPLICATE_CACHE_SIZE:
        duplicate_cache.popitem(last=False)

    logging.info(f"[{pair_id}] Дубликат: {is_dupe}, Доверие: {trust_score}%")
    return is_dupe

# --- ✅ УЛУЧШЕННАЯ ПАРАЛЛЕЛЬНАЯ ДЕДУБЛИКАЦИЯ С ОГРАНИЧЕНИЕМ ПАР ---
async def deduplicate_in_parallel(all_news, session, semaphore=None, similarity_threshold=0.7, max_parallel_pairs=20):
    if len(all_news) <= 1:
        return all_news

    local_sem = semaphore or asyncio.Semaphore(max_parallel_pairs)

    normalized_news = []
    for news in all_news:
        try:
            event_date = normalize_date(news["event_date"])
        except:
            event_date = normalize_date(news["date"])
        event_type = normalize_event_type(news.get("event_type", ""))
        normalized_news.append({**news, "event_date_norm": event_date, "event_type_norm": event_type})

    groups_by_type = defaultdict(list)
    for i, news in enumerate(normalized_news):
        et = news["event_type_norm"]
        if et in ["обычная", "реклама", "неизвестно"]:
            continue
        groups_by_type[et].append(i)

    all_indices = list(range(len(normalized_news)))
    groups_by_type["__all__"] = all_indices

    pairs = set()
    for group_indices in groups_by_type.values():
        n = len(group_indices)
        if n <= 1:
            continue
        for i in range(n):
            for j in range(i):
                idx1, idx2 = group_indices[i], group_indices[j]
                if idx1 == idx2:
                    continue
                date1 = normalized_news[idx1]["event_date_norm"]
                date2 = normalized_news[idx2]["event_date_norm"]
                if abs((date1 - date2).days) <= 3:
                    pair = tuple(sorted((idx1, idx2)))
                    pairs.add(pair)

    pairs = list(pairs)
    logging.info(f"Сгенерировано {len(pairs)} пар для дедубликации (после фильтрации по типу и дате ±3 дня)")

    if not pairs:
        return all_news

    async def check_pair_limited(pair):
        async with local_sem:
            i, j = pair
            news_i = all_news[i]
            news_j = all_news[j]
            is_dupe = await is_duplicate(
                session,
                news_i["summary"], news_j["summary"],
                news_i["event_date"], news_j["event_date"],
                news_i["event_type"], news_j["event_type"],
                news_i["entities"], news_j["entities"],
                threshold=similarity_threshold
            )
            return (i, j, is_dupe)

    tasks = [check_pair_limited(pair) for pair in pairs]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    graph = defaultdict(list)
    for res in results:
        if isinstance(res, Exception):
            continue
        i, j, is_dupe = res
        if is_dupe:
            graph[i].append(j)
            graph[j].append(i)

    visited = set()
    clusters = []
    for idx in range(len(all_news)):
        if idx not in visited:
            cluster = []
            stack = [idx]
            while stack:
                node = stack.pop()
                if node not in visited:
                    visited.add(node)
                    cluster.append(node)
                    stack.extend(graph[node])
            clusters.append(cluster)

    unique_news = []
    for cluster in clusters:
        if len(cluster) == 1:
            unique_news.append(all_news[cluster[0]])
        else:
            candidates = [all_news[i] for i in cluster]
            best = max(
                candidates,
                key=lambda x: (
                    x["source"] in TRUSTED_SOURCES,
                    x["informativeness"],
                    x["category"] in ["Важная", "Риск"],
                    x["date"]
                )
            )
            unique_news.append(best)
            logging.debug(f"Кластер дубликатов ({len(cluster)}): выбрана '{best['summary'][:60]}...'")

    logging.info(f"Дедубликация завершена: {len(unique_news)} уникальных из {len(all_news)}")
    return unique_news

# -----------------------------------
def check_bank_name(text, bank_name):
    normalized_text = normalize_text(text)
    if bank_name.lower() in normalized_text:
        return True
    if bank_name in BANKS:
        for alias in BANKS[bank_name].get("aliases", []):
            if normalize_text(alias) in normalized_text:
                return True
    return False

async def generate_news_dict(news_item, session, topic=None, semaphore=None, is_monitoring=False):
    text = news_item.get("text", "")
    bank_name = news_item.get("bank", "")
    date = news_item.get("date", "")
    link = news_item.get("link", "")
    source = link
    provided_summary = news_item.get("summary", "")
    provided_category = news_item.get("category", "")
    if not text or not bank_name or not date:
        logging.info(f"Новость исключена: отсутствует текст, банк или дата")
        return None

    normalized_text = normalize_text(text)
    if not is_topic_relevant(text, topic):
        logging.info(f"Новость исключена: тема '{topic}' не найдена в тексте. Текст: {text[:100]}...")
        return None

    if not check_bank_name(normalized_text, bank_name):
        logging.info(f"Новость исключена: банк {bank_name} не найден в тексте. Текст: {text[:100]}...")
        return None

    if any(keyword in normalized_text for keyword in IRRELEVANT_KEYWORDS):
        logging.info(f"Новость исключена: содержит нерелевантные ключевые слова для банка {bank_name}. Текст: {text[:100]}...")
        return None

    financial_keywords = [
        "банк", "кредит", "ипотек", "вклад", "ставка", "санкци", "штраф", "ЦБ", "финанс", 
        "инвестиц", "акция", "облигаци", "платеж", "перевод", "карта", "счет", "офис", 
        "банкомат", "приложени", "онлайн", "мобильный", "прибыль", "убыток", "риск", 
        "регулятор", "лицензи", "страхован", "вкладчик", "заемщик", "кредитор", "депозит"
    ]
    has_financial_keyword = any(kw in normalized_text for kw in financial_keywords)
    if not has_financial_keyword:
        logging.info(f"Новость исключена: отсутствуют ключевые финансовые термины. Текст: {text[:100]}...")
        return None

    try:
        news_date = normalize_date(date)
        if news_date < datetime.now().replace(tzinfo=None) - timedelta(days=30):
            logging.info(f"Новость исключена: слишком старая (дата: {date}). Текст: {text[:100]}...")
            return None
    except ValueError:
        logging.warning(f"Не удалось разобрать дату: {date}")

    prompt_relevance = (
        f"Относится ли новость к банку (АО,ПАО,ООО, КБ) '{bank_name}'{f' и теме \"{topic}\"' if topic else ''}? "
        f"Текст: '{text}'. "
        f"Контекст: '{bank_name}' — это банк, предоставляющий финансовые услуги (вклады, ипотека, кредиты, недвижимость, санкции, технологии, финансы, регуляторы, IPO, инфраструктура, установка банкоматов, открытие офисов{' и ' + topic if topic else ''}). "
        f"Исключи новости, где вместо банка '{bank_name}' упоминаются другие организации с похожими названиями (например, 'МТС Юрент', 'МТС Развлечения', 'МТС AdTech', 'МТС Телеком', или 'ЭКСПО-2017' вместо 'ЭКСПОБАНК'),банк может фигурировать в разных финансовых контекстах(повышение рейтинга акций, выкуп земли для строительства и тд) "
        f"Ответь одним словом: Да/Нет"
    )
    prompt_summary = (
        f"Составь выжимку новости для банка '{bank_name}' на основе текста: '{text}', которая будет содержать важные события и изменения в банке. "
        f"Укажи тип события, дату события и ключевые сущности (упоминая '{bank_name}' и связанные организации, через запятую). "
        f"При-examples:"
        f"- Текст: 'МТС Банк снизил ставки по ипотеке до 7% с 28 июля.' Выжимка: '{bank_name} снизил ставки по ипотеке до 7% с 28 июля.' Тип события: ипотека. Дата события: 2025-07-28. Ключевые сущности: {bank_name}."
        f"- Текст: 'ЦБ оштрафовал МТС Банк на 1 млн руб за нарушения.' Выжимка: 'ЦБ оштрафовал {bank_name} на 1 млн руб за нарушения.' Тип события: штраф. Дата события: {date}. Ключевые сущности: {bank_name}, ЦБ."
        f"- Текст: 'Клиент жалуется на МТС Банк из-за задержки закрытия вклада.' Выжимка: 'Клиент жалуется на задержку закрытия вклада в {bank_name}.' Тип события: жалоба клиента. Дата события: {date}. Ключевые сущности: {bank_name}, клиент."
        f"- Текст: 'МТС запустила новый сервис.' Выжимка: 'Отсутствуют релевантные события, связанные с банком.' Тип события: нет. Дата события: {date}. Ключевые сущности: МТС."
        f"Формат:"
        f"Выжимка: [текст]"
        f"Тип события: [тип]"
        f"Дата события: [дата]"
        f"Ключевые сущности: [сущности]"
    )
    prompt_category = (
        f"Определи категорию новости для банка '{bank_name}': '{text}'. "
        f"Ответь одним словом: Реклама, Важная, Риск, Обычная. "
        f"Реклама — продукты (вклады, ипотека, кредиты, недвижимость); Важная — IPO, смена руководства, технологии, санкции, установка банкоматов; "
        f"Риск — штрафы, санкции, убытки, жалобы клиентов; Обычная — остальные."
    )
    prompt_sentiment = (
        f"Определи тональность новости для банка '{bank_name}' на основе текста: '{text}'. "
        f"Ответь в формате: 'Тональность: [Позитивная/Негативная/Нейтральная]. Объяснение: [краткое объяснение (до 20 слов)]'. "
        f"Критерии:"
        f"- Позитивная: прибыль, рост, новые продукты, технологии, награды, расширение услуг, успешные сделки."
        f"- Негативная: санкции, штрафы, убытки, клиентские жалобы, проблемы с услугами, скандалы, закрытие филиалов."
        f"- Нейтральная: нейтральные события, статистика, открытие филиалов, регуляторные изменения без явных последствий."
        f"Если новость связана с клиентскими претензиями или проблемами, считай её Негативной, если нет явного положительного разрешения."
    )

    tasks = [
        send_gemini_request(session, prompt_relevance),
        send_gemini_request(session, prompt_summary),
        send_gemini_request(session, prompt_category),
        send_gemini_request(session, prompt_sentiment)
    ]
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    if any(isinstance(r, Exception) for r in responses) or any(r == "Ошибка" for r in responses):
        logging.warning(f"Ошибка в запросах LLM: {text[:50]}...")
        return None

    relevance, summary_response, category, sentiment_response = responses
    if relevance.strip().lower() != "да":
        logging.info(f"Новость исключена: не релевантна для банка {bank_name}. Текст: {text[:100]}...")
        return None

    match_summary = re.search(
        r"Выжимка:\s*(.*?)\s*"
        r"Тип события:\s*(.*?)\s*"
        r"Дата события:\s*(.*?)\s*"
        r"Ключевые сущности:\s*(.*)",
        summary_response, re.DOTALL
    )
    if not match_summary:
        logging.warning(f"Не удалось разобрать ответ LLM для выжимки: {summary_response[:100]}...")
        if provided_summary:
            summary = provided_summary
            event_type = "неизвестно"
            event_date = date
            entities = [bank_name]
        else:
            logging.info(f"Новость исключена: не удалось разобрать summary, нет provided_summary. Текст: {text[:100]}...")
            return None
    else:
        summary = match_summary.group(1).strip() or provided_summary
        event_type = normalize_event_type(match_summary.group(2).strip().lower())
        event_date = match_summary.group(3).strip()
        entities_text = match_summary.group(4).strip()
        if entities_text:
            raw_entities = [e.strip() for e in re.split(r'[;,]', entities_text) if e.strip()]
            filtered_entities = []
            for entity in raw_entities:
                entity = re.sub(r'^[0-9]+\.\s*', '', entity)
                entity = re.sub(r'^-\s*', '', entity)
                if len(entity) > 2:
                    filtered_entities.append(entity)
            entities = filtered_entities
        else:
            entities = extract_keywords(text, top_n=3)

    normalized_summary = normalize_text(summary)
    normalized_text = normalize_text(text)
    irrelevant_indicators = ["отсутствуют релевантные события", "нет событий", "отсутствует информация"]
    if any(indicator in normalized_summary for indicator in irrelevant_indicators):
        logging.info(f"Новость исключена после вторичной проверки: summary содержит индикатор нерелевантности ({summary[:50]}...). Текст: {text[:100]}...")
        return None

    if not check_bank_name(normalized_summary, bank_name):
        if check_bank_name(normalized_text, bank_name):
            summary = f"{bank_name}: {summary}"
            logging.info(f"Добавлено имя банка в summary: {summary[:50]}...")
        else:
            logging.info(f"Новость исключена после вторичной проверки: банк не найден ни в summary, ни в тексте ({summary[:50]}...). Текст: {text[:100]}...")
            return None

    logging.info(f"Новость прошла вторичную проверку: summary={summary[:50]}..., текст содержит банк={check_bank_name(normalized_text, bank_name)}")

    match_sentiment = re.match(r"Тональность:\s*(\w+)\.\s*Объяснение:\s*(.*)", sentiment_response.strip())
    if match_sentiment:
        sentiment = match_sentiment.group(1)
        sentiment_explanation = match_sentiment.group(2)
        logging.info(f"Тональность: {sentiment}. Объяснение: {sentiment_explanation}")
    else:
        logging.warning(f"Не удалось разобрать тональность: {sentiment_response}")
        sentiment = "Нейтральная"

    if event_date == 'неизвестно':
        event_date = date
    try:
        event_date = datetime.strptime(event_date, "%Y-%m-%d").replace(tzinfo=None)
    except ValueError:
        try:
            event_date = datetime.strptime(date, "%d.%m.%Y").replace(tzinfo=None)
        except ValueError:
            event_date = datetime.now().replace(tzinfo=None)

    news_dict = {
        "bank": bank_name,
        "reg_number": news_item.get("reg_number", bank_name),
        "text": text,
        "summary": summary,
        "event_type": event_type,
        "event_date": event_date,
        "entities": entities,
        "date": date,
        "link": link,
        "source": source,
        "category": category.strip() or provided_category,
        "sentiment": sentiment,
        "informativeness": calculate_informativeness(text),
        "summary_hash": hashlib.md5(summary.encode('utf-8')).hexdigest()
    }
    return news_dict

# --- УСКОРЕННАЯ ФУНКЦИЯ analyze_all_news ---
async def analyze_all_news(news_list, topic=None, max_per_event=2, similarity_threshold=0.7, is_monitoring=False):
    timeout = aiohttp.ClientTimeout(total=120)
    semaphore = asyncio.Semaphore(10)
    analyzed_news = []
    async with aiohttp.ClientSession(timeout=timeout) as session:
        filtered_news = [news for news in news_list if check_bank_name(normalize_text(news.get("text", "")), news.get("bank", ""))]
        logging.info(f"После предварительной фильтрации: {len(filtered_news)} новостей из {len(news_list)}")

        async def process_news(news_item):
            return await generate_news_dict(news_item, session, topic, semaphore, is_monitoring)

        tasks = [process_news(news_item) for news_item in filtered_news]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_news = [r for r in results if r is not None and not isinstance(r, Exception)]

        logging.info(f"Запуск параллельной дедубликации для {len(all_news)} новостей...")
        # Отдельный семафор для дедубликации
        dedup_sem = asyncio.Semaphore(15)
        unique_news = await deduplicate_in_parallel(
            all_news, 
            session, 
            semaphore=dedup_sem, 
            similarity_threshold=similarity_threshold,
            max_parallel_pairs=15
        )

        groups = defaultdict(list)
        for news in unique_news:
            group_key = (normalize_event_type(news["event_type"]), news["event_date"])
            groups[group_key].append(news)

        final_news = []
        for (event_type, event_date), group in groups.items():
            sorted_group = sorted(
                group,
                key=lambda x: (
                    x["source"] in TRUSTED_SOURCES,
                    x["informativeness"],
                    x["category"] in ["Важная", "Риск"]
                ),
                reverse=True
            )
            top_news = sorted_group[:max_per_event]
            final_news.extend(top_news)
            logging.info(
                f"Событие '{event_type}' на дату {event_date}: выбрано {len(top_news)} новостей из {len(group)} уникальных"
            )

        logging.info(f"Анализ завершен: {len(final_news)} финальных новостей")
        if final_news:
            if is_monitoring:
                from monitoring import save_to_monitoring_db_async
                await save_to_monitoring_db_async(final_news, table_name="analyzed_monitored_news")
            else:
                from utils import save_to_db_async
                await save_to_db_async(final_news, table_name="analyzed_news")

        save_cache()
        save_duplicate_cache()
        save_hard_duplicate_cache()
        save_soft_duplicate_cache()

    return final_news