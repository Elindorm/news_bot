# bot.py (с фиксом для длинного имени файла)

import logging
import asyncio
import csv
import os
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError
from news_parser import *
from news_analyzer import analyze_all_news
from datetime import datetime, timedelta
import locale
import html
from config import *
from utils import *
from monitoring import *
import sqlite3

# Установка локали для корректного отображения месяцев на русском
try:
    locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
except locale.Error:
    logging.warning("Не удалось установить локаль ru_RU.UTF-8, используем стандартную")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="bot.log"
)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher()

# Ограничение на количество одновременных задач
MAX_CONCURRENT_TASKS = 100
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

# Хранилище данных пользователей
user_data = {}

# Состояния
WAITING_FOR_BANK = "waiting_for_bank"
WAITING_FOR_TOPIC = "waiting_for_topic"
WAITING_FOR_DATE_RANGE_START = "waiting_for_date_range_start"
WAITING_FOR_DATE_RANGE_END = "waiting_for_date_range_end"
WAITING_FOR_CONFIRMATION = "waiting_for_confirmation"
WAITING_FOR_MONITORING_BANK = "waiting_for_monitoring_bank"
WAITING_FOR_FEEDBACK = "waiting_for_feedback"
WAITING_FOR_MONITORING_BANK_UNSUBSCRIBE = "waiting_for_monitoring_bank_unsubscribe"
# Список месяцев в именительном падеже
MONTHS_NOMINATIVE = [
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
]

# Функция сохранения в CSV
def save_to_csv(news_list, filename):
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([
            "Банк", "Рег. номер", "Текст", "Краткое содержание", "Тип события", 
            "Дата события", "Сущности", "Дата новости", "Ссылка", "Источник", 
            "Категория", "Тональность", "Информативность"
        ])
        for item in news_list:
            writer.writerow([
                item.get("bank", ""),
                item.get("reg_number", ""),
                item.get("text", "")[:1000],
                item.get("summary", "")[:1000],
                item.get("event_type", ""),
                item.get("event_date", ""),
                "; ".join(item.get("entities", [])),
                item.get("date", ""),
                item.get("link", ""),
                item.get("source", ""),
                item.get("category", ""),
                item.get("sentiment", ""),
                item.get("informativeness", 0)
            ])

def sanitize_text(text):
    if not text:
        return ""
    text = html.escape(str(text))
    return text

def normalize_text(text):
    if not text:
        return ""
    text = str(text).lower().strip()
    text = ' '.join(text.split())  
    return text

async def start_command(message: types.Message):
    logging.info(f"Команда /start или /menu от {message.chat.id}")
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="Новости банков", callback_data="category_banks"),
        InlineKeyboardButton(text="Мониторинг", callback_data="monitoring")
    )
    keyboard.row(
        InlineKeyboardButton(text="Просмотреть мониторинг новостей", callback_data="view_monitoring_news")
    )
    keyboard.row(
        InlineKeyboardButton(text="Обратная связь", callback_data="feedback"),
        InlineKeyboardButton(text="📖 Инструкция", callback_data="show_instructions")
    )
    await message.answer(
        "Я бот для парсинга новостей! Выберите категорию новостей, мониторинг или обратную связь:",
        reply_markup=keyboard.as_markup(),
        disable_web_page_preview=True
    )

async def generate_calendar(year, month):
    keyboard = InlineKeyboardBuilder()
    first_day = datetime(year, month, 1)
    last_day = (first_day.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    days_in_month = last_day.day
    days = ["Пн", "Вт", "Ср", "Чт","Пт", "Сб", "Вс"]
    for day in days:
        keyboard.add(InlineKeyboardButton(text=day, callback_data="none"))
    keyboard.adjust(7)
    first_weekday = first_day.weekday()
    for _ in range(first_weekday):
        keyboard.add(InlineKeyboardButton(text=" ", callback_data="none"))
    for day in range(1, days_in_month + 1):
        keyboard.add(InlineKeyboardButton(
            text=str(day),
            callback_data=f"date_{year}-{month:02d}-{day:02d}"
        ))
    keyboard.adjust(7)
    last_day_weekday = last_day.weekday()
    for _ in range((6 - last_day_weekday) % 7):
        keyboard.add(InlineKeyboardButton(text=" ", callback_data="none"))
    prev_month = (first_day - timedelta(days=1)).replace(day=1)
    next_month = (first_day + timedelta(days=31)).replace(day=1)
    keyboard.row(
        InlineKeyboardButton(text="<<", callback_data=f"month_{prev_month.year}_{prev_month.month}"),
        InlineKeyboardButton(text=">>", callback_data=f"month_{next_month.year}_{next_month.month}")
    )
    keyboard.row(InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu"))
    return keyboard.as_markup()

async def update_status_message(message: types.Message, statuses: list, bot: Bot):
    for status in statuses:
        try:
            await message.edit_text(status)
            await asyncio.sleep(1.5)
        except Exception as e:
            logging.warning(f"Ошибка при обновлении статуса: {e}")
            break

async def process_news_for_category(message: types.Message, categories: list, chat_id: int, date_from: str, date_to: str, topic: str = None):
    async with semaphore:
        status_message = await bot.send_message(
            chat_id=chat_id,
            text=f"Начинаю сбор новостей для категорий {', '.join(categories)}{f' по теме \"{topic}\"' if topic else ''}..."
        )
        try:
            fetch_statuses = [
                "Собираю новости... 🕵️‍♂️",
                "Просматриваю сайты... 🌐",
                "Загружаю данные... 📥",
            ]
            status_task = asyncio.create_task(
                update_status_message(status_message, fetch_statuses, bot)
            )
            all_news = []
            for category in categories:
                news = await fetch_all_news(category, date_from, date_to, topic=topic)
                all_news.extend(news)
            status_task.cancel()
            logging.info(f"Получено {len(all_news)} новостей для категорий {categories}")
            if not all_news:
                keyboard = InlineKeyboardBuilder()
                if any(cat in BANKS for cat in categories):
                    keyboard.row(
                        InlineKeyboardButton(text="🔙 К выбору банка", callback_data="return_to_bank_selection"),
                        InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")
                    )
                else:
                    keyboard.row(InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu"))
                await status_message.edit_text(
                    f"Новостей{f' по теме \"{topic}\"' if topic else ''} не найдено.",
                    reply_markup=keyboard.as_markup(),
                    disable_web_page_preview=True
                )
                return
            analyze_statuses = [
                "Анализирую новости... 🧠",
                "Оцениваю тональность... 📊",
                "Категоризирую данные... 📑",
            ]
            status_task = asyncio.create_task(
                update_status_message(status_message, analyze_statuses, bot)
            )
            analyzed_news = await analyze_all_news(all_news, topic=topic)
            status_task.cancel()
            logging.info(f"После анализа: {len(analyzed_news)} новостей для {categories}")
            if not analyzed_news:
                keyboard = InlineKeyboardBuilder()
                if any(cat in BANKS for cat in categories):
                    keyboard.row(
                        InlineKeyboardButton(text="🔙 К выбору банка", callback_data="return_to_bank_selection"),
                        InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")
                    )
                else:
                    keyboard.row(InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu"))
                await status_message.edit_text(
                    f"Релевантных новостей{f' по теме \"{topic}\"' if topic else ''} после анализа не найдено.",
                    reply_markup=keyboard.as_markup(),
                    disable_web_page_preview=True
                )
                return
            sentiment_groups = {
                "Негативная": [],
                "Нейтральная": [],
                "Позитивная": []
            }
            for item in analyzed_news:
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
            sorted_news = (
                sentiment_groups["Негативная"] +
                sentiment_groups["Нейтральная"] +
                sentiment_groups["Позитивная"]
            )
            await status_message.edit_text("Сохраняю результаты... 💾")
            sentiment_counts = {
                "Негативная": len(sentiment_groups["Негативная"]),
                "Нейтральная": len(sentiment_groups["Нейтральная"]),
                "Позитивная": len(sentiment_groups["Позитивная"])
            }
            total_news = len(sorted_news)
            user_data[chat_id] = {
                "news": sorted_news,
                "current_page": 0,
                "categories": categories,
                "date_from": date_from,
                "date_to": date_to,
                "topic": topic
            }
            # Фикс для длинного имени файла: используем фиксированное имя с timestamp
            csv_filename = f"news_{int(datetime.now().timestamp())}.csv"
            save_to_csv(sorted_news, csv_filename)
            document = FSInputFile(csv_filename)
            await bot.send_document(
                chat_id=chat_id,
                document=document,
                caption=f"Новости для категорий {', '.join(categories)}{f' по теме \"{topic}\"' if topic else ''} в CSV"
            )
            try:
                if os.path.exists(csv_filename):
                    os.remove(csv_filename)
                    logging.info(f"CSV файл {csv_filename} удалён")
                else:
                    logging.warning(f"CSV файл {csv_filename} не существует, не удалось удалить")
            except Exception as e:
                logging.error(f"Ошибка удаления CSV: {e}")
            await status_message.delete()
            keyboard = InlineKeyboardBuilder()
            keyboard.row(
                InlineKeyboardButton(text="Показать новости", callback_data="start_display_news")
            )
            news_count_text = (
                f"Найдено новостей: {total_news}\n"
                f"🔴 Негативных: {sentiment_counts['Негативная']}\n"
                f"🔵 Нейтральных: {sentiment_counts['Нейтральная']}\n"
                f"🟢 Позитивных: {sentiment_counts['Позитивная']}"
            )
            await bot.send_message(
                chat_id,
                f"Новости{f' по теме \"{topic}\"' if topic else ''} собраны и проанализированы:\n{news_count_text}\nНажмите, чтобы просмотреть:",
                reply_markup=keyboard.as_markup()
            )
        except Exception as e:
            logging.error(f"Ошибка в process_news_for_category: {e}", exc_info=True)
            await status_message.edit_text("Произошла ошибка. Попробуйте позже.")

async def send_news_page(message_or_query: types.Message | types.CallbackQuery, chat_id: int, page: int):
    logging.info(f"send_news_page для chat_id {chat_id}, страница {page}")
    try:
        if chat_id not in user_data or not user_data[chat_id].get("news"):
            await bot.send_message(chat_id, "Новостей не найдено.")
            return
        news_list = user_data[chat_id]["news"]
        categories = user_data[chat_id]["categories"]
        news_per_page = 5
        total_news = len(news_list)
        total_pages = (total_news + news_per_page - 1) // news_per_page
        page = max(0, min(page, total_pages - 1))
        user_data[chat_id]["current_page"] = page
        start_idx = page * news_per_page
        end_idx = min(start_idx + news_per_page, total_news)
        news_subset = news_list[start_idx:end_idx]
        topic_for_display = user_data[chat_id].get("display_topic", "Отсутствует")
        bank_display = categories[0] if len(categories) == 1 and categories[0] in BANKS else "банков"
        message_text = (
        f"<b>Новости для {bank_display}"
        f"(страница {page + 1} из {total_pages}, всего новостей: {total_news}):</b>\n"
    )
        for idx, news in enumerate(news_subset, start=start_idx + 1):
            sentiment = news.get("sentiment", "Неизвестно")
            sentiment_text = (
                "🔴 <b>Негативная</b>" if sentiment == "Негативная" else
                "🟢 <b>Позитивная</b>" if sentiment == "Позитивная" else
                "🔵 <b>Нейтральная</b>"
            )
            summary = news.get("summary", news.get("text", "Текст отсутствует"))
            if len(summary) > 500:
                summary = summary[:500] + "..."
            summary = sanitize_text(summary)
            try:
                date_obj = datetime.strptime(news.get('date', ''), "%Y-%m-%d")
                date_str = date_obj.strftime("%d.%m.%Y")
            except ValueError:
                date_str = news.get('date', 'Неизвестно')
            link = news.get('link', 'Неизвестно')
            link = sanitize_text(link)
            message_text += (
                f"<b>{idx}. Выжимка:</b> {summary}\n"
                f"Банк: {sanitize_text(news.get('bank', 'Неизвестно'))}\n"
                f"Дата: {date_str}\n"
                f"Категория: {sanitize_text(news.get('category', 'Неизвестно'))}\n"
                f"Тональность: {sentiment_text}\n"
                f"Ссылка: {link}\n"
            )
        keyboard = InlineKeyboardBuilder()
        if page > 0:
            keyboard.add(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"page_{page-1}"))
        if page < total_pages - 1:
            keyboard.add(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"page_{page+1}"))
        if "monitoring" in categories:
            keyboard.row(InlineKeyboardButton(text="💾 Сохранить в Excel", callback_data="save_monitoring_to_excel"))
        if any(cat in BANKS for cat in categories):
            keyboard.row(
                InlineKeyboardButton(text="🔙 К выбору банка", callback_data="return_to_bank_selection"),
                InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")
            )
        else:
            keyboard.row(InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu"))
        if isinstance(message_or_query, types.Message):
            await bot.send_message(
                chat_id,
                message_text,
                reply_markup=keyboard.as_markup(),
                disable_web_page_preview=True
            )
        else:
            current_text = message_or_query.message.text
            if current_text != message_text:
                await bot.edit_message_text(
                    message_text,
                    chat_id=chat_id,
                    message_id=message_or_query.message.message_id,
                    reply_markup=keyboard.as_markup(),
                    disable_web_page_preview=True
                )
            else:
                logging.debug("Текст сообщения не изменился, пропускаем редактирование.")
    except Exception as e:
        logging.error(f"Ошибка в send_news_page: {e}", exc_info=True)
        await bot.send_message(chat_id, "Произошла ошибка при отображении новостей.")

async def handle_text(message: types.Message):
    logging.info(f"Текстовое сообщение от {message.chat.id}")
    chat_id = message.chat.id
    if chat_id not in user_data:
        user_data[chat_id] = {}
    state = user_data[chat_id].get("state")
    if message.text is None:
        if state == WAITING_FOR_FEEDBACK and message.photo:
            await handle_photo(message)
            return
        return
    query = message.text.strip()
    if not query:
        await message.answer(
            "Пожалуйста, отправьте непустое текстовое сообщение.\n"
            "Используйте /start или /menu, чтобы начать заново."
        )
        return
    if state == WAITING_FOR_BANK:
        selected_banks = []
        reg_numbers = []
        queries = query.split(',')
        for q in queries:
            q = q.strip()
            normalized_q = normalize_text(q)
            found = False
            for bank_name, info in BANKS.items():
                if info["reg_number"] == q:
                    selected_banks.append(bank_name)
                    reg_numbers.append(info["reg_number"])
                    found = True
                    break
                if normalize_text(bank_name) == normalized_q:
                    selected_banks.append(bank_name)
                    reg_numbers.append(info["reg_number"])
                    found = True
                    break
                for alias in info.get("aliases", []):
                    if normalize_text(alias) == normalized_q:
                        selected_banks.append(bank_name)
                        reg_numbers.append(info["reg_number"])
                        found = True
                        break
                if found:
                    break
            if not found:
                await message.answer(f"Банк с номером или названием '{q}' не найден.")
                return
        user_data[chat_id]["selected_banks"] = selected_banks
        user_data[chat_id]["reg_numbers"] = reg_numbers
        user_data[chat_id]["state"] = WAITING_FOR_TOPIC
        await message.answer(
            "Введите тему для фильтрации новостей (например, ипотека, кредиты, санкции) или нажмите кнопку для пропуска:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Пропустить ⏭️", callback_data="skip_topic")]
            ])
        )
        return
    elif state == WAITING_FOR_TOPIC:
        if len(query) < 3:
            await message.answer("Тема слишком короткая. Пожалуйста, введите более описательную тему (минимум 3 символа):")
            return
        # Сохраняем введенную пользователем тему и для отображения, и как актуальную для парсинга
        user_data[chat_id]["display_topic"] = query
        user_data[chat_id]["actual_topic"] = query
        if "date_from" in user_data[chat_id] and "date_to" in user_data[chat_id]:
            user_data[chat_id]["state"] = WAITING_FOR_CONFIRMATION
            selected_banks = user_data[chat_id].get("selected_banks", [])
            bank_display = ", ".join(selected_banks) if selected_banks else "не выбран"
            formatted_date_from = format_date_for_display(user_data[chat_id]["date_from"])
            formatted_date_to = format_date_for_display(user_data[chat_id]["date_to"])
            new_text = (
                f"Вы выбрали:\n"
                f"🏦 Банк(и): {bank_display}\n"
                f"📅 Период: с {formatted_date_from} по {formatted_date_to}\n"
                f"📌 Тема: {query}\n"
                f"Проверьте параметры перед подтверждением:"
            )
            await message.answer(
                new_text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_date_range")],
                    [InlineKeyboardButton(text="🔄 Изменить даты", callback_data="change_dates")],
                    [InlineKeyboardButton(text="🔄 Изменить тему", callback_data="change_topic")],
                    [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                ])
            )
        else:
            user_data[chat_id]["state"] = WAITING_FOR_DATE_RANGE_START
            now = datetime.now()
            month_name = MONTHS_NOMINATIVE[now.month - 1]
            year = now.year
            await message.answer(
                f"<b>{month_name} {year}</b>\nВыберите начальную дату периода:",
                reply_markup=await generate_calendar(now.year, now.month)
            )
        return
    elif state == WAITING_FOR_MONITORING_BANK:
        banks_input = [bank.strip() for bank in query.split(",")]
        selected_banks = []
        for bank in banks_input:
            normalized_bank = normalize_text(bank)
            for bank_name, info in BANKS.items():
                aliases = info.get("aliases", [bank_name])
                if normalized_bank == normalize_text(bank_name) or normalized_bank in [normalize_text(alias) for alias in aliases] or normalized_bank == info.get("reg_number"):
                    selected_banks.append(bank_name)
                    break
        if not selected_banks:
            await message.answer("Ни один из введенных банков не найден. Попробуйте снова:")
            return
        for bank in selected_banks:
            add_subscription(chat_id, bank)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Запарсить новости за неделю", callback_data="parse_last_week_monitoring")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
        ])
        await message.answer(
            f"✅ <b>Вы успешно подписаны на мониторинг новостей для банков: {', '.join(selected_banks)}</b>\n"
            "<b>🔔 Новости будут автоматически отправляться вам каждые 4 часа.</b>\n"
            "📥 Вы также можете вручную проверить все новости в любое время, выбрав в меню: 'Новости банков'.\n\n"
            "📝 По кнопке 'Посмотреть мониторинг новостей' вы модете увидеть новости, полученные мониторингом ранее'.\n\n"
            "Хотите запарсить новости по вашим банкам за последнюю неделю?",
            reply_markup=keyboard
        )
        user_data[chat_id]["state"] = None
        return
    elif state == WAITING_FOR_MONITORING_BANK_UNSUBSCRIBE:
        banks_input = [bank.strip() for bank in query.split(",")]
        subscriptions = get_user_subscriptions(chat_id)
        current_banks = [bank[0] for bank in subscriptions]
        
        removed_banks = []
        for bank in banks_input:
            normalized_bank = normalize_text(bank)
            for bank_name, info in BANKS.items():
                aliases = info.get("aliases", [bank_name])
                if (normalized_bank == normalize_text(bank_name) or 
                    normalized_bank in [normalize_text(alias) for alias in aliases] or 
                    normalized_bank == info.get("reg_number")):
                    if bank_name in current_banks:
                        remove_subscription(chat_id, bank_name)
                        removed_banks.append(bank_name)
                    break
        
        if not removed_banks:
            await message.answer("❌ Ни один из введенных банков не найден в ваших подписках. Попробуйте снова:")
            return
        
        # Обновляем список подписок
        remaining_subscriptions = get_user_subscriptions(chat_id)
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
        ])
        
        if remaining_subscriptions:
            remaining_banks = [bank[0] for bank in remaining_subscriptions]
            message_text = (
                f"✅ Вы отписались от мониторинга новостей для банков: {', '.join(removed_banks)}\n\n"
                f"📋 Ваши текущие подписки:\n" + "\n".join([f"• {bank}" for bank in remaining_banks])
            )
        else:
            message_text = (
                f"✅ Вы отписались от мониторинга новостей для банков: {', '.join(removed_banks)}\n\n"
                "📭 Теперь у вас нет активных подписок."
            )
        
        await message.answer(message_text, reply_markup=keyboard)
        user_data[chat_id]["state"] = None
        return
    elif state == WAITING_FOR_FEEDBACK:
        feedback_message = (
            f"📬 <b>Новое сообщение от пользователя</b>\n"
            f"👤 ID пользователя: {chat_id}\n"
            f"📝 Текст сообщения: {sanitize_text(query)}"
        )
        try:
            await bot.send_message(
                chat_id=SUPPORT_GROUP_ID,
                text=feedback_message,
                parse_mode='HTML'
            )
            await message.answer(
                "✅ Ваше сообщение успешно отправлено!\n"
                "Вы можете отправить еще текст или фото, или вернуться в главное меню.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                ])
            )
        except Exception as e:
            logging.error(f"Ошибка при отправке обратной связи: {e}")
            await message.answer(
                "❌ Произошла ошибка при отправке сообщения.\n"
                "Пожалуйста, попробуйте позже или свяжитесь с администратором.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                ])
            )
        # Не сбрасываем состояние, чтобы позволить дописать
        return
    await message.answer(
        "Пожалуйста, выберите категорию новостей с помощью кнопок.\n"
        "Используйте /start или /menu, чтобы начать заново."
    )

async def handle_photo(message: types.Message):
    chat_id = message.chat.id
    if chat_id not in user_data:
        user_data[chat_id] = {}
    user = user_data[chat_id]
    state = user.get("state")
    if state == WAITING_FOR_FEEDBACK:
        feedback_text = "Текст не указан"
        if message.caption:
            feedback_text = message.caption
        feedback_message = (
            f"📬 <b>Новое сообщение с фото от пользователя\n</b>"
            f"👤 ID пользователя: {chat_id}\n"
            f"📝 Текст сообщения: {sanitize_text(feedback_text)}"
        )
        try:
            photo = message.photo[-1]
            file_id = photo.file_id
            await bot.send_photo(
                chat_id=SUPPORT_GROUP_ID,
                photo=file_id,
                caption=feedback_message,
                parse_mode='HTML'
            )
            await message.answer(
                "✅ <b>Ваше фото и сообщение успешно отправлены</b>\n"
                "Вы можете отправить еще текст или фото, или вернуться в главное меню.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                ])
            )
        except Exception as e:
            logging.error(f"Ошибка при отправке фото в группу {SUPPORT_GROUP_ID}: {e}")
            await message.answer(
                "❌ <b>Произошла ошибка при отправке вашего фото.</b>\n"
                "Пожалуйста, попробуйте отправить его позже или свяжитесь с администратором напрямую.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                ])
            )
        # Не сбрасываем состояние, чтобы позволить дописать
    else:
        await message.answer(
            "ℹ️ <b>Фото можно отправлять только в разделе 'Обратная связь'.</b>\n"
            "Пожалуйста, используйте команду /start или /menu, чтобы начать, или выберите пункт 'Обратная связь' для отправки скриншота или изображения."
        )

def format_date_for_display(date_str):
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        day_str = str(dt.day)
        return f"{day_str} {dt.strftime('%B')} {dt.year}"
    except ValueError:
        return date_str

async def handle_callback(query: types.CallbackQuery):
    data = query.data
    chat_id = query.message.chat.id
    logging.info(f"Callback: {data} от {chat_id}")
    
    try:
        await query.answer()
    except TelegramBadRequest as e:
        if "query is too old" in str(e):
            logging.warning(f"Query is too old for chat_id {chat_id}, ignoring")
            return
        else:
            logging.error(f"TelegramBadRequest in query.answer for chat_id {chat_id}: {e}")
    except TelegramNetworkError as e:
        logging.error(f"Network error in callback for chat_id {chat_id}: {e}")

    try:
        if chat_id not in user_data:
            user_data[chat_id] = {}

        # Helper function to safely edit message only if content or markup changes
        async def safe_edit_message(new_text: str, reply_markup: InlineKeyboardMarkup = None):
            current_text = query.message.text or ""
            current_markup = query.message.reply_markup
            new_markup = reply_markup or InlineKeyboardMarkup(inline_keyboard=[])
            
            # Compare text and markup
            if (current_text == new_text and 
                current_markup == new_markup):
                logging.debug(f"Skipping edit for chat_id {chat_id}: message content and markup unchanged")
                return False
            try:
                await query.message.edit_text(
                    text=new_text,
                    reply_markup=new_markup,
                    parse_mode='HTML',
                    disable_web_page_preview=True
                )
                return True
            except TelegramBadRequest as e:
                if "message is not modified" in str(e):
                    logging.debug(f"Skipped edit due to unchanged message for chat_id {chat_id}")
                    return False
                raise
            except Exception as e:
                logging.error(f"Failed to edit message for chat_id {chat_id}: {e}")
                return False
            
    

        if data == "feedback":
            user_data[chat_id]["state"] = WAITING_FOR_FEEDBACK
            user_data[chat_id]["feedback_text"] = None
            new_text = (
                "Пожалуйста, опишите проблему или отправьте сообщение. Вы также можете прикрепить скриншот.\n"
                "Вы можете отправить несколько сообщений подряд."
            )
            await safe_edit_message(
                new_text,
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                ])
            )
            return

        elif data == "skip_topic":
            logging.info(f"Пользователь {chat_id} пропустил выбор темы через кнопку")
            user_data[chat_id]["display_topic"] = "Отсутствует"
            user_data[chat_id]["actual_topic"] = None
            user_data[chat_id]["state"] = WAITING_FOR_DATE_RANGE_START
            now = datetime.now()
            month_name = MONTHS_NOMINATIVE[now.month - 1]
            year = now.year
            new_text = f"<b>{month_name} {year}</b>\nВыберите начальную дату периода:"
            await safe_edit_message(
                new_text,
                await generate_calendar(now.year, now.month)
            )
            return

        elif data.startswith("page_"):
            page = int(data.split("_")[1])
            await send_news_page(query, chat_id, page)
            return

        elif data == "info":
            new_text = "Загружаю информацию об источниках..."
            if await safe_edit_message(new_text):
                info_text = (
                    "<b>Информация об источниках новостей</b>\n"
                    "<b>1. NewsAPI, GNews, Mediastack, Currents</b>\n"
                    "Используется для получения новостей из множества источников:\n"
                    "- Reuters\n- BBC\n- The Guardian\n- ТАСС\n- Коммерсант\n- Интерфакс\n- РБК\n"
                    "<b>2. Telegram-каналы</b>\n" + "\n".join([f"- @{channel}" for channel in NEWS_CHANNELS]) + "\n"
                    "<b>3. RSS-ленты</b>\n" + "\n".join([f"- {feed}" for feed in RSS_FEEDS]) + "\n"
                    "<b>4. 1000bankov.ru</b>\nПозволяет получать агрегированные новости по конкретному банку."
                )
                await bot.send_message(chat_id, info_text, disable_web_page_preview=True)
            return

        elif data == "show_instructions":
            instructions_text = (
                "<b>Подробная инструкция по использованию бота</b>\n\n"
                "Если бот не функционирует или возникла проблема, напишите /start для перезапуски.\n\n"
                "<b>1. Новости банков:</b>\n"
                "Выберите этот раздел для поиска новостей по конкретным банкам. Шаги:\n"
                "- Введите название или регистрационный номер банка (например, Сбербанк или 1481).\n"
                "- Введите тему для фильтрации (например, ипотека) или пропустите.\n"
                "- Выберите начальную и конечную дату периода с помощью календаря.\n"
                "- Подтвердите параметры (банки, период, тема).\n"
                "- Бот соберет, проанализирует новости и покажет результаты с пагинацией.\n"
                "Вы можете изменить даты или тему на экране подтверждения.\n\n"
                "<b>2. Мониторинг:</b>\n"
                "Настройте автоматический мониторинг новостей за текущий день (проверяется каждые 4 часа в 7, 11, 15, 19 часов).\n"
                "- Подписаться: Введите банки через запятую.\n"
                "- Отписаться: Выберите банк из списка.\n"
                "- После подписки предложено запарсить новости за неделю.\n"
                "- Новости отправляются автоматически.\n\n"
                "<b>3. Просмотреть мониторинг новостей:</b>\n"
                "Показывает весь архив новостей по подпискам, накопленный за все время работы мониторинга.\n"
                "Новости отсортированы по банкам, тональности и дате.\n\n"
                "<b>4. Обратная связь:</b>\n"
                "Отправьте текст или фото для связи с администратором.\n\n"
               
            )
            await safe_edit_message(instructions_text, InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                ])
            )
            return

        elif data == "return_to_main_menu":
            logging.info(f"Пользователь {chat_id} вернулся в главное меню")
            user_data.pop(chat_id, None)
            try:
                await query.message.delete()
            except Exception as e:
                logging.error(f"Failed to delete message for chat_id {chat_id}: {e}")
            await start_command(query.message)
            return

        elif data == "return_to_bank_selection":
            logging.info(f"Пользователь {chat_id} вернулся к выбору банка")
            user_data[chat_id]["state"] = WAITING_FOR_BANK
            new_text = "Введите название или регистрационный номер банка (например, Сбербанк или 1481):"
            await safe_edit_message(
                new_text,
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                ])
            )
            return

        elif data == "category_banks":
            user_data[chat_id]["state"] = WAITING_FOR_BANK
            new_text = "Введите название или регистрационный номер банка (например, Сбербанк или 1481):"
            await safe_edit_message(
                new_text,
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                ])
            )
            return

        elif data == "monitoring":
            keyboard = InlineKeyboardBuilder()
            keyboard.row(
                InlineKeyboardButton(text="➕ Подписаться", callback_data="subscribe_monitoring"),
                InlineKeyboardButton(text="➖ Отписаться", callback_data="unsubscribe_monitoring")
            )
            keyboard.row(InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu"))
            subscriptions = get_user_subscriptions(chat_id)
            if subscriptions:
                current_subscriptions = "\n".join([f"• {bank[0]}" for bank in subscriptions])
                message_text = (
                    "Выберите действие для мониторинга новостей (проверяет за текущий день каждые 4 часа):\n"
                    f"<b>Ваши текущие подписки:</b>\n{current_subscriptions}"
                )
            else:
                message_text = "Выберите действие для мониторинга новостей (проверяет за текущий день каждые 4 часа):\nВы не подписаны ни на один банк.\n"
            await safe_edit_message(
                message_text,
                keyboard.as_markup()
            )
            return

        elif data == "subscribe_monitoring":
            user_data[chat_id]["state"] = WAITING_FOR_MONITORING_BANK
            new_text = "Введите названия банков через запятую (например, Сбербанк, ВТБ):"
            await safe_edit_message(
                new_text,
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                ])
            )
            return

        elif data == "unsubscribe_monitoring":
            subscriptions = get_user_subscriptions(chat_id)
            if not subscriptions:
                new_text = (
                    "📭 <b>У вас пока нет активных подписок на мониторинг новостей.</b>\n"
                    "Чтобы начать получать автоматические уведомления каждые 4 часа, выберите '➕ Подписаться'."
                )
                await safe_edit_message(
                    new_text,
                    InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="➕ Подписаться", callback_data="subscribe_monitoring")],
                        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                    ])
                )
                return
            
            # Устанавливаем состояние ожидания ввода банков для отписки
            user_data[chat_id]["state"] = WAITING_FOR_MONITORING_BANK_UNSUBSCRIBE
            current_banks = [bank[0] for bank in subscriptions]
            new_text = (
                "Введите названия банков, от которых хотите отписаться, через запятую:\n"
                f"<b>Ваши текущие подписки:</b>\n" + "\n".join([f"• {bank}" for bank in current_banks])
            )
            await safe_edit_message(
                new_text,
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                ])
            )
            return

        elif data == "view_monitoring_news":
            try:
                conn = sqlite3.connect('monitoring.db')
                cursor = conn.cursor()
                cursor.execute('SELECT bank_name FROM subscriptions WHERE chat_id = ?', (chat_id,))
                banks = [row[0] for row in cursor.fetchall()]
                if not banks:
                    new_text = "Вы не подписаны ни на один банк."
                    await safe_edit_message(
                        new_text,
                        InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                        ])
                    )
                    return
                placeholders = ','.join(['?'] * len(banks))
                cursor.execute(f'''
                    SELECT bank_name, reg_number, text, summary, event_type, event_date, entities, 
                           date, link, source, category, sentiment, informativeness
                    FROM analyzed_monitored_news
                    WHERE bank_name IN ({placeholders})
                    ORDER BY created_at DESC
                ''', banks)
                rows = cursor.fetchall()
                conn.close()
                if not rows:
                    new_text = "Новостей для отслеживаемых банков пока нет."
                    await safe_edit_message(
                        new_text,
                        InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                        ])
                    )
                    return
                all_news = []
                for row in rows:
                    all_news.append({
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
                    })
                
                    all_news.sort(
                         key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d") if x.get("date") else datetime.min,
                        reverse=True
                        )
                user_data[chat_id] = {
                    "news": all_news,
                    "current_page": 0,
                    "categories": ["monitoring"],
                    "date_from": None,
                    "date_to": None,
                    "topic": None
                }
                news_by_bank = {}
                for item in all_news:
                    bank = item.get("bank", "Неизвестно")
                    if bank not in news_by_bank:
                        news_by_bank[bank] = []
                    news_by_bank[bank].append(item)
                message_lines = [f"Найдено {len(all_news)} новостей для отслеживаемых банков."]
                for bank, news_list in news_by_bank.items():
                    negative_count = sum(1 for n in news_list if n.get("sentiment") == "Негативная")
                    message_lines.append(f"• {bank}: {len(news_list)} (🔴 {negative_count} негативных)")
                message_text = "\n".join(message_lines)
                keyboard = InlineKeyboardBuilder()
                keyboard.row(
                    InlineKeyboardButton(text="Показать новости", callback_data="start_display_news")
                )
                await safe_edit_message(
                    message_text,
                    keyboard.as_markup()
                )
            except sqlite3.Error as e:
                logging.error(f"Ошибка при получении новостей мониторинга для chat_id {chat_id}: {e}")
                new_text = "Не удалось загрузить новости. Пожалуйста, попробуйте снова."
                await safe_edit_message(
                    new_text,
                    InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                    ])
                )
            return

        # Обработка просмотра конкретной итерации мониторинга
        elif data.startswith("view_monitoring_iteration_"):
            iteration_id = data.replace("view_monitoring_iteration_", "")
            
            # Ищем новости в кеше по идентификатору итерации
            iteration_news = None
            if chat_id in hot_news_cache and iteration_id in hot_news_cache[chat_id]:
                iteration_news = hot_news_cache[chat_id][iteration_id]["news"]
            
            if not iteration_news:
                new_text = "❌ Новости этой итерации больше не доступны. Пожалуйста, проверьте архив новостей."
                await safe_edit_message(
                    new_text,
                    InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="📁 Просмотреть архив новостей", callback_data="view_monitoring_archive")],
                        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                    ])
                )
                return
            
            # Сохраняем новости для отображения
            user_data[chat_id] = {
                "news": iteration_news,
                "current_page": 0,
                "categories": ["monitoring_iteration"],
                "date_from": None,
                "date_to": None,
                "topic": None
            }
            
            # Показываем статистику
            news_by_bank = {}
            for item in iteration_news:
                bank = item.get("bank", "Неизвестно")
                if bank not in news_by_bank:
                    news_by_bank[bank] = []
                news_by_bank[bank].append(item)
            
            message_lines = [f"✅ Найдено {len(iteration_news)} новостей в этой итерации:"]
            for bank, news_list in news_by_bank.items():
                negative_count = sum(1 for n in news_list if n.get("sentiment") == "Негативная")
                message_lines.append(f"• {bank}: {len(news_list)} (🔴 {negative_count} негативных)")
            
            message_text = "\n".join(message_lines)
            keyboard = InlineKeyboardBuilder()
            keyboard.row(
                InlineKeyboardButton(text="📰 Показать новости", callback_data="start_display_news")
            )
            keyboard.row(
                InlineKeyboardButton(text="📁 Просмотреть архив новостей", callback_data="view_monitoring_archive"),
                InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")
            )
            
            await safe_edit_message(message_text, keyboard.as_markup())
            return

        # Обработка просмотра архива мониторинга (всех новостей из базы)
        elif data == "view_monitoring_archive":
            try:
                conn = sqlite3.connect('monitoring.db')
                cursor = conn.cursor()
                cursor.execute('SELECT bank_name FROM subscriptions WHERE chat_id = ?', (chat_id,))
                banks = [row[0] for row in cursor.fetchall()]
                if not banks:
                    new_text = "Вы не подписаны ни на один банк."
                    await safe_edit_message(
                        new_text,
                        InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                        ])
                    )
                    return
                placeholders = ','.join(['?'] * len(banks))
                cursor.execute(f'''
                    SELECT bank_name, reg_number, text, summary, event_type, event_date, entities, 
                           date, link, source, category, sentiment, informativeness
                    FROM analyzed_monitored_news
                    WHERE bank_name IN ({placeholders})
                    ORDER BY created_at DESC
                    LIMIT 100  # Ограничиваем для производительности
                ''', banks)
                rows = cursor.fetchall()
                conn.close()
                
                if not rows:
                    new_text = "Новостей для отслеживаемых банков пока нет."
                    await safe_edit_message(
                        new_text,
                        InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                        ])
                    )
                    return
                
                all_news = []
                for row in rows:
                    all_news.append({
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
                    })
                
                # Сортируем по дате
                all_news.sort(
                    key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d") if x.get("date") else datetime.min,
                    reverse=True
                )
                
                user_data[chat_id] = {
                    "news": all_news,
                    "current_page": 0,
                    "categories": ["monitoring_archive"],
                    "date_from": None,
                    "date_to": None,
                    "topic": None
                }
                
                news_by_bank = {}
                for item in all_news:
                    bank = item.get("bank", "Неизвестно")
                    if bank not in news_by_bank:
                        news_by_bank[bank] = []
                    news_by_bank[bank].append(item)
                
                message_lines = [f"📁 Архив новостей: найдено {len(all_news)} новостей для отслеживаемых банков."]
                for bank, news_list in news_by_bank.items():
                    negative_count = sum(1 for n in news_list if n.get("sentiment") == "Негативная")
                    message_lines.append(f"• {bank}: {len(news_list)} (🔴 {negative_count} негативных)")
                
                message_text = "\n".join(message_lines)
                keyboard = InlineKeyboardBuilder()
                keyboard.row(
                    InlineKeyboardButton(text="📰 Показать новости", callback_data="start_display_news")
                )
                keyboard.row(
                    InlineKeyboardButton(text="💾 Сохранить в Excel", callback_data="save_monitoring_to_excel"),
                    InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")
                )
                
                await safe_edit_message(message_text, keyboard.as_markup())
                
            except sqlite3.Error as e:
                logging.error(f"Ошибка при получении архива новостей для chat_id {chat_id}: {e}")
                new_text = "Не удалось загрузить архив новостей. Пожалуйста, попробуйте снова."
                await safe_edit_message(
                    new_text,
                    InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                    ])
                )
            return

        elif data.startswith("view_monitoring_news_"):
            target_chat_id = int(data.replace("view_monitoring_news_", ""))
            if target_chat_id != chat_id:
                new_text = "❌ У вас нет доступа к этим новостям."
                await safe_edit_message(new_text)
                return
            new_news = hot_news_cache.get(chat_id, [])
            if not new_news:
                new_news = get_new_analyzed_news(chat_id)
                if not new_news:
                    new_text = "📭 Новых новостей для отслеживаемых банков пока нет."
                    await safe_edit_message(
                        new_text,
                        InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                        ])
                    )
                    return
            hot_news_cache.pop(chat_id, None)
            sentiment_order = {"Негативная": 0, "Нейтральная": 1, "Позитивная": 2}
            new_news.sort(
                key=lambda x: (
                    x["bank"],
                    sentiment_order.get(x["sentiment"], 1),
                    -datetime.strptime(x["date"], "%Y-%m-%d").timestamp() if x["date"] else 0
                )
            )
            user_data[chat_id] = {
                "news": new_news,
                "current_page": 0,
                "categories": ["monitoring"],
                "date_from": None,
                "date_to": None,
                "topic": None
            }
            news_by_bank = {}
            for item in new_news:
                bank = item.get("bank", "Неизвестно")
                if bank not in news_by_bank:
                    news_by_bank[bank] = []
                news_by_bank[bank].append(item)
            message_lines = [f"✅ Найдено {len(new_news)} новых новостей для отслеживаемых банков."]
            for bank, news_list in news_by_bank.items():
                negative_count = sum(1 for n in news_list if n.get("sentiment") == "Негативная")
                message_lines.append(f"• {bank}: {len(news_list)} новых (🔴 {negative_count} негативных)")
            message_text = "\n".join(message_lines)
            subscriptions = get_user_subscriptions(chat_id)
            if subscriptions:
                current_subscriptions = "\n".join([f"• {bank[0]}" for bank in subscriptions])
                message_text += f"\n<b>Ваши подписки:</b>\n{current_subscriptions}"
            message_text += "\n\nНажмите, чтобы просмотреть:"
            keyboard = InlineKeyboardBuilder()
            keyboard.row(
                InlineKeyboardButton(text="Показать новости", callback_data="start_display_news")
            )
            await safe_edit_message(
                message_text,
                keyboard.as_markup()
            )
            try:
                conn = sqlite3.connect('monitoring.db')
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE subscriptions 
                    SET last_notification = CURRENT_TIMESTAMP 
                    WHERE chat_id = ?
                ''', (chat_id,))
                conn.commit()
                conn.close()
                logging.info(f"Обновлено время last_notification для chat_id={chat_id}")
            except sqlite3.Error as e:
                logging.error(f"Ошибка обновления last_notification для chat_id {chat_id}: {e}")
            return

        elif data == "start_display_news":
            await send_news_page(query, chat_id, 0)
            return

        elif data == "save_monitoring_to_excel":
            if chat_id not in user_data or "news" not in user_data[chat_id]:
                new_text = "Новости не найдены."
                await safe_edit_message(new_text)
                return
            news_list = user_data[chat_id]["news"]
            csv_filename = f"monitoring_news_{chat_id}.csv"
            save_to_csv(news_list, csv_filename)
            document = FSInputFile(csv_filename)
            await bot.send_document(
                chat_id=chat_id,
                document=document,
                caption="📊 Ваши новости из мониторинга в формате CSV"
            )
            try:
                if os.path.exists(csv_filename):
                    os.remove(csv_filename)
                    logging.info(f"CSV файл {csv_filename} удалён")
                else:
                    logging.warning(f"CSV файл {csv_filename} не существует, не удалось удалить")
            except Exception as e:
                logging.error(f"Ошибка удаления CSV: {e}")
            await query.answer("Файл отправлен!")
            return

        elif data == "parse_last_week_monitoring":
            subscriptions = get_user_subscriptions(chat_id)
            if not subscriptions:
                new_text = "У вас нет подписок на банки."
                await safe_edit_message(new_text)
                return
            categories = [bank[0] for bank in subscriptions]
            now = datetime.now()
            date_to = now.strftime("%Y-%m-%d")
            date_from = (now - timedelta(days=7)).strftime("%Y-%m-%d")
            topic = None
            new_text = "Начинаю парсинг новостей за последнюю неделю по вашим подпискам..."
            if await safe_edit_message(new_text):
                await process_news_for_category(query.message, categories, chat_id, date_from, date_to, topic)
            return

        elif data.startswith("date_"):
            date_str = data.split("_")[1]
            try:
                selected_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                if selected_date > datetime.now().date():
                    date_str = datetime.now().strftime("%Y-%m-%d")
                    selected_date = datetime.now().date()
                    await query.answer("Дата в будущем заменена на текущую.", show_alert=True)
            except ValueError:
                new_text = (
                    "❌ Ошибка: Некорректный формат даты.\n"
                    "Пожалуйста, выберите дату с помощью календаря."
                )
                await safe_edit_message(
                    new_text,
                    await generate_calendar(datetime.now().year, datetime.now().month)
                )
                return
            if user_data[chat_id].get("state") == WAITING_FOR_DATE_RANGE_START:
                user_data[chat_id]["date_from"] = date_str
                user_data[chat_id]["state"] = WAITING_FOR_DATE_RANGE_END
                now = datetime.now()
                month_name = MONTHS_NOMINATIVE[now.month - 1]
                year = now.year
                new_text = f"<b>{month_name} {year}</b>\nВыберите конечную дату периода:"
                await safe_edit_message(
                    new_text,
                    await generate_calendar(now.year, now.month)
                )
            elif user_data[chat_id].get("state") == WAITING_FOR_DATE_RANGE_END:
                date_from_str = user_data[chat_id]["date_from"]
                try:
                    date_from_dt = datetime.strptime(date_from_str, "%Y-%m-%d")
                    date_to_dt = datetime.strptime(date_str, "%Y-%m-%d")
                    if date_to_dt.date() > datetime.now().date():
                        date_str = datetime.now().strftime("%Y-%m-%d")
                        date_to_dt = datetime.now()
                        await query.answer("Дата в будущем заменена на текущую.", show_alert=True)
                    if date_to_dt < date_from_dt:
                        new_text = (
                            "❌ Ошибка: Конечная дата не может быть раньше начальной.\n"
                            "Пожалуйста, выберите корректную дату."
                        )
                        await safe_edit_message(
                            new_text,
                            await generate_calendar(date_from_dt.year, date_from_dt.month)
                        )
                        return
                except ValueError:
                    new_text = (
                        "❌ Ошибка: Некорректный формат даты.\n"
                        "Пожалуйста, выберите дату с помощью календаря."
                    )
                    await safe_edit_message(
                        new_text,
                        await generate_calendar(now.year, now.month)
                    )
                    return
                user_data[chat_id]["date_to"] = date_str
                user_data[chat_id]["state"] = WAITING_FOR_CONFIRMATION
                selected_banks = user_data[chat_id].get("selected_banks", [])
                bank_display = ", ".join(selected_banks) if selected_banks else "не выбран"
                topic_display = user_data[chat_id].get("display_topic", "Отсутствует")
                formatted_date_from = format_date_for_display(date_from_str)
                formatted_date_to = format_date_for_display(date_str)
                new_text = (
                    f"Вы выбрали:\n"
                    f"🏦 Банк(и): {bank_display}\n"
                    f"📅 Период: с {formatted_date_from} по {formatted_date_to}\n"
                    f"📌 Тема: {topic_display}\n"
                    f"Проверьте параметры перед подтверждением:"
                )
                await safe_edit_message(
                    new_text,
                    InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_date_range")],
                        [InlineKeyboardButton(text="🔄 Изменить даты", callback_data="change_dates")],
                        [InlineKeyboardButton(text="🔄 Изменить тему", callback_data="change_topic")],
                        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                    ])
                )

        elif data.startswith("month_"):
            year, month = map(int, data.split("_")[1:])
            new_text = f"<b>{MONTHS_NOMINATIVE[month-1]} {year}</b>\nВыберите дату:"
            await safe_edit_message(
                new_text,
                await generate_calendar(year, month)
            )

        elif data == "confirm_date_range":
            date_from = user_data[chat_id]["date_from"]
            date_to = user_data[chat_id]["date_to"]
            topic_for_display = user_data[chat_id].get("display_topic", "Отсутствует")
            actual_topic = user_data[chat_id].get("actual_topic", None)
            categories = user_data[chat_id].get("selected_banks", []) or ["banks"]
            new_text = "Начинаю обработку запроса..."
            if await safe_edit_message(new_text):
                await process_news_for_category(query.message, categories, chat_id, date_from, date_to, actual_topic)

        elif data == "change_dates":
            user_data[chat_id]["state"] = WAITING_FOR_DATE_RANGE_START
            now = datetime.now()
            month_name = MONTHS_NOMINATIVE[now.month - 1]
            year = now.year
            new_text = f"<b>{month_name} {year}</b>\nВыберите начальную дату периода:"
            await safe_edit_message(
                new_text,
                await generate_calendar(now.year, now.month)
            )

        elif data == "change_topic":
            user_data[chat_id]["state"] = WAITING_FOR_TOPIC
            new_text = "Введите тему для фильтрации новостей (например, ипотека, кредиты, санкции) или нажмите кнопку для пропуска:"
            await safe_edit_message(
                new_text,
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Пропустить ⏭️", callback_data="skip_topic")]
                ])
            )
            return

    except Exception as e:
        logging.error(f"Ошибка в handle_callback для chat_id {chat_id}: {e}", exc_info=True)
        # Instead of sending an error message to the user, silently return to main menu
        try:
            new_text = "Произошла ошибка. Возвращаю вас в главное меню."
            await safe_edit_message(
                new_text,
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 В главное меню", callback_data="return_to_main_menu")]
                ])
            )
        except Exception as inner_e:
            logging.error(f"Failed to send fallback message for chat_id {chat_id}: {inner_e}")

async def main():
    dp.message.register(start_command, Command(commands=["start", "menu"]))
    dp.message.register(handle_text, F.text)
    dp.message.register(handle_photo, F.photo)
    dp.callback_query.register(handle_callback)
    asyncio.create_task(monitoring_loop(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())