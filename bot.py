# Scripts Agent - Telegram-бот каталога скриптов для Roblox.
# Переменные окружения (задаются на хосте):
#   BOT_TOKEN       - токен бота (обязательно)
#   BACKUP_CHAT_ID  - ваш Telegram ID или ID приватного канала для авто-бэкапа базы (желательно)
import asyncio
import logging
import os
import random
import re
import shutil
import time
import uuid
from datetime import datetime, timedelta, timezone
from html import escape as esc, unescape
from io import BytesIO
from urllib.parse import quote

import aiosqlite
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramRetryAfter
from aiogram.filters import BaseFilter, Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (BufferedInputFile, CallbackQuery, FSInputFile,
                           InlineKeyboardButton, InlineKeyboardMarkup,
                           InputMediaPhoto, LinkPreviewOptions, Message,
                           ReplyParameters)

# ───────────────────────── НАСТРОЙКИ ─────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Не задана переменная окружения BOT_TOKEN")

OWNER_USERNAME = "ruxdev"  # владелец (полный доступ всегда), без @
OWNER_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}

# База лежит в постоянной папке данных (на хосте это /app/data)
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except OSError:
    DATA_DIR = "."
DB_PATH = os.path.join(DATA_DIR, "bot.db")
if not os.path.exists(DB_PATH) and os.path.exists("bot.db"):  # перенос старой базы, если она есть
    shutil.copy("bot.db", DB_PATH)

# Авто-бэкап базы в Telegram (бесплатное хранилище)
_bc = os.getenv("BACKUP_CHAT_ID", "").strip()
BACKUP_CHAT_ID = (int(_bc) if _bc.lstrip("-").isdigit() else _bc) if _bc else None
BACKUP_EVERY_MIN = int(os.getenv("BACKUP_EVERY_MIN", "60"))
BACKUP_FILENAME = "bot_backup.db"

START_IMAGE_FILE = "start.jpg"  # запасной способ задать картинку для /start
PER_PAGE = 8        # скриптов на странице
CATS_PER_PAGE = 10  # категорий на странице
TOP_DAYS = 7        # за сколько дней считается «Топ»
COMMENTS_PER_PAGE = 5     # комментариев на странице
COMMENT_MAX = 500         # максимум символов в комментарии
COMMENT_COOLDOWN = 20     # секунд между комментариями одного человека
POST_STYLES = {"": "обычная", "primary": "синяя", "success": "зелёная", "danger": "красная"}
TZ_OFFSET = int(os.getenv("TZ_OFFSET", "3"))  # часовой пояс для розыгрышей (3 = МСК)
TZ = timezone(timedelta(hours=TZ_OFFSET))
TZ_NAME = "МСК" if TZ_OFFSET == 3 else f"UTC{TZ_OFFSET:+d}"

DEFAULTS = {
    "start_text": "<b>Scripts Agent</b> - Твой каталог скриптов для Roblox без подписок на множество каналов!\n\nНачинай прямо сейчас.",
    "btn_search": "ПОИСК",
    "btn_categories": "Категории",
    "btn_top": "Топ",
    "btn_fav": "Избранное",
    "btn_support": "Поддержка",
    "support_text": "<b>Поддержка</b>\n\nЕсли возник вопрос или проблема, напишите нам.",
    "search_prompt": "Введите название скрипта или игры:",
    "sub_text": "Чтобы получить этот скрипт, подпишитесь на каналы ниже и нажмите «Проверить подписку».",
    "banned_text": "Вы заблокированы в этом боте.",
    "post_btn_text": "Открыть бота",
}
HTML_KEYS = {"start_text", "support_text", "search_prompt", "sub_text", "banned_text"}
LABELS = {
    "start_text": "Текст приветствия", "btn_search": "Кнопка «Поиск»",
    "btn_categories": "Кнопка «Категории»", "btn_top": "Кнопка «Топ»",
    "btn_fav": "Кнопка «Избранное»", "btn_support": "Кнопка «Поддержка»",
    "support_text": "Текст поддержки", "support_url": "Ссылка поддержки (@user или URL)",
    "search_prompt": "Текст поиска", "sub_text": "Текст обязательной подписки",
    "banned_text": "Текст для забаненных", "post_channel": "Канал для постов (@канал или ID)",
    "post_btn_text": "Текст кнопки под постом",
}
# Картинки вкладок (экранов бота)
PHOTO_KEYS = {
    "start_photo": "Главная (/start)",
    "img_cats": "Список категорий",
    "img_top": "Топ",
    "img_fav": "Избранное",
    "img_search": "Поиск",
    "img_support": "Поддержка",
    "img_sub": "Экран обязательной подписки",
}

# ───────────────────────── БАЗА ─────────────────────────
db: aiosqlite.Connection
BOT_REF: Bot = None
SCHEMA = """
CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, joined INTEGER, last_active INTEGER, banned INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS categories(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, sub_required INTEGER DEFAULT 0, photo TEXT);
CREATE TABLE IF NOT EXISTS scripts(id INTEGER PRIMARY KEY AUTOINCREMENT, category_id INTEGER, title TEXT, description TEXT, content TEXT, file_id TEXT, sub_required INTEGER DEFAULT 0, views INTEGER DEFAULT 0, created INTEGER, photo TEXT, token TEXT);
CREATE TABLE IF NOT EXISTS channels(id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, title TEXT, url TEXT);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, kind TEXT, ts INTEGER, script_id INTEGER);
CREATE TABLE IF NOT EXISTS admins(user_id INTEGER PRIMARY KEY, username TEXT, level INTEGER NOT NULL, added INTEGER);
CREATE TABLE IF NOT EXISTS favorites(user_id INTEGER, script_id INTEGER, created INTEGER, PRIMARY KEY(user_id, script_id));
CREATE TABLE IF NOT EXISTS votes(user_id INTEGER, script_id INTEGER, value INTEGER, PRIMARY KEY(user_id, script_id));
CREATE TABLE IF NOT EXISTS giveaways(id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, description TEXT DEFAULT '', photo TEXT, winners INTEGER DEFAULT 1, end_ts INTEGER, status TEXT DEFAULT 'draft', channel TEXT, post_msg_id INTEGER, result_msg_id INTEGER, btn_text TEXT DEFAULT 'Участвовать', btn_style TEXT DEFAULT 'success', channels TEXT DEFAULT '', boost_channels TEXT DEFAULT '', min_boosts INTEGER DEFAULT 1, min_views INTEGER DEFAULT 0, extra TEXT DEFAULT '', show_count INTEGER DEFAULT 1, recheck INTEGER DEFAULT 1, no_staff INTEGER DEFAULT 1, win_text TEXT DEFAULT '', admin_id INTEGER, created INTEGER, started INTEGER, finished INTEGER, shown_count INTEGER DEFAULT -1);
CREATE TABLE IF NOT EXISTS gw_users(giveaway_id INTEGER, user_id INTEGER, username TEXT, name TEXT, joined INTEGER, excluded INTEGER DEFAULT 0, PRIMARY KEY(giveaway_id, user_id));
CREATE TABLE IF NOT EXISTS gw_winners(giveaway_id INTEGER, user_id INTEGER, place INTEGER, username TEXT, name TEXT, notified INTEGER DEFAULT 0, PRIMARY KEY(giveaway_id, user_id));
CREATE TABLE IF NOT EXISTS comments(id INTEGER PRIMARY KEY AUTOINCREMENT, script_id INTEGER, user_id INTEGER, username TEXT, name TEXT, text TEXT, created INTEGER);
"""

dirty = False            # были ли изменения с последнего бэкапа
backup_scheduled = False
backup_lock = asyncio.Lock()
bg_tasks: set = set()


def spawn(coro):
    t = asyncio.create_task(coro)
    bg_tasks.add(t)
    t.add_done_callback(bg_tasks.discard)
    return t


async def fetchall(sql, args=()):
    async with db.execute(sql, args) as c:
        return await c.fetchall()


async def fetchone(sql, args=()):
    async with db.execute(sql, args) as c:
        return await c.fetchone()


async def run(sql, args=()):
    global dirty
    cur = await db.execute(sql, args)
    await db.commit()
    low = sql.lower()
    if "into events" not in low and "views=views+1" not in low:
        dirty = True
        # важные изменения (не просто активность пользователей) -> бэкап через 30 секунд
        if not any(k in low for k in ("into users", "favorites", "votes", "comments", "gw_users")):
            schedule_backup(30)
    return cur.lastrowid


async def ensure_column(table, column, decl):
    cols = [r["name"] for r in await fetchall(f"PRAGMA table_info({table})")]
    if column not in cols:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
        await db.commit()


def new_token() -> str:
    return uuid.uuid4().hex  # 32 символа, например 80595032244449d186ba49f39ec7e5f2


async def fill_tokens() -> int:
    """Выдаёт ссылки (токены) всем скриптам, у которых их ещё нет."""
    rows = await fetchall("SELECT id FROM scripts WHERE token IS NULL OR token=''")
    for r in rows:
        await db.execute("UPDATE scripts SET token=? WHERE id=?", (new_token(), r["id"]))
    await db.commit()
    return len(rows)


async def get_setting(key):
    row = await fetchone("SELECT value FROM settings WHERE key=?", (key,))
    return row["value"] if row else DEFAULTS.get(key, "")


async def set_setting(key, value):
    if value == "":
        await run("DELETE FROM settings WHERE key=?", (key,))
    else:
        await run("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))


# ───────────────────────── БЭКАП В TELEGRAM ─────────────────────────
BACKUP_HELP = (
    "<b>Бэкап базы</b>\n\n"
    "Сейчас бэкап <b>выключен</b>. Если хост сотрёт файлы, все скрипты пропадут.\n\n"
    "Как включить (бесплатно):\n"
    "1. Отправьте боту /id и скопируйте свой ID.\n"
    "2. В «Переменных окружения» хоста добавьте <code>BACKUP_CHAT_ID</code> = ваш ID.\n"
    "3. Перезапустите бота.\n\n"
    "Дальше бот сам будет присылать файл базы (и закреплять его в чате с ботом), "
    "а если на хосте база исчезнет, сам её восстановит при запуске."
)


def schedule_backup(delay: int):
    global backup_scheduled
    if not BACKUP_CHAT_ID or BOT_REF is None or backup_scheduled:
        return
    backup_scheduled = True

    async def _later():
        global backup_scheduled
        await asyncio.sleep(delay)
        backup_scheduled = False
        await backup_if_dirty()

    spawn(_later())


async def make_backup(bot: Bot):
    """Делает копию базы и отправляет её в BACKUP_CHAT_ID (с закреплением)."""
    if not BACKUP_CHAT_ID:
        raise RuntimeError("BACKUP_CHAT_ID не задан")
    async with backup_lock:
        tmp = os.path.join(DATA_DIR, "backup_tmp.db")
        if os.path.exists(tmp):
            os.remove(tmp)
        await db.execute("VACUUM INTO '" + tmp.replace("'", "''") + "'")
        old = None
        try:
            old = (await bot.get_chat(BACKUP_CHAT_ID)).pinned_message
        except Exception:
            pass
        stamp = datetime.now().strftime("%d.%m.%Y %H:%M")
        msg = await bot.send_document(
            BACKUP_CHAT_ID, FSInputFile(tmp, filename=BACKUP_FILENAME),
            caption=f"Бэкап базы · {stamp}", disable_notification=True)
        try:
            await bot.pin_chat_message(BACKUP_CHAT_ID, msg.message_id, disable_notification=True)
        except Exception as e:
            logging.warning("Не удалось закрепить бэкап (нужны права закрепления): %s", e)
        doc = getattr(old, "document", None)
        if old and doc and doc.file_name == BACKUP_FILENAME and old.message_id != msg.message_id:
            try:
                await bot.delete_message(BACKUP_CHAT_ID, old.message_id)
            except Exception:
                pass
        try:
            os.remove(tmp)
        except OSError:
            pass


async def backup_if_dirty():
    global dirty
    if not dirty or BOT_REF is None:
        return
    dirty = False
    try:
        await make_backup(BOT_REF)
        logging.info("Бэкап базы отправлен")
    except Exception:
        dirty = True
        logging.exception("Не удалось сделать бэкап")


async def backup_loop():
    while True:
        await asyncio.sleep(BACKUP_EVERY_MIN * 60)
        await backup_if_dirty()


async def try_restore(bot: Bot):
    """Если файла базы нет — пробуем достать последний закреплённый бэкап из Telegram."""
    if os.path.exists(DB_PATH) or not BACKUP_CHAT_ID:
        return
    try:
        pm = (await bot.get_chat(BACKUP_CHAT_ID)).pinned_message
        doc = getattr(pm, "document", None)
        if doc and doc.file_name == BACKUP_FILENAME:
            await bot.download(doc, destination=DB_PATH)
            logging.info("База восстановлена из бэкапа в Telegram")
        else:
            logging.info("Закреплённого бэкапа нет, начинаем с чистой базы")
    except Exception as e:
        logging.warning("Не удалось восстановить базу из бэкапа: %s", e)


# ───────────────────────── ПРАВА (УРОВНИ АДМИНОВ) ─────────────────────────
# 1 - добавление скриптов
# 2 - уровень 1 + активность и бэкап
# 3 - полный доступ (кроме выдачи админок — её делает только владелец)
LEVEL_INFO = {
    1: "добавление скриптов",
    2: "добавление скриптов, активность, бэкап",
    3: "полный доступ",
}


def is_owner(user) -> bool:
    if not user:
        return False
    return user.id in OWNER_IDS or (user.username or "").lower() == OWNER_USERNAME.lower()


async def get_level(user) -> int:
    if not user:
        return 0
    if is_owner(user):
        return 3
    row = await fetchone("SELECT level FROM admins WHERE user_id=?", (user.id,))
    return int(row["level"]) if row else 0


async def is_staff(user_id: int, username) -> bool:
    """Админ любого уровня или владелец (по id и username)."""
    if user_id in OWNER_IDS or (username or "").lower() == OWNER_USERNAME.lower():
        return True
    return bool(await fetchone("SELECT 1 FROM admins WHERE user_id=?", (user_id,)))


class AdminLevel(BaseFilter):
    def __init__(self, need: int):
        self.need = need

    async def __call__(self, event, level: int = None) -> bool:
        if level is None:  # уровень кладёт в данные Track; если вдруг нет — считаем сами
            level = await get_level(event.from_user)
        return level >= self.need


class IsOwner(BaseFilter):
    async def __call__(self, event) -> bool:
        return is_owner(event.from_user)


# ───────────────────────── УТИЛИТЫ ─────────────────────────
def btn(text, cb=None, url=None, style=None):
    """style: 'success' (зелёная) | 'danger' (красная) | 'primary' (синяя) | None (обычная)"""
    kw = {"text": text}
    if cb:
        kw["callback_data"] = cb
    if url:
        kw["url"] = url
    if style:
        kw["style"] = style
    return InlineKeyboardButton(**kw)


def ub(text, url, style=None):
    return btn(text, url=url, style=style)


def kb(rows):
    return InlineKeyboardMarkup(inline_keyboard=rows)


def paginate(items, page, per, make_btn, prefix, cols=1):
    """Кнопки элементов + строка навигации ‹ 2/5 ›. Колбэк страницы: '{prefix}:{номер}'."""
    pages = max(1, -(-len(items) // per))
    page = min(max(page, 0), pages - 1)
    buttons = [make_btn(i) for i in items[page * per:(page + 1) * per]]
    rows = [buttons[i:i + cols] for i in range(0, len(buttons), cols)]
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(btn("‹", f"{prefix}:{page - 1}"))
        nav.append(btn(f"{page + 1}/{pages}", "noop"))
        if page < pages - 1:
            nav.append(btn("›", f"{prefix}:{page + 1}"))
        rows.append(nav)
    return rows


def page_of(data: str) -> int:
    parts = data.split(":")
    return int(parts[-1]) if len(parts) > 1 and parts[-1].isdigit() else 0


async def show(cb: CallbackQuery, text, markup=None, photo=None):
    """Показывает экран: с картинкой (если есть) или без. Сам решает, редактировать сообщение или пересоздать."""
    msg = cb.message
    if photo and len(text) <= 1024:
        if msg.photo:
            try:
                await msg.edit_media(InputMediaPhoto(media=photo, caption=text, parse_mode=ParseMode.HTML),
                                     reply_markup=markup)
                return
            except Exception:
                pass
        try:
            await msg.delete()
        except Exception:
            pass
        try:
            await msg.answer_photo(photo, caption=text, reply_markup=markup)
            return
        except Exception:
            logging.exception("Не удалось отправить картинку")
        await msg.answer(text, reply_markup=markup, disable_web_page_preview=True)
        return
    if not (msg.photo or msg.document):
        try:
            await msg.edit_text(text, reply_markup=markup, disable_web_page_preview=True)
            return
        except Exception:
            pass
    try:
        await msg.delete()
    except Exception:
        pass
    await msg.answer(text, reply_markup=markup, disable_web_page_preview=True)


async def send_screen(msg: Message, text, markup=None, photo=None):
    """То же, что show, но для обычного сообщения (например, при переходе по ссылке)."""
    if photo and len(text) <= 1024:
        try:
            await msg.answer_photo(photo, caption=text, reply_markup=markup)
            return
        except Exception:
            logging.exception("Не удалось отправить картинку")
    await msg.answer(text, reply_markup=markup, disable_web_page_preview=True)


def chat_ref(v: str):
    v = str(v).strip()
    return int(v) if v.lstrip("-").isdigit() else v


async def missing_channels(bot: Bot, user_id: int):
    missing = []
    for ch in await fetchall("SELECT * FROM channels"):
        try:
            m = await bot.get_chat_member(chat_ref(ch["chat_id"]), user_id)
            if m.status in ("left", "kicked"):
                missing.append(ch)
        except Exception as e:
            logging.warning("Не могу проверить канал %s (бот админ в нём?): %s", ch["chat_id"], e)
    return missing


def sub_choice_kb(prefix):
    return kb([[btn("ВКЛ", f"{prefix}:1", style="success"), btn("ВЫКЛ", f"{prefix}:0", style="danger")],
               [btn("Отмена", "adm")]])


async def script_link(bot: Bot, token: str) -> str:
    me = await bot.me()
    return f"https://t.me/{me.username}?start={token}"


# ───────────────────────── MIDDLEWARE ─────────────────────────
class Track(BaseMiddleware):
    async def __call__(self, handler, event, data):
        u = data.get("event_from_user")
        level = 0
        if u:
            now = int(time.time())
            await run(
                "INSERT INTO users(id,username,first_name,joined,last_active) VALUES(?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET username=excluded.username, first_name=excluded.first_name, last_active=excluded.last_active",
                (u.id, u.username, u.first_name, now, now))
            level = await get_level(u)
            if level == 0:
                row = await fetchone("SELECT banned FROM users WHERE id=?", (u.id,))
                if row and row["banned"]:
                    text = await get_setting("banned_text")
                    if isinstance(event, CallbackQuery):
                        await event.answer("Вы заблокированы", show_alert=True)
                    elif isinstance(event, Message):
                        await event.answer(text)
                    return
        data["level"] = level
        return await handler(event, data)


# ───────────────────────── СОСТОЯНИЯ ─────────────────────────
class Search(StatesGroup):
    query = State()


class AddCat(StatesGroup):
    name = State()
    photo = State()
    sub = State()


class AddScript(StatesGroup):
    title = State()
    desc = State()
    content = State()
    photo = State()
    sub = State()


class SetPhoto(StatesGroup):
    photo = State()


class Edit(StatesGroup):
    value = State()


class Bc(StatesGroup):
    msg = State()


class PostS(StatesGroup):
    msg = State()


class Ban(StatesGroup):
    target = State()


class Chan(StatesGroup):
    chat = State()


class BulkScripts(StatesGroup):
    data = State()


class BulkCats(StatesGroup):
    data = State()


class AdminAdd(StatesGroup):
    target = State()


class Comment(StatesGroup):
    text = State()


class GwEdit(StatesGroup):
    value = State()


# ───────────────────────── РОУТЕРЫ ─────────────────────────
cmd_router = Router()
owner = Router()   # только владелец: выдача админок
adm1 = Router()    # уровень 1+
adm2 = Router()    # уровень 2+
adm3 = Router()    # уровень 3
user = Router()
owner.message.filter(IsOwner())
owner.callback_query.filter(IsOwner())
for _r, _lvl in ((adm1, 1), (adm2, 2), (adm3, 3)):
    _r.message.filter(AdminLevel(_lvl))
    _r.callback_query.filter(AdminLevel(_lvl))


# ───────────────────────── /start и команды ─────────────────────────
async def main_kb():
    s = await get_setting("btn_search")
    c = await get_setting("btn_categories")
    t = await get_setting("btn_top")
    f = await get_setting("btn_fav")
    sp = await get_setting("btn_support")
    return kb([
        [btn(s, "search", style="success")],
        [btn(c, "cats"), btn(t, "top")],
        [btn(f, "favs"), btn(sp, "support", style="danger")],
    ])


async def send_start(m: Message):
    text, photo, markup = await get_setting("start_text"), await get_setting("start_photo"), await main_kb()
    if photo:
        try:
            await m.answer_photo(photo, caption=text, reply_markup=markup)
            return
        except Exception:
            logging.exception("Не удалось отправить картинку")
    elif os.path.exists(START_IMAGE_FILE):
        try:
            sent = await m.answer_photo(FSInputFile(START_IMAGE_FILE), caption=text, reply_markup=markup)
            await set_setting("start_photo", sent.photo[-1].file_id)
            return
        except Exception:
            logging.exception("Не удалось отправить start.jpg")
    await m.answer(text, reply_markup=markup)


async def akb(user_obj):
    """Главная админ-панель с кнопками по уровню доступа."""
    lvl = await get_level(user_obj)
    rows = [[btn("Скрипты", "adm_scripts")]]
    if lvl >= 2:
        rows.append([btn("Активность", "adm_stats"), btn("Бэкап", "adm_backup")])
    if lvl >= 3:
        rows += [
            [btn("Категории", "adm_cats"), btn("Пост в канал", "adm_post")],
            [btn("Рассылка", "adm_bc"), btn("Розыгрыши", "gw_list")],
            [btn("Бан", "adm_ban"), btn("Разбан", "adm_unban")],
            [btn("Дизайн", "adm_design"), btn("Картинки вкладок", "adm_imgs")],
            [btn("Тексты", "adm_texts"), btn("Обяз. подписка", "adm_subs")],
        ]
    if is_owner(user_obj):
        rows.append([btn("Администраторы", "adm_admins")])
    rows.append([btn("Закрыть", "adm_close", style="danger")])
    return kb(rows)


def panel_title(level: int) -> str:
    return f"<b>Админ-панель</b>\nУровень доступа: {level}"


# Ссылка вида /start <токен> открывает конкретный скрипт (регистрируем раньше обычного /start)
@cmd_router.message(CommandStart(deep_link=True))
async def cmd_start_link(m: Message, command: CommandObject, state: FSMContext):
    await state.clear()
    token = (command.args or "").strip()
    if token.startswith("gw_") and token[3:].isdigit():  # участие в розыгрыше
        await join_flow(m, m.from_user, int(token[3:]))
        return
    s = await fetchone("SELECT id FROM scripts WHERE token=?", (token,)) if token else None
    if not s:
        await send_start(m)
        return
    await open_script(m, m.from_user.id, s["id"])


@cmd_router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    await send_start(m)


@cmd_router.message(Command("cancel"))
async def cmd_cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Отменено.")


@cmd_router.message(Command("id"))
async def cmd_id(m: Message):
    await m.answer(f"Ваш ID: <code>{m.from_user.id}</code>")


@cmd_router.message(Command("admin"))
async def cmd_admin(m: Message, state: FSMContext):
    lvl = await get_level(m.from_user)
    if lvl == 0:
        return
    await state.clear()
    await m.answer(panel_title(lvl), reply_markup=await akb(m.from_user))


# ───────────────────────── ПОЛЬЗОВАТЕЛЬ ─────────────────────────
@user.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await cb.answer()


@user.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await cb.message.delete()
    except Exception:
        pass
    await send_start(cb.message)
    await cb.answer()


@user.callback_query(F.data == "support")
async def cb_support(cb: CallbackQuery):
    url = await get_setting("support_url")
    rows = []
    if url:
        rows.append([ub("Написать в поддержку", url, style="danger")])
    rows.append([btn("Назад", "menu")])
    await show(cb, await get_setting("support_text"), kb(rows), await get_setting("img_support"))
    await cb.answer()


@user.callback_query(F.data == "search")
async def cb_search(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Search.query)
    await show(cb, await get_setting("search_prompt"), kb([[btn("Назад", "menu")]]), await get_setting("img_search"))
    await cb.answer()


async def render_search(q: str, page: int):
    like = f"%{q.lower()}%"
    rows = await fetchall(
        "SELECT s.id, s.title, c.name FROM scripts s LEFT JOIN categories c ON c.id=s.category_id "
        "WHERE pylower(s.title) LIKE ? OR pylower(s.description) LIKE ? OR pylower(c.name) LIKE ? "
        "ORDER BY s.views DESC LIMIT 200", (like, like, like))
    if not rows:
        return "Ничего не найдено. Попробуйте другой запрос.", kb([[btn("Назад", "menu")]])
    buttons = paginate(rows, page, PER_PAGE,
                       lambda r: btn(f"{r['title']} - {r['name']}" if r["name"] else r["title"], f"scr:{r['id']}"), "sp")
    buttons.append([btn("Назад", "menu")])
    return f"Найдено: {len(rows)}", kb(buttons)


@user.message(Search.query, F.text)
async def do_search(m: Message, state: FSMContext):
    q = m.text.strip()
    await run("INSERT INTO events(user_id,kind,ts) VALUES(?,?,?)", (m.from_user.id, "search", int(time.time())))
    await state.update_data(q=q)
    text, markup = await render_search(q, 0)
    await m.answer(text, reply_markup=markup)


@user.callback_query(F.data.regexp(r"^sp:\d+$"))
async def cb_search_page(cb: CallbackQuery, state: FSMContext):
    q = (await state.get_data()).get("q")
    if not q:
        return await cb.answer("Повторите поиск", show_alert=True)
    text, markup = await render_search(q, page_of(cb.data))
    await show(cb, text, markup)
    await cb.answer()


@user.callback_query(F.data.regexp(r"^cats(:\d+)?$"))
async def cb_cats(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    cats = await fetchall("SELECT * FROM categories ORDER BY name")
    photo = await get_setting("img_cats")
    if not cats:
        await show(cb, "Категорий пока нет.", kb([[btn("Назад", "menu")]]), photo)
        return await cb.answer()
    rows = paginate(cats, page_of(cb.data), CATS_PER_PAGE,
                    lambda c: btn(c["name"], f"cat:{c['id']}:0"), "cats", cols=2)
    rows.append([btn("Назад", "menu")])
    await show(cb, "<b>Выберите игру</b>", kb(rows), photo)
    await cb.answer()


@user.callback_query(F.data.regexp(r"^cat:\d+:\d+$"))
async def cb_cat(cb: CallbackQuery):
    _, cid, page = cb.data.split(":")
    cid, page = int(cid), int(page)
    cat = await fetchone("SELECT * FROM categories WHERE id=?", (cid,))
    if not cat:
        return await cb.answer("Категория не найдена", show_alert=True)
    items = await fetchall("SELECT id,title FROM scripts WHERE category_id=? ORDER BY id DESC", (cid,))
    rows = paginate(items, page, PER_PAGE, lambda r: btn(r["title"], f"scr:{r['id']}"), f"cat:{cid}")
    rows.append([btn("Назад", "cats")])
    text = f"<b>{esc(cat['name'])}</b>\nСкриптов: {len(items)}" if items else f"<b>{esc(cat['name'])}</b>\n\nПока пусто."
    await show(cb, text, kb(rows), cat["photo"])
    await cb.answer()


# ───── Топ ─────
@user.callback_query(F.data == "top")
async def cb_top(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    since = int(time.time()) - TOP_DAYS * 86400
    rows = await fetchall(
        "SELECT s.id, s.title, s.views, c.name AS cat, "
        "(SELECT COUNT(*) FROM events e WHERE e.script_id=s.id AND e.kind='view' AND e.ts>=?) AS recent "
        "FROM scripts s LEFT JOIN categories c ON c.id=s.category_id "
        "ORDER BY recent DESC, s.views DESC, s.id DESC LIMIT 3", (since,))
    photo = await get_setting("img_top")
    if not rows:
        await show(cb, "Скриптов пока нет.", kb([[btn("Назад", "menu")]]), photo)
        return await cb.answer()
    # свежая статистика есть, только когда за период открывали минимум 3 разных скрипта
    fresh = sum(1 for r in rows if r["recent"] > 0) >= 3
    period = f"за последние {TOP_DAYS} дней" if fresh else "за всё время"
    lines = []
    for i, r in enumerate(rows, 1):
        n = r["recent"] if fresh else r["views"]
        cat = f" ({esc(r['cat'])})" if r["cat"] else ""
        lines.append(f"{i}. <b>{esc(r['title'])}</b>{cat} - открытий: {n}")
    text = f"<b>Топ-3 сейчас</b>\n{period}\n\n" + "\n".join(lines)
    buttons = [[btn(r["title"], f"sct:{r['id']}")] for r in rows]
    buttons.append([btn("Назад", "menu")])
    await show(cb, text, kb(buttons), photo)
    await cb.answer()


# ───── Избранное ─────
@user.callback_query(F.data.regexp(r"^favs(:\d+)?$"))
async def cb_favs(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    items = await fetchall(
        "SELECT s.id, s.title FROM favorites f JOIN scripts s ON s.id=f.script_id "
        "WHERE f.user_id=? ORDER BY f.created DESC", (cb.from_user.id,))
    photo = await get_setting("img_fav")
    if not items:
        await show(cb, "<b>Избранное</b>\n\nПока пусто. Откройте скрипт и нажмите «В избранное».",
                   kb([[btn("Назад", "menu")]]), photo)
        return await cb.answer()
    rows = paginate(items, page_of(cb.data), PER_PAGE, lambda r: btn(r["title"], f"scf:{r['id']}"), "favs")
    rows.append([btn("Назад", "menu")])
    await show(cb, f"<b>Избранное</b>\nСкриптов: {len(items)}", kb(rows), photo)
    await cb.answer()


@user.callback_query(F.data.regexp(r"^fav:\d+:[cft]$"))
async def cb_fav(cb: CallbackQuery):
    _, sid, src = cb.data.split(":")
    sid = int(sid)
    s = await fetchone("SELECT * FROM scripts WHERE id=?", (sid,))
    if not s:
        return await cb.answer("Скрипт не найден", show_alert=True)
    uid = cb.from_user.id
    if await fetchone("SELECT 1 FROM favorites WHERE user_id=? AND script_id=?", (uid, sid)):
        await run("DELETE FROM favorites WHERE user_id=? AND script_id=?", (uid, sid))
        note = "Убрано из избранного"
    else:
        await run("INSERT OR IGNORE INTO favorites(user_id,script_id,created) VALUES(?,?,?)", (uid, sid, int(time.time())))
        note = "Добавлено в избранное"
    try:
        await cb.message.edit_reply_markup(reply_markup=await script_keyboard(cb.bot, s, src, uid))
    except Exception:
        pass
    await cb.answer(note)


# ───── Открытие скрипта ─────
def back_for(src: str, category_id) -> str:
    if src == "f":
        return "favs"
    if src == "t":
        return "top"
    return f"cat:{category_id}:0"


async def script_keyboard(bot: Bot, s, src: str, user_id: int):
    sid = s["id"]
    fav = bool(await fetchone("SELECT 1 FROM favorites WHERE user_id=? AND script_id=?", (user_id, sid)))
    v = await fetchone("SELECT COALESCE(SUM(value=1),0) up, COALESCE(SUM(value=-1),0) down FROM votes WHERE script_id=?", (sid,))
    mine = await fetchone("SELECT value FROM votes WHERE user_id=? AND script_id=?", (user_id, sid))
    n_comments = (await fetchone("SELECT COUNT(*) n FROM comments WHERE script_id=?", (sid,)))["n"]
    link = await script_link(bot, s["token"])
    share = "https://t.me/share/url?url=" + quote(link, safe="") + "&text=" + quote(f"{s['title']} - скрипт для Roblox")
    up, down = v["up"], v["down"]
    mv = mine["value"] if mine else 0
    return kb([
        [btn(f"👍 {up}" if up else "👍", f"vote:{sid}:u:{src}", style="success" if mv == 1 else None),
         btn(f"👎 {down}" if down else "👎", f"vote:{sid}:d:{src}", style="danger" if mv == -1 else None)],
        [btn(f"Комментарии ({n_comments})" if n_comments else "Комментарии", f"cmo:{sid}")],
        [btn("Убрать из избранного" if fav else "В избранное", f"fav:{sid}:{src}")],
        [ub("Поделиться скриптом", share)],
        [btn("Назад", back_for(src, s["category_id"]))],
    ])


async def open_script(msg: Message, user_id: int, sid: int, src: str = "c", cb: CallbackQuery = None):
    """Открывает скрипт: из кнопки (cb) или из ссылки /start (cb=None)."""
    s = await fetchone(
        "SELECT s.*, c.sub_required AS csub FROM scripts s LEFT JOIN categories c ON c.id=s.category_id WHERE s.id=?", (sid,))
    if not s:
        if cb:
            await cb.answer("Скрипт не найден", show_alert=True)
        else:
            await msg.answer("Скрипт не найден.")
        return
    bot = msg.bot
    back_data = back_for(src, s["category_id"])
    if s["sub_required"] or s["csub"]:
        missing = await missing_channels(bot, user_id)
        if missing:
            rows = [[ub(ch["title"], ch["url"])] for ch in missing if ch["url"]]
            rows.append([btn("Проверить подписку", f"chk:{sid}", style="success")])
            rows.append([btn("Назад", back_data)])
            text, photo = await get_setting("sub_text"), await get_setting("img_sub")
            if cb:
                await show(cb, text, kb(rows), photo)
                await cb.answer()
            else:
                await send_screen(msg, text, kb(rows), photo)
            return
    await run("UPDATE scripts SET views=views+1 WHERE id=?", (sid,))
    await run("INSERT INTO events(user_id,kind,ts,script_id) VALUES(?,?,?,?)", (user_id, "view", int(time.time()), sid))

    title, desc = s["title"], s["description"] or ""
    content = s["content"] or ""
    photo = s["photo"]
    head = f"<b>{esc(title)}</b>" + (f"\n\n{esc(desc)}" if desc else "")
    code_html = f"<pre>{esc(content)}</pre>" if content else ""
    # длина «видимого» текста (Telegram считает подпись к фото без тегов): лимит подписи 1024
    head_len = len(title) + (2 + len(desc) if desc else 0)
    full_len = head_len + (2 + len(content) if content else 0)

    senders, as_file = [], None
    if photo and full_len <= 1024:
        # всё помещается в подпись: картинка, название, описание и код в ОДНОМ сообщении
        full_html = head + (("\n\n" + code_html) if code_html else "")
        senders.append(lambda mk: msg.answer_photo(photo, caption=full_html, reply_markup=mk))
    else:
        code_inline = bool(content) and full_len < 3800
        if content and not code_inline:
            as_file = BufferedInputFile(content.encode("utf-8"), filename="script.lua")
        if photo and head_len <= 1024:
            senders.append(lambda mk: msg.answer_photo(photo, caption=head, reply_markup=mk))
            body = code_html if code_inline else ""
        elif photo:
            senders.append(lambda mk: msg.answer_photo(photo, reply_markup=mk))
            body = head + (("\n\n" + code_html) if code_inline else "")
        else:
            body = head + (("\n\n" + code_html) if code_inline else "")
        if body:
            senders.append(lambda mk: msg.answer(body, reply_markup=mk))
    if as_file:
        senders.append(lambda mk: msg.answer_document(as_file, reply_markup=mk))
    if s["file_id"]:
        senders.append(lambda mk: msg.answer_document(s["file_id"], reply_markup=mk))
    markup = await script_keyboard(bot, s, src, user_id)
    for i, send in enumerate(senders):
        await send(markup if i == len(senders) - 1 else None)  # кнопки — на последнем сообщении
    if cb:
        await cb.answer()


# ───── Лайки / дизлайки ─────
@user.callback_query(F.data.regexp(r"^vote:\d+:[ud]:[cft]$"))
async def cb_vote(cb: CallbackQuery):
    _, sid, kind, src = cb.data.split(":")
    sid, val = int(sid), (1 if kind == "u" else -1)
    s = await fetchone("SELECT * FROM scripts WHERE id=?", (sid,))
    if not s:
        return await cb.answer("Скрипт не найден", show_alert=True)
    uid = cb.from_user.id
    cur = await fetchone("SELECT value FROM votes WHERE user_id=? AND script_id=?", (uid, sid))
    if cur and cur["value"] == val:
        await run("DELETE FROM votes WHERE user_id=? AND script_id=?", (uid, sid))
        note = "Оценка снята"
    else:
        await run("INSERT INTO votes(user_id,script_id,value) VALUES(?,?,?) "
                  "ON CONFLICT(user_id,script_id) DO UPDATE SET value=excluded.value", (uid, sid, val))
        note = "Спасибо за оценку"
    try:
        await cb.message.edit_reply_markup(reply_markup=await script_keyboard(cb.bot, s, src, uid))
    except Exception:
        pass
    await cb.answer(note)


# ───── Комментарии ─────
def fmt_date(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%d.%m %H:%M")


async def render_comments(sid: int, page: int, can_delete: bool):
    s = await fetchone("SELECT title FROM scripts WHERE id=?", (sid,))
    if not s:
        return None, None
    items = await fetchall("SELECT * FROM comments WHERE script_id=? ORDER BY id DESC", (sid,))
    pages = max(1, -(-len(items) // COMMENTS_PER_PAGE))
    page = min(max(page, 0), pages - 1)
    chunk = items[page * COMMENTS_PER_PAGE:(page + 1) * COMMENTS_PER_PAGE]
    lines = [f"<b>{esc(c['name'] or 'Пользователь')}</b>, {fmt_date(c['created'])}\n{esc(c['text'])}" for c in chunk]
    text = (f"<b>Комментарии: {esc(s['title'])}</b>\nВсего: {len(items)}\n\n"
            + ("\n\n".join(lines) if lines else "Пока нет комментариев. Будьте первым."))
    rows = []
    if can_delete:
        rows += [[btn(f"Удалить #{c['id']} ({(c['name'] or '')[:15]})", f"cmd:{c['id']}:{sid}:{page}", style="danger")] for c in chunk]
    rows.append([btn("Написать комментарий", f"cmw:{sid}", style="primary")])
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(btn("‹", f"cmp:{sid}:{page - 1}"))
        nav.append(btn(f"{page + 1}/{pages}", "noop"))
        if page < pages - 1:
            nav.append(btn("›", f"cmp:{sid}:{page + 1}"))
        rows.append(nav)
    rows.append([btn("Закрыть", "cmx")])
    return text, kb(rows)


@user.callback_query(F.data.regexp(r"^cmo:\d+$"))
async def cb_comments_open(cb: CallbackQuery):
    """Комментарии открываются НОВЫМ сообщением, чтобы скрипт выше остался на месте."""
    sid = int(cb.data.split(":")[1])
    text, markup = await render_comments(sid, 0, await get_level(cb.from_user) >= 3)
    if text is None:
        return await cb.answer("Скрипт не найден", show_alert=True)
    await cb.message.answer(text, reply_markup=markup)
    await cb.answer()


@user.callback_query(F.data.regexp(r"^cmp:\d+:\d+$"))
async def cb_comments_page(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    _, sid, page = cb.data.split(":")
    text, markup = await render_comments(int(sid), int(page), await get_level(cb.from_user) >= 3)
    if text is None:
        return await cb.answer("Скрипт не найден", show_alert=True)
    await show(cb, text, markup)
    await cb.answer()


@user.callback_query(F.data == "cmx")
async def cb_comments_close(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await cb.message.delete()
    except Exception:
        pass
    await cb.answer()


@user.callback_query(F.data.regexp(r"^cmw:\d+$"))
async def cb_comment_write(cb: CallbackQuery, state: FSMContext):
    sid = int(cb.data.split(":")[1])
    await state.set_state(Comment.text)
    await state.update_data(sid=sid, pmid=cb.message.message_id)
    await show(cb, f"Напишите комментарий одним сообщением (до {COMMENT_MAX} символов).",
               kb([[btn("Отмена", f"cmp:{sid}:0")]]))
    await cb.answer()


@user.message(Comment.text, F.text)
async def comment_save(m: Message, state: FSMContext):
    d = await state.get_data()
    sid = d.get("sid")
    text = m.text.strip()
    if not sid or not text:
        return
    staff = await get_level(m.from_user) > 0
    if len(text) > COMMENT_MAX:
        return await m.answer(f"Слишком длинно: максимум {COMMENT_MAX} символов, у вас {len(text)}. Сократите и отправьте снова.")
    low = text.lower()
    if not staff and any(k in low for k in ("http", "t.me", "www.")):
        return await m.answer("Ссылки в комментариях запрещены. Напишите комментарий без ссылок.")
    now = int(time.time())
    last = await fetchone("SELECT MAX(created) t FROM comments WHERE user_id=?", (m.from_user.id,))
    if last and last["t"] and now - last["t"] < COMMENT_COOLDOWN:
        return await m.answer("Подождите немного перед следующим комментарием.")
    if not await fetchone("SELECT 1 FROM scripts WHERE id=?", (sid,)):
        await state.clear()
        return await m.answer("Скрипт уже удалён.")
    name = (m.from_user.first_name or "Пользователь")[:30]
    await run("INSERT INTO comments(script_id,user_id,username,name,text,created) VALUES(?,?,?,?,?,?)",
              (sid, m.from_user.id, m.from_user.username, name, text, now))
    await state.clear()
    if d.get("pmid"):
        try:
            await m.bot.delete_message(m.chat.id, d["pmid"])
        except Exception:
            pass
    out, markup = await render_comments(sid, 0, staff and await get_level(m.from_user) >= 3)
    await m.answer(out, reply_markup=markup)


@user.message(Comment.text)
async def comment_not_text(m: Message):
    await m.answer("Комментарий нужно отправить текстом.")


@user.callback_query(F.data.regexp(r"^(scr|scf|sct):\d+$"))
async def cb_scr(cb: CallbackQuery):
    kind, sid = cb.data.split(":")
    src = {"scr": "c", "scf": "f", "sct": "t"}[kind]
    await open_script(cb.message, cb.from_user.id, int(sid), src, cb)


@user.callback_query(F.data.regexp(r"^chk:\d+$"))
async def cb_chk(cb: CallbackQuery):
    sid = int(cb.data.split(":")[1])
    if await missing_channels(cb.bot, cb.from_user.id):
        return await cb.answer("Вы подписались не на все каналы", show_alert=True)
    try:
        await cb.message.delete()
    except Exception:
        pass
    await open_script(cb.message, cb.from_user.id, sid, "c", cb)


# ═════════════════════════ АДМИНКА: УРОВЕНЬ 1+ ═════════════════════════
@adm1.callback_query(F.data == "adm")
async def adm_home(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await show(cb, panel_title(await get_level(cb.from_user)), await akb(cb.from_user))
    await cb.answer()


@adm1.callback_query(F.data == "adm_close")
async def adm_close(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await cb.message.delete()
    except Exception:
        pass
    await cb.answer()


async def ask(cb: CallbackQuery, state: FSMContext, st, text, data=None):
    await state.set_state(st)
    if data:
        await state.update_data(**data)
    await show(cb, text, kb([[btn("Отмена", "adm")]]))
    await cb.answer()


@adm1.callback_query(F.data == "adm_scripts")
async def adm_scripts(cb: CallbackQuery):
    lvl = await get_level(cb.from_user)
    rows = [[btn("Добавить скрипт", "ascr_add", style="success")],
            [btn("Массовая заливка", "bulk_scr", style="primary")]]
    if lvl >= 3:
        rows.append([btn("Управление скриптами", "ascr_list")])
    rows.append([btn("Назад", "adm")])
    await show(cb, "<b>Скрипты</b>", kb(rows))
    await cb.answer()


# — добавление одного скрипта —
@adm1.callback_query(F.data.regexp(r"^ascr_add(:\d+)?$"))
async def ascr_add(cb: CallbackQuery):
    cats = await fetchall("SELECT * FROM categories ORDER BY name")
    if not cats:
        return await cb.answer("Сначала нужно создать категорию (это делает администратор уровня 3)", show_alert=True)
    rows = paginate(cats, page_of(cb.data) if ":" in cb.data else 0, CATS_PER_PAGE,
                    lambda c: btn(c["name"], f"ascat:{c['id']}"), "ascr_add")
    rows.append([btn("Назад", "adm_scripts")])
    await show(cb, "Выберите категорию для скрипта:", kb(rows))
    await cb.answer()


@adm1.callback_query(F.data.regexp(r"^ascat:\d+$"))
async def ascr_cat(cb: CallbackQuery, state: FSMContext):
    await ask(cb, state, AddScript.title, "Введите <b>название</b> скрипта:", {"cid": int(cb.data.split(":")[1])})


@adm1.message(AddScript.title, F.text)
async def ascr_title(m: Message, state: FSMContext):
    await state.update_data(title=m.text.strip())
    await state.set_state(AddScript.desc)
    await m.answer("Введите <b>описание</b> (или «-» чтобы пропустить):")


@adm1.message(AddScript.desc, F.text)
async def ascr_desc(m: Message, state: FSMContext):
    await state.update_data(desc="" if m.text.strip() == "-" else m.text.strip())
    await state.set_state(AddScript.content)
    await m.answer("Отправьте <b>сам скрипт</b>: текстом (код) или файлом (.lua/.txt).\n"
                   "Длинные скрипты лучше файлом.")


@adm1.message(AddScript.content, F.text | F.document)
async def ascr_content(m: Message, state: FSMContext):
    if m.document:
        await state.update_data(file_id=m.document.file_id, content=None)
    else:
        await state.update_data(content=m.text, file_id=None)
    await state.set_state(AddScript.photo)
    await m.answer("Отправьте <b>картинку</b> для скрипта (как фото) или «-», чтобы пропустить:")


@adm1.message(AddScript.photo, F.photo | F.text)
async def ascr_photo(m: Message, state: FSMContext):
    if m.photo:
        await state.update_data(photo=m.photo[-1].file_id)
    elif (m.text or "").strip() == "-":
        await state.update_data(photo=None)
    else:
        return await m.answer("Отправьте картинку (как фото) или «-».")
    await state.set_state(AddScript.sub)
    await m.answer("Обязательная подписка для получения этого скрипта?", reply_markup=sub_choice_kb("asub"))


@adm1.callback_query(F.data.regexp(r"^asub:[01]$"))
async def ascr_sub(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    if "title" not in d:
        return await cb.answer()
    token = new_token()
    await run("INSERT INTO scripts(category_id,title,description,content,file_id,sub_required,created,photo,token) VALUES(?,?,?,?,?,?,?,?,?)",
              (d["cid"], d["title"], d.get("desc", ""), d.get("content"), d.get("file_id"),
               int(cb.data.split(":")[1]), int(time.time()), d.get("photo"), token))
    await state.clear()
    link = await script_link(cb.bot, token)
    await show(cb, f"Скрипт «{esc(d['title'])}» добавлен.\n\nСсылка на скрипт:\n<code>{link}</code>",
               kb([[btn("В админку", "adm")]]))
    await cb.answer()


# — массовая заливка скриптов —
SEP = "/n"


def bulk_scr_help(level: int) -> str:
    cat_rule = ("• Категории нет в боте: создам автоматически\n" if level >= 3
                else "• Категория должна уже существовать в боте\n")
    return (
        "<b>Массовая заливка скриптов</b>\n\n"
        "Каждый скрипт с новой строки, поля разделяются <code>/n</code>:\n\n"
        "<code>название /n описание /n скрипт /n категория</code>\n\n"
        "Пример:\n"
        "<code>Auto Farm /n Автофарм монет /n loadstring(game:HttpGet(\"https://site.com/a.lua\"))() /n Blox Fruits\n"
        "Speed Hub /n - /n print(\"hello\") /n Pet Simulator 99</code>\n\n"
        "• Нет описания: поставьте «-»\n"
        + cat_rule +
        "• Сам скрипт может быть на нескольких строках, но внутри него не должно быть текста «/n»\n"
        "• Много скриптов: отправьте <b>.txt файлом</b> (сообщение в Telegram ограничено 4096 символами). "
        "Можно слать и несколькими сообщениями, главное, чтобы каждая запись была целиком в одном.\n"
        "• Картинки при массовой заливке не добавляются.\n"
        "• После заливки пришлю файл со ссылками на добавленные скрипты.\n\n"
        "Обязательную подписку для загружаемых скриптов включайте кнопкой ниже <b>до</b> отправки."
    )


BULK_CATS_HELP = (
    "<b>Массовое добавление категорий</b>\n\n"
    "Отправьте названия игр, <b>каждое с новой строки</b> (текстом или .txt файлом):\n\n"
    "<code>Blox Fruits\nPet Simulator 99\nBlade Ball</code>\n\n"
    "Уже существующие категории пропущу. Обязательную подписку для новых категорий включайте кнопкой ниже <b>до</b> отправки."
)


def bulk_kb(kind: str, sub: int):
    done = "adm_scripts" if kind == "scr" else "adm_cats"
    return kb([
        [btn(f"Обяз. подписка: {'ВКЛ' if sub else 'ВЫКЛ'}", f"bulkt:{kind}")],
        [btn("Готово", done, style="success")],
    ])


def parse_bulk(text: str):
    """Разбирает текст в записи: название /n описание /n скрипт /n категория.
    Запись заканчивается на строке, где встретился третий разделитель /n (категория идёт после него)."""
    text = text.replace("\ufeff", "").replace("\r\n", "\n").replace("\r", "\n")
    records, errors = [], []
    buf = ""
    for line in text.split("\n"):
        if not buf and not line.strip():
            continue
        buf = f"{buf}\n{line}" if buf else line
        if buf.count(SEP) >= 3:
            parts = buf.split(SEP)
            name = parts[0].strip()
            desc = parts[1].strip()
            category = parts[-1].strip()
            script = SEP.join(parts[2:-1]).strip()
            if desc == "-":
                desc = ""
            if not name:
                errors.append("пустое название")
            elif not category:
                errors.append(f"«{name}»: не указана категория")
            elif not script:
                errors.append(f"«{name}»: пустой скрипт")
            else:
                records.append({"title": name, "desc": desc, "content": script, "category": category})
            buf = ""
    if buf.strip():
        errors.append(f"«{buf.strip()[:40]}»: запись неполная (нужно 3 разделителя /n)")
    return records, errors


async def read_text(m: Message):
    """Текст из сообщения или из .txt файла."""
    if m.document:
        if (m.document.file_size or 0) > 5 * 1024 * 1024:
            await m.answer("Файл слишком большой (максимум 5 МБ).")
            return None
        buf = BytesIO()
        await m.bot.download(m.document, destination=buf)
        raw = buf.getvalue()
        for enc in ("utf-8-sig", "cp1251"):
            try:
                return raw.decode(enc)
            except UnicodeDecodeError:
                pass
        await m.answer("Не удалось прочитать файл. Нужен текстовый файл (.txt) в кодировке UTF-8.")
        return None
    return m.text


async def resolve_category(name: str, sub: int, cache: dict, created: list, allow_new: bool):
    key = name.lower()
    if key in cache:
        return cache[key]
    row = await fetchone("SELECT id FROM categories WHERE pylower(name)=?", (key,))
    if row:
        cid = row["id"]
    elif allow_new:
        cid = await run("INSERT INTO categories(name,sub_required) VALUES(?,?)", (name, sub))
        created.append(name)
    else:
        return None
    cache[key] = cid
    return cid


@adm1.callback_query(F.data == "bulk_scr")
async def bulk_scr(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(BulkScripts.data)
    await state.update_data(sub=0)
    await show(cb, bulk_scr_help(await get_level(cb.from_user)), bulk_kb("scr", 0))
    await cb.answer()


async def toggle_bulk(cb: CallbackQuery, state: FSMContext, kind: str):
    need = BulkScripts.data if kind == "scr" else BulkCats.data
    if await state.get_state() != need.state:
        return await cb.answer("Сессия закончилась, откройте заливку заново", show_alert=True)
    sub = 0 if (await state.get_data()).get("sub") else 1
    await state.update_data(sub=sub)
    try:
        await cb.message.edit_reply_markup(reply_markup=bulk_kb(kind, sub))
    except Exception:
        pass
    await cb.answer("Подписка: " + ("ВКЛ" if sub else "ВЫКЛ"))


@adm1.callback_query(F.data == "bulkt:scr")
async def bulkt_scr(cb: CallbackQuery, state: FSMContext):
    await toggle_bulk(cb, state, "scr")


@adm1.message(BulkScripts.data, F.text | F.document)
async def bulk_scr_msg(m: Message, state: FSMContext):
    text = await read_text(m)
    if text is None:
        return
    allow_new = await get_level(m.from_user) >= 3
    sub = int((await state.get_data()).get("sub", 0))
    records, errors = parse_bulk(text)
    cache, created, links = {}, [], []
    for r in records:
        cid = await resolve_category(r["category"], 0, cache, created, allow_new)
        if cid is None:
            errors.append(f"«{r['title']}»: категории «{r['category']}» нет в боте")
            continue
        token = new_token()
        await run(
            "INSERT INTO scripts(category_id,title,description,content,file_id,sub_required,created,token) VALUES(?,?,?,?,?,?,?,?)",
            (cid, r["title"], r["desc"], r["content"], None, sub, int(time.time()), token))
        links.append(f"{r['title']} - {await script_link(m.bot, token)}")
    msg = f"Добавлено скриптов: <b>{len(links)}</b>"
    if created:
        msg += f"\nНовые категории ({len(created)}): " + esc(", ".join(created))
    if errors:
        msg += f"\n\nПропущено записей: <b>{len(errors)}</b>\n" + "\n".join("• " + esc(e) for e in errors[:10])
        if len(errors) > 10:
            msg += f"\n… и ещё {len(errors) - 10}"
    msg += "\n\nМожно отправить ещё или нажать «Готово»."
    await m.answer(msg, reply_markup=bulk_kb("scr", sub))
    if links:
        await m.answer_document(BufferedInputFile("\n".join(links).encode("utf-8"), filename="links.txt"),
                                caption="Ссылки на добавленные скрипты")


# ═════════════════════════ АДМИНКА: УРОВЕНЬ 2+ (активность, бэкап) ═════════════════════════
@adm2.callback_query(F.data == "adm_stats")
async def adm_stats(cb: CallbackQuery):
    now = int(time.time())

    async def n(sql, args=()):
        return (await fetchone(sql, args))["n"]

    total = await n("SELECT COUNT(*) n FROM users")
    d1 = await n("SELECT COUNT(*) n FROM users WHERE last_active>=?", (now - 86400,))
    d7 = await n("SELECT COUNT(*) n FROM users WHERE last_active>=?", (now - 7 * 86400,))
    new1 = await n("SELECT COUNT(*) n FROM users WHERE joined>=?", (now - 86400,))
    banned = await n("SELECT COUNT(*) n FROM users WHERE banned=1")
    cats = await n("SELECT COUNT(*) n FROM categories")
    scr = await n("SELECT COUNT(*) n FROM scripts")
    searches = await n("SELECT COUNT(*) n FROM events WHERE kind='search'")
    views = await n("SELECT COUNT(*) n FROM events WHERE kind='view'")
    favs = await n("SELECT COUNT(*) n FROM favorites")
    likes = await n("SELECT COUNT(*) n FROM votes WHERE value=1")
    dislikes = await n("SELECT COUNT(*) n FROM votes WHERE value=-1")
    n_comments = await n("SELECT COUNT(*) n FROM comments")
    top = await fetchall("SELECT title,views FROM scripts ORDER BY views DESC LIMIT 5")
    top_txt = "\n".join(f"{i}. {esc(t['title'])} - {t['views']}" for i, t in enumerate(top, 1)) or "-"
    text = (f"<b>Активность</b>\n\nВсего пользователей: {total}\nНовых за 24ч: {new1}\n"
            f"Активных за 24ч: {d1}\nАктивных за 7 дней: {d7}\nЗабанено: {banned}\n\n"
            f"Категорий: {cats}\nСкриптов: {scr}\nПоисков: {searches}\nОткрытий скриптов: {views}\n"
            f"В избранном: {favs}\nЛайков: {likes}, дизлайков: {dislikes}\nКомментариев: {n_comments}\n\n"
            f"<b>Топ скриптов:</b>\n{top_txt}")
    await show(cb, text, kb([[btn("Обновить", "adm_stats")], [btn("Назад", "adm")]]))
    await cb.answer()


@adm2.callback_query(F.data == "adm_backup")
async def adm_backup(cb: CallbackQuery):
    if BACKUP_CHAT_ID:
        text = ("<b>Бэкап базы</b>\n\nАвто-бэкап включён: бот отправляет файл базы в чат бэкапов "
                "после изменений и раз в час.\n\nЭкспорт скриптов: текстовый файл, который можно загрузить обратно через массовую заливку.")
        rows = [[btn("Сделать бэкап сейчас", "adm_backup_now", style="success")],
                [btn("Экспорт скриптов (txt)", "adm_export")], [btn("Назад", "adm")]]
    else:
        text = BACKUP_HELP
        rows = [[btn("Экспорт скриптов (txt)", "adm_export")], [btn("Назад", "adm")]]
    await show(cb, text, kb(rows))
    await cb.answer()


@adm2.callback_query(F.data == "adm_backup_now")
async def adm_backup_now(cb: CallbackQuery):
    await cb.answer("Делаю бэкап")
    try:
        await make_backup(cb.bot)
        text = "Бэкап базы отправлен в чат бэкапов и закреплён."
    except Exception as e:
        text = f"Не получилось: {esc(str(e))}"
    await show(cb, text, kb([[btn("Назад", "adm_backup")]]))


@adm2.callback_query(F.data == "adm_export")
async def adm_export(cb: CallbackQuery):
    rows = await fetchall(
        "SELECT s.title, s.description, s.content, c.name AS cat FROM scripts s "
        "LEFT JOIN categories c ON c.id=s.category_id WHERE s.content IS NOT NULL AND s.content<>'' ORDER BY c.name, s.id")
    cats = await fetchall("SELECT name FROM categories ORDER BY name")
    only_files = (await fetchone("SELECT COUNT(*) n FROM scripts WHERE content IS NULL OR content=''"))["n"]
    if not rows:
        return await cb.answer("Нет скриптов для экспорта", show_alert=True)
    out = "\n".join(
        f"{r['title']} {SEP} {r['description'] or '-'} {SEP} {r['content']} {SEP} {r['cat'] or 'Без категории'}" for r in rows)
    await cb.message.answer_document(
        BufferedInputFile(out.encode("utf-8"), filename="scripts_export.txt"),
        caption=(f"Экспорт: {len(rows)} скриптов, {len(cats)} категорий.\n"
                 "Этот файл можно отправить в «Массовая заливка», чтобы восстановить скрипты."
                 + (f"\n{only_files} скриптов, загруженных файлами, в экспорт не вошли." if only_files else "")))
    await cb.answer()


# ═════════════════════════ АДМИНКА: УРОВЕНЬ 3 ═════════════════════════
# — категории —
async def admin_cats(cb: CallbackQuery, page: int = 0):
    cats = await fetchall("SELECT * FROM categories ORDER BY name")
    rows = paginate(cats, page, CATS_PER_PAGE,
                    lambda c: btn(("[подписка] " if c["sub_required"] else "") + ("[фото] " if c["photo"] else "") + c["name"], f"acat:{c['id']}"),
                    "acats")
    rows.append([btn("Добавить категорию", "acat_add", style="success")])
    rows.append([btn("Добавить списком", "bulk_cats", style="primary")])
    rows.append([btn("Назад", "adm")])
    await show(cb, "<b>Категории (игры)</b>", kb(rows))


@adm3.callback_query(F.data.regexp(r"^(adm_cats|acats:\d+)$"))
async def adm_cats(cb: CallbackQuery):
    await admin_cats(cb, page_of(cb.data) if ":" in cb.data else 0)
    await cb.answer()


@adm3.callback_query(F.data == "acat_add")
async def acat_add(cb: CallbackQuery, state: FSMContext):
    await ask(cb, state, AddCat.name, "Введите название категории (игры):")


@adm3.message(AddCat.name, F.text)
async def acat_name(m: Message, state: FSMContext):
    await state.update_data(name=m.text.strip())
    await state.set_state(AddCat.photo)
    await m.answer("Отправьте <b>картинку</b> для категории (как фото) или «-», чтобы пропустить:")


@adm3.message(AddCat.photo, F.photo | F.text)
async def acat_photo(m: Message, state: FSMContext):
    if m.photo:
        await state.update_data(photo=m.photo[-1].file_id)
    elif (m.text or "").strip() == "-":
        await state.update_data(photo=None)
    else:
        return await m.answer("Отправьте картинку (как фото) или «-».")
    await state.set_state(AddCat.sub)
    await m.answer("Обязательная подписка для скриптов этой категории?", reply_markup=sub_choice_kb("acsub"))


@adm3.callback_query(F.data.regexp(r"^acsub:[01]$"))
async def acat_sub(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    if "name" not in d:
        return await cb.answer()
    await run("INSERT INTO categories(name,sub_required,photo) VALUES(?,?,?)",
              (d["name"], int(cb.data.split(":")[1]), d.get("photo")))
    await state.clear()
    await show(cb, f"Категория «{esc(d['name'])}» создана.", kb([[btn("К категориям", "adm_cats")]]))
    await cb.answer()


async def view_cat(cb: CallbackQuery, cid: int):
    c = await fetchone("SELECT * FROM categories WHERE id=?", (cid,))
    if not c:
        return await cb.answer("Не найдено")
    n = (await fetchone("SELECT COUNT(*) n FROM scripts WHERE category_id=?", (cid,)))["n"]
    text = (f"<b>{esc(c['name'])}</b>\nСкриптов: {n}\n"
            f"Обяз. подписка: {'ВКЛ' if c['sub_required'] else 'ВЫКЛ'}\n"
            f"Картинка: {'есть' if c['photo'] else 'нет'}")
    await show(cb, text, kb([
        [btn("Картинка категории", f"acatp:{cid}")],
        [btn("Переключить подписку", f"acatt:{cid}")],
        [btn("Удалить", f"acatd:{cid}", style="danger")],
        [btn("Назад", "adm_cats")]]), c["photo"])
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^acat:\d+$"))
async def acat_view(cb: CallbackQuery):
    await view_cat(cb, int(cb.data.split(":")[1]))


@adm3.callback_query(F.data.regexp(r"^acatt:\d+$"))
async def acat_toggle(cb: CallbackQuery):
    cid = int(cb.data.split(":")[1])
    await run("UPDATE categories SET sub_required=1-sub_required WHERE id=?", (cid,))
    await view_cat(cb, cid)


@adm3.callback_query(F.data.regexp(r"^acatd:\d+$"))
async def acat_del(cb: CallbackQuery):
    cid = cb.data.split(":")[1]
    await show(cb, "Удалить категорию <b>вместе со всеми скриптами</b>?", kb([
        [btn("Да, удалить", f"acatdy:{cid}", style="danger"), btn("Отмена", f"acat:{cid}")]]))
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^acatdy:\d+$"))
async def acat_del_yes(cb: CallbackQuery):
    cid = int(cb.data.split(":")[1])
    await run("DELETE FROM favorites WHERE script_id IN (SELECT id FROM scripts WHERE category_id=?)", (cid,))
    await run("DELETE FROM votes WHERE script_id IN (SELECT id FROM scripts WHERE category_id=?)", (cid,))
    await run("DELETE FROM comments WHERE script_id IN (SELECT id FROM scripts WHERE category_id=?)", (cid,))
    await run("DELETE FROM scripts WHERE category_id=?", (cid,))
    await run("DELETE FROM categories WHERE id=?", (cid,))
    await cb.answer("Удалено")
    await admin_cats(cb)


@adm3.callback_query(F.data == "bulk_cats")
async def bulk_cats(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(BulkCats.data)
    await state.update_data(sub=0)
    await show(cb, BULK_CATS_HELP, bulk_kb("cats", 0))
    await cb.answer()


@adm3.callback_query(F.data == "bulkt:cats")
async def bulkt_cats(cb: CallbackQuery, state: FSMContext):
    await toggle_bulk(cb, state, "cats")


@adm3.message(BulkCats.data, F.text | F.document)
async def bulk_cats_msg(m: Message, state: FSMContext):
    text = await read_text(m)
    if text is None:
        return
    sub = int((await state.get_data()).get("sub", 0))
    names, seen = [], set()
    for line in text.replace("\ufeff", "").splitlines():
        name = line.strip()
        if name and name.lower() not in seen:
            seen.add(name.lower())
            names.append(name)
    created, skipped = [], 0
    for name in names:
        if await fetchone("SELECT id FROM categories WHERE pylower(name)=?", (name.lower(),)):
            skipped += 1
        else:
            await run("INSERT INTO categories(name,sub_required) VALUES(?,?)", (name, sub))
            created.append(name)
    msg = f"Добавлено категорий: <b>{len(created)}</b>"
    if skipped:
        msg += f"\nУже были: {skipped}"
    msg += "\n\nМожно отправить ещё или нажать «Готово»."
    await m.answer(msg, reply_markup=bulk_kb("cats", sub))


# — картинки категорий / скриптов —
@adm3.callback_query(F.data.regexp(r"^(acatp|ascrp):\d+$"))
async def photo_ask(cb: CallbackQuery, state: FSMContext):
    kind, tid = cb.data.split(":")
    await ask(cb, state, SetPhoto.photo,
              "Отправьте <b>картинку</b> (как фото).\n«-» — убрать текущую картинку.",
              {"kind": kind, "tid": int(tid)})


@adm3.message(SetPhoto.photo, F.photo | F.text)
async def photo_set(m: Message, state: FSMContext):
    d = await state.get_data()
    if m.photo:
        fid = m.photo[-1].file_id
    elif (m.text or "").strip() == "-":
        fid = None
    else:
        return await m.answer("Отправьте картинку (как фото) или «-», чтобы убрать.")
    table = "categories" if d.get("kind") == "acatp" else "scripts"
    await run(f"UPDATE {table} SET photo=? WHERE id=?", (fid, d["tid"]))
    await state.clear()
    await m.answer("Картинка сохранена" if fid else "Картинка убрана", reply_markup=await akb(m.from_user))


# — управление скриптами —
@adm3.callback_query(F.data.regexp(r"^ascr_list(:\d+)?$"))
async def ascr_list(cb: CallbackQuery):
    cats = await fetchall("SELECT * FROM categories ORDER BY name")
    rows = paginate(cats, page_of(cb.data) if ":" in cb.data else 0, CATS_PER_PAGE,
                    lambda c: btn(c["name"], f"aslc:{c['id']}"), "ascr_list")
    rows.append([btn("Назад", "adm_scripts")])
    await show(cb, "Выберите категорию:", kb(rows))
    await cb.answer()


async def scripts_in_cat(cb: CallbackQuery, cid: int, page: int = 0):
    items = await fetchall("SELECT id,title,sub_required,photo FROM scripts WHERE category_id=? ORDER BY id DESC", (cid,))
    rows = paginate(items, page, PER_PAGE,
                    lambda s: btn(("[подписка] " if s["sub_required"] else "") + ("[фото] " if s["photo"] else "") + s["title"], f"ascr:{s['id']}"),
                    f"aslc:{cid}")
    rows.append([btn("Назад", "ascr_list")])
    await show(cb, "Скрипты категории:" if items else "Тут пусто.", kb(rows))


@adm3.callback_query(F.data.regexp(r"^aslc:\d+(:\d+)?$"))
async def ascr_in_cat(cb: CallbackQuery):
    parts = cb.data.split(":")
    await scripts_in_cat(cb, int(parts[1]), int(parts[2]) if len(parts) > 2 else 0)
    await cb.answer()


async def view_script(cb: CallbackQuery, sid: int):
    s = await fetchone("SELECT * FROM scripts WHERE id=?", (sid,))
    if not s:
        return await cb.answer("Не найдено")
    link = await script_link(cb.bot, s["token"]) if s["token"] else "-"
    text = (f"<b>{esc(s['title'])}</b>\nПросмотров: {s['views']}\n"
            f"Обяз. подписка: {'ВКЛ' if s['sub_required'] else 'ВЫКЛ'}\n"
            f"Картинка: {'есть' if s['photo'] else 'нет'}\n\nСсылка:\n<code>{link}</code>")
    await show(cb, text, kb([
        [btn("Картинка скрипта", f"ascrp:{sid}")],
        [btn("Переключить подписку", f"ascrt:{sid}")],
        [btn("Удалить", f"ascrd:{sid}", style="danger")],
        [btn("Назад", f"aslc:{s['category_id']}")]]), s["photo"])
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^ascr:\d+$"))
async def ascr_view(cb: CallbackQuery):
    await view_script(cb, int(cb.data.split(":")[1]))


@adm3.callback_query(F.data.regexp(r"^ascrt:\d+$"))
async def ascr_toggle(cb: CallbackQuery):
    sid = int(cb.data.split(":")[1])
    await run("UPDATE scripts SET sub_required=1-sub_required WHERE id=?", (sid,))
    await view_script(cb, sid)


@adm3.callback_query(F.data.regexp(r"^ascrd:\d+$"))
async def ascr_del(cb: CallbackQuery):
    sid = int(cb.data.split(":")[1])
    s = await fetchone("SELECT category_id FROM scripts WHERE id=?", (sid,))
    await run("DELETE FROM favorites WHERE script_id=?", (sid,))
    await run("DELETE FROM votes WHERE script_id=?", (sid,))
    await run("DELETE FROM comments WHERE script_id=?", (sid,))
    await run("DELETE FROM scripts WHERE id=?", (sid,))
    await cb.answer("Удалено")
    if s:
        await scripts_in_cat(cb, s["category_id"])


# — удаление комментариев (модерация) —
@adm3.callback_query(F.data.regexp(r"^cmd:\d+:\d+:\d+$"))
async def comment_delete(cb: CallbackQuery):
    _, cid, sid, page = cb.data.split(":")
    await run("DELETE FROM comments WHERE id=?", (int(cid),))
    text, markup = await render_comments(int(sid), int(page), True)
    if text is None:
        return await cb.answer("Скрипт не найден", show_alert=True)
    await show(cb, text, markup)
    await cb.answer("Комментарий удалён")


# — пост в канал —
def post_markup(text: str, style: str, username: str):
    return kb([[ub(text or "Открыть бота", f"https://t.me/{username}", style=style or None)]])


async def current_post_markup(bot: Bot):
    me = await bot.me()
    return post_markup(await get_setting("post_btn_text"), await get_setting("post_btn_style"), me.username)


async def post_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    ch = await get_setting("post_channel")
    style = await get_setting("post_btn_style")
    text = ("<b>Пост в канал</b>\n\n"
            f"Канал: {esc(ch) if ch else 'не указан'}\n"
            f"Текст кнопки: {esc(await get_setting('post_btn_text'))}\n"
            f"Цвет кнопки: {POST_STYLES.get(style, 'обычная')}")
    await show(cb, text, kb([
        [btn("Создать пост", "post_new", style="success")],
        [btn("Текст кнопки", "aset:post_btn_text"), btn("Цвет кнопки", "post_color")],
        [btn("Посмотреть кнопку", "post_prev")],
        [btn("Канал для постов", "aset:post_channel")],
        [btn("Назад", "adm")]]))


@adm3.callback_query(F.data == "adm_post")
async def adm_post(cb: CallbackQuery, state: FSMContext):
    await post_menu(cb, state)
    await cb.answer()


@adm3.callback_query(F.data == "post_color")
async def post_color(cb: CallbackQuery):
    cur = await get_setting("post_btn_style")

    def label(key, name):
        return name + (" (выбрано)" if cur == key else "")

    await show(cb, "Выберите цвет кнопки под постом:", kb([
        [btn(label("", "Обычная"), "pstyle:none")],
        [btn(label("primary", "Синяя"), "pstyle:primary", style="primary")],
        [btn(label("success", "Зелёная"), "pstyle:success", style="success")],
        [btn(label("danger", "Красная"), "pstyle:danger", style="danger")],
        [btn("Назад", "adm_post")]]))
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^pstyle:(none|primary|success|danger)$"))
async def post_style_set(cb: CallbackQuery, state: FSMContext):
    val = cb.data.split(":")[1]
    await set_setting("post_btn_style", "" if val == "none" else val)
    await post_menu(cb, state)
    await cb.answer("Цвет сохранён")


@adm3.callback_query(F.data == "post_prev")
async def post_prev(cb: CallbackQuery):
    await cb.message.answer("Так выглядит кнопка под постом:", reply_markup=await current_post_markup(cb.bot))
    await cb.answer()


@adm3.callback_query(F.data == "post_new")
async def post_new(cb: CallbackQuery, state: FSMContext):
    ch = await get_setting("post_channel")
    if not ch:
        return await cb.answer("Сначала укажите канал для постов", show_alert=True)
    await ask(cb, state, PostS.msg, f"Отправьте пост (текст/фото/видео), он уйдёт в {esc(ch)} с кнопкой под ним:")


@adm3.message(PostS.msg)
async def post_preview(m: Message, state: FSMContext):
    await state.update_data(mid=m.message_id, cid=m.chat.id)
    try:  # предпросмотр: тот же пост, уже с кнопкой нужного текста и цвета
        await m.bot.copy_message(m.chat.id, m.chat.id, m.message_id, reply_markup=await current_post_markup(m.bot))
    except Exception:
        pass
    await m.answer("Выше предпросмотр поста с кнопкой. Опубликовать?", reply_markup=kb([
        [btn("Опубликовать", "post_go", style="success"), btn("Отмена", "adm", style="danger")]]))


@adm3.callback_query(F.data == "post_go")
async def post_go(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    if "mid" not in d:
        return await cb.answer()
    try:
        await cb.bot.copy_message(chat_ref(await get_setting("post_channel")), d["cid"], d["mid"],
                                  reply_markup=await current_post_markup(cb.bot))
        res = "Пост опубликован."
    except Exception as e:
        res = f"Ошибка: {esc(str(e))}"
    await state.clear()
    await show(cb, res, kb([[btn("К настройкам поста", "adm_post")], [btn("В админку", "adm")]]))
    await cb.answer()


@adm3.callback_query(F.data == "adm_bc")
async def adm_bc(cb: CallbackQuery, state: FSMContext):
    await ask(cb, state, Bc.msg, "Отправьте сообщение для рассылки (текст/фото/видео, что угодно):")


@adm3.message(Bc.msg)
async def bc_preview(m: Message, state: FSMContext):
    n = (await fetchone("SELECT COUNT(*) n FROM users WHERE banned=0"))["n"]
    await state.update_data(mid=m.message_id, cid=m.chat.id)
    await m.answer(f"Разослать это {n} пользователям?", reply_markup=kb([
        [btn("Отправить", "bc_go", style="success"), btn("Отмена", "adm", style="danger")]]))


async def do_broadcast(bot: Bot, admin_chat, from_chat, mid):
    ok = fail = 0
    for r in await fetchall("SELECT id FROM users WHERE banned=0"):
        try:
            await bot.copy_message(r["id"], from_chat, mid)
            ok += 1
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
            try:
                await bot.copy_message(r["id"], from_chat, mid)
                ok += 1
            except Exception:
                fail += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    await bot.send_message(admin_chat, f"Рассылка завершена\nДоставлено: {ok}\nНе доставлено: {fail}")


@adm3.callback_query(F.data == "bc_go")
async def bc_go(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    if "mid" not in d:
        return await cb.answer()
    await state.clear()
    spawn(do_broadcast(cb.bot, cb.message.chat.id, d["cid"], d["mid"]))
    await show(cb, "Рассылка запущена, по завершении пришлю отчёт.", kb([[btn("В админку", "adm")]]))
    await cb.answer()


# — бан —
@adm3.callback_query(F.data.in_({"adm_ban", "adm_unban"}))
async def adm_ban(cb: CallbackQuery, state: FSMContext):
    ban = cb.data == "adm_ban"
    await ask(cb, state, Ban.target, f"Введите ID или @username пользователя для {'бана' if ban else 'разбана'}:", {"ban": ban})


@adm3.message(Ban.target, F.text)
async def ban_target(m: Message, state: FSMContext):
    ban = (await state.get_data()).get("ban", True)
    t = m.text.strip().lstrip("@")
    if t.isdigit():
        row = await fetchone("SELECT id, username FROM users WHERE id=?", (int(t),))
    else:
        row = await fetchone("SELECT id, username FROM users WHERE pylower(username)=?", (t.lower(),))
    if not row:
        return await m.answer("Пользователь не найден (он должен был хотя бы раз запустить бота). Попробуйте ещё раз или /cancel")
    if await is_staff(row["id"], row["username"]):
        return await m.answer("Администраторов банить нельзя.")
    await run("UPDATE users SET banned=? WHERE id=?", (1 if ban else 0, row["id"]))
    await state.clear()
    await m.answer(f"{'Забанен' if ban else 'Разбанен'}: <code>{row['id']}</code>", reply_markup=await akb(m.from_user))


# — дизайн / картинки / тексты —
@adm3.callback_query(F.data == "adm_design")
async def adm_design(cb: CallbackQuery):
    await show(cb, "<b>Дизайн</b>", kb([
        [btn("Картинки вкладок", "adm_imgs")],
        [btn("Текст приветствия", "aset:start_text")],
        [btn("Кнопка «Поиск»", "aset:btn_search"), btn("Кнопка «Категории»", "aset:btn_categories")],
        [btn("Кнопка «Топ»", "aset:btn_top"), btn("Кнопка «Избранное»", "aset:btn_fav")],
        [btn("Кнопка «Поддержка»", "aset:btn_support")],
        [btn("Назад", "adm")]]))
    await cb.answer()


@adm3.callback_query(F.data == "adm_imgs")
async def adm_imgs(cb: CallbackQuery):
    rows = []
    for key, label in PHOTO_KEYS.items():
        mark = "[есть]" if await get_setting(key) else "[нет]"
        rows.append([btn(f"{label} {mark}", f"aset:{key}")])
    rows.append([btn("Назад", "adm")])
    await show(cb, "<b>Картинки вкладок</b>\n\nНажмите на вкладку, чтобы поставить или убрать картинку.\n\n"
                   "Картинки категорий и скриптов ставятся в их карточках: "
                   "Категории / Скрипты → Управление → Картинка.", kb(rows))
    await cb.answer()


@adm3.callback_query(F.data == "adm_texts")
async def adm_texts(cb: CallbackQuery):
    keys = ["support_text", "support_url", "search_prompt", "sub_text", "banned_text", "post_channel"]
    rows = [[btn(LABELS[k], f"aset:{k}")] for k in keys] + [[btn("Назад", "adm")]]
    await show(cb, "<b>Тексты и настройки</b>", kb(rows))
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^aset:\w+$"))
async def aset(cb: CallbackQuery, state: FSMContext):
    key = cb.data.split(":", 1)[1]
    if key in PHOTO_KEYS:
        hint = f"<b>{PHOTO_KEYS[key]}</b>\n\nОтправьте картинку (как фото).\n«-» — убрать картинку."
    elif key in LABELS:
        cur = esc(await get_setting(key)) or "-"
        hint = (f"<b>{LABELS[key]}</b>\n\nСейчас:\n<code>{cur}</code>\n\n"
                f"Отправьте новое значение{' (форматирование сохранится)' if key in HTML_KEYS else ''}.\n"
                f"«-» — сбросить на значение по умолчанию.")
    else:
        return await cb.answer()
    await ask(cb, state, Edit.value, hint, {"key": key})


def to_html(text: str, entities) -> str:
    """Превращает форматирование Telegram (жирный, ссылки...) в HTML."""
    tags = {"bold": ("<b>", "</b>"), "italic": ("<i>", "</i>"), "underline": ("<u>", "</u>"),
            "strikethrough": ("<s>", "</s>"), "spoiler": ("<tg-spoiler>", "</tg-spoiler>"),
            "code": ("<code>", "</code>"), "pre": ("<pre>", "</pre>"), "blockquote": ("<blockquote>", "</blockquote>")}
    items = []
    for e in entities or []:
        if e.type in tags:
            items.append((e.offset, e.length, tags[e.type]))
        elif e.type == "text_link" and e.url:
            items.append((e.offset, e.length, (f'<a href="{esc(e.url, quote=True)}">', "</a>")))
    items.sort(key=lambda x: (x[0], -x[1]))
    stack, out = [], ""
    for i in range(len(text) + 1):
        while stack and stack[-1][0] == i:
            out += stack.pop()[1]
        while items and items[0][0] == i:
            off, ln, (op, cl) = items.pop(0)
            out += op
            stack.append((i + ln, cl))
        if i < len(text):
            out += esc(text[i])
    return out


@adm3.message(Edit.value)
async def edit_value(m: Message, state: FSMContext):
    key = (await state.get_data()).get("key")
    if key in PHOTO_KEYS:
        if m.photo:
            val = m.photo[-1].file_id
        elif (m.text or "").strip() == "-":
            val = ""
        else:
            return await m.answer("Нужна картинка (как фото) или «-», чтобы убрать.")
    else:
        if not m.text:
            return await m.answer("Нужен текст")
        val = to_html(m.text, m.entities) if key in HTML_KEYS else m.text.strip()
        if m.text.strip() == "-":
            val = ""
        if key == "support_url" and val.startswith("@"):
            val = "https://t.me/" + val[1:]
    await set_setting(key, val)
    await state.clear()
    back = kb([[btn("К настройкам поста", "adm_post")]]) if key.startswith("post_") else await akb(m.from_user)
    await m.answer("Сохранено", reply_markup=back)


# — обязательная подписка (каналы) —
async def admin_subs(cb: CallbackQuery):
    chs = await fetchall("SELECT * FROM channels")
    rows = [[btn(f"Удалить: {c['title']}", f"chd:{c['id']}")] for c in chs]
    rows.append([btn("Добавить канал", "chadd", style="success")])
    rows.append([btn("Назад", "adm")])
    await show(cb, "<b>Каналы для обязательной подписки</b>\n\nНажмите на канал, чтобы удалить.\n"
                   "Бот должен быть админом в этих каналах. Включать подписку нужно у категорий и скриптов.", kb(rows))


@adm3.callback_query(F.data == "adm_subs")
async def adm_subs(cb: CallbackQuery):
    await admin_subs(cb)
    await cb.answer()


@adm3.callback_query(F.data == "chadd")
async def ch_add(cb: CallbackQuery, state: FSMContext):
    await ask(cb, state, Chan.chat, "Отправьте @username канала или его ID (-100...):")


@adm3.message(Chan.chat, F.text)
async def ch_add_msg(m: Message, state: FSMContext):
    ref = m.text.strip()
    if not ref.lstrip("-").isdigit() and not ref.startswith("@"):
        ref = "@" + ref
    try:
        chat = await m.bot.get_chat(chat_ref(ref))
    except Exception as e:
        return await m.answer(f"Не нашёл канал: {esc(str(e))}\nДобавьте бота админом и повторите.")
    url = f"https://t.me/{chat.username}" if chat.username else chat.invite_link
    if not url:
        try:
            url = await m.bot.export_chat_invite_link(chat.id)
        except Exception:
            url = None
    await run("INSERT INTO channels(chat_id,title,url) VALUES(?,?,?)", (str(chat.id), chat.title or ref, url))
    await state.clear()
    await m.answer(f"Канал «{esc(chat.title or ref)}» добавлен.", reply_markup=await akb(m.from_user))


@adm3.callback_query(F.data.regexp(r"^chd:\d+$"))
async def ch_del(cb: CallbackQuery):
    await run("DELETE FROM channels WHERE id=?", (int(cb.data.split(":")[1]),))
    await cb.answer("Удалено")
    await admin_subs(cb)


# ═════════════════════════ РОЗЫГРЫШИ ═════════════════════════
GW_STATUS = {"draft": "черновик", "active": "идёт", "finishing": "подведение итогов",
             "finished": "завершён", "cancelled": "отменён"}
GW_MAX_WINNERS = 100
GW_PRESETS = [("1 час", 60), ("6 часов", 360), ("24 часа", 1440), ("3 дня", 4320), ("7 дней", 10080)]
GW_LOOP_SECONDS = 30
NO_PREVIEW = LinkPreviewOptions(is_disabled=True)


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, TZ).strftime("%d.%m.%Y %H:%M") + f" {TZ_NAME}"


def html_len(s: str) -> int:
    """Длина «видимого» текста (Telegram считает подпись без тегов)."""
    return len(unescape(re.sub(r"<[^>]+>", "", s)))


def norm_channel(v: str) -> str:
    v = (v or "").strip()
    if not v:
        return ""
    m = re.match(r"https?://t\.me/([A-Za-z0-9_]+)/?$", v)
    if m:
        return "@" + m.group(1)
    if v.lstrip("-").isdigit() or v.startswith("@"):
        return v
    return "@" + v


def msg_link(channel: str, mid) -> str:
    """Ссылка на сообщение в канале (для кнопки «Итоги»)."""
    c = str(channel or "").strip()
    if not mid:
        return None
    if c.startswith("@"):
        return f"https://t.me/{c[1:]}/{mid}"
    if c.startswith("-100") and c[4:].isdigit():
        return f"https://t.me/c/{c[4:]}/{mid}"
    return None


def parse_dt(text: str):
    """ДД.ММ.ГГГГ ЧЧ:ММ (или ДД.ММ ЧЧ:ММ) в часовом поясе TZ -> unix-время."""
    text = text.strip()
    for f in ("%d.%m.%Y %H:%M", "%d.%m.%y %H:%M"):
        try:
            return int(datetime.strptime(text, f).replace(tzinfo=TZ).timestamp())
        except ValueError:
            pass
    try:
        dt = datetime.strptime(text, "%d.%m %H:%M")
        return int(dt.replace(year=datetime.now(TZ).year, tzinfo=TZ).timestamp())
    except ValueError:
        return None


def mention(u) -> str:
    if u["username"]:
        return f"@{esc(u['username'])}"
    return f'<a href="tg://user?id={u["user_id"]}">{esc((u["name"] or "участник")[:40])}</a>'


async def get_gw(gid: int):
    return await fetchone("SELECT * FROM giveaways WHERE id=?", (gid,))


async def gw_count(gid: int) -> int:
    return (await fetchone("SELECT COUNT(*) n FROM gw_users WHERE giveaway_id=?", (gid,)))["n"]


async def _gw_channels(csv_ids: str):
    rows = []
    for x in (csv_ids or "").split(","):
        if x.strip().isdigit():
            ch = await fetchone("SELECT * FROM channels WHERE id=?", (int(x),))
            if ch:
                rows.append(ch)
    return rows


async def gw_required(g):
    """Каналы, на которые нужно подписаться (из списка «Обяз. подписка»)."""
    return await _gw_channels(g["channels"])


async def gw_boost_required(g):
    """Каналы, которые нужно забустить."""
    return await _gw_channels(g["boost_channels"])


def boost_link(ch):
    """Ссылка, по которой человек может забустить канал."""
    m = re.match(r"https?://t\.me/([A-Za-z0-9_]{4,})/?$", ch["url"] or "")
    if m:
        return f"https://t.me/{m.group(1)}?boost"
    cid = str(ch["chat_id"])
    if cid.startswith("-100") and cid[4:].isdigit():
        return f"https://t.me/boost?c={cid[4:]}"
    if cid.startswith("@"):
        return f"https://t.me/{cid[1:]}?boost"
    return ch["url"]


async def gw_boosts(bot: Bot, ch, user_id: int) -> int:
    """Сколько действующих бустов канала сделал пользователь (боту нужны права админа в канале)."""
    res = await bot.get_user_chat_boosts(chat_id=chat_ref(ch["chat_id"]), user_id=user_id)
    n = 0
    for b in res.boosts:
        exp = b.expiration_date
        ts = exp if isinstance(exp, (int, float)) else (exp.timestamp() if exp else None)
        if ts is None or ts > time.time():
            n += 1
    return n


async def gw_boost_missing(bot: Bot, user_id: int, req, need: int) -> list:
    """Каналы из req, на которые у человека меньше нужного числа бустов. [(канал, сколько_есть)]"""
    missing = []
    for ch in req:
        try:
            have = await gw_boosts(bot, ch, user_id)
            if have < need:
                missing.append((ch, have))
        except Exception as e:
            logging.warning("Не могу проверить бусты канала %s: %s", ch["chat_id"], e)
    return missing


async def gw_render_post(g) -> str:
    parts = [f"<b>{esc(g['title'])}</b>"]
    if g["description"]:
        parts.append(g["description"])
    info = [f"Победителей: {g['winners']}"]
    if g["status"] == "finished":
        info.append("Розыгрыш завершён.")
    elif g["status"] == "cancelled":
        info.append("Розыгрыш отменён.")
    else:
        info.append(f"Итоги: {fmt_ts(g['end_ts'])}")
    parts.append("\n".join(info))
    if g["status"] in ("draft", "active", "finishing"):
        conds = []
        for ch in await gw_required(g):
            conds.append(f'Подписаться на <a href="{esc(ch["url"], quote=True)}">{esc(ch["title"])}</a>' if ch["url"]
                         else f"Подписаться на {esc(ch['title'])}")
        need_b = g["min_boosts"] or 1
        for ch in await gw_boost_required(g):
            link = boost_link(ch)
            name = f'<a href="{esc(link, quote=True)}">{esc(ch["title"])}</a>' if link else esc(ch["title"])
            conds.append(f"Забустить канал {name}" + (f" (бустов: {need_b})" if need_b > 1 else ""))
        if g["min_views"]:
            conds.append(f"Открыть в боте минимум {g['min_views']} разных скриптов")
        if g["extra"]:
            conds.append(esc(g["extra"]))
        if conds:
            parts.append("<b>Условия участия:</b>\n" + "\n".join("• " + c for c in conds))
    return "\n\n".join(parts)


async def gw_post_kb(bot: Bot, g, results_url=None):
    if g["status"] in ("finished", "cancelled"):
        return kb([[ub("Итоги розыгрыша", results_url)]]) if results_url else None
    me = await bot.me()
    label = g["btn_text"] or "Участвовать"
    if g["show_count"]:
        label += f" ({await gw_count(g['id'])})"
    return kb([[ub(label, f"https://t.me/{me.username}?start=gw_{g['id']}", style=g["btn_style"] or None)]])


async def gw_send_post(bot: Bot, chat, g):
    text = await gw_render_post(g)
    markup = await gw_post_kb(bot, g)
    if g["photo"]:
        return await bot.send_photo(chat, g["photo"], caption=text, reply_markup=markup)
    return await bot.send_message(chat, text, reply_markup=markup, link_preview_options=NO_PREVIEW)


async def gw_edit_post(bot: Bot, g, markup):
    """Обновляет пост розыгрыша в канале (текст/подпись и кнопку)."""
    if not g["post_msg_id"] or not g["channel"]:
        return
    chat = chat_ref(g["channel"])
    text = await gw_render_post(g)
    try:
        if g["photo"]:
            await bot.edit_message_caption(chat_id=chat, message_id=g["post_msg_id"], caption=text, reply_markup=markup)
        else:
            await bot.edit_message_text(text, chat_id=chat, message_id=g["post_msg_id"], reply_markup=markup,
                                        link_preview_options=NO_PREVIEW)
    except Exception as e:
        if "not modified" not in str(e).lower():
            logging.warning("Не удалось обновить пост розыгрыша #%s: %s", g["id"], e)


async def gw_refresh_post(bot: Bot, gid: int):
    g = await get_gw(gid)
    if g and g["status"] == "active":
        await gw_edit_post(bot, g, await gw_post_kb(bot, g))


# ───── проверка условий и выбор победителей ─────
async def gw_missing(bot: Bot, user_id: int, req) -> list:
    missing = []
    for ch in req:
        try:
            m = await bot.get_chat_member(chat_ref(ch["chat_id"]), user_id)
            if m.status in ("left", "kicked"):
                missing.append(ch)
        except Exception as e:
            logging.warning("Не могу проверить канал %s: %s", ch["chat_id"], e)
    return missing


async def gw_views_done(user_id: int, g) -> int:
    return (await fetchone(
        "SELECT COUNT(DISTINCT script_id) n FROM events WHERE user_id=? AND kind='view' AND script_id IS NOT NULL AND ts>=?",
        (user_id, g["started"] or g["created"] or 0)))["n"]


async def gw_pick(bot: Bot, g, count: int, skip_ids: set):
    """Случайно выбирает победителей: не забаненные, не админы (если включено), условия подписки ещё выполнены."""
    users = await fetchall(
        "SELECT gu.* FROM gw_users gu JOIN users u ON u.id=gu.user_id "
        "WHERE gu.giveaway_id=? AND u.banned=0 AND gu.excluded=0", (g["id"],))
    pool = [u for u in users if u["user_id"] not in skip_ids]
    random.SystemRandom().shuffle(pool)
    req = await gw_required(g) if g["recheck"] else []
    breq = await gw_boost_required(g) if g["recheck"] else []
    need_b = g["min_boosts"] or 1
    chosen = []
    for u in pool:
        if len(chosen) >= count:
            break
        if g["no_staff"] and await is_staff(u["user_id"], u["username"]):
            continue
        if req or breq:
            if req and await gw_missing(bot, u["user_id"], req):
                continue
            if breq and await gw_boost_missing(bot, u["user_id"], breq, need_b):
                continue
            await asyncio.sleep(0.03)
        chosen.append(u)
    return chosen


def gw_results_text(g, winners, total: int) -> str:
    head = f"<b>Итоги розыгрыша: {esc(g['title'])}</b>\n\nУчастников: {total}\n"
    if not winners:
        return head + "\nПодходящих участников не нашлось, победителей нет."
    lines = [f"{i}. {mention(w)}" for i, w in enumerate(winners, 1)]
    return head + "\nПобедители:\n" + "\n".join(lines) + "\n\nПоздравляем!"


async def gw_win_message(g) -> str:
    if g["win_text"]:
        return g["win_text"].replace("{title}", esc(g["title"]))
    txt = f"Поздравляем! Вы стали победителем розыгрыша «{esc(g['title'])}»."
    support = await get_setting("support_url")
    if support:
        return txt + f"\n\nЧтобы получить приз, свяжитесь с администратором: {esc(support)}"
    return txt + "\n\nАдминистратор свяжется с вами в ближайшее время."


async def gw_announce(bot: Bot, gid: int, dm: bool = True):
    """Публикует итоги в канале, обновляет исходный пост, пишет победителям и отчитывается админу."""
    g = await get_gw(gid)
    winners = await fetchall("SELECT * FROM gw_winners WHERE giveaway_id=? ORDER BY place", (gid,))
    total = await gw_count(gid)
    problems, result_id = [], None
    try:
        msg = await bot.send_message(
            chat_ref(g["channel"]), gw_results_text(g, winners, total),
            reply_parameters=ReplyParameters(message_id=g["post_msg_id"], allow_sending_without_reply=True),
            link_preview_options=NO_PREVIEW)
        result_id = msg.message_id
        await run("UPDATE giveaways SET result_msg_id=? WHERE id=?", (result_id, gid))
    except Exception as e:
        problems.append(f"не удалось опубликовать итоги в канале: {esc(str(e))}")
    g = await get_gw(gid)
    link = msg_link(g["channel"], result_id)
    await gw_edit_post(bot, g, await gw_post_kb(bot, g, link))
    failed = []
    if dm:
        text = await gw_win_message(g)
        for w in winners:
            try:
                await bot.send_message(w["user_id"], text)
                await run("UPDATE gw_winners SET notified=1 WHERE giveaway_id=? AND user_id=?", (gid, w["user_id"]))
            except Exception:
                failed.append(w)
            await asyncio.sleep(0.05)
    if g["admin_id"]:
        report = f"Розыгрыш «{esc(g['title'])}» завершён.\nУчастников: {total}\n"
        report += ("Победители:\n" + "\n".join(f"{i}. {mention(w)}" for i, w in enumerate(winners, 1))) if winners \
            else "Победителей нет."
        if failed:
            report += "\n\nНе удалось написать в личку (свяжитесь сами):\n" + "\n".join(
                f"{mention(w)} (id {w['user_id']})" for w in failed)
        if problems:
            report += "\n\nВнимание: " + "; ".join(problems) + "\nИтоги можно опубликовать заново в карточке розыгрыша."
        try:
            await bot.send_message(g["admin_id"], report)
        except Exception:
            pass


async def finish_giveaway(bot: Bot, gid: int) -> bool:
    cur = await db.execute("UPDATE giveaways SET status='finishing' WHERE id=? AND status='active'", (gid,))
    await db.commit()
    if cur.rowcount != 1:
        return False
    g = await get_gw(gid)
    try:
        chosen = await gw_pick(bot, g, g["winners"], set())
        await run("DELETE FROM gw_winners WHERE giveaway_id=?", (gid,))
        for i, u in enumerate(chosen, 1):
            await run("INSERT INTO gw_winners(giveaway_id,user_id,place,username,name) VALUES(?,?,?,?,?)",
                      (gid, u["user_id"], i, u["username"], u["name"]))
        await run("UPDATE giveaways SET status='finished', finished=? WHERE id=?", (int(time.time()), gid))
    except Exception:
        logging.exception("Не удалось выбрать победителей розыгрыша #%s", gid)
        await run("UPDATE giveaways SET status='active' WHERE id=?", (gid,))
        return False
    await gw_announce(bot, gid)
    return True


async def gw_reroll(bot: Bot, gid: int, old_uid: int):
    """Заменяет победителя другим случайным участником. Возвращает (новый_победитель, ошибка)."""
    g = await get_gw(gid)
    old = await fetchone("SELECT * FROM gw_winners WHERE giveaway_id=? AND user_id=?", (gid, old_uid))
    if not g or not old:
        return None, "Победитель не найден"
    current = {r["user_id"] for r in await fetchall("SELECT user_id FROM gw_winners WHERE giveaway_id=?", (gid,))}
    chosen = await gw_pick(bot, g, 1, current)
    if not chosen:
        return None, "Подходящих участников больше нет"
    u = chosen[0]
    await run("UPDATE gw_users SET excluded=1 WHERE giveaway_id=? AND user_id=?", (gid, old_uid))
    await run("DELETE FROM gw_winners WHERE giveaway_id=? AND user_id=?", (gid, old_uid))
    await run("INSERT INTO gw_winners(giveaway_id,user_id,place,username,name) VALUES(?,?,?,?,?)",
              (gid, u["user_id"], old["place"], u["username"], u["name"]))
    winners = await fetchall("SELECT * FROM gw_winners WHERE giveaway_id=? ORDER BY place", (gid,))
    if g["result_msg_id"]:
        try:
            await bot.edit_message_text(gw_results_text(g, winners, await gw_count(gid)), chat_id=chat_ref(g["channel"]),
                                        message_id=g["result_msg_id"], link_preview_options=NO_PREVIEW)
        except Exception as e:
            logging.warning("Не удалось обновить итоги в канале: %s", e)
    try:
        await bot.send_message(u["user_id"], await gw_win_message(g))
    except Exception:
        pass
    return u, None


async def giveaway_tick(bot: Bot):
    """Один проход: подводит итоги по истёкшим розыгрышам и обновляет счётчик участников на кнопке."""
    for g in await fetchall("SELECT id FROM giveaways WHERE status='active' AND end_ts<=?", (int(time.time()),)):
        await finish_giveaway(bot, g["id"])
    for g in await fetchall("SELECT * FROM giveaways WHERE status='active' AND show_count=1 AND post_msg_id IS NOT NULL"):
        n = await gw_count(g["id"])
        if n != g["shown_count"]:
            try:
                await bot.edit_message_reply_markup(chat_id=chat_ref(g["channel"]), message_id=g["post_msg_id"],
                                                    reply_markup=await gw_post_kb(bot, g))
            except Exception as e:
                if "not modified" not in str(e).lower():
                    logging.warning("Счётчик розыгрыша #%s: %s", g["id"], e)
            await db.execute("UPDATE giveaways SET shown_count=? WHERE id=?", (n, g["id"]))
            await db.commit()


async def giveaway_loop(bot: Bot):
    """Каждые 30 секунд проверяет розыгрыши (итоги и счётчики)."""
    while True:
        try:
            await giveaway_tick(bot)
        except Exception:
            logging.exception("Ошибка в цикле розыгрышей")
        await asyncio.sleep(GW_LOOP_SECONDS)


# ───── участие (пользователь) ─────
async def join_flow(msg: Message, u, gid: int, cb: CallbackQuery = None):
    bot = msg.bot

    async def out(text, markup=None, note=None):
        if cb:
            await show(cb, text, markup)
            await cb.answer(note)
        else:
            await msg.answer(text, reply_markup=markup)

    g = await get_gw(gid)
    if not g or g["status"] == "draft":
        return await out("Розыгрыш не найден.")
    if g["status"] != "active":
        rows = []
        link = msg_link(g["channel"], g["result_msg_id"]) if g["status"] == "finished" else None
        if link:
            rows.append([ub("Итоги розыгрыша", link)])
        rows.append([btn("В каталог", "menu")])
        return await out("Этот розыгрыш отменён." if g["status"] == "cancelled" else "Этот розыгрыш уже завершён.", kb(rows))

    title = esc(g["title"])
    mine = await fetchone("SELECT joined FROM gw_users WHERE giveaway_id=? AND user_id=?", (gid, u.id))
    if mine:
        n = (await fetchone("SELECT COUNT(*) n FROM gw_users WHERE giveaway_id=? AND joined<=?", (gid, mine["joined"])))["n"]
        return await out(f"Вы уже участвуете в розыгрыше «{title}».\nНомер участника: {n}\nИтоги: {fmt_ts(g['end_ts'])}",
                         kb([[btn("В каталог", "menu")]]))

    req = await gw_required(g)
    missing = await gw_missing(bot, u.id, req) if req else []
    breq = await gw_boost_required(g)
    need_b = g["min_boosts"] or 1
    bmissing = await gw_boost_missing(bot, u.id, breq, need_b) if breq else []
    need_views = g["min_views"] or 0
    done_views = await gw_views_done(u.id, g) if need_views else 0
    if missing or bmissing or done_views < need_views:
        missing_ids = {c["id"] for c in missing}
        bhave = {c["id"]: have for c, have in bmissing}
        lines = [f"Чтобы участвовать в розыгрыше «{title}», выполните условия:\n"]
        for ch in req:
            lines.append(f"Подписка на «{esc(ch['title'])}»: " + ("не выполнено" if ch["id"] in missing_ids else "выполнено"))
        for ch in breq:
            if ch["id"] in bhave:
                lines.append(f"Буст канала «{esc(ch['title'])}»: не выполнено" + (f" ({bhave[ch['id']]} из {need_b})" if need_b > 1 else ""))
            else:
                lines.append(f"Буст канала «{esc(ch['title'])}»: выполнено")
        if need_views:
            lines.append(f"Открыть разных скриптов в боте: {min(done_views, need_views)} из {need_views}")
        rows = [[ub(f"Подписаться: {ch['title']}", ch["url"])] for ch in missing if ch["url"]]
        for ch, _ in bmissing:
            link = boost_link(ch)
            if link:
                rows.append([ub(f"Забустить: {ch['title']}", link)])
        if bmissing:
            lines.append("\nБуст доступен пользователям Telegram Premium. После буста нажмите «Проверить условия».")
        if done_views < need_views:
            rows.append([btn("Открыть каталог", "cats")])
        rows.append([btn("Проверить условия", f"gwchk:{gid}", style="success")])
        return await out("\n".join(lines), kb(rows), note="Условия ещё не выполнены" if cb else None)

    await run("INSERT OR IGNORE INTO gw_users(giveaway_id,user_id,username,name,joined) VALUES(?,?,?,?,?)",
              (gid, u.id, u.username, (u.first_name or "")[:40], int(time.time())))
    n = await gw_count(gid)
    await out(f"Вы участвуете в розыгрыше «{title}».\nНомер участника: {n}\nИтоги: {fmt_ts(g['end_ts'])}",
              kb([[btn("В каталог", "menu")]]), note="Вы участвуете")


@user.callback_query(F.data.regexp(r"^gwchk:\d+$"))
async def cb_gw_check(cb: CallbackQuery):
    await join_flow(cb.message, cb.from_user, int(cb.data.split(":")[1]), cb)


# ───── админка: карточка и список ─────
def gw_mark(st: str) -> str:
    return {"draft": "[черновик]", "active": "[идёт]", "finishing": "[итоги]",
            "finished": "[завершён]", "cancelled": "[отменён]"}.get(st, "")


async def gw_card(bot: Bot, gid: int):
    g = await get_gw(gid)
    if not g:
        return None, None
    n = await gw_count(gid)
    req = await gw_required(g)
    conds = []
    if req:
        conds.append("подписка на: " + ", ".join(esc(c["title"]) for c in req))
    breq = await gw_boost_required(g)
    if breq:
        conds.append("буст канала: " + ", ".join(esc(c["title"]) for c in breq)
                     + (f" (бустов: {g['min_boosts']})" if (g["min_boosts"] or 1) > 1 else ""))
    if g["min_views"]:
        conds.append(f"открыть минимум {g['min_views']} скриптов")
    if g["extra"]:
        conds.append(f"доп. условие: {esc(g['extra'])}")
    st = g["status"]
    lines = [f"<b>Розыгрыш #{gid}: {esc(g['title'])}</b>", f"Статус: {GW_STATUS.get(st, st)}",
             f"Канал: {esc(g['channel']) if g['channel'] else 'не указан'}",
             f"Победителей: {g['winners']}", f"Окончание: {fmt_ts(g['end_ts'])}", f"Участников: {n}",
             "Условия: " + ("; ".join(conds) if conds else "нет"),
             f"Кнопка: «{esc(g['btn_text'])}», цвет: {POST_STYLES.get(g['btn_style'] or '', 'обычная')}",
             f"Картинка: {'есть' if g['photo'] else 'нет'}"]
    if st == "finished":
        winners = await fetchall("SELECT * FROM gw_winners WHERE giveaway_id=? ORDER BY place", (gid,))
        lines.append("\nПобедители:\n" + ("\n".join(f"{i}. {mention(w)}" for i, w in enumerate(winners, 1)) if winners else "нет"))
    edit_rows = [
        [btn("Название", f"gwe:{gid}:title"), btn("Описание", f"gwe:{gid}:desc")],
        [btn("Победители", f"gwe:{gid}:winners"), btn("Время окончания", f"gwe:{gid}:time")],
        [btn("Условия", f"gwc:{gid}"), btn("Кнопка", f"gwb:{gid}")],
        [btn("Дополнительно", f"gwo:{gid}")],
    ]
    if st == "draft":
        rows = [[btn("Картинка", f"gwe:{gid}:photo"), btn("Канал", f"gwe:{gid}:channel")]] + edit_rows + [
            [btn("Предпросмотр", f"gwprev:{gid}"), btn("Опубликовать", f"gwpub:{gid}", style="success")],
            [btn("Удалить", f"gwdel:{gid}", style="danger")], [btn("К списку", "gw_list")]]
    elif st == "active":
        rows = [[btn(f"Участники ({n})", f"gwpart:{gid}")]] + edit_rows + [
            [btn("Предпросмотр", f"gwprev:{gid}"), btn("Обновить пост", f"gwref:{gid}")],
            [btn("Завершить сейчас", f"gwnow:{gid}", style="success"), btn("Отменить", f"gwcancel:{gid}", style="danger")],
            [btn("К списку", "gw_list")]]
    elif st == "finished":
        rows = [[btn(f"Участники ({n})", f"gwpart:{gid}"), btn("Перевыбрать победителя", f"gwrr:{gid}")],
                [btn("Опубликовать итоги заново", f"gwrepost:{gid}")],
                [btn("Удалить", f"gwdel:{gid}", style="danger")], [btn("К списку", "gw_list")]]
    else:
        rows = [[btn("Удалить", f"gwdel:{gid}", style="danger")], [btn("К списку", "gw_list")]]
    return "\n".join(lines), kb(rows)


async def open_card(cb: CallbackQuery, gid: int, note: str = None):
    text, markup = await gw_card(cb.bot, gid)
    if text is None:
        return await cb.answer("Розыгрыш не найден", show_alert=True)
    await show(cb, text, markup)
    await cb.answer(note)


async def send_card(m: Message, gid: int):
    text, markup = await gw_card(m.bot, gid)
    if text:
        await m.answer(text, reply_markup=markup)


@adm3.callback_query(F.data.regexp(r"^(gw_list|gwl:\d+)$"))
async def gw_list(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    items = await fetchall("SELECT id,title,status FROM giveaways ORDER BY id DESC")
    rows = paginate(items, page_of(cb.data) if ":" in cb.data else 0, 8,
                    lambda g: btn(f"{gw_mark(g['status'])} {g['title'][:28]}", f"gw:{g['id']}"), "gwl")
    rows.append([btn("Создать розыгрыш", "gw_new", style="success")])
    rows.append([btn("Назад", "adm")])
    await show(cb, "<b>Розыгрыши</b>" + ("" if items else "\n\nПока нет ни одного."), kb(rows))
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^gw:\d+$"))
async def gw_open(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await open_card(cb, int(cb.data.split(":")[1]))


async def gw_ask(cb: CallbackQuery, state: FSMContext, gid: int, field: str, text: str, extra_rows=None):
    await state.set_state(GwEdit.value)
    await state.update_data(gid=gid, field=field)
    rows = (extra_rows or []) + [[btn("Отмена", f"gw:{gid}" if gid else "gw_list")]]
    await show(cb, text, kb(rows))
    await cb.answer()


@adm3.callback_query(F.data == "gw_new")
async def gw_new(cb: CallbackQuery, state: FSMContext):
    await gw_ask(cb, state, 0, "new", "Введите название розыгрыша (оно будет заголовком поста):")


GW_HINTS = {
    "title": "Введите новое название розыгрыша:",
    "desc": "Отправьте описание или приз (форматирование сохранится). «-» — убрать:",
    "photo": "Отправьте картинку для поста (как фото). «-» — убрать:",
    "winners": f"Сколько будет победителей? Отправьте число от 1 до {GW_MAX_WINNERS}:",
    "time": f"Отправьте дату и время окончания в формате ДД.ММ.ГГГГ ЧЧ:ММ ({TZ_NAME}), например 25.12.2026 18:00.\nИли выберите срок от текущего момента:",
    "extra": "Введите дополнительное условие текстом. Оно попадёт в пост, но бот его не проверяет. «-» — убрать:",
    "minv": "Сколько разных скриптов нужно открыть в боте, чтобы участвовать? Число от 0 до 20 (0 — условие выключено):",
    "minb": "Сколько бустов канала нужно от одного человека? Число от 1 до 10:",
    "btntext": "Введите текст кнопки участия (до 30 символов):",
    "wintext": "Введите сообщение победителю (можно использовать {title} для названия розыгрыша). «-» — стандартное:",
    "channel": "Отправьте @username канала или его ID (-100...), куда выйдет пост. Бот должен быть админом канала:",
}


@adm3.callback_query(F.data.regexp(r"^gwe:\d+:(title|desc|photo|winners|time|extra|minv|minb|btntext|wintext|channel)$"))
async def gw_edit_start(cb: CallbackQuery, state: FSMContext):
    _, gid, field = cb.data.split(":")
    gid = int(gid)
    g = await get_gw(gid)
    if not g:
        return await cb.answer("Розыгрыш не найден", show_alert=True)
    if g["status"] not in ("draft", "active"):
        return await cb.answer("Завершённый розыгрыш изменить нельзя", show_alert=True)
    if field in ("photo", "channel") and g["status"] != "draft":
        return await cb.answer("Это можно менять только в черновике", show_alert=True)
    extra = None
    if field == "time":
        extra = [[btn(label, f"gwq:{gid}:{mins}") for label, mins in GW_PRESETS[i:i + 3]] for i in range(0, len(GW_PRESETS), 3)]
    await gw_ask(cb, state, gid, field, GW_HINTS[field], extra)


@adm3.callback_query(F.data.regexp(r"^gwq:\d+:\d+$"))
async def gw_quick_time(cb: CallbackQuery, state: FSMContext):
    _, gid, mins = cb.data.split(":")
    gid = int(gid)
    g = await get_gw(gid)
    if not g or g["status"] not in ("draft", "active"):
        return await cb.answer("Нельзя изменить", show_alert=True)
    await run("UPDATE giveaways SET end_ts=? WHERE id=?", (int(time.time()) + int(mins) * 60, gid))
    await state.clear()
    await gw_refresh_post(cb.bot, gid)
    await open_card(cb, gid, "Время сохранено")


@adm3.message(GwEdit.value, F.photo | F.text)
async def gw_edit_msg(m: Message, state: FSMContext):
    d = await state.get_data()
    field, gid = d.get("field"), d.get("gid")
    text = (m.text or "").strip()
    now = int(time.time())
    if field == "new":
        if not text:
            return await m.answer("Отправьте название текстом.")
        ch = norm_channel(await get_setting("post_channel"))
        gid = await run("INSERT INTO giveaways(title,end_ts,status,channel,created,admin_id) VALUES(?,?,?,?,?,?)",
                        (text[:100], now + 86400, "draft", ch, now, m.from_user.id))
        await state.clear()
        return await send_card(m, gid)
    g = await get_gw(gid) if gid else None
    if not g or g["status"] not in ("draft", "active"):
        await state.clear()
        return await m.answer("Розыгрыш не найден или уже завершён.")
    q, args = None, ()
    if field == "photo":
        if m.photo:
            q, args = "photo=?", (m.photo[-1].file_id,)
        elif text == "-":
            q, args = "photo=?", (None,)
        else:
            return await m.answer("Отправьте картинку (как фото) или «-».")
    else:
        if not text:
            return await m.answer("Отправьте текстом.")
        if field == "title":
            q, args = "title=?", (text[:100],)
        elif field == "desc":
            q, args = "description=?", ("" if text == "-" else to_html(m.text, m.entities).strip(),)
        elif field == "winners":
            if not text.isdigit() or not 1 <= int(text) <= GW_MAX_WINNERS:
                return await m.answer(f"Нужно число от 1 до {GW_MAX_WINNERS}.")
            q, args = "winners=?", (int(text),)
        elif field == "time":
            ts = parse_dt(text)
            if ts is None:
                return await m.answer(f"Не понял дату. Формат: ДД.ММ.ГГГГ ЧЧ:ММ ({TZ_NAME}), например 25.12.2026 18:00.")
            if ts <= now + 60:
                return await m.answer("Это время уже прошло или слишком близко. Укажите время в будущем.")
            q, args = "end_ts=?", (ts,)
        elif field == "extra":
            q, args = "extra=?", ("" if text == "-" else text[:200],)
        elif field == "minv":
            if not text.isdigit() or int(text) > 20:
                return await m.answer("Нужно число от 0 до 20.")
            q, args = "min_views=?", (int(text),)
        elif field == "minb":
            if not text.isdigit() or not 1 <= int(text) <= 10:
                return await m.answer("Нужно число от 1 до 10.")
            q, args = "min_boosts=?", (int(text),)
        elif field == "btntext":
            if len(text) > 30:
                return await m.answer("Слишком длинно: максимум 30 символов.")
            q, args = "btn_text=?", (text,)
        elif field == "wintext":
            q, args = "win_text=?", ("" if text == "-" else to_html(m.text, m.entities).strip(),)
        elif field == "channel":
            ref = norm_channel(text)
            try:
                chat = await m.bot.get_chat(chat_ref(ref))
            except Exception as e:
                return await m.answer(f"Не нашёл канал: {esc(str(e))}\nДобавьте бота админом канала и повторите.")
            q, args = "channel=?", (ref if ref.startswith("@") else str(chat.id),)
        else:
            return
    await run(f"UPDATE giveaways SET {q} WHERE id=?", args + (gid,))
    await state.clear()
    await gw_refresh_post(m.bot, gid)
    await send_card(m, gid)


# ───── условия ─────
async def gw_conditions_screen(cb: CallbackQuery, gid: int):
    g = await get_gw(gid)
    if not g:
        return await cb.answer("Розыгрыш не найден", show_alert=True)
    sub = {int(x) for x in (g["channels"] or "").split(",") if x.strip().isdigit()}
    boost = {int(x) for x in (g["boost_channels"] or "").split(",") if x.strip().isdigit()}
    chans = await fetchall("SELECT * FROM channels ORDER BY id")
    rows = [[btn(("[x] " if c["id"] in sub else "[ ] ") + "Подписка: " + c["title"][:14], f"gwch:{gid}:{c['id']}"),
             btn(("[x] " if c["id"] in boost else "[ ] ") + "Буст", f"gwbo:{gid}:{c['id']}")] for c in chans]
    rows.append([btn(f"Минимум бустов: {g['min_boosts'] or 1}", f"gwe:{gid}:minb")])
    rows.append([btn(f"Открыть скриптов в боте: {g['min_views']}", f"gwe:{gid}:minv")])
    rows.append([btn("Дополнительное условие", f"gwe:{gid}:extra")])
    rows.append([btn("Назад", f"gw:{gid}")])
    note = "" if chans else "\n\nКаналов пока нет. Добавьте их в разделе «Обяз. подписка»."
    await show(cb, "<b>Условия участия</b>\n\nДля каждого канала отметьте, нужна ли подписка и/или буст. "
                   "Бот должен быть админом этих каналов, иначе он не сможет ничего проверить. "
                   "Буст может сделать только пользователь с Telegram Premium." + note, kb(rows))


@adm3.callback_query(F.data.regexp(r"^gwc:\d+$"))
async def gw_conditions(cb: CallbackQuery):
    await gw_conditions_screen(cb, int(cb.data.split(":")[1]))
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^gwch:\d+:\d+$"))
async def gw_toggle_channel(cb: CallbackQuery):
    _, gid, chid = cb.data.split(":")
    gid, chid = int(gid), int(chid)
    g = await get_gw(gid)
    if not g or g["status"] not in ("draft", "active"):
        return await cb.answer("Нельзя изменить", show_alert=True)
    chosen = [int(x) for x in (g["channels"] or "").split(",") if x.strip().isdigit()]
    chosen = [x for x in chosen if x != chid] if chid in chosen else chosen + [chid]
    await run("UPDATE giveaways SET channels=? WHERE id=?", (",".join(map(str, chosen)), gid))
    await gw_refresh_post(cb.bot, gid)
    await gw_conditions_screen(cb, gid)
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^gwbo:\d+:\d+$"))
async def gw_toggle_boost(cb: CallbackQuery):
    _, gid, chid = cb.data.split(":")
    gid, chid = int(gid), int(chid)
    g = await get_gw(gid)
    if not g or g["status"] not in ("draft", "active"):
        return await cb.answer("Нельзя изменить", show_alert=True)
    chosen = [int(x) for x in (g["boost_channels"] or "").split(",") if x.strip().isdigit()]
    chosen = [x for x in chosen if x != chid] if chid in chosen else chosen + [chid]
    await run("UPDATE giveaways SET boost_channels=? WHERE id=?", (",".join(map(str, chosen)), gid))
    await gw_refresh_post(cb.bot, gid)
    await gw_conditions_screen(cb, gid)
    await cb.answer()


# ───── кнопка ─────
@adm3.callback_query(F.data.regexp(r"^gwb:\d+$"))
async def gw_button(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    g = await get_gw(gid)
    if not g:
        return await cb.answer("Розыгрыш не найден", show_alert=True)
    cur = g["btn_style"] or ""

    def label(key, name):
        return name + (" (выбрано)" if cur == key else "")

    await show(cb, f"<b>Кнопка участия</b>\n\nТекст: {esc(g['btn_text'])}\nЦвет: {POST_STYLES.get(cur, 'обычная')}", kb([
        [btn("Изменить текст", f"gwe:{gid}:btntext")],
        [btn(label("", "Обычная"), f"gwcol:{gid}:none")],
        [btn(label("primary", "Синяя"), f"gwcol:{gid}:primary", style="primary")],
        [btn(label("success", "Зелёная"), f"gwcol:{gid}:success", style="success")],
        [btn(label("danger", "Красная"), f"gwcol:{gid}:danger", style="danger")],
        [btn("Назад", f"gw:{gid}")]]))
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^gwcol:\d+:(none|primary|success|danger)$"))
async def gw_color(cb: CallbackQuery):
    _, gid, val = cb.data.split(":")
    gid = int(gid)
    g = await get_gw(gid)
    if not g or g["status"] not in ("draft", "active"):
        return await cb.answer("Нельзя изменить", show_alert=True)
    await run("UPDATE giveaways SET btn_style=? WHERE id=?", ("" if val == "none" else val, gid))
    await gw_refresh_post(cb.bot, gid)
    await gw_button(cb)


# ───── дополнительные настройки ─────
GW_TOGGLES = {"count": ("show_count", "Счётчик участников на кнопке"),
              "recheck": ("recheck", "Проверять подписку при подведении итогов"),
              "staff": ("no_staff", "Админы не могут выиграть")}


@adm3.callback_query(F.data.regexp(r"^gwo:\d+$"))
async def gw_options(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    g = await get_gw(gid)
    if not g:
        return await cb.answer("Розыгрыш не найден", show_alert=True)
    rows = [[btn(f"{name}: {'ВКЛ' if g[col] else 'ВЫКЛ'}", f"gwt:{gid}:{key}")] for key, (col, name) in GW_TOGGLES.items()]
    rows.append([btn("Сообщение победителю", f"gwe:{gid}:wintext")])
    rows.append([btn("Назад", f"gw:{gid}")])
    await show(cb, "<b>Дополнительные настройки</b>\n\nСообщение победителю приходит ему в личку, когда итоги подведены.",
               kb(rows))
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^gwt:\d+:(count|recheck|staff)$"))
async def gw_toggle(cb: CallbackQuery):
    _, gid, key = cb.data.split(":")
    gid = int(gid)
    g = await get_gw(gid)
    if not g or g["status"] not in ("draft", "active"):
        return await cb.answer("Нельзя изменить", show_alert=True)
    col = GW_TOGGLES[key][0]
    await run(f"UPDATE giveaways SET {col}=1-{col} WHERE id=?", (gid,))
    if key == "count":
        await gw_refresh_post(cb.bot, gid)
    await gw_options(cb)


# ───── предпросмотр, публикация ─────
async def gw_validate(bot: Bot, g) -> list:
    errs = []
    if not g["channel"]:
        errs.append("не указан канал для поста")
    if g["end_ts"] <= int(time.time()) + 60:
        errs.append("время окончания уже прошло или слишком близко")
    text = await gw_render_post(g)
    if g["photo"] and html_len(text) > 1024:
        errs.append(f"текст поста слишком длинный для картинки: {html_len(text)} из 1024 символов "
                    "(сократите описание или уберите картинку)")
    if html_len(text) > 4000:
        errs.append("текст поста слишком длинный")
    if g["channel"]:
        try:
            member = await bot.get_chat_member(chat_ref(g["channel"]), bot.id)
            if member.status not in ("administrator", "creator"):
                errs.append("бот должен быть админом канала, куда выходит пост")
            elif getattr(member, "can_post_messages", None) is False:
                errs.append("у бота нет права публиковать сообщения в канале")
        except Exception as e:
            errs.append(f"не удалось проверить канал публикации: {e}")
    seen = set()
    for ch in (await gw_required(g)) + (await gw_boost_required(g)):
        if ch["id"] in seen:
            continue
        seen.add(ch["id"])
        try:
            member = await bot.get_chat_member(chat_ref(ch["chat_id"]), bot.id)
            if member.status not in ("administrator", "creator"):
                errs.append(f"бот не админ в канале «{ch['title']}», проверить подписку или буст он не сможет")
        except Exception:
            errs.append(f"бот не видит канал «{ch['title']}», проверить подписку или буст он не сможет")
    return errs


@adm3.callback_query(F.data.regexp(r"^gwprev:\d+$"))
async def gw_preview(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    g = await get_gw(gid)
    if not g:
        return await cb.answer("Розыгрыш не найден", show_alert=True)
    try:
        await gw_send_post(cb.bot, cb.message.chat.id, g)
    except Exception as e:
        return await cb.answer(f"Ошибка предпросмотра: {e}", show_alert=True)
    await cb.answer("Предпросмотр отправлен")


@adm3.callback_query(F.data.regexp(r"^gwpub:\d+$"))
async def gw_publish_ask(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    g = await get_gw(gid)
    if not g or g["status"] != "draft":
        return await cb.answer("Розыгрыш не найден", show_alert=True)
    errs = await gw_validate(cb.bot, g)
    if errs:
        await show(cb, "<b>Пока нельзя опубликовать:</b>\n" + "\n".join("• " + esc(e) for e in errs),
                   kb([[btn("Назад", f"gw:{gid}")]]))
        return await cb.answer()
    await show(cb, f"Опубликовать розыгрыш «{esc(g['title'])}» в {esc(g['channel'])}?\n"
                   f"Итоги будут подведены {fmt_ts(g['end_ts'])} автоматически.",
               kb([[btn("Опубликовать", f"gwpubyes:{gid}", style="success"), btn("Отмена", f"gw:{gid}")]]))
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^gwpubyes:\d+$"))
async def gw_publish(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    g = await get_gw(gid)
    if not g or g["status"] != "draft":
        return await cb.answer("Розыгрыш уже опубликован или не найден", show_alert=True)
    errs = await gw_validate(cb.bot, g)
    if errs:
        return await cb.answer("Нельзя опубликовать: " + "; ".join(errs)[:180], show_alert=True)
    try:
        msg = await gw_send_post(cb.bot, chat_ref(g["channel"]), g)
    except Exception as e:
        return await cb.answer(f"Не удалось опубликовать: {e}"[:190], show_alert=True)
    await run("UPDATE giveaways SET status='active', post_msg_id=?, started=?, shown_count=0 WHERE id=?",
              (msg.message_id, int(time.time()), gid))
    await open_card(cb, gid, "Розыгрыш опубликован")


@adm3.callback_query(F.data.regexp(r"^gwref:\d+$"))
async def gw_refresh_btn(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    await gw_refresh_post(cb.bot, gid)
    await cb.answer("Пост обновлён")


# ───── завершить, отменить, удалить ─────
@adm3.callback_query(F.data.regexp(r"^gwnow:\d+$"))
async def gw_finish_ask(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    await show(cb, "Завершить розыгрыш сейчас и выбрать победителей?",
               kb([[btn("Да, завершить", f"gwnowyes:{gid}", style="success"), btn("Отмена", f"gw:{gid}")]]))
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^gwnowyes:\d+$"))
async def gw_finish_now(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    await cb.answer("Подвожу итоги")
    ok = await finish_giveaway(cb.bot, gid)
    text, markup = await gw_card(cb.bot, gid)
    if text:
        await show(cb, text if ok else "Не получилось подвести итоги (розыгрыш уже завершён или идёт подсчёт).\n\n" + text, markup)


@adm3.callback_query(F.data.regexp(r"^gwcancel:\d+$"))
async def gw_cancel_ask(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    await show(cb, "Отменить розыгрыш? Пост в канале останется с пометкой «Розыгрыш отменён», кнопка пропадёт.",
               kb([[btn("Да, отменить", f"gwcancelyes:{gid}", style="danger"), btn("Нет", f"gw:{gid}")]]))
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^gwcancelyes:\d+$"))
async def gw_cancel(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    cur = await db.execute("UPDATE giveaways SET status='cancelled', finished=? WHERE id=? AND status='active'",
                           (int(time.time()), gid))
    await db.commit()
    if cur.rowcount == 1:
        g = await get_gw(gid)
        await gw_edit_post(cb.bot, g, None)
    await open_card(cb, gid, "Розыгрыш отменён" if cur.rowcount == 1 else "Уже нельзя отменить")


@adm3.callback_query(F.data.regexp(r"^gwdel:\d+$"))
async def gw_delete_ask(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    await show(cb, "Удалить розыгрыш вместе со списком участников?",
               kb([[btn("Да, удалить", f"gwdelyes:{gid}", style="danger"), btn("Нет", f"gw:{gid}")]]))
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^gwdelyes:\d+$"))
async def gw_delete(cb: CallbackQuery, state: FSMContext):
    gid = int(cb.data.split(":")[1])
    g = await get_gw(gid)
    if g and g["status"] in ("active", "finishing"):
        return await cb.answer("Сначала завершите или отмените розыгрыш", show_alert=True)
    await run("DELETE FROM gw_users WHERE giveaway_id=?", (gid,))
    await run("DELETE FROM gw_winners WHERE giveaway_id=?", (gid,))
    await run("DELETE FROM giveaways WHERE id=?", (gid,))
    await cb.answer("Удалено")
    await gw_list(cb, state)


# ───── участники, итоги ─────
@adm3.callback_query(F.data.regexp(r"^gwpart:\d+$"))
async def gw_participants(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    n = await gw_count(gid)
    last = await fetchall("SELECT * FROM gw_users WHERE giveaway_id=? ORDER BY joined DESC LIMIT 15", (gid,))
    lines = [f"{mention(u)}" for u in last]
    await show(cb, f"<b>Участники</b>\nВсего: {n}\n\nПоследние:\n" + ("\n".join(lines) if lines else "никого нет"),
               kb([[btn("Скачать список (txt)", f"gwexp:{gid}")], [btn("Назад", f"gw:{gid}")]]))
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^gwexp:\d+$"))
async def gw_export(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    rows = await fetchall("SELECT * FROM gw_users WHERE giveaway_id=? ORDER BY joined", (gid,))
    if not rows:
        return await cb.answer("Участников пока нет", show_alert=True)
    out = "\n".join(f"{i}. id {u['user_id']} | {('@' + u['username']) if u['username'] else '-'} | {u['name'] or '-'} | "
                    f"{fmt_ts(u['joined'])}" for i, u in enumerate(rows, 1))
    await cb.message.answer_document(BufferedInputFile(out.encode("utf-8"), filename=f"giveaway_{gid}_participants.txt"),
                                     caption=f"Участников: {len(rows)}")
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^gwrepost:\d+$"))
async def gw_repost(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    g = await get_gw(gid)
    if not g or g["status"] != "finished":
        return await cb.answer("Нельзя", show_alert=True)
    await gw_announce(cb.bot, gid, dm=False)
    await cb.answer("Итоги опубликованы заново")


@adm3.callback_query(F.data.regexp(r"^gwrr:\d+$"))
async def gw_reroll_menu(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    winners = await fetchall("SELECT * FROM gw_winners WHERE giveaway_id=? ORDER BY place", (gid,))
    if not winners:
        return await cb.answer("Победителей нет", show_alert=True)
    rows = [[btn(f"Заменить: {w['place']}. {('@' + w['username']) if w['username'] else (w['name'] or w['user_id'])}",
                 f"gwrr1:{gid}:{w['user_id']}")] for w in winners]
    rows.append([btn("Назад", f"gw:{gid}")])
    await show(cb, "Кого заменить другим случайным участником? Заменённый человек больше не сможет выиграть в этом розыгрыше.",
               kb(rows))
    await cb.answer()


@adm3.callback_query(F.data.regexp(r"^gwrr1:\d+:\d+$"))
async def gw_reroll_do(cb: CallbackQuery):
    _, gid, uid = cb.data.split(":")
    u, err = await gw_reroll(cb.bot, int(gid), int(uid))
    if err:
        return await cb.answer(err, show_alert=True)
    await open_card(cb, int(gid), f"Новый победитель: {u['username'] or u['name'] or u['user_id']}")


# ═════════════════════════ ВЛАДЕЛЕЦ: АДМИНИСТРАТОРЫ ═════════════════════════
ADMINS_HELP = (
    "Уровни доступа:\n"
    "1 - добавление скриптов (по одному и массово)\n"
    "2 - всё из уровня 1, плюс активность и бэкап\n"
    "3 - полный доступ\n\n"
    "Выдавать и снимать админки может только владелец."
)


def admin_label(a) -> str:
    return f"@{a['username']}" if a["username"] else str(a["user_id"])


@owner.callback_query(F.data == "adm_admins")
async def adm_admins(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    items = await fetchall("SELECT * FROM admins ORDER BY level DESC, added")
    rows = [[btn(f"{admin_label(a)} - уровень {a['level']}", f"aadm:{a['user_id']}")] for a in items]
    rows.append([btn("Добавить администратора", "aadm_add", style="success")])
    rows.append([btn("Назад", "adm")])
    await show(cb, "<b>Администраторы</b>\n\n" + ADMINS_HELP + ("" if items else "\n\nПока никого нет."), kb(rows))
    await cb.answer()


@owner.callback_query(F.data == "aadm_add")
async def aadm_add(cb: CallbackQuery, state: FSMContext):
    await ask(cb, state, AdminAdd.target,
              "Отправьте ID или @username человека. Он должен был хотя бы раз запустить бота (команда /id покажет его ID).")


@owner.message(AdminAdd.target, F.text)
async def aadm_target(m: Message, state: FSMContext):
    t = m.text.strip().lstrip("@")
    if t.isdigit():
        uid = int(t)
        row = await fetchone("SELECT id, username FROM users WHERE id=?", (uid,))
        username = row["username"] if row else None
    else:
        row = await fetchone("SELECT id, username FROM users WHERE pylower(username)=?", (t.lower(),))
        if not row:
            return await m.answer("Не нашёл такого пользователя: он должен хотя бы раз запустить бота. "
                                  "Можно отправить его числовой ID. Попробуйте ещё раз или /cancel")
        uid, username = row["id"], row["username"]
    if uid in OWNER_IDS or (username or "").lower() == OWNER_USERNAME.lower():
        return await m.answer("Владелец уже имеет полный доступ.")
    await state.clear()
    await m.answer(f"Какой уровень выдать {('@' + username) if username else uid}?\n\n" + ADMINS_HELP, reply_markup=kb([
        [btn("1", f"agrant:{uid}:1"), btn("2", f"agrant:{uid}:2"), btn("3", f"agrant:{uid}:3")],
        [btn("Отмена", "adm_admins")]]))


async def view_admin(cb: CallbackQuery, uid: int):
    a = await fetchone("SELECT * FROM admins WHERE user_id=?", (uid,))
    if not a:
        return await cb.answer("Не найдено")
    await show(cb, f"<b>{esc(admin_label(a))}</b>\nУровень: {a['level']} ({LEVEL_INFO[a['level']]})", kb([
        [btn("Уровень 1", f"agrant:{uid}:1"), btn("Уровень 2", f"agrant:{uid}:2"), btn("Уровень 3", f"agrant:{uid}:3")],
        [btn("Снять права", f"adel:{uid}", style="danger")],
        [btn("Назад", "adm_admins")]]))


@owner.callback_query(F.data.regexp(r"^aadm:\d+$"))
async def aadm_view(cb: CallbackQuery):
    await view_admin(cb, int(cb.data.split(":")[1]))
    await cb.answer()


@owner.callback_query(F.data.regexp(r"^agrant:\d+:[123]$"))
async def agrant(cb: CallbackQuery):
    _, uid, lvl = cb.data.split(":")
    uid, lvl = int(uid), int(lvl)
    row = await fetchone("SELECT username FROM users WHERE id=?", (uid,))
    await run("INSERT INTO admins(user_id,username,level,added) VALUES(?,?,?,?) "
              "ON CONFLICT(user_id) DO UPDATE SET level=excluded.level, username=excluded.username",
              (uid, row["username"] if row else None, lvl, int(time.time())))
    try:
        await cb.bot.send_message(uid, f"Вам выданы права администратора, уровень {lvl}: {LEVEL_INFO[lvl]}.\n"
                                       "Откройте панель командой /admin")
        note = "Права выданы, человек уведомлён."
    except Exception:
        note = "Права выданы, но написать человеку не получилось (он должен запустить бота)."
    await cb.answer(note, show_alert=True)
    await view_admin(cb, uid)


@owner.callback_query(F.data.regexp(r"^adel:\d+$"))
async def adel(cb: CallbackQuery, state: FSMContext):
    uid = int(cb.data.split(":")[1])
    await run("DELETE FROM admins WHERE user_id=?", (uid,))
    try:
        await cb.bot.send_message(uid, "Права администратора сняты.")
    except Exception:
        pass
    await adm_admins(cb, state)


# ───────────────────────── ЗАПУСК ─────────────────────────
async def main():
    global db, BOT_REF, dirty
    logging.basicConfig(level=logging.INFO)

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    BOT_REF = bot

    await try_restore(bot)  # если базы нет, а бэкап есть — восстановим

    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.create_function("pylower", 1, lambda s: s.lower() if isinstance(s, str) else s)
    await db.executescript(SCHEMA)
    # миграции старых баз
    await ensure_column("categories", "photo", "TEXT")
    await ensure_column("scripts", "photo", "TEXT")
    await ensure_column("scripts", "token", "TEXT")
    await ensure_column("events", "script_id", "INTEGER")
    filled = await fill_tokens()  # ссылки для уже существующих скриптов
    await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_scripts_token ON scripts(token)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_events_kind_ts ON events(kind, ts)")
    await db.execute("UPDATE giveaways SET status='active' WHERE status='finishing'")  # недоподсчитанные заново
    await db.commit()
    if filled:
        logging.info("Выданы ссылки существующим скриптам: %s", filled)
        dirty = True

    if BACKUP_CHAT_ID:
        spawn(backup_loop())
        if filled:
            schedule_backup(30)
        logging.info("Авто-бэкап включён (чат %s)", BACKUP_CHAT_ID)
    else:
        logging.warning("BACKUP_CHAT_ID не задан: бэкап базы выключен")

    spawn(giveaway_loop(bot))  # автоподведение итогов розыгрышей

    dp = Dispatcher(storage=MemoryStorage())
    dp.message.outer_middleware(Track())
    dp.callback_query.outer_middleware(Track())
    for r in (cmd_router, owner, adm1, adm2, adm3, user):
        r.message.filter(F.chat.type == "private")
    dp.include_routers(cmd_router, owner, adm1, adm2, adm3, user)  # порядок важен
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
