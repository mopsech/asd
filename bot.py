# Scripts Agent - Telegram-бот каталога скриптов для Roblox.
# Переменные окружения (задаются на хосте):
#   BOT_TOKEN       - токен бота (обязательно)
#   BACKUP_CHAT_ID  - ваш Telegram ID или ID приватного канала для авто-бэкапа базы (желательно)
import asyncio
import logging
import os
import shutil
import time
from datetime import datetime
from html import escape as esc
from io import BytesIO

import aiosqlite
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramRetryAfter
from aiogram.filters import BaseFilter, Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (BufferedInputFile, CallbackQuery, FSInputFile,
                           InlineKeyboardButton, InlineKeyboardMarkup,
                           InputMediaPhoto, Message)

# ───────────────────────── НАСТРОЙКИ ─────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Не задана переменная окружения BOT_TOKEN")

ADMIN_USERNAME = "ruxdev"  # без @
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}

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

DEFAULTS = {
    "start_text": "<b>Scripts Agent</b> - Твой каталог скриптов для Roblox без подписок на множество каналов!\n\nНачинай прямо сейчас.",
    "btn_search": "ПОИСК",
    "btn_categories": "Категории",
    "btn_support": "Поддержка",
    "support_text": "🆘 <b>Поддержка</b>\n\nЕсли возник вопрос или проблема — напишите нам.",
    "search_prompt": "🔍 Введите название скрипта или игры:",
    "sub_text": "🔐 Чтобы получить этот скрипт, подпишитесь на каналы ниже и нажмите «Проверить подписку».",
    "banned_text": "🚫 Вы заблокированы в этом боте.",
}
HTML_KEYS = {"start_text", "support_text", "search_prompt", "sub_text", "banned_text"}
LABELS = {
    "start_text": "Текст приветствия", "btn_search": "Кнопка «Поиск»",
    "btn_categories": "Кнопка «Категории»", "btn_support": "Кнопка «Поддержка»",
    "support_text": "Текст поддержки", "support_url": "Ссылка поддержки (@user или URL)",
    "search_prompt": "Текст поиска", "sub_text": "Текст обязательной подписки",
    "banned_text": "Текст для забаненных", "post_channel": "Канал для постов (@канал или ID)",
}
# Картинки вкладок (экранов бота)
PHOTO_KEYS = {
    "start_photo": "Главная (/start)",
    "img_cats": "Список категорий",
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
CREATE TABLE IF NOT EXISTS scripts(id INTEGER PRIMARY KEY AUTOINCREMENT, category_id INTEGER, title TEXT, description TEXT, content TEXT, file_id TEXT, sub_required INTEGER DEFAULT 0, views INTEGER DEFAULT 0, created INTEGER, photo TEXT);
CREATE TABLE IF NOT EXISTS channels(id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, title TEXT, url TEXT);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, kind TEXT, ts INTEGER);
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
        if "into users" not in low and "update users set last_active" not in low:
            schedule_backup(30)
    return cur.lastrowid


async def ensure_column(table, column, decl):
    cols = [r["name"] for r in await fetchall(f"PRAGMA table_info({table})")]
    if column not in cols:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
        await db.commit()


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
    "💾 <b>Бэкап базы</b>\n\n"
    "Сейчас бэкап <b>выключен</b>. Если хост сотрёт файлы, все скрипты пропадут.\n\n"
    "Как включить (бесплатно):\n"
    "1. Отправьте боту /id и скопируйте свой ID.\n"
    "2. В «Переменных окружения» хоста добавьте <code>BACKUP_CHAT_ID</code> = ваш ID.\n"
    "3. Перезапустите бота.\n\n"
    "Дальше бот сам будет присылать вам файл базы (и закреплять его в чате с ботом), "
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
            caption=f"💾 Бэкап базы · {stamp}", disable_notification=True)
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
            logging.info("✅ База восстановлена из бэкапа в Telegram")
        else:
            logging.info("Закреплённого бэкапа нет, начинаем с чистой базы")
    except Exception as e:
        logging.warning("Не удалось восстановить базу из бэкапа: %s", e)


# ───────────────────────── УТИЛИТЫ ─────────────────────────
def is_admin(user) -> bool:
    if not user:
        return False
    return user.id in ADMIN_IDS or (user.username or "").lower() == ADMIN_USERNAME.lower()


class IsAdmin(BaseFilter):
    async def __call__(self, event) -> bool:
        return is_admin(event.from_user)


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


def kb(rows):
    return InlineKeyboardMarkup(inline_keyboard=rows)


def paginate(items, page, per, make_btn, prefix, cols=1):
    """Кнопки элементов + строка навигации ◀️ 2/5 ▶️. Колбэк страницы: '{prefix}:{номер}'."""
    pages = max(1, -(-len(items) // per))
    page = min(max(page, 0), pages - 1)
    buttons = [make_btn(i) for i in items[page * per:(page + 1) * per]]
    rows = [buttons[i:i + cols] for i in range(0, len(buttons), cols)]
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(btn("◀️", f"{prefix}:{page - 1}"))
        nav.append(btn(f"{page + 1}/{pages}", "noop"))
        if page < pages - 1:
            nav.append(btn("▶️", f"{prefix}:{page + 1}"))
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
    return kb([[btn("🔒 ВКЛ", f"{prefix}:1", style="success"), btn("🔓 ВЫКЛ", f"{prefix}:0", style="danger")],
               [btn("❌ Отмена", "adm")]])


# ───────────────────────── MIDDLEWARE ─────────────────────────
class Track(BaseMiddleware):
    async def __call__(self, handler, event, data):
        u = data.get("event_from_user")
        if u:
            now = int(time.time())
            await run(
                "INSERT INTO users(id,username,first_name,joined,last_active) VALUES(?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET username=excluded.username, first_name=excluded.first_name, last_active=excluded.last_active",
                (u.id, u.username, u.first_name, now, now))
            if not is_admin(u):
                row = await fetchone("SELECT banned FROM users WHERE id=?", (u.id,))
                if row and row["banned"]:
                    text = await get_setting("banned_text")
                    if isinstance(event, CallbackQuery):
                        await event.answer("🚫 Вы заблокированы", show_alert=True)
                    elif isinstance(event, Message):
                        await event.answer(text)
                    return
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


# ───────────────────────── РОУТЕРЫ ─────────────────────────
cmd_router = Router()
admin = Router()
user = Router()
admin.message.filter(IsAdmin())
admin.callback_query.filter(IsAdmin())


# ───────────────────────── /start и команды ─────────────────────────
async def main_kb():
    s, c, sp = await get_setting("btn_search"), await get_setting("btn_categories"), await get_setting("btn_support")
    return kb([
        [btn(s, "search", style="success")],
        [btn(c, "cats"), btn(sp, "support", style="danger")],
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


def admin_kb():
    return kb([
        [btn("📜 Скрипты", "adm_scripts"), btn("📂 Категории", "adm_cats")],
        [btn("📝 Создать пост", "adm_post"), btn("📢 Рассылка", "adm_bc")],
        [btn("🚫 Бан", "adm_ban"), btn("✅ Разбан", "adm_unban")],
        [btn("📊 Активность", "adm_stats"), btn("🎨 Дизайн", "adm_design")],
        [btn("🖼 Картинки вкладок", "adm_imgs"), btn("✏️ Тексты", "adm_texts")],
        [btn("🔐 Обяз. подписка", "adm_subs"), btn("💾 Бэкап", "adm_backup")],
        [btn("❌ Закрыть", "adm_close", style="danger")],
    ])


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
    if not is_admin(m.from_user):
        return
    await state.clear()
    await m.answer("⚙️ <b>Админ-панель</b>", reply_markup=admin_kb())


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
        rows.append([btn("💬 Написать в поддержку", url=url, style="danger")])
    rows.append([btn("⬅️ Назад", "menu")])
    await show(cb, await get_setting("support_text"), kb(rows), await get_setting("img_support"))
    await cb.answer()


@user.callback_query(F.data == "search")
async def cb_search(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Search.query)
    await show(cb, await get_setting("search_prompt"), kb([[btn("⬅️ В меню", "menu")]]), await get_setting("img_search"))
    await cb.answer()


async def render_search(q: str, page: int):
    like = f"%{q.lower()}%"
    rows = await fetchall(
        "SELECT s.id, s.title, c.name FROM scripts s LEFT JOIN categories c ON c.id=s.category_id "
        "WHERE pylower(s.title) LIKE ? OR pylower(s.description) LIKE ? OR pylower(c.name) LIKE ? "
        "ORDER BY s.views DESC LIMIT 200", (like, like, like))
    if not rows:
        return "😕 Ничего не найдено. Попробуйте другой запрос.", kb([[btn("⬅️ В меню", "menu")]])
    buttons = paginate(rows, page, PER_PAGE,
                       lambda r: btn(f"{r['title']} — {r['name']}" if r["name"] else r["title"], f"scr:{r['id']}"), "sp")
    buttons.append([btn("⬅️ В меню", "menu")])
    return f"🔎 Найдено: {len(rows)}", kb(buttons)


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
        await show(cb, "📂 Категорий пока нет.", kb([[btn("⬅️ Назад", "menu")]]), photo)
        return await cb.answer()
    rows = paginate(cats, page_of(cb.data), CATS_PER_PAGE,
                    lambda c: btn(("🔒 " if c["sub_required"] else "") + c["name"], f"cat:{c['id']}:0"), "cats", cols=2)
    rows.append([btn("⬅️ Назад", "menu")])
    await show(cb, "📂 <b>Выберите игру:</b>", kb(rows), photo)
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
    rows.append([btn("⬅️ Назад", "cats")])
    text = f"📂 <b>{esc(cat['name'])}</b>\nСкриптов: {len(items)}" if items else f"📂 <b>{esc(cat['name'])}</b>\n\nПока пусто."
    await show(cb, text, kb(rows), cat["photo"])
    await cb.answer()


async def open_script(cb: CallbackQuery, sid: int):
    s = await fetchone(
        "SELECT s.*, c.sub_required AS csub FROM scripts s LEFT JOIN categories c ON c.id=s.category_id WHERE s.id=?", (sid,))
    if not s:
        return await cb.answer("Скрипт не найден", show_alert=True)
    back_data = f"cat:{s['category_id']}:0"
    if s["sub_required"] or s["csub"]:
        missing = await missing_channels(cb.bot, cb.from_user.id)
        if missing:
            rows = [[btn(f"📢 {ch['title']}", url=ch["url"])] for ch in missing if ch["url"]]
            rows.append([btn("✅ Проверить подписку", f"chk:{sid}", style="success")])
            rows.append([btn("⬅️ Назад", back_data)])
            await show(cb, await get_setting("sub_text"), kb(rows), await get_setting("img_sub"))
            return await cb.answer()
    await run("UPDATE scripts SET views=views+1 WHERE id=?", (sid,))
    await run("INSERT INTO events(user_id,kind,ts) VALUES(?,?,?)", (cb.from_user.id, "view", int(time.time())))

    head = f"<b>{esc(s['title'])}</b>"
    if s["description"]:
        head += f"\n\n{esc(s['description'])}"
    content = s["content"] or ""
    photo = s["photo"]
    caption_ok = len(head) <= 1024

    code_text, as_file = "", None
    if content:
        if len(head) + len(content) < 3800:
            code_text = f"<pre>{esc(content)}</pre>"
        else:
            as_file = BufferedInputFile(content.encode("utf-8"), filename="script.lua")

    msg = cb.message
    back = kb([[btn("⬅️ Назад", back_data)]])
    senders = []
    if photo:
        if caption_ok:
            senders.append(lambda mk: msg.answer_photo(photo, caption=head, reply_markup=mk))
            body = code_text
        else:
            senders.append(lambda mk: msg.answer_photo(photo, reply_markup=mk))
            body = head + (("\n\n" + code_text) if code_text else "")
    else:
        body = head + (("\n\n" + code_text) if code_text else "")
    if body:
        senders.append(lambda mk: msg.answer(body, reply_markup=mk))
    if as_file:
        senders.append(lambda mk: msg.answer_document(as_file, reply_markup=mk))
    if s["file_id"]:
        senders.append(lambda mk: msg.answer_document(s["file_id"], reply_markup=mk))
    for i, send in enumerate(senders):
        await send(back if i == len(senders) - 1 else None)  # кнопка «Назад» — на последнем сообщении
    await cb.answer()


@user.callback_query(F.data.regexp(r"^scr:\d+$"))
async def cb_scr(cb: CallbackQuery):
    await open_script(cb, int(cb.data.split(":")[1]))


@user.callback_query(F.data.regexp(r"^chk:\d+$"))
async def cb_chk(cb: CallbackQuery):
    sid = int(cb.data.split(":")[1])
    if await missing_channels(cb.bot, cb.from_user.id):
        return await cb.answer("❗ Вы подписались не на все каналы", show_alert=True)
    try:
        await cb.message.delete()
    except Exception:
        pass
    await open_script(cb, sid)


# ───────────────────────── АДМИНКА: общее ─────────────────────────
@admin.callback_query(F.data == "adm")
async def adm_home(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await show(cb, "⚙️ <b>Админ-панель</b>", admin_kb())
    await cb.answer()


@admin.callback_query(F.data == "adm_close")
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
    await show(cb, text, kb([[btn("❌ Отмена", "adm")]]))
    await cb.answer()


# ───────────────────────── АДМИНКА: категории ─────────────────────────
async def admin_cats(cb: CallbackQuery, page: int = 0):
    cats = await fetchall("SELECT * FROM categories ORDER BY name")
    rows = paginate(cats, page, CATS_PER_PAGE,
                    lambda c: btn(("🔒 " if c["sub_required"] else "") + ("🖼 " if c["photo"] else "") + c["name"], f"acat:{c['id']}"),
                    "acats")
    rows.append([btn("➕ Добавить категорию", "acat_add", style="success")])
    rows.append([btn("📥 Добавить списком", "bulk_cats", style="primary")])
    rows.append([btn("⬅️ Назад", "adm")])
    await show(cb, "📂 <b>Категории (игры)</b>\n🔒 — подписка · 🖼 — есть картинка", kb(rows))


@admin.callback_query(F.data.regexp(r"^(adm_cats|acats:\d+)$"))
async def adm_cats(cb: CallbackQuery):
    await admin_cats(cb, page_of(cb.data) if ":" in cb.data else 0)
    await cb.answer()


@admin.callback_query(F.data == "acat_add")
async def acat_add(cb: CallbackQuery, state: FSMContext):
    await ask(cb, state, AddCat.name, "Введите название категории (игры):")


@admin.message(AddCat.name, F.text)
async def acat_name(m: Message, state: FSMContext):
    await state.update_data(name=m.text.strip())
    await state.set_state(AddCat.photo)
    await m.answer("🖼 Отправьте <b>картинку</b> для категории (как фото) или «-», чтобы пропустить:")


@admin.message(AddCat.photo, F.photo | F.text)
async def acat_photo(m: Message, state: FSMContext):
    if m.photo:
        await state.update_data(photo=m.photo[-1].file_id)
    elif (m.text or "").strip() == "-":
        await state.update_data(photo=None)
    else:
        return await m.answer("Отправьте картинку (как фото) или «-».")
    await state.set_state(AddCat.sub)
    await m.answer("🔐 Обязательная подписка для скриптов этой категории?", reply_markup=sub_choice_kb("acsub"))


@admin.callback_query(F.data.regexp(r"^acsub:[01]$"))
async def acat_sub(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    if "name" not in d:
        return await cb.answer()
    await run("INSERT INTO categories(name,sub_required,photo) VALUES(?,?,?)",
              (d["name"], int(cb.data.split(":")[1]), d.get("photo")))
    await state.clear()
    await show(cb, f"✅ Категория «{esc(d['name'])}» создана.", kb([[btn("⬅️ К категориям", "adm_cats")]]))
    await cb.answer()


async def view_cat(cb: CallbackQuery, cid: int):
    c = await fetchone("SELECT * FROM categories WHERE id=?", (cid,))
    if not c:
        return await cb.answer("Не найдено")
    n = (await fetchone("SELECT COUNT(*) n FROM scripts WHERE category_id=?", (cid,)))["n"]
    text = (f"📂 <b>{esc(c['name'])}</b>\nСкриптов: {n}\n"
            f"Обяз. подписка: {'ВКЛ 🔒' if c['sub_required'] else 'ВЫКЛ 🔓'}\n"
            f"Картинка: {'есть 🖼' if c['photo'] else 'нет'}")
    await show(cb, text, kb([
        [btn("🖼 Картинка категории", f"acatp:{cid}")],
        [btn("🔐 Переключить подписку", f"acatt:{cid}")],
        [btn("🗑 Удалить", f"acatd:{cid}", style="danger")],
        [btn("⬅️ Назад", "adm_cats")]]), c["photo"])
    await cb.answer()


@admin.callback_query(F.data.regexp(r"^acat:\d+$"))
async def acat_view(cb: CallbackQuery):
    await view_cat(cb, int(cb.data.split(":")[1]))


@admin.callback_query(F.data.regexp(r"^acatt:\d+$"))
async def acat_toggle(cb: CallbackQuery):
    cid = int(cb.data.split(":")[1])
    await run("UPDATE categories SET sub_required=1-sub_required WHERE id=?", (cid,))
    await view_cat(cb, cid)


@admin.callback_query(F.data.regexp(r"^acatd:\d+$"))
async def acat_del(cb: CallbackQuery):
    cid = cb.data.split(":")[1]
    await show(cb, "⚠️ Удалить категорию <b>вместе со всеми скриптами</b>?", kb([
        [btn("Да, удалить", f"acatdy:{cid}", style="danger"), btn("Отмена", f"acat:{cid}")]]))
    await cb.answer()


@admin.callback_query(F.data.regexp(r"^acatdy:\d+$"))
async def acat_del_yes(cb: CallbackQuery):
    cid = int(cb.data.split(":")[1])
    await run("DELETE FROM scripts WHERE category_id=?", (cid,))
    await run("DELETE FROM categories WHERE id=?", (cid,))
    await cb.answer("Удалено")
    await admin_cats(cb)


# ───────────────────────── АДМИНКА: картинки (категории / скрипты) ─────────────────────────
@admin.callback_query(F.data.regexp(r"^(acatp|ascrp):\d+$"))
async def photo_ask(cb: CallbackQuery, state: FSMContext):
    kind, tid = cb.data.split(":")
    await ask(cb, state, SetPhoto.photo,
              "🖼 Отправьте <b>картинку</b> (как фото).\n«-» — убрать текущую картинку.",
              {"kind": kind, "tid": int(tid)})


@admin.message(SetPhoto.photo, F.photo | F.text)
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
    await m.answer("✅ Картинка сохранена" if fid else "🗑 Картинка убрана", reply_markup=admin_kb())


# ───────────────────────── АДМИНКА: скрипты ─────────────────────────
@admin.callback_query(F.data == "adm_scripts")
async def adm_scripts(cb: CallbackQuery):
    await show(cb, "📜 <b>Скрипты</b>", kb([
        [btn("➕ Добавить скрипт", "ascr_add", style="success")],
        [btn("📥 Массовая заливка", "bulk_scr", style="primary")],
        [btn("📋 Управление скриптами", "ascr_list")],
        [btn("📤 Экспорт (бэкап скриптов)", "adm_export")],
        [btn("⬅️ Назад", "adm")]]))
    await cb.answer()


@admin.callback_query(F.data.regexp(r"^ascr_add(:\d+)?$"))
async def ascr_add(cb: CallbackQuery):
    cats = await fetchall("SELECT * FROM categories ORDER BY name")
    if not cats:
        return await cb.answer("Сначала создайте категорию", show_alert=True)
    rows = paginate(cats, page_of(cb.data) if ":" in cb.data else 0, CATS_PER_PAGE,
                    lambda c: btn(c["name"], f"ascat:{c['id']}"), "ascr_add")
    rows.append([btn("⬅️ Назад", "adm_scripts")])
    await show(cb, "Выберите категорию для скрипта:", kb(rows))
    await cb.answer()


@admin.callback_query(F.data.regexp(r"^ascat:\d+$"))
async def ascr_cat(cb: CallbackQuery, state: FSMContext):
    await ask(cb, state, AddScript.title, "Введите <b>название</b> скрипта:", {"cid": int(cb.data.split(":")[1])})


@admin.message(AddScript.title, F.text)
async def ascr_title(m: Message, state: FSMContext):
    await state.update_data(title=m.text.strip())
    await state.set_state(AddScript.desc)
    await m.answer("Введите <b>описание</b> (или «-» чтобы пропустить):")


@admin.message(AddScript.desc, F.text)
async def ascr_desc(m: Message, state: FSMContext):
    await state.update_data(desc="" if m.text.strip() == "-" else m.text.strip())
    await state.set_state(AddScript.content)
    await m.answer("Отправьте <b>сам скрипт</b>: текстом (код) или файлом (.lua/.txt).\n"
                   "Длинные скрипты лучше файлом.")


@admin.message(AddScript.content, F.text | F.document)
async def ascr_content(m: Message, state: FSMContext):
    if m.document:
        await state.update_data(file_id=m.document.file_id, content=None)
    else:
        await state.update_data(content=m.text, file_id=None)
    await state.set_state(AddScript.photo)
    await m.answer("🖼 Отправьте <b>картинку</b> для скрипта (как фото) или «-», чтобы пропустить:")


@admin.message(AddScript.photo, F.photo | F.text)
async def ascr_photo(m: Message, state: FSMContext):
    if m.photo:
        await state.update_data(photo=m.photo[-1].file_id)
    elif (m.text or "").strip() == "-":
        await state.update_data(photo=None)
    else:
        return await m.answer("Отправьте картинку (как фото) или «-».")
    await state.set_state(AddScript.sub)
    await m.answer("🔐 Обязательная подписка для получения этого скрипта?", reply_markup=sub_choice_kb("asub"))


@admin.callback_query(F.data.regexp(r"^asub:[01]$"))
async def ascr_sub(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    if "title" not in d:
        return await cb.answer()
    await run("INSERT INTO scripts(category_id,title,description,content,file_id,sub_required,created,photo) VALUES(?,?,?,?,?,?,?,?)",
              (d["cid"], d["title"], d.get("desc", ""), d.get("content"), d.get("file_id"),
               int(cb.data.split(":")[1]), int(time.time()), d.get("photo")))
    await state.clear()
    await show(cb, f"✅ Скрипт «{esc(d['title'])}» добавлен.", kb([[btn("⬅️ В админку", "adm")]]))
    await cb.answer()


@admin.callback_query(F.data.regexp(r"^ascr_list(:\d+)?$"))
async def ascr_list(cb: CallbackQuery):
    cats = await fetchall("SELECT * FROM categories ORDER BY name")
    rows = paginate(cats, page_of(cb.data) if ":" in cb.data else 0, CATS_PER_PAGE,
                    lambda c: btn(c["name"], f"aslc:{c['id']}"), "ascr_list")
    rows.append([btn("⬅️ Назад", "adm_scripts")])
    await show(cb, "Выберите категорию:", kb(rows))
    await cb.answer()


async def scripts_in_cat(cb: CallbackQuery, cid: int, page: int = 0):
    items = await fetchall("SELECT id,title,sub_required,photo FROM scripts WHERE category_id=? ORDER BY id DESC", (cid,))
    rows = paginate(items, page, PER_PAGE,
                    lambda s: btn(("🔒 " if s["sub_required"] else "") + ("🖼 " if s["photo"] else "") + s["title"], f"ascr:{s['id']}"),
                    f"aslc:{cid}")
    rows.append([btn("⬅️ Назад", "ascr_list")])
    await show(cb, "Скрипты категории:\n🔒 — подписка · 🖼 — есть картинка" if items else "Тут пусто.", kb(rows))


@admin.callback_query(F.data.regexp(r"^aslc:\d+(:\d+)?$"))
async def ascr_in_cat(cb: CallbackQuery):
    parts = cb.data.split(":")
    await scripts_in_cat(cb, int(parts[1]), int(parts[2]) if len(parts) > 2 else 0)
    await cb.answer()


async def view_script(cb: CallbackQuery, sid: int):
    s = await fetchone("SELECT * FROM scripts WHERE id=?", (sid,))
    if not s:
        return await cb.answer("Не найдено")
    text = (f"<b>{esc(s['title'])}</b>\nПросмотров: {s['views']}\n"
            f"Обяз. подписка: {'ВКЛ 🔒' if s['sub_required'] else 'ВЫКЛ 🔓'}\n"
            f"Картинка: {'есть 🖼' if s['photo'] else 'нет'}")
    await show(cb, text, kb([
        [btn("🖼 Картинка скрипта", f"ascrp:{sid}")],
        [btn("🔐 Переключить подписку", f"ascrt:{sid}")],
        [btn("🗑 Удалить", f"ascrd:{sid}", style="danger")],
        [btn("⬅️ Назад", f"aslc:{s['category_id']}")]]), s["photo"])
    await cb.answer()


@admin.callback_query(F.data.regexp(r"^ascr:\d+$"))
async def ascr_view(cb: CallbackQuery):
    await view_script(cb, int(cb.data.split(":")[1]))


@admin.callback_query(F.data.regexp(r"^ascrt:\d+$"))
async def ascr_toggle(cb: CallbackQuery):
    sid = int(cb.data.split(":")[1])
    await run("UPDATE scripts SET sub_required=1-sub_required WHERE id=?", (sid,))
    await view_script(cb, sid)


@admin.callback_query(F.data.regexp(r"^ascrd:\d+$"))
async def ascr_del(cb: CallbackQuery):
    sid = int(cb.data.split(":")[1])
    s = await fetchone("SELECT category_id FROM scripts WHERE id=?", (sid,))
    await run("DELETE FROM scripts WHERE id=?", (sid,))
    await cb.answer("Удалено")
    if s:
        await scripts_in_cat(cb, s["category_id"])


# ───────────────────────── АДМИНКА: массовая заливка ─────────────────────────
SEP = "/n"

BULK_SCR_HELP = (
    "📥 <b>Массовая заливка скриптов</b>\n\n"
    "Каждый скрипт — с новой строки, поля разделяются <code>/n</code>:\n\n"
    "<code>название /n описание /n скрипт /n категория</code>\n\n"
    "Пример:\n"
    "<code>Auto Farm /n Автофарм монет /n loadstring(game:HttpGet(\"https://site.com/a.lua\"))() /n Blox Fruits\n"
    "Speed Hub /n - /n print(\"hello\") /n Pet Simulator 99</code>\n\n"
    "• Нет описания — поставьте «-»\n"
    "• Категории нет в боте — создам автоматически\n"
    "• Сам скрипт может быть на нескольких строках, но внутри него не должно быть текста «/n»\n"
    "• Много скриптов — отправьте <b>.txt файлом</b> (сообщение в Telegram ограничено 4096 символами). "
    "Можно слать и несколькими сообщениями, главное, чтобы каждая запись была целиком в одном.\n"
    "• Картинки при массовой заливке не добавляются — их можно поставить потом в «Управление скриптами».\n\n"
    "Обязательную подписку для загружаемых скриптов включайте кнопкой ниже <b>до</b> отправки."
)

BULK_CATS_HELP = (
    "📥 <b>Массовое добавление категорий</b>\n\n"
    "Отправьте названия игр — <b>каждое с новой строки</b> (текстом или .txt файлом):\n\n"
    "<code>Blox Fruits\nPet Simulator 99\nBlade Ball</code>\n\n"
    "Уже существующие категории пропущу. Обязательную подписку для новых категорий включайте кнопкой ниже <b>до</b> отправки."
)


def bulk_kb(kind: str, sub: int):
    done = "adm_scripts" if kind == "scr" else "adm_cats"
    return kb([
        [btn(f"🔐 Обяз. подписка: {'ВКЛ 🔒' if sub else 'ВЫКЛ 🔓'}", f"bulkt:{kind}")],
        [btn("✅ Готово", done, style="success")],
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


async def get_or_create_category(name: str, sub: int, cache: dict, created: list):
    key = name.lower()
    if key in cache:
        return cache[key]
    row = await fetchone("SELECT id FROM categories WHERE pylower(name)=?", (key,))
    if row:
        cid = row["id"]
    else:
        cid = await run("INSERT INTO categories(name,sub_required) VALUES(?,?)", (name, sub))
        created.append(name)
    cache[key] = cid
    return cid


@admin.callback_query(F.data == "bulk_scr")
async def bulk_scr(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(BulkScripts.data)
    await state.update_data(sub=0)
    await show(cb, BULK_SCR_HELP, bulk_kb("scr", 0))
    await cb.answer()


@admin.callback_query(F.data == "bulk_cats")
async def bulk_cats(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(BulkCats.data)
    await state.update_data(sub=0)
    await show(cb, BULK_CATS_HELP, bulk_kb("cats", 0))
    await cb.answer()


@admin.callback_query(F.data.regexp(r"^bulkt:(scr|cats)$"))
async def bulk_toggle(cb: CallbackQuery, state: FSMContext):
    kind = cb.data.split(":")[1]
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


@admin.message(BulkScripts.data, F.text | F.document)
async def bulk_scr_msg(m: Message, state: FSMContext):
    text = await read_text(m)
    if text is None:
        return
    sub = int((await state.get_data()).get("sub", 0))
    records, errors = parse_bulk(text)
    cache, created = {}, []
    for r in records:
        cid = await get_or_create_category(r["category"], 0, cache, created)
        await run(
            "INSERT INTO scripts(category_id,title,description,content,file_id,sub_required,created) VALUES(?,?,?,?,?,?,?)",
            (cid, r["title"], r["desc"], r["content"], None, sub, int(time.time())))
    msg = f"✅ Добавлено скриптов: <b>{len(records)}</b>"
    if created:
        msg += f"\n🆕 Новые категории ({len(created)}): " + esc(", ".join(created))
    if errors:
        msg += f"\n\n⚠️ Пропущено записей: <b>{len(errors)}</b>\n" + "\n".join("• " + esc(e) for e in errors[:10])
        if len(errors) > 10:
            msg += f"\n… и ещё {len(errors) - 10}"
    msg += "\n\nМожно отправить ещё или нажать «Готово»."
    await m.answer(msg, reply_markup=bulk_kb("scr", sub))


@admin.message(BulkCats.data, F.text | F.document)
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
    msg = f"✅ Добавлено категорий: <b>{len(created)}</b>"
    if skipped:
        msg += f"\n↪️ Уже были: {skipped}"
    msg += "\n\nМожно отправить ещё или нажать «Готово»."
    await m.answer(msg, reply_markup=bulk_kb("cats", sub))


@admin.callback_query(F.data == "adm_export")
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
        caption=(f"📤 Экспорт: {len(rows)} скриптов, {len(cats)} категорий.\n"
                 "Этот файл можно отправить в «Массовая заливка», чтобы восстановить скрипты."
                 + (f"\n⚠️ {only_files} скриптов, загруженных файлами, в экспорт не вошли." if only_files else "")))
    await cb.answer()


# ───────────────────────── АДМИНКА: бэкап базы ─────────────────────────
@admin.callback_query(F.data == "adm_backup")
async def adm_backup(cb: CallbackQuery):
    if not BACKUP_CHAT_ID:
        await show(cb, BACKUP_HELP, kb([[btn("⬅️ Назад", "adm")]]))
        return await cb.answer()
    await cb.answer("Делаю бэкап…")
    try:
        await make_backup(cb.bot)
        text = "✅ Бэкап базы отправлен в чат бэкапов и закреплён."
    except Exception as e:
        text = f"❌ Не получилось: {esc(str(e))}"
    await show(cb, text, kb([[btn("⬅️ Назад", "adm")]]))


# ───────────────────────── АДМИНКА: пост и рассылка ─────────────────────────
@admin.callback_query(F.data == "adm_post")
async def adm_post(cb: CallbackQuery, state: FSMContext):
    ch = await get_setting("post_channel")
    if not ch:
        await show(cb, "Сначала укажите канал для постов: ✏️ Тексты → «Канал для постов».\n"
                       "Бот должен быть админом канала.", kb([[btn("⬅️ Назад", "adm")]]))
        return await cb.answer()
    await ask(cb, state, PostS.msg, f"Отправьте пост (текст/фото/видео), он уйдёт в {esc(ch)} с кнопкой «Открыть бота»:")


@admin.message(PostS.msg)
async def post_preview(m: Message, state: FSMContext):
    await state.update_data(mid=m.message_id, cid=m.chat.id)
    await m.answer("👆 Так будет выглядеть пост. Опубликовать?", reply_markup=kb([
        [btn("✅ Опубликовать", "post_go", style="success"), btn("❌ Отмена", "adm", style="danger")]]))


@admin.callback_query(F.data == "post_go")
async def post_go(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    if "mid" not in d:
        return await cb.answer()
    me = await cb.bot.get_me()
    try:
        await cb.bot.copy_message(chat_ref(await get_setting("post_channel")), d["cid"], d["mid"],
                                  reply_markup=kb([[btn("🤖 Открыть бота", url=f"https://t.me/{me.username}")]]))
        res = "✅ Пост опубликован."
    except Exception as e:
        res = f"❌ Ошибка: {esc(str(e))}"
    await state.clear()
    await show(cb, res, kb([[btn("⬅️ В админку", "adm")]]))
    await cb.answer()


@admin.callback_query(F.data == "adm_bc")
async def adm_bc(cb: CallbackQuery, state: FSMContext):
    await ask(cb, state, Bc.msg, "Отправьте сообщение для рассылки (текст/фото/видео — что угодно):")


@admin.message(Bc.msg)
async def bc_preview(m: Message, state: FSMContext):
    n = (await fetchone("SELECT COUNT(*) n FROM users WHERE banned=0"))["n"]
    await state.update_data(mid=m.message_id, cid=m.chat.id)
    await m.answer(f"👆 Разослать это {n} пользователям?", reply_markup=kb([
        [btn("✅ Отправить", "bc_go", style="success"), btn("❌ Отмена", "adm", style="danger")]]))


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
    await bot.send_message(admin_chat, f"📢 Рассылка завершена\n✅ Доставлено: {ok}\n❌ Не доставлено: {fail}")


@admin.callback_query(F.data == "bc_go")
async def bc_go(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    if "mid" not in d:
        return await cb.answer()
    await state.clear()
    spawn(do_broadcast(cb.bot, cb.message.chat.id, d["cid"], d["mid"]))
    await show(cb, "🚀 Рассылка запущена, по завершении пришлю отчёт.", kb([[btn("⬅️ В админку", "adm")]]))
    await cb.answer()


# ───────────────────────── АДМИНКА: бан ─────────────────────────
@admin.callback_query(F.data.in_({"adm_ban", "adm_unban"}))
async def adm_ban(cb: CallbackQuery, state: FSMContext):
    ban = cb.data == "adm_ban"
    await ask(cb, state, Ban.target, f"Введите ID или @username пользователя для {'бана' if ban else 'разбана'}:", {"ban": ban})


@admin.message(Ban.target, F.text)
async def ban_target(m: Message, state: FSMContext):
    ban = (await state.get_data()).get("ban", True)
    t = m.text.strip().lstrip("@")
    if t.isdigit():
        row = await fetchone("SELECT id, username FROM users WHERE id=?", (int(t),))
    else:
        row = await fetchone("SELECT id, username FROM users WHERE pylower(username)=?", (t.lower(),))
    if not row:
        return await m.answer("Пользователь не найден (он должен был хотя бы раз запустить бота). Попробуйте ещё раз или /cancel")
    if (row["username"] or "").lower() == ADMIN_USERNAME.lower() or row["id"] in ADMIN_IDS:
        return await m.answer("Админа банить нельзя 🙂")
    await run("UPDATE users SET banned=? WHERE id=?", (1 if ban else 0, row["id"]))
    await state.clear()
    await m.answer(f"{'🚫 Забанен' if ban else '✅ Разбанен'}: <code>{row['id']}</code>", reply_markup=admin_kb())


# ───────────────────────── АДМИНКА: статистика ─────────────────────────
@admin.callback_query(F.data == "adm_stats")
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
    top = await fetchall("SELECT title,views FROM scripts ORDER BY views DESC LIMIT 5")
    top_txt = "\n".join(f"{i}. {esc(t['title'])} — {t['views']}" for i, t in enumerate(top, 1)) or "—"
    text = (f"📊 <b>Активность</b>\n\n👥 Всего пользователей: {total}\n🆕 Новых за 24ч: {new1}\n"
            f"🔥 Активных за 24ч: {d1}\n📅 Активных за 7 дней: {d7}\n🚫 Забанено: {banned}\n\n"
            f"📂 Категорий: {cats}\n📜 Скриптов: {scr}\n🔎 Поисков: {searches}\n👁 Открытий скриптов: {views}\n\n"
            f"🏆 <b>Топ скриптов:</b>\n{top_txt}")
    await show(cb, text, kb([[btn("🔄 Обновить", "adm_stats")], [btn("⬅️ Назад", "adm")]]))
    await cb.answer()


# ───────────────────────── АДМИНКА: дизайн / картинки / тексты ─────────────────────────
@admin.callback_query(F.data == "adm_design")
async def adm_design(cb: CallbackQuery):
    await show(cb, "🎨 <b>Дизайн</b>", kb([
        [btn("🖼 Картинки вкладок", "adm_imgs")],
        [btn("✏️ Текст приветствия", "aset:start_text")],
        [btn("Кнопка «Поиск»", "aset:btn_search")],
        [btn("Кнопка «Категории»", "aset:btn_categories")],
        [btn("Кнопка «Поддержка»", "aset:btn_support")],
        [btn("⬅️ Назад", "adm")]]))
    await cb.answer()


@admin.callback_query(F.data == "adm_imgs")
async def adm_imgs(cb: CallbackQuery):
    rows = []
    for key, label in PHOTO_KEYS.items():
        mark = "✅" if await get_setting(key) else "⬜"
        rows.append([btn(f"{mark} {label}", f"aset:{key}")])
    rows.append([btn("⬅️ Назад", "adm")])
    await show(cb, "🖼 <b>Картинки вкладок</b>\n\n✅ — картинка установлена. Нажмите на вкладку, чтобы поставить или убрать.\n\n"
                   "Картинки <b>категорий</b> и <b>скриптов</b> ставятся в их карточках: "
                   "📂 Категории / 📜 Скрипты → Управление → 🖼 Картинка.", kb(rows))
    await cb.answer()


@admin.callback_query(F.data == "adm_texts")
async def adm_texts(cb: CallbackQuery):
    keys = ["support_text", "support_url", "search_prompt", "sub_text", "banned_text", "post_channel"]
    rows = [[btn(LABELS[k], f"aset:{k}")] for k in keys] + [[btn("⬅️ Назад", "adm")]]
    await show(cb, "✏️ <b>Тексты и настройки</b>", kb(rows))
    await cb.answer()


@admin.callback_query(F.data.regexp(r"^aset:\w+$"))
async def aset(cb: CallbackQuery, state: FSMContext):
    key = cb.data.split(":", 1)[1]
    if key in PHOTO_KEYS:
        hint = f"🖼 <b>{PHOTO_KEYS[key]}</b>\n\nОтправьте картинку (как фото).\n«-» — убрать картинку."
    elif key in LABELS:
        cur = esc(await get_setting(key)) or "—"
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


@admin.message(Edit.value)
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
    await m.answer("✅ Сохранено", reply_markup=admin_kb())


# ───────────────────────── АДМИНКА: обязательная подписка (каналы) ─────────────────────────
async def admin_subs(cb: CallbackQuery):
    chs = await fetchall("SELECT * FROM channels")
    rows = [[btn(f"🗑 {c['title']}", f"chd:{c['id']}")] for c in chs]
    rows.append([btn("➕ Добавить канал", "chadd", style="success")])
    rows.append([btn("⬅️ Назад", "adm")])
    await show(cb, "🔐 <b>Каналы для обязательной подписки</b>\n\nНажмите на канал, чтобы удалить.\n"
                   "Бот должен быть админом в этих каналах. Включать подписку нужно у категорий / скриптов.", kb(rows))


@admin.callback_query(F.data == "adm_subs")
async def adm_subs(cb: CallbackQuery):
    await admin_subs(cb)
    await cb.answer()


@admin.callback_query(F.data == "chadd")
async def ch_add(cb: CallbackQuery, state: FSMContext):
    await ask(cb, state, Chan.chat, "Отправьте @username канала или его ID (-100...):")


@admin.message(Chan.chat, F.text)
async def ch_add_msg(m: Message, state: FSMContext):
    ref = m.text.strip()
    if not ref.lstrip("-").isdigit() and not ref.startswith("@"):
        ref = "@" + ref
    try:
        chat = await m.bot.get_chat(chat_ref(ref))
    except Exception as e:
        return await m.answer(f"❌ Не нашёл канал: {esc(str(e))}\nДобавьте бота админом и повторите.")
    url = f"https://t.me/{chat.username}" if chat.username else chat.invite_link
    if not url:
        try:
            url = await m.bot.export_chat_invite_link(chat.id)
        except Exception:
            url = None
    await run("INSERT INTO channels(chat_id,title,url) VALUES(?,?,?)", (str(chat.id), chat.title or ref, url))
    await state.clear()
    await m.answer(f"✅ Канал «{esc(chat.title or ref)}» добавлен.", reply_markup=admin_kb())


@admin.callback_query(F.data.regexp(r"^chd:\d+$"))
async def ch_del(cb: CallbackQuery):
    await run("DELETE FROM channels WHERE id=?", (int(cb.data.split(":")[1]),))
    await cb.answer("Удалено")
    await admin_subs(cb)


# ───────────────────────── ЗАПУСК ─────────────────────────
async def main():
    global db, BOT_REF
    logging.basicConfig(level=logging.INFO)

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    BOT_REF = bot

    await try_restore(bot)  # если базы нет, а бэкап есть — восстановим

    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.create_function("pylower", 1, lambda s: s.lower() if isinstance(s, str) else s)
    await db.executescript(SCHEMA)
    await ensure_column("categories", "photo", "TEXT")  # миграция старых баз
    await ensure_column("scripts", "photo", "TEXT")
    await db.commit()

    if BACKUP_CHAT_ID:
        spawn(backup_loop())
        logging.info("Авто-бэкап включён (чат %s)", BACKUP_CHAT_ID)
    else:
        logging.warning("BACKUP_CHAT_ID не задан: бэкап базы выключен")

    dp = Dispatcher(storage=MemoryStorage())
    dp.message.outer_middleware(Track())
    dp.callback_query.outer_middleware(Track())
    for r in (cmd_router, admin, user):
        r.message.filter(F.chat.type == "private")
    dp.include_routers(cmd_router, admin, user)  # порядок важен
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
