import asyncio
import logging
import os
import time
from html import escape as esc

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
                           InlineKeyboardButton, InlineKeyboardMarkup, Message)

# ───────────────────────── НАСТРОЙКИ ─────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")  # токен задаётся в «Переменных окружения» хоста, не в коде!
if not BOT_TOKEN:
    raise SystemExit("Не задана переменная окружения BOT_TOKEN")
ADMIN_USERNAME = "ruxdev"          # без @
ADMIN_IDS: set[int] = set()        # (необязательно) можно добавить ID админа для надёжности
# База лежит в постоянной папке данных (на хосте это /app/data), чтобы не пропадать при пересборке
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except OSError:
    DATA_DIR = "."
DB_PATH = os.path.join(DATA_DIR, "bot.db")
if not os.path.exists(DB_PATH) and os.path.exists("bot.db"):  # перенос старой базы, если она есть
    import shutil
    shutil.copy("bot.db", DB_PATH)
START_IMAGE_FILE = "start.jpg"     # запасной способ: положи картинку рядом с bot.py
PER_PAGE = 8

DEFAULTS = {
    "start_text": "<b>Scripts Agent</b> - Твой каталог скриптов для Roblox без подписок на множество каналов!\n\nНачинай прямо сейчас.",
    "start_photo": "",
    "btn_search": "ПОИСК",
    "btn_categories": "Категории",
    "btn_support": "Поддержка",
    "support_text": "🆘 <b>Поддержка</b>\n\nЕсли возник вопрос или проблема — напишите нам.",
    "support_url": "",
    "search_prompt": "🔍 Введите название скрипта или игры:",
    "sub_text": "🔐 Чтобы получить этот скрипт, подпишитесь на каналы ниже и нажмите «Проверить подписку».",
    "banned_text": "🚫 Вы заблокированы в этом боте.",
    "post_channel": "",
}
HTML_KEYS = {"start_text", "support_text", "search_prompt", "sub_text", "banned_text"}
LABELS = {
    "start_text": "Текст приветствия", "btn_search": "Кнопка «Поиск»",
    "btn_categories": "Кнопка «Категории»", "btn_support": "Кнопка «Поддержка»",
    "support_text": "Текст поддержки", "support_url": "Ссылка поддержки (@user или URL)",
    "search_prompt": "Текст поиска", "sub_text": "Текст обязательной подписки",
    "banned_text": "Текст для забаненных", "post_channel": "Канал для постов (@канал или ID)",
}

# ───────────────────────── БАЗА ─────────────────────────
db: aiosqlite.Connection
SCHEMA = """
CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, joined INTEGER, last_active INTEGER, banned INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS categories(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, sub_required INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS scripts(id INTEGER PRIMARY KEY AUTOINCREMENT, category_id INTEGER, title TEXT, description TEXT, content TEXT, file_id TEXT, sub_required INTEGER DEFAULT 0, views INTEGER DEFAULT 0, created INTEGER);
CREATE TABLE IF NOT EXISTS channels(id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, title TEXT, url TEXT);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, kind TEXT, ts INTEGER);
"""


async def fetchall(sql, args=()):
    async with db.execute(sql, args) as c:
        return await c.fetchall()


async def fetchone(sql, args=()):
    async with db.execute(sql, args) as c:
        return await c.fetchone()


async def run(sql, args=()):
    cur = await db.execute(sql, args)
    await db.commit()
    return cur.lastrowid


async def get_setting(key):
    row = await fetchone("SELECT value FROM settings WHERE key=?", (key,))
    return row["value"] if row else DEFAULTS.get(key, "")


async def set_setting(key, value):
    if value == "":
        await run("DELETE FROM settings WHERE key=?", (key,))
    else:
        await run("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))


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


async def show(cb: CallbackQuery, text, markup=None):
    """Редактирует сообщение; если нельзя (например, это фото) — удаляет и отправляет новое."""
    try:
        if cb.message.photo or cb.message.document:
            raise ValueError
        await cb.message.edit_text(text, reply_markup=markup, disable_web_page_preview=True)
    except Exception:
        try:
            await cb.message.delete()
        except Exception:
            pass
        await cb.message.answer(text, reply_markup=markup, disable_web_page_preview=True)


def chat_ref(v: str):
    v = v.strip()
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
    sub = State()


class AddScript(StatesGroup):
    title = State()
    desc = State()
    content = State()
    sub = State()


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


# ───────────────────────── РОУТЕРЫ ─────────────────────────
cmd_router = Router()
admin = Router()
user = Router()
admin.message.filter(IsAdmin())
admin.callback_query.filter(IsAdmin())


# ───────────────────────── /start ─────────────────────────
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


@cmd_router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    await send_start(m)


@cmd_router.message(Command("cancel"))
async def cmd_cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Отменено.")


def admin_kb():
    return kb([
        [btn("📜 Скрипты", "adm_scripts"), btn("📂 Категории", "adm_cats")],
        [btn("📝 Создать пост", "adm_post"), btn("📢 Рассылка", "adm_bc")],
        [btn("🚫 Бан", "adm_ban"), btn("✅ Разбан", "adm_unban")],
        [btn("📊 Активность", "adm_stats"), btn("🎨 Дизайн", "adm_design")],
        [btn("✏️ Тексты", "adm_texts"), btn("🔐 Обяз. подписка", "adm_subs")],
        [btn("❌ Закрыть", "adm_close", style="danger")],
    ])


@cmd_router.message(Command("admin"))
async def cmd_admin(m: Message, state: FSMContext):
    if not is_admin(m.from_user):
        return
    await state.clear()
    await m.answer("⚙️ <b>Админ-панель</b>", reply_markup=admin_kb())


# ───────────────────────── ПОЛЬЗОВАТЕЛЬ ─────────────────────────
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
    await show(cb, await get_setting("support_text"), kb(rows))
    await cb.answer()


@user.callback_query(F.data == "search")
async def cb_search(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Search.query)
    await show(cb, await get_setting("search_prompt"), kb([[btn("⬅️ В меню", "menu")]]))
    await cb.answer()


@user.message(Search.query, F.text)
async def do_search(m: Message):
    q = m.text.strip().lower()
    await run("INSERT INTO events(user_id,kind,ts) VALUES(?,?,?)", (m.from_user.id, "search", int(time.time())))
    like = f"%{q}%"
    rows = await fetchall(
        "SELECT s.id, s.title, c.name FROM scripts s LEFT JOIN categories c ON c.id=s.category_id "
        "WHERE pylower(s.title) LIKE ? OR pylower(s.description) LIKE ? OR pylower(c.name) LIKE ? "
        "ORDER BY s.views DESC LIMIT 20", (like, like, like))
    if not rows:
        await m.answer("😕 Ничего не найдено. Попробуйте другой запрос.", reply_markup=kb([[btn("⬅️ В меню", "menu")]]))
        return
    buttons = [[btn(f"{r['title']} — {r['name']}" if r["name"] else r["title"], f"scr:{r['id']}")] for r in rows]
    buttons.append([btn("⬅️ В меню", "menu")])
    await m.answer(f"🔎 Найдено: {len(rows)}", reply_markup=kb(buttons))


@user.callback_query(F.data == "cats")
async def cb_cats(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    cats = await fetchall("SELECT * FROM categories ORDER BY name")
    if not cats:
        await show(cb, "📂 Категорий пока нет.", kb([[btn("⬅️ Назад", "menu")]]))
        return await cb.answer()
    rows, line = [], []
    for c in cats:
        line.append(btn(("🔒 " if c["sub_required"] else "") + c["name"], f"cat:{c['id']}:0"))
        if len(line) == 2:
            rows.append(line)
            line = []
    if line:
        rows.append(line)
    rows.append([btn("⬅️ Назад", "menu")])
    await show(cb, "📂 <b>Выберите игру:</b>", kb(rows))
    await cb.answer()


@user.callback_query(F.data.startswith("cat:"))
async def cb_cat(cb: CallbackQuery):
    _, cid, page = cb.data.split(":")
    cid, page = int(cid), int(page)
    cat = await fetchone("SELECT * FROM categories WHERE id=?", (cid,))
    if not cat:
        return await cb.answer("Категория не найдена", show_alert=True)
    total = (await fetchone("SELECT COUNT(*) n FROM scripts WHERE category_id=?", (cid,)))["n"]
    rows = await fetchall("SELECT id,title FROM scripts WHERE category_id=? ORDER BY id DESC LIMIT ? OFFSET ?",
                          (cid, PER_PAGE, page * PER_PAGE))
    buttons = [[btn(r["title"], f"scr:{r['id']}")] for r in rows]
    nav = []
    if page > 0:
        nav.append(btn("◀️", f"cat:{cid}:{page - 1}"))
    if (page + 1) * PER_PAGE < total:
        nav.append(btn("▶️", f"cat:{cid}:{page + 1}"))
    if nav:
        buttons.append(nav)
    buttons.append([btn("⬅️ Назад", "cats")])
    text = f"📂 <b>{esc(cat['name'])}</b>\nСкриптов: {total}" if total else f"📂 <b>{esc(cat['name'])}</b>\n\nПока пусто."
    await show(cb, text, kb(buttons))
    await cb.answer()


async def open_script(cb: CallbackQuery, sid: int):
    s = await fetchone(
        "SELECT s.*, c.sub_required AS csub FROM scripts s LEFT JOIN categories c ON c.id=s.category_id WHERE s.id=?", (sid,))
    if not s:
        return await cb.answer("Скрипт не найден", show_alert=True)
    back = kb([[btn("⬅️ Назад", f"cat:{s['category_id']}:0")]])
    if s["sub_required"] or s["csub"]:
        missing = await missing_channels(cb.bot, cb.from_user.id)
        if missing:
            rows = [[btn(f"📢 {ch['title']}", url=ch["url"])] for ch in missing if ch["url"]]
            rows.append([btn("✅ Проверить подписку", f"chk:{sid}", style="success")])
            rows.append([btn("⬅️ Назад", f"cat:{s['category_id']}:0")])
            await show(cb, await get_setting("sub_text"), kb(rows))
            return await cb.answer()
    await run("UPDATE scripts SET views=views+1 WHERE id=?", (sid,))
    await run("INSERT INTO events(user_id,kind,ts) VALUES(?,?,?)", (cb.from_user.id, "view", int(time.time())))
    head = f"<b>{esc(s['title'])}</b>"
    if s["description"]:
        head += f"\n\n{esc(s['description'])}"
    content, as_file = s["content"] or "", None
    text = head
    if content:
        if len(head) + len(content) < 3800:
            text += f"\n\n<pre>{esc(content)}</pre>"
        else:
            as_file = BufferedInputFile(content.encode("utf-8"), filename="script.lua")
    await cb.message.answer(text, reply_markup=back)
    if as_file:
        await cb.message.answer_document(as_file)
    if s["file_id"]:
        await cb.message.answer_document(s["file_id"])
    await cb.answer()


@user.callback_query(F.data.startswith("scr:"))
async def cb_scr(cb: CallbackQuery):
    await open_script(cb, int(cb.data.split(":")[1]))


@user.callback_query(F.data.startswith("chk:"))
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


async def ask(cb: CallbackQuery, state: FSMContext, st, text):
    await state.set_state(st)
    await show(cb, text, kb([[btn("❌ Отмена", "adm")]]))
    await cb.answer()


# ───────────────────────── АДМИНКА: категории ─────────────────────────
@admin.callback_query(F.data == "adm_cats")
async def adm_cats(cb: CallbackQuery):
    cats = await fetchall("SELECT * FROM categories ORDER BY name")
    rows = [[btn(("🔒 " if c["sub_required"] else "") + c["name"], f"acat:{c['id']}")] for c in cats]
    rows.append([btn("➕ Добавить категорию", "acat_add", style="success")])
    rows.append([btn("⬅️ Назад", "adm")])
    await show(cb, "📂 <b>Категории (игры)</b>", kb(rows))
    await cb.answer()


@admin.callback_query(F.data == "acat_add")
async def acat_add(cb: CallbackQuery, state: FSMContext):
    await ask(cb, state, AddCat.name, "Введите название категории (игры):")


@admin.message(AddCat.name, F.text)
async def acat_name(m: Message, state: FSMContext):
    await state.update_data(name=m.text.strip())
    await state.set_state(AddCat.sub)
    await m.answer("🔐 Обязательная подписка для скриптов этой категории?", reply_markup=sub_choice_kb("acsub"))


@admin.callback_query(F.data.startswith("acsub:"))
async def acat_sub(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    if "name" not in d:
        return await cb.answer()
    await run("INSERT INTO categories(name,sub_required) VALUES(?,?)", (d["name"], int(cb.data.split(":")[1])))
    await state.clear()
    await show(cb, f"✅ Категория «{esc(d['name'])}» создана.", kb([[btn("⬅️ К категориям", "adm_cats")]]))
    await cb.answer()


@admin.callback_query(F.data.startswith("acat:"))
async def acat_view(cb: CallbackQuery):
    cid = int(cb.data.split(":")[1])
    c = await fetchone("SELECT * FROM categories WHERE id=?", (cid,))
    if not c:
        return await cb.answer("Не найдено")
    n = (await fetchone("SELECT COUNT(*) n FROM scripts WHERE category_id=?", (cid,)))["n"]
    st = "ВКЛ 🔒" if c["sub_required"] else "ВЫКЛ 🔓"
    await show(cb, f"📂 <b>{esc(c['name'])}</b>\nСкриптов: {n}\nОбяз. подписка: {st}", kb([
        [btn("🔐 Переключить подписку", f"acatt:{cid}")],
        [btn("🗑 Удалить", f"acatd:{cid}", style="danger")],
        [btn("⬅️ Назад", "adm_cats")]]))
    await cb.answer()


@admin.callback_query(F.data.startswith("acatt:"))
async def acat_toggle(cb: CallbackQuery):
    cid = int(cb.data.split(":")[1])
    await run("UPDATE categories SET sub_required=1-sub_required WHERE id=?", (cid,))
    cb.data = f"acat:{cid}"
    await acat_view(cb)


@admin.callback_query(F.data.startswith("acatd:"))
async def acat_del(cb: CallbackQuery):
    cid = cb.data.split(":")[1]
    await show(cb, "⚠️ Удалить категорию <b>вместе со всеми скриптами</b>?", kb([
        [btn("Да, удалить", f"acatdy:{cid}", style="danger"), btn("Отмена", f"acat:{cid}")]]))
    await cb.answer()


@admin.callback_query(F.data.startswith("acatdy:"))
async def acat_del_yes(cb: CallbackQuery):
    cid = int(cb.data.split(":")[1])
    await run("DELETE FROM scripts WHERE category_id=?", (cid,))
    await run("DELETE FROM categories WHERE id=?", (cid,))
    await cb.answer("Удалено")
    await adm_cats(cb)


# ───────────────────────── АДМИНКА: скрипты ─────────────────────────
@admin.callback_query(F.data == "adm_scripts")
async def adm_scripts(cb: CallbackQuery):
    await show(cb, "📜 <b>Скрипты</b>", kb([
        [btn("➕ Добавить скрипт", "ascr_add", style="success")],
        [btn("📋 Управление скриптами", "ascr_list")],
        [btn("⬅️ Назад", "adm")]]))
    await cb.answer()


@admin.callback_query(F.data == "ascr_add")
async def ascr_add(cb: CallbackQuery):
    cats = await fetchall("SELECT * FROM categories ORDER BY name")
    if not cats:
        return await cb.answer("Сначала создайте категорию", show_alert=True)
    rows = [[btn(c["name"], f"ascat:{c['id']}")] for c in cats] + [[btn("⬅️ Назад", "adm_scripts")]]
    await show(cb, "Выберите категорию для скрипта:", kb(rows))
    await cb.answer()


@admin.callback_query(F.data.startswith("ascat:"))
async def ascr_cat(cb: CallbackQuery, state: FSMContext):
    await state.update_data(cid=int(cb.data.split(":")[1]))
    await ask(cb, state, AddScript.title, "Введите <b>название</b> скрипта:")


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
    await state.set_state(AddScript.sub)
    await m.answer("🔐 Обязательная подписка для получения этого скрипта?", reply_markup=sub_choice_kb("asub"))


@admin.callback_query(F.data.startswith("asub:"))
async def ascr_sub(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    if "title" not in d:
        return await cb.answer()
    await run("INSERT INTO scripts(category_id,title,description,content,file_id,sub_required,created) VALUES(?,?,?,?,?,?,?)",
              (d["cid"], d["title"], d.get("desc", ""), d.get("content"), d.get("file_id"),
               int(cb.data.split(":")[1]), int(time.time())))
    await state.clear()
    await show(cb, f"✅ Скрипт «{esc(d['title'])}» добавлен.", kb([[btn("⬅️ В админку", "adm")]]))
    await cb.answer()


@admin.callback_query(F.data == "ascr_list")
async def ascr_list(cb: CallbackQuery):
    cats = await fetchall("SELECT * FROM categories ORDER BY name")
    rows = [[btn(c["name"], f"aslc:{c['id']}")] for c in cats] + [[btn("⬅️ Назад", "adm_scripts")]]
    await show(cb, "Выберите категорию:", kb(rows))
    await cb.answer()


@admin.callback_query(F.data.startswith("aslc:"))
async def ascr_in_cat(cb: CallbackQuery):
    cid = int(cb.data.split(":")[1])
    items = await fetchall("SELECT id,title,sub_required FROM scripts WHERE category_id=? ORDER BY id DESC LIMIT 50", (cid,))
    rows = [[btn(("🔒 " if s["sub_required"] else "") + s["title"], f"ascr:{s['id']}")] for s in items]
    rows.append([btn("⬅️ Назад", "ascr_list")])
    await show(cb, "Скрипты категории:" if items else "Тут пусто.", kb(rows))
    await cb.answer()


@admin.callback_query(F.data.startswith("ascr:"))
async def ascr_view(cb: CallbackQuery):
    sid = int(cb.data.split(":")[1])
    s = await fetchone("SELECT * FROM scripts WHERE id=?", (sid,))
    if not s:
        return await cb.answer("Не найдено")
    st = "ВКЛ 🔒" if s["sub_required"] else "ВЫКЛ 🔓"
    await show(cb, f"<b>{esc(s['title'])}</b>\nПросмотров: {s['views']}\nОбяз. подписка: {st}", kb([
        [btn("🔐 Переключить подписку", f"ascrt:{sid}")],
        [btn("🗑 Удалить", f"ascrd:{sid}", style="danger")],
        [btn("⬅️ Назад", f"aslc:{s['category_id']}")]]))
    await cb.answer()


@admin.callback_query(F.data.startswith("ascrt:"))
async def ascr_toggle(cb: CallbackQuery):
    sid = int(cb.data.split(":")[1])
    await run("UPDATE scripts SET sub_required=1-sub_required WHERE id=?", (sid,))
    cb.data = f"ascr:{sid}"
    await ascr_view(cb)


@admin.callback_query(F.data.startswith("ascrd:"))
async def ascr_del(cb: CallbackQuery):
    sid = int(cb.data.split(":")[1])
    s = await fetchone("SELECT category_id FROM scripts WHERE id=?", (sid,))
    await run("DELETE FROM scripts WHERE id=?", (sid,))
    await cb.answer("Удалено")
    if s:
        cb.data = f"aslc:{s['category_id']}"
        await ascr_in_cat(cb)


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


tasks: set = set()


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
    t = asyncio.create_task(do_broadcast(cb.bot, cb.message.chat.id, d["cid"], d["mid"]))
    tasks.add(t)
    t.add_done_callback(tasks.discard)
    await show(cb, "🚀 Рассылка запущена, по завершении пришлю отчёт.", kb([[btn("⬅️ В админку", "adm")]]))
    await cb.answer()


# ───────────────────────── АДМИНКА: бан ─────────────────────────
@admin.callback_query(F.data.in_({"adm_ban", "adm_unban"}))
async def adm_ban(cb: CallbackQuery, state: FSMContext):
    ban = cb.data == "adm_ban"
    await state.update_data(ban=ban)
    await ask(cb, state, Ban.target, f"Введите ID или @username пользователя для {'бана' if ban else 'разбана'}:")


@admin.message(Ban.target, F.text)
async def ban_target(m: Message, state: FSMContext):
    ban = (await state.get_data()).get("ban", True)
    t = m.text.strip().lstrip("@")
    if t.isdigit():
        row = await fetchone("SELECT id,username FROM users WHERE id=?", (int(t),))
    else:
        row = await fetchone("SELECT id,username FROM users WHERE lower(username)=lower(?)", (t,))
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


# ───────────────────────── АДМИНКА: дизайн / тексты ─────────────────────────
@admin.callback_query(F.data == "adm_design")
async def adm_design(cb: CallbackQuery):
    await show(cb, "🎨 <b>Дизайн стартового экрана</b>", kb([
        [btn("🖼 Сменить картинку", "aset:start_photo")],
        [btn("🗑 Убрать картинку", "adelphoto")],
        [btn("✏️ Текст приветствия", "aset:start_text")],
        [btn("Кнопка «Поиск»", "aset:btn_search")],
        [btn("Кнопка «Категории»", "aset:btn_categories")],
        [btn("Кнопка «Поддержка»", "aset:btn_support")],
        [btn("⬅️ Назад", "adm")]]))
    await cb.answer()


@admin.callback_query(F.data == "adm_texts")
async def adm_texts(cb: CallbackQuery):
    keys = ["support_text", "support_url", "search_prompt", "sub_text", "banned_text", "post_channel"]
    rows = [[btn(LABELS[k], f"aset:{k}")] for k in keys] + [[btn("⬅️ Назад", "adm")]]
    await show(cb, "✏️ <b>Тексты и настройки</b>", kb(rows))
    await cb.answer()


@admin.callback_query(F.data == "adelphoto")
async def adel_photo(cb: CallbackQuery):
    await set_setting("start_photo", "")
    await cb.answer("Картинка убрана")


@admin.callback_query(F.data.startswith("aset:"))
async def aset(cb: CallbackQuery, state: FSMContext):
    key = cb.data.split(":", 1)[1]
    await state.update_data(key=key)
    if key == "start_photo":
        hint = "Отправьте новую <b>картинку</b> (как фото, не файлом):"
    else:
        cur = esc(await get_setting(key)) or "—"
        hint = (f"<b>{LABELS[key]}</b>\n\nСейчас:\n<code>{cur}</code>\n\n"
                f"Отправьте новое значение{' (HTML-разметка разрешена)' if key in HTML_KEYS else ''}.\n"
                f"«-» — сбросить на значение по умолчанию.")
    await ask(cb, state, Edit.value, hint)


@admin.message(Edit.value)
async def edit_value(m: Message, state: FSMContext):
    key = (await state.get_data()).get("key")
    if key == "start_photo":
        if not m.photo:
            return await m.answer("Нужна именно картинка 🖼")
        val = m.photo[-1].file_id
    else:
        if not m.text:
            return await m.answer("Нужен текст")
        val = m.html_text if key in HTML_KEYS else m.text.strip()
        if val == "-":
            val = ""
        if key == "support_url" and val.startswith("@"):
            val = "https://t.me/" + val[1:]
    await set_setting(key, val)
    await state.clear()
    await m.answer("✅ Сохранено", reply_markup=admin_kb())


# ───────────────────────── АДМИНКА: обязательная подписка ─────────────────────────
@admin.callback_query(F.data == "adm_subs")
async def adm_subs(cb: CallbackQuery):
    chs = await fetchall("SELECT * FROM channels")
    rows = [[btn(f"🗑 {c['title']}", f"chd:{c['id']}")] for c in chs]
    rows.append([btn("➕ Добавить канал", "chadd", style="success")])
    rows.append([btn("⬅️ Назад", "adm")])
    await show(cb, "🔐 <b>Каналы для обязательной подписки</b>\n\nНажмите на канал, чтобы удалить.\n"
                   "Бот должен быть админом в этих каналах. Включать подписку нужно у категорий / скриптов.",
               kb(rows))
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
    await run("INSERT INTO channels(chat_id,title,url) VALUES(?,?,?)", (str(chat.id), chat.title, url))
    await state.clear()
    await m.answer(f"✅ Канал «{esc(chat.title)}» добавлен.", reply_markup=admin_kb())


@admin.callback_query(F.data.startswith("chd:"))
async def ch_del(cb: CallbackQuery):
    await run("DELETE FROM channels WHERE id=?", (int(cb.data.split(":")[1]),))
    await cb.answer("Удалено")
    await adm_subs(cb)


# ───────────────────────── ЗАПУСК ─────────────────────────
async def main():
    global db
    logging.basicConfig(level=logging.INFO)
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.create_function("pylower", 1, lambda s: s.lower() if isinstance(s, str) else s)
    await db.executescript(SCHEMA)
    await db.commit()

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
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
