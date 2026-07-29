import asyncio
import hashlib
import re
import os
import io
import zipfile
import aiohttp
import urllib.parse
import random
import json
import logging
import time
import asyncpg
from datetime import datetime, timedelta
from collections import deque
from typing import Optional, Dict, Set, List, Tuple, Any
from dataclasses import dataclass, field

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import ChatAdminRequiredError, UserNotParticipantError, RPCError, FloodWaitError
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, InputMediaPhoto
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telegram.constants import ParseMode
from telegram.request import HTTPXRequest
from aiohttp import web

# ==================== تنظیمات اصلی ====================
BOT_TOKEN = "8861568420:AAFpoJ0EMyGhZ4rJ3zC_DEcDHGMII3oiI_U"
API_ID = 32233583
API_HASH = "ce6caac5e6e987ff33fc613d076570a4"
USER_SESSION_STR = "1BJWap1wBu7fjjHtteoJChPEPZ3HOEY1EmLn0pZPI2Fz08EwbRKi37tMVPpsbIp9aQN4D5tVJF8-uOQLtz9uSEJ1nndHfdPOsOQItGD5tOwbnMI7g4taPDk_jDBgGZcVD3CzCoWPDzI0H--GCI_zOUBPIGNbrDczxIaKz3CA9922MX5BsZwu9Kx_M6kmmdgQtAzBBaZ5BqxgqurtAWw6h7BpiAvj5Fc8emVEjEkLNmV26pvP5nkRcfDrZbM9ERVceMhGi1SJJ7EGQOLu0BJUgiKC-IlaXLvqOj-z4Jbhfj7ZXXYz64T32-I9USAnchyp5I3oUKDxy5Oy0zTOVsxj6HgCV-Gf0czc="
OWNER_ID = 8879869880
PORT = int(os.environ.get('PORT', 8080))
DATABASE_URL = os.environ.get('DATABASE_URL', '')
RAILWAY_URL = os.environ.get('RAILWAY_PUBLIC_DOMAIN', '')
WEBHOOK_URL = f"https://{RAILWAY_URL}/webhook" if RAILWAY_URL else ""

# ==================== تنظیمات کانال‌ها ====================
CHANNEL_1_LINK = "https://t.me/v2reya88"
CHANNEL_2_LINK = "https://t.me/confinghub2"
CHANNEL_1_DISPLAY = "@v2reya88"
CHANNEL_2_DISPLAY = "@confinghub2"

CONFIG_TARGET_CHANNEL = CHANNEL_1_DISPLAY
PROXY_TARGET_CHANNEL = CHANNEL_2_DISPLAY

# کانال‌های اسکن کانفیگ
SOURCE_CONFIG_CHANNELS = [
    "@NamazVPN",
    "@Farah_VPN",
    "@ConfigsHUB"
]

# کانال‌های اسکن پروکسی
SOURCE_PROXY_CHANNELS = [
    "@ProxysHUB", "@iMTProto", "@ProxyDaemi", "@iRoProxy",
    "@PinkProxy", "@PyroProxy", "@darkproxy", "@Forall_Proxy",
    "@Myporoxy", "@TelMTProto"
]

# ریجکس‌های تشخیص کانفیگ و پروکسی
CONFIG_REGEX = re.compile(r'(vless|vmess|trojan|ss|ssr|hy2|hysteria2)://[^\s]+', re.IGNORECASE)
PROXY_REGEX = re.compile(r'https://t\.me/proxy\?server=[^\s]+', re.IGNORECASE)

# ==================== تنظیمات لاگ ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== پایگاه داده PostgreSQL ====================
class Database:
    def __init__(self):
        self.pool = None
        self._initialized = False
    
    async def init(self):
        if self._initialized:
            return
        
        try:
            if not DATABASE_URL:
                logger.error("❌ متغیر DATABASE_URL تنظیم نشده است!")
                return
            
            self.pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=1,
                max_size=10,
                command_timeout=30
            )
            
            logger.info("✅ اتصال به PostgreSQL برقرار شد.")
            await self._create_tables()
            self._initialized = True
            
        except Exception as e:
            logger.error(f"❌ خطای اتصال به PostgreSQL: {e}")
            raise
    
    async def _create_tables(self):
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    language TEXT DEFAULT 'fa',
                    referral_code TEXT,
                    referred_by BIGINT,
                    joined_at TIMESTAMP DEFAULT NOW(),
                    is_banned BOOLEAN DEFAULT FALSE
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS configs (
                    config_id TEXT PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    size TEXT,
                    duration TEXT,
                    service_name TEXT,
                    tracking_code TEXT,
                    subscription_link TEXT,
                    config_text TEXT,
                    status TEXT DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT NOW(),
                    expiry_date TIMESTAMP
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS orders (
                    order_id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    plan_key TEXT,
                    plan_name TEXT,
                    price INTEGER,
                    duration TEXT,
                    status TEXT DEFAULT 'pending',
                    receipt_photo TEXT,
                    receipt_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS successful_orders (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    config_id TEXT,
                    size TEXT,
                    duration TEXT,
                    service_name TEXT,
                    tracking_code TEXT,
                    subscription_link TEXT,
                    purchase_date TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS sent_configs (
                    id SERIAL PRIMARY KEY,
                    config_text TEXT,
                    config_hash TEXT UNIQUE,
                    source_channel TEXT,
                    location TEXT,
                    country TEXT,
                    sent_to_channel BOOLEAN DEFAULT TRUE,
                    sent_to_topic BOOLEAN DEFAULT FALSE,
                    sent_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS sent_proxies (
                    id SERIAL PRIMARY KEY,
                    proxy_url TEXT,
                    proxy_hash TEXT UNIQUE,
                    source_channel TEXT,
                    location TEXT,
                    country TEXT,
                    sent_to_channel BOOLEAN DEFAULT TRUE,
                    sent_to_topic BOOLEAN DEFAULT FALSE,
                    sent_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS stats (
                    key TEXT PRIMARY KEY,
                    value BIGINT DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS reports (
                    report_id TEXT PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    report_type TEXT,
                    config_id TEXT,
                    description TEXT,
                    status TEXT DEFAULT 'pending',
                    admin_reply TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS referrals (
                    id SERIAL PRIMARY KEY,
                    referrer_id BIGINT,
                    referrer_username TEXT,
                    referred_id BIGINT,
                    referred_username TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS redeem_codes (
                    code TEXT PRIMARY KEY,
                    config_text TEXT,
                    created_by BIGINT,
                    used_by BIGINT,
                    used_by_username TEXT,
                    used_at TIMESTAMP,
                    is_used BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS redeem_requests (
                    request_id TEXT PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    code TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS admins (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    added_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_configs_user_id ON configs(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_reports_user_id ON reports(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_sent_configs_hash ON sent_configs(config_hash)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_sent_proxies_hash ON sent_proxies(proxy_hash)')
            
            await conn.execute('''
                INSERT INTO stats (key, value) VALUES 
                ('total_users', 0),
                ('total_configs_sold', 0),
                ('total_orders', 0),
                ('total_configs_sent', 0),
                ('total_proxies_sent', 0),
                ('total_reports', 0),
                ('total_referrals', 0)
                ON CONFLICT (key) DO NOTHING
            ''')
            logger.info("✅ تمامی جداول دیتابیس آماده شدند.")
    
    async def add_user(self, user_id: int, username: str = None, first_name: str = None, last_name: str = None, referred_by: int = None):
        if not self.pool:
            return False
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT user_id FROM users WHERE user_id = $1', user_id)
            if not row:
                import string
                ref_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
                await conn.execute(
                    'INSERT INTO users (user_id, username, first_name, last_name, referral_code, referred_by) VALUES ($1, $2, $3, $4, $5, $6)',
                    user_id, username, first_name, last_name, ref_code, referred_by
                )
                await conn.execute('UPDATE stats SET value = value + 1 WHERE key = $1', 'total_users')
                if referred_by:
                    await conn.execute('INSERT INTO referrals (referrer_id, referred_id) VALUES ($1, $2)', referred_by, user_id)
                    await conn.execute('UPDATE stats SET value = value + 1 WHERE key = $1', 'total_referrals')
                return True
            return False

    async def is_config_sent(self, config_hash: str) -> bool:
        if not self.pool:
            return False
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT id FROM sent_configs WHERE config_hash = $1', config_hash)
            return row is not None

    async def add_sent_config(self, config_text: str, config_hash: str, source_channel: str):
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                '''INSERT INTO sent_configs (config_text, config_hash, source_channel) 
                   VALUES ($1, $2, $3) ON CONFLICT (config_hash) DO NOTHING''',
                config_text, config_hash, source_channel
            )
            await conn.execute('UPDATE stats SET value = value + 1 WHERE key = $1', 'total_configs_sent')

    async def is_proxy_sent(self, proxy_hash: str) -> bool:
        if not self.pool:
            return False
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT id FROM sent_proxies WHERE proxy_hash = $1', proxy_hash)
            return row is not None

    async def add_sent_proxy(self, proxy_url: str, proxy_hash: str, source_channel: str):
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                '''INSERT INTO sent_proxies (proxy_url, proxy_hash, source_channel) 
                   VALUES ($1, $2, $3) ON CONFLICT (proxy_hash) DO NOTHING''',
                proxy_url, proxy_hash, source_channel
            )
            await conn.execute('UPDATE stats SET value = value + 1 WHERE key = $1', 'total_proxies_sent')

    async def get_successful_orders(self, user_id: int = None) -> List[Dict]:
        if not self.pool:
            return []
        async with self.pool.acquire() as conn:
            if user_id:
                rows = await conn.fetch('SELECT * FROM successful_orders WHERE user_id = $1 ORDER BY purchase_date DESC', user_id)
            else:
                rows = await conn.fetch('SELECT * FROM successful_orders ORDER BY purchase_date DESC')
            return [dict(row) for row in rows]

# ==================== مدیریت وضعیت (State & Memory Cache) ====================
@dataclass
class BotState:
    config_scanner_running: bool = True
    proxy_scanner_running: bool = True
    admins: Set[int] = field(default_factory=lambda: {OWNER_ID})
    sent_config_hashes: Set[str] = field(default_factory=set)
    sent_proxy_hashes: Set[str] = field(default_factory=set)
    MAX_HASHES: int = 2000

    def add_config_hash(self, hash_value: str):
        if len(self.sent_config_hashes) >= self.MAX_HASHES:
            self.sent_config_hashes.pop()
        self.sent_config_hashes.add(hash_value)

    def add_proxy_hash(self, hash_value: str):
        if len(self.sent_proxy_hashes) >= self.MAX_HASHES:
            self.sent_proxy_hashes.pop()
        self.sent_proxy_hashes.add(hash_value)

    def is_admin(self, user_id: int) -> bool:
        return user_id == OWNER_ID or user_id in self.admins

# ==================== موتور اسکن سریع و کنترل‌شده ====================
class FastScanner:
    def __init__(self, client: TelegramClient, db: Database, state: BotState):
        self.client = client
        self.db = db
        self.state = state
        self.scan_delay = 10  # افزایش زمان حلقه برای جلوگیری از محدودیت تلگرام

    @staticmethod
    def generate_hash(text: str) -> str:
        return hashlib.md5(text.strip().encode('utf-8')).hexdigest()

    async def process_channel_configs(self, channel: str):
        """اسکن پیام‌های کانال برای کانفیگ با کنترل محدودیت نرخ"""
        try:
            messages = await self.client.get_messages(channel, limit=5)
            for msg in reversed(messages):
                if not msg.text:
                    continue

                configs = CONFIG_REGEX.findall(msg.text)
                for config in configs:
                    clean_config = config.strip()
                    cfg_hash = self.generate_hash(clean_config)

                    if cfg_hash in self.state.sent_config_hashes:
                        continue

                    if await self.db.is_config_sent(cfg_hash):
                        self.state.add_config_hash(cfg_hash)
                        continue

                    self.state.add_config_hash(cfg_hash)

                    await self.client.send_message(CONFIG_TARGET_CHANNEL, clean_config)
                    logger.info(f"⚡ [CONFIG] کانفیگ جدید از {channel} دریافت و ارسال شد.")

                    asyncio.create_task(self.db.add_sent_config(clean_config, cfg_hash, channel))
                    await asyncio.sleep(2)  # تاخیر جهت عدم مواجهه با FloodWait

        except FloodWaitError as e:
            logger.warning(f"⚠️ محدودیت تلگرام برای {channel}: باید {e.seconds} ثانیه صبر کنیم.")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"خطا در اسکن کانفیگ {channel}: {e}")

    async def process_channel_proxies(self, channel: str):
        """اسکن پیام‌های کانال برای پروکسی با کنترل محدودیت نرخ"""
        try:
            messages = await self.client.get_messages(channel, limit=5)
            for msg in reversed(messages):
                if not msg.text:
                    continue

                proxies = PROXY_REGEX.findall(msg.text)
                for proxy in proxies:
                    clean_proxy = proxy.strip()
                    prx_hash = self.generate_hash(clean_proxy)

                    if prx_hash in self.state.sent_proxy_hashes:
                        continue

                    if await self.db.is_proxy_sent(prx_hash):
                        self.state.add_proxy_hash(prx_hash)
                        continue

                    self.state.add_proxy_hash(prx_hash)

                    await self.client.send_message(PROXY_TARGET_CHANNEL, clean_proxy)
                    logger.info(f"⚡ [PROXY] پروکسی جدید از {channel} دریافت و ارسال شد.")

                    asyncio.create_task(self.db.add_sent_proxy(clean_proxy, prx_hash, channel))
                    await asyncio.sleep(2)  # تاخیر جهت عدم مواجهه با FloodWait

        except FloodWaitError as e:
            logger.warning(f"⚠️ محدودیت تلگرام برای {channel}: باید {e.seconds} ثانیه صبر کنیم.")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"خطا در اسکن پروکسی {channel}: {e}")

    async def start_config_scanner_loop(self):
        """حلقه اسکن کانفیگ‌ها به‌صورت توالی جزیی برای تعادل بار"""
        while self.state.config_scanner_running:
            try:
                for ch in SOURCE_CONFIG_CHANNELS:
                    await self.process_channel_configs(ch)
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"خطا در حلقه اصلی کانفیگ: {e}")
            await asyncio.sleep(self.scan_delay)

    async def start_proxy_scanner_loop(self):
        """حلقه اسکن پروکسی‌ها به‌صورت توالی جزیی برای تعادل بار"""
        while self.state.proxy_scanner_running:
            try:
                for ch in SOURCE_PROXY_CHANNELS:
                    await self.process_channel_proxies(ch)
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"خطا در حلقه اصلی پروکسی: {e}")
            await asyncio.sleep(self.scan_delay)

# ==================== نمونه‌های اصلی برنامه‌نویسی ====================
db_instance = Database()
state_instance = BotState()

# ==================== دستگیره‌های دستورات ربات (Bot Handlers) ====================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    
    await db_instance.add_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name
    )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("کانال کانفیگ 🟢", url=CHANNEL_1_LINK),
            InlineKeyboardButton("کانال پروکسی 🟡", url=CHANNEL_2_LINK)
        ]
    ])
    
    await update.message.reply_text(
        f"سلام {user.first_name} عزیز!\nبه ربات دریافت سریع کانفیگ و پروکسی خوش آمدید.",
        reply_markup=keyboard
    )

async def admin_successful_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    
    if not state_instance.is_admin(user_id):
        await query.answer("فقط ادمین‌ها دسترسی دارند!", show_alert=True)
        return
    
    await query.answer()
    
    try:
        orders = await db_instance.get_successful_orders()
        if not orders:
            await query.edit_message_text(
                "📭 هیچ خرید موفقی ثبت نشده است.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin")]
                ])
            )
            return

        text = "🛒 **لیست آخرین خریدهای موفق:**\n\n"
        for order in orders[:10]:
            text += f"👤 کاربر: `{order.get('user_id')}`\n📦 حجم: {order.get('size')}\n⏳ مدت: {order.get('duration')}\n📅 تاریخ: {str(order.get('purchase_date'))[:10]}\n-------------------\n"
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin")]
            ])
        )
    except Exception as e:
        logger.error(f"Error fetching orders: {e}")
        await query.edit_message_text("❌ خطا در دریافت اطلاعات سفارشات.")

# ==================== نقطه اجرای اصلی (Main Execution) ====================
async def main():
    # ۱. راه‌اندازی و اتصال پایگاه‌داده
    await db_instance.init()

    # ۲. ایجاد کلاینت Telethon برای اسکن پرسرعت کانال‌ها
    telethon_client = TelegramClient(StringSession(USER_SESSION_STR), API_ID, API_HASH)
    await telethon_client.start()
    logger.info("✅ اکانت Telethon با موفقیت متصل شد.")

    # ۳. تنظیم ربات رسمی تلگرام
    bot_app = Application.builder().token(BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("start", start_command))
    bot_app.add_handler(CallbackQueryHandler(admin_successful_orders, pattern="^admin_successful_orders$"))

    # راه‌اندازی اولیه و ضروری Application قبل از ساخت وب‌سرور
    await bot_app.initialize()
    await bot_app.start()

    # ۴. راه‌اندازی حلقه اسکنرهای هم‌زمان
    scanner = FastScanner(telethon_client, db_instance, state_instance)
    asyncio.create_task(scanner.start_config_scanner_loop())
    asyncio.create_task(scanner.start_proxy_scanner_loop())

    # ۵. اجرای متد Webhook برای پلتفرم Railway یا Polling
    if RAILWAY_URL:
        await bot_app.bot.set_webhook(url=WEBHOOK_URL)
        app = web.Application()

        async def webhook_handler(request):
            try:
                data = await request.json()
                update = Update.de_json(data, bot_app.bot)
                await bot_app.process_update(update)
                return web.Response(text="OK")
            except Exception as e:
                logger.error(f"خطا در پردازش Webhook: {e}")
                return web.Response(status=500)

        app.router.add_post('/webhook', webhook_handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        await site.start()
        logger.info(f"🌐 وب‌سرویس Webhook روی پورت {PORT} فعال شد.")
    else:
        await bot_app.updater.start_polling()
        logger.info("🤖 حالت Polling ربات فعال شد...")

    # فعال نگه‌داشتن برنامه
    await asyncio.Event().wait()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("برنامه متوقف شد.")
