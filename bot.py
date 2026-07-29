import asyncio
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

# ==================== تنظیمات اصلی (جدید) ====================
BOT_TOKEN = "8861568420:AAF97D4VJBTKyrWIUv1Z5aZd0RLZaC0SLoQ"
API_ID = 32233583
API_HASH = "ce6caac5e6e987ff33fc613d076570a4"
USER_SESSION_STR = "1BJWap1wBu7fjjHtteoJChPEPZ3HOEY1EmLn0pZPI2Fz08EwbRKi37tMVPpsbIp9aQN4D5tVJF8-uOQLtz9uSEJ1nndHfdPOsOQItGD5tOwbnMI7g4taPDk_jDBgGZcVD3CzCoWPDzI0H--GCI_zOUBPIGNbrDczxIaKz3CA9922MX5BsZwu9Kx_M6kmmdgQtAzBBaZ5BqxgqurtAWw6h7BpiAvj5Fc8emVEjEkLNmV26pvP5nkRcfDrZbM9ERVceMhGi1SJJ7EGQOLu0BJUgiKC-IlaXLvqOj-z4Jbhfj7ZXXYz64T32-I9USAnchyp5I3oUKDxy5Oy0zTOVsxj6HgCV-Gf0czc="
OWNER_ID = 8879869880
PORT = 8080
GROQ_API_KEY = "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"
DATABASE_URL = os.environ.get('DATABASE_URL', '')
RAILWAY_URL = os.environ.get('RAILWAY_PUBLIC_DOMAIN', '')
WEBHOOK_URL = f"https://{RAILWAY_URL}/webhook" if RAILWAY_URL else ""

# ==================== تنظیمات لاگ ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== دیتابیس PostgreSQL ====================
class Database:
    def __init__(self):
        self.pool = None
        self._initialized = False
    
    async def init(self):
        if self._initialized:
            return
        
        try:
            if not DATABASE_URL:
                logger.error("❌ DATABASE_URL not set!")
                return
            
            self.pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=1,
                max_size=10,
                command_timeout=30
            )
            
            logger.info("✅ PostgreSQL connected!")
            await self._create_tables()
            self._initialized = True
            
        except Exception as e:
            logger.error(f"❌ PostgreSQL error: {e}")
            raise
    
    async def _create_tables(self):
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
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS scanner_state (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT NOW()
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
            
            logger.info("✅ Tables created!")
    
    # ==================== متدهای کاربران ====================
    async def add_user(self, user_id: int, username: str = None, first_name: str = None, last_name: str = None, referred_by: int = None):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT user_id FROM users WHERE user_id = $1', user_id)
            if not row:
                import random, string
                ref_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
                await conn.execute(
                    'INSERT INTO users (user_id, referral_code, referred_by) VALUES ($1, $2, $3)',
                    user_id, ref_code, referred_by
                )
                await conn.execute('UPDATE stats SET value = value + 1 WHERE key = $1', 'total_users')
                
                if referred_by:
                    await conn.execute(
                        'INSERT INTO referrals (referrer_id, referred_id) VALUES ($1, $2)',
                        referred_by, user_id
                    )
                    await conn.execute('UPDATE stats SET value = value + 1 WHERE key = $1', 'total_referrals')
                return True
            return False
    
    async def get_user(self, user_id: int):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow('SELECT * FROM users WHERE user_id = $1', user_id)
    
    async def get_user_by_username(self, username: str):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow('SELECT * FROM users WHERE username = $1', username)
    
    async def get_all_users(self) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM users ORDER BY joined_at DESC')
            return [dict(row) for row in rows]
    
    async def update_language(self, user_id: int, lang: str):
        async with self.pool.acquire() as conn:
            await conn.execute('UPDATE users SET language = $1 WHERE user_id = $2', lang, user_id)
    
    async def get_user_language(self, user_id: int) -> str:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT language FROM users WHERE user_id = $1', user_id)
            return row['language'] if row else "fa"
    
    async def ban_user(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('UPDATE users SET is_banned = TRUE WHERE user_id = $1', user_id)
    
    async def unban_user(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('UPDATE users SET is_banned = FALSE WHERE user_id = $1', user_id)
    
    async def is_banned(self, user_id: int) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT is_banned FROM users WHERE user_id = $1', user_id)
            return row['is_banned'] if row else False
    
    async def get_referral_code(self, user_id: int) -> str:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT referral_code FROM users WHERE user_id = $1', user_id)
            if row and row['referral_code']:
                return row['referral_code']
            import random, string
            code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            await conn.execute('UPDATE users SET referral_code = $1 WHERE user_id = $2', code, user_id)
            return code
    
    async def get_referral_count(self, user_id: int) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT COUNT(*) FROM referrals WHERE referrer_id = $1', user_id)
            return row[0] if row else 0
    
    async def get_referrals(self, user_id: int) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT * FROM referrals WHERE referrer_id = $1 ORDER BY created_at DESC',
                user_id
            )
            return [dict(row) for row in rows]
    
    # ==================== متدهای ادمین ====================
    async def add_admin(self, user_id: int, username: str = None):
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO admins (user_id, username) VALUES ($1, $2) ON CONFLICT DO NOTHING',
                user_id, username
            )
    
    async def remove_admin(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('DELETE FROM admins WHERE user_id = $1', user_id)
    
    async def get_admins(self) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM admins ORDER BY added_at DESC')
            return [dict(row) for row in rows]
    
    # ==================== متدهای کانفنیگ ====================
    async def add_config(self, config_id: str, user_id: int, username: str, size: str, duration: str,
                         service_name: str, tracking_code: str, subscription_link: str, config_text: str):
        async with self.pool.acquire() as conn:
            days = 30
            if "روز" in duration:
                match = re.search(r'(\d+)', duration)
                if match:
                    days = int(match.group(1))
            elif "ماه" in duration:
                match = re.search(r'(\d+)', duration)
                if match:
                    days = int(match.group(1)) * 30
            
            expiry_date = datetime.now() + timedelta(days=days)
            
            await conn.execute(
                '''INSERT INTO configs 
                   (config_id, user_id, username, size, duration, service_name, tracking_code, subscription_link, config_text, expiry_date) 
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)''',
                config_id, user_id, username, size, duration, service_name, tracking_code, subscription_link, config_text, expiry_date
            )
            await conn.execute('UPDATE stats SET value = value + 1 WHERE key = $1', 'total_configs_sold')
    
    async def get_user_configs(self, user_id: int) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT * FROM configs WHERE user_id = $1 AND status = $2 ORDER BY created_at DESC',
                user_id, 'active'
            )
            return [dict(row) for row in rows]
    
    async def get_config(self, config_id: str):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow('SELECT * FROM configs WHERE config_id = $1', config_id)
    
    # ==================== متدهای سفارش ====================
    async def add_order(self, user_id: int, username: str, plan_key: str, plan_name: str, price: int, duration: str) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                '''INSERT INTO orders (user_id, username, plan_key, plan_name, price, duration, status) 
                   VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING order_id''',
                user_id, username, plan_key, plan_name, price, duration, 'pending'
            )
            await conn.execute('UPDATE stats SET value = value + 1 WHERE key = $1', 'total_orders')
            return row['order_id'] if row else None
    
    async def update_order_receipt(self, order_id: int, photo_file_id: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE orders SET receipt_photo = $1, receipt_time = NOW(), status = $2 WHERE order_id = $3',
                photo_file_id, 'waiting_confirm', order_id
            )
    
    async def get_pending_orders(self) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT * FROM orders WHERE status IN ($1, $2) ORDER BY created_at DESC',
                'pending', 'waiting_confirm'
            )
            return [dict(row) for row in rows]
    
    async def get_all_orders(self) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM orders ORDER BY created_at DESC')
            return [dict(row) for row in rows]
    
    async def confirm_order(self, order_id: int, config_id: str):
        async with self.pool.acquire() as conn:
            await conn.execute('UPDATE orders SET status = $1 WHERE order_id = $2', 'confirmed', order_id)
    
    async def reject_order(self, order_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('UPDATE orders SET status = $1 WHERE order_id = $2', 'rejected', order_id)
    
    # ==================== متدهای خرید موفق ====================
    async def add_successful_order(self, user_id: int, username: str, config_id: str, size: str, duration: str,
                                    service_name: str, tracking_code: str, subscription_link: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                '''INSERT INTO successful_orders 
                   (user_id, username, config_id, size, duration, service_name, tracking_code, subscription_link) 
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8)''',
                user_id, username, config_id, size, duration, service_name, tracking_code, subscription_link
            )
    
    async def get_successful_orders(self, user_id: int = None) -> List[Dict]:
        async with self.pool.acquire() as conn:
            if user_id:
                rows = await conn.fetch(
                    'SELECT * FROM successful_orders WHERE user_id = $1 ORDER BY purchase_date DESC',
                    user_id
                )
            else:
                rows = await conn.fetch('SELECT * FROM successful_orders ORDER BY purchase_date DESC')
            return [dict(row) for row in rows]
    
    # ==================== متدهای کانفنیگ ارسال شده ====================
    async def add_sent_config(self, config_text: str, config_hash: str, source_channel: str, 
                               location: str = None, country: str = None, sent_to_topic: bool = False):
        async with self.pool.acquire() as conn:
            await conn.execute(
                '''INSERT INTO sent_configs (config_text, config_hash, source_channel, location, country, sent_to_topic) 
                   VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (config_hash) DO NOTHING''',
                config_text, config_hash, source_channel, location, country, sent_to_topic
            )
            await conn.execute('UPDATE stats SET value = value + 1 WHERE key = $1', 'total_configs_sent')
    
    async def is_config_sent(self, config_hash: str) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT id FROM sent_configs WHERE config_hash = $1', config_hash)
            return row is not None
    
    async def get_sent_configs_count(self) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT COUNT(*) FROM sent_configs')
            return row[0] if row else 0
    
    # ==================== متدهای پروکسی ارسال شده ====================
    async def add_sent_proxy(self, proxy_url: str, proxy_hash: str, source_channel: str,
                              location: str = None, country: str = None, sent_to_topic: bool = False):
        async with self.pool.acquire() as conn:
            await conn.execute(
                '''INSERT INTO sent_proxies (proxy_url, proxy_hash, source_channel, location, country, sent_to_topic) 
                   VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (proxy_hash) DO NOTHING''',
                proxy_url, proxy_hash, source_channel, location, country, sent_to_topic
            )
            await conn.execute('UPDATE stats SET value = value + 1 WHERE key = $1', 'total_proxies_sent')
    
    async def is_proxy_sent(self, proxy_hash: str) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT id FROM sent_proxies WHERE proxy_hash = $1', proxy_hash)
            return row is not None
    
    async def get_sent_proxies_count(self) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT COUNT(*) FROM sent_proxies')
            return row[0] if row else 0

    # ==================== متدهای گزارش ====================
    async def add_report(self, report_id: str, user_id: int, username: str, report_type: str, config_id: str, description: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                '''INSERT INTO reports (report_id, user_id, username, report_type, config_id, description) 
                   VALUES ($1, $2, $3, $4, $5, $6)''',
                report_id, user_id, username, report_type, config_id, description
            )
            await conn.execute('UPDATE stats SET value = value + 1 WHERE key = $1', 'total_reports')
    
    async def get_reports(self, status: str = None) -> List[Dict]:
        async with self.pool.acquire() as conn:
            if status:
                rows = await conn.fetch(
                    'SELECT * FROM reports WHERE status = $1 ORDER BY created_at DESC',
                    status
                )
            else:
                rows = await conn.fetch('SELECT * FROM reports ORDER BY created_at DESC')
            return [dict(row) for row in rows]
    
    async def reply_report(self, report_id: str, reply_text: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE reports SET status = $1, admin_reply = $2 WHERE report_id = $3',
                'answered', reply_text, report_id
            )
    
    # ==================== متدهای کد تخفیف ====================
    async def add_redeem_code(self, code: str, created_by: int, config_text: str = None):
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO redeem_codes (code, created_by, config_text) VALUES ($1, $2, $3)',
                code, created_by, config_text
            )
    
    async def use_redeem_code(self, code: str, user_id: int, username: str) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT is_used FROM redeem_codes WHERE code = $1', code)
            if row and not row['is_used']:
                await conn.execute(
                    'UPDATE redeem_codes SET is_used = TRUE, used_by = $1, used_by_username = $2, used_at = NOW() WHERE code = $3',
                    user_id, username, code
                )
                return True
            return False
    
    async def get_redeem_code(self, code: str):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow('SELECT * FROM redeem_codes WHERE code = $1', code)
    
    # ==================== متدهای وضعیت اسکنر ====================
    async def get_scanner_state(self, key: str) -> Optional[str]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT value FROM scanner_state WHERE key = $1', key)
            if row:
                return row['value']
            await self.set_scanner_state(key, "False")
            return "False"
    
    async def set_scanner_state(self, key: str, value: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                '''INSERT INTO scanner_state (key, value) VALUES ($1, $2) 
                   ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()''',
                key, value
            )
    
    # ==================== متدهای آمار ====================
    async def get_all_stats(self) -> Dict:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM stats')
            stats = {row['key']: row['value'] for row in rows}
            
            user_count = await conn.fetchrow('SELECT COUNT(*) FROM users')
            stats['total_users'] = user_count[0] if user_count else 0
            
            config_count = await conn.fetchrow('SELECT COUNT(*) FROM configs')
            stats['total_configs_sold'] = config_count[0] if config_count else 0
            
            order_count = await conn.fetchrow('SELECT COUNT(*) FROM orders')
            stats['total_orders'] = order_count[0] if order_count else 0
            
            sent_config_count = await conn.fetchrow('SELECT COUNT(*) FROM sent_configs')
            stats['total_configs_sent'] = sent_config_count[0] if sent_config_count else 0
            
            sent_proxy_count = await conn.fetchrow('SELECT COUNT(*) FROM sent_proxies')
            stats['total_proxies_sent'] = sent_proxy_count[0] if sent_proxy_count else 0
            
            report_count = await conn.fetchrow('SELECT COUNT(*) FROM reports')
            stats['total_reports'] = report_count[0] if report_count else 0
            
            referral_count = await conn.fetchrow('SELECT COUNT(*) FROM referrals')
            stats['total_referrals'] = referral_count[0] if referral_count else 0
            
            pending_orders = await conn.fetchrow('SELECT COUNT(*) FROM orders WHERE status = $1', 'pending')
            stats['pending_orders'] = pending_orders[0] if pending_orders else 0
            
            active_users = await conn.fetchrow('SELECT COUNT(*) FROM users WHERE is_banned = FALSE')
            stats['active_users'] = active_users[0] if active_users else 0
            
            return stats

# ==================== لینک چنل‌ها ====================
CHANNEL_1_LINK = "https://t.me/v2reya88"
CHANNEL_2_LINK = "https://t.me/confinghub2"
CHANNEL_1_USERNAME = "v2reya88"
CHANNEL_2_USERNAME = "confinghub2"
CHANNEL_1_DISPLAY = "@v2reya88"
CHANNEL_2_DISPLAY = "@confinghub2"

# ==================== چنل‌ها و تاپیک‌ها ====================
CONFIG_TARGET_CHANNEL = CHANNEL_1_DISPLAY
PROXY_TARGET_CHANNEL = CHANNEL_2_DISPLAY
GROUP_ID = -1001796213998
CONFIG_TOPIC_ID = 108538
PROXY_TOPIC_ID = 108613

# ==================== لیست چنل‌های منبع کانفیگ (دقیقا ۳ کانال) ====================
SOURCE_CONFIG_CHANNELS = [
    "@FarazV2ray",
    "@ConfigsHUB",
    "@v2reya88"
]

SOURCE_PROXY_CHANNELS = [
    "@ProxysHUB",
    "@iMTProto",
    "@ProxyDaemi",
    "@iRoProxy",
    "@PinkProxy",
    "@PyroProxy",
    "@darkproxy",
    "@Forall_Proxy",
    "@Myporoxy",
    "@TelMTProto"
]

# ==================== قیمت‌ها ====================
PRICES = {
    "1gb": 5000, "2gb": 10000, "5gb": 25000, "10gb": 50000,
    "15gb": 75000, "20gb": 100000, "40gb": 160000, "50gb": 200000,
    "60gb": 180000, "80gb": 240000, "100gb": 300000, "150gb": 450000,
    "200gb": 500000, "unlimited_1m": 100000, "unlimited_3m": 250000,
    "unlimited_5m": 400000
}

PRICE_NAMES = {
    "1gb": "۱ گیگ", "2gb": "۲ گیگ", "5gb": "۵ گیگ", "10gb": "۱۰ گیگ",
    "15gb": "۱۵ گیگ", "20gb": "۲۰ گیگ", "40gb": "۴۰ گیگ", "50gb": "۵۰ گیگ",
    "60gb": "۶۰ گیگ", "80gb": "۸۰ گیگ", "100gb": "۱۰۰ گیگ", "150gb": "۱۵۰ گیگ",
    "200gb": "۲۰۰ گیگ (تخفیف ویژه)", "unlimited_1m": "نامحدود ۱ ماهه",
    "unlimited_3m": "نامحدود ۳ ماهه", "unlimited_5m": "نامحدود ۵ ماهه"
}

PLAN_DETAILS = {
    "1gb": {"duration": "۳۰ روز", "desc": "مناسب برای کاربری روزمره و وبگردی"},
    "2gb": {"duration": "۳۰ روز", "desc": "مناسب برای وبگردی و شبکه‌های اجتماعی"},
    "5gb": {"duration": "۳۰ روز", "desc": "مناسب برای تماشای ویدیو و وبگردی"},
    "10gb": {"duration": "۳۰ روز", "desc": "مناسب برای گیم و تماشای فیلم"},
    "15gb": {"duration": "۳۰ روز", "desc": "مناسب برای دانلود و گیم"},
    "20gb": {"duration": "۳۰ روز", "desc": "مناسب برای دانلود سنگین و گیم آنلاین"},
    "40gb": {"duration": "۳۰ روز", "desc": "مناسب برای کاربران حرفه‌ای"},
    "50gb": {"duration": "۳۰ روز", "desc": "مناسب برای دانلود حجم بالا"},
    "60gb": {"duration": "۳۰ روز", "desc": "مناسب برای کاربران ویژه"},
    "80gb": {"duration": "۳۰ روز", "desc": "مناسب برای کاربران پر مصرف"},
    "100gb": {"duration": "۳۰ روز", "desc": "مناسب برای کاربران حرفه‌ای"},
    "150gb": {"duration": "۳۰ روز", "desc": "مناسب برای کاربران ویژه"},
    "200gb": {"duration": "۳۰ روز", "desc": "🎁 تخفیف ویژه - مناسب برای کاربران حرفه‌ای"},
    "unlimited_1m": {"duration": "۱ ماه", "desc": "اینترنت نامحدود برای کاربران حرفه‌ای"},
    "unlimited_3m": {"duration": "۳ ماه", "desc": "اینترنت نامحدود با تخفیف ویژه"},
    "unlimited_5m": {"duration": "۵ ماه", "desc": "اینترنت نامحدود با بهترین قیمت"}
}

# ==================== کلاس مدیریت وضعیت ====================
@dataclass
class BotState:
    config_scanner_running: bool = False
    proxy_scanner_running: bool = False
    config_log_enabled: bool = False
    proxy_log_enabled: bool = False
    send_to_topic_enabled: bool = True
    
    sent_config_hashes: Set[str] = field(default_factory=set)
    sent_proxy_hashes: Set[str] = field(default_factory=set)
    
    admins: Set[int] = field(default_factory=lambda: {OWNER_ID})
    users: Set[int] = field(default_factory=set)
    banned_users: Set[int] = field(default_factory=set)
    user_referrals: Dict[int, Dict] = field(default_factory=dict)
    config_redeem_codes: Dict[str, Optional[str]] = field(default_factory=dict)
    config_redeem_used: Set[str] = field(default_factory=set)
    config_redeem_usage: Dict[str, int] = field(default_factory=dict)
    redeem_requests: Dict[str, Dict] = field(default_factory=dict)
    user_language: Dict[int, str] = field(default_factory=dict)
    user_last_prompt: Dict[int, str] = field(default_factory=dict)
    
    pending_orders: Dict[int, Dict] = field(default_factory=dict)
    user_orders: Dict[int, List[Dict]] = field(default_factory=dict)
    config_ids: Dict[str, Dict] = field(default_factory=dict)
    reports: Dict[str, Dict] = field(default_factory=dict)
    successful_orders: Dict[int, List[Dict]] = field(default_factory=dict)
    
    pending_config_user: Optional[int] = None
    pending_subscription_user: Optional[int] = None
    pending_plan_info: Optional[Dict] = None
    temp_config: Optional[Dict] = None
    
    flood_wait_until: Optional[datetime] = None
    scan_counter: int = 0
    last_scan_reset: datetime = field(default_factory=datetime.now)
    current_batch_index: int = 0
    config_last_msg_id: Dict[str, int] = field(default_factory=dict)
    proxy_last_msg_id: Dict[str, int] = field(default_factory=dict)
    last_proxy_scan_time: datetime = field(default_factory=datetime.now)
    proxy_scan_retry_count: int = 0
    
    db: Optional[Database] = None
    
    MAX_HASHES: int = 1000
    
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
    
    def is_banned(self, user_id: int) -> bool:
        return user_id in self.banned_users
    
    def get_next_batch(self) -> List[str]:
        return SOURCE_CONFIG_CHANNELS

# ==================== کلاس مدیریت هوش مصنوعی ====================
class AIManager:
    def __init__(self, api_key: str, state: BotState):
        self.api_key = api_key
        self.state = state
        self._semaphore = asyncio.Semaphore(5)
        self._cache: Dict[str, Any] = {}
        self._cache_timeout = 300
    
    async def get_chat_response(self, message: str) -> Dict[str, str]:
        cache_key = hash(message)
        if cache_key in self._cache:
            cache_time, response = self._cache[cache_key]
            if (datetime.now() - cache_time).seconds < self._cache_timeout:
                return response
        
        async with self._semaphore:
            try:
                if not self.api_key:
                    return {"type": "text", "text": "❌ کلید API تنظیم نشده!"}
                
                url = "https://api.groq.com/openai/v1/chat/completions"
                headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
                
                system_prompt = """شما یک دستیار هوشمند و دقیق هستید.
                
                قوانین:
                1. اگر کاربر کد فرستاد → تحلیل و دیباگ کن
                2. اگر سوال برنامه‌نویسی بود → پاسخ دقیق با مثال بده
                3. اگر سوال عمومی بود → مختصر و مفید پاسخ بده
                4. کدها را در بلاک کد قرار بده
                5. به فارسی پاسخ بده"""
                
                data = {
                    "model": "llama-3.1-8b-instant",
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": message}
                    ],
                    "temperature": 0.5,
                    "max_tokens": 2000
                }
                
                timeout = aiohttp.ClientTimeout(total=15)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(url, headers=headers, json=data) as response:
                        if response.status == 429:
                            return {"type": "text", "text": "⏳ تعداد درخواست‌ها زیاد شد. لطفاً چند لحظه صبر کنید."}
                        elif response.status == 401:
                            return {"type": "text", "text": "❌ کلید API نامعتبر است. با ادمین تماس بگیرید."}
                        elif response.status == 403:
                            return {"type": "text", "text": "❌ دسترسی به API محدود شده است."}
                        elif response.status == 500:
                            return {"type": "text", "text": "❌ سرور API با مشکل مواجه شده. لطفاً دوباره امتحان کنید."}
                        
                        result = await response.json()
                        if "error" in result:
                            error_msg = result["error"].get("message", "خطای ناشناخته")
                            return {"type": "text", "text": f"❌ خطای API: {error_msg}"}
                        
                        response_text = result["choices"][0]["message"]["content"]
                        self._cache[cache_key] = (datetime.now(), {"type": "text", "text": response_text})
                        return {"type": "text", "text": response_text}
                        
            except asyncio.TimeoutError:
                return {"type": "text", "text": "⏱️ زمان پاسخگویی به پایان رسید. لطفاً دوباره امتحان کنید."}
            except aiohttp.ClientError as e:
                logger.error(f"Network Error: {e}")
                return {"type": "text", "text": "❌ مشکل در ارتباط با سرور. لطفاً دوباره امتحان کنید."}
            except Exception as e:
                logger.error(f"AI Error: {e}")
                return {"type": "text", "text": f"❌ خطا: {str(e)[:100]}"}

# ==================== کلاس مدیریت اسکنر (بهینه‌شده برای حداکثر سرعت) ====================
class ChannelScanner:
    def __init__(self, state: BotState, db: Database, user_client: TelegramClient, bot_app: Application):
        self.state = state
        self.db = db
        self.user_client = user_client
        self.bot_app = bot_app
        self.config_regex = re.compile(r"(vless://\S+|vmess://\S+|trojan://\S+|ss://\S+|hy2://\S+|wireguard://\S+)")
        self._flood_lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(10) # افزایش همزمانی پردازش
    
    async def check_flood_wait(self) -> Tuple[bool, int]:
        async with self._flood_lock:
            if self.state.flood_wait_until:
                remaining = (self.state.flood_wait_until - datetime.now()).total_seconds()
                if remaining > 0:
                    return True, remaining
                else:
                    self.state.flood_wait_until = None
            return False, 0
    
    async def wait_if_needed(self):
        in_flood, remaining = await self.check_flood_wait()
        if in_flood:
            wait = min(remaining, 5)
            if wait > 0:
                logger.info(f"⏳ Waiting {wait:.0f}s for flood limit...")
                await asyncio.sleep(wait)
                return True
            else:
                self.state.flood_wait_until = None
                return False
        return False
    
    async def extract_configs_from_text(self, text: str) -> List[str]:
        if not text:
            return []
        return self.config_regex.findall(text)
    
    async def extract_configs_from_file(self, file_content: bytes) -> List[str]:
        try:
            text = file_content.decode('utf-8', errors='ignore')
            return await self.extract_configs_from_text(text)
        except Exception as e:
            logger.error(f"Error reading file: {e}")
            return []
    
    async def is_config_for_iran(self, config_text: str) -> bool:
        if 'IR' in config_text.upper() or 'IRAN' in config_text.upper():
            return False
        iran_ips = ['5.134', '31.7', '37.98', '37.148', '37.152', '37.154', '37.156', '37.158']
        for ip in iran_ips:
            if ip in config_text:
                return False
        return True
    
    async def extract_host(self, config_text: str) -> Optional[str]:
        m = re.search(r'@([^:]+):(\d+)', config_text)
        if m:
            return m.group(1)
        m = re.search(r'host=([^&]+)', config_text)
        if m:
            return m.group(1)
        m = re.search(r'([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):(\d+)', config_text)
        if m:
            return m.group(1)
        return None
    
    async def get_ip_info(self, ip: Optional[str] = None) -> Dict[str, str]:
        try:
            url = f"http://ip-api.com/json/{ip}?fields=status,country,countryCode,regionName,city,isp,query" if ip else "http://ip-api.com/json/?fields=status,country,countryCode,regionName,city,isp,query"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=2) as response: # زمان تایم‌اوت به ۲ ثانیه کاهش یافت
                    if response.status == 200:
                        data = await response.json()
                        if data.get('status') == 'success':
                            return {
                                'ip': data.get('query', 'Unknown'),
                                'country': data.get('country', 'Unknown'),
                                'countryCode': data.get('countryCode', ''),
                                'city': data.get('city', 'Unknown'),
                                'region': data.get('regionName', 'Unknown'),
                                'isp': data.get('isp', 'Unknown')
                            }
        except:
            pass
        return {'ip': ip or 'Unknown', 'country': 'Unknown', 'countryCode': '', 'city': 'Unknown', 'region': 'Unknown', 'isp': 'Unknown'}
    
    def get_country_flag(self, country_code: str) -> str:
        flags = {
            'US': '🇺🇸', 'GB': '🇬🇧', 'DE': '🇩🇪', 'FR': '🇫🇷', 'NL': '🇳🇱',
            'CA': '🇨🇦', 'IR': '🇮🇷', 'AE': '🇦🇪', 'TR': '🇹🇷', 'RU': '🇷🇺'
        }
        return flags.get(country_code, '🌍')
    
    def is_valid_config(self, config_text: str) -> bool:
        if not config_text:
            return False
        valid_protocols = ['vless://', 'vmess://', 'trojan://', 'ss://', 'hy2://', 'wireguard://']
        return any(config_text.startswith(p) for p in valid_protocols)
    
    def is_proxy_link(self, url: str) -> bool:
        if not url:
            return False
        proxy_patterns = [
            r't\.me/proxy\?',
            r'telegram\.me/proxy\?',
            r'proxy\.(?:ir|com|org|net)',
            r'mtproto://',
            r'https?://[^\s]+(?:proxy|mtproto)',
            r'^(?!.*(?:youtube|google|instagram|twitter|facebook|github|stackoverflow)).*proxy.*'
        ]
        url_lower = url.lower()
        for pattern in proxy_patterns:
            if re.search(pattern, url_lower, re.IGNORECASE):
                return True
        return False
    
    async def scan_config_channel(self, channel: str) -> Optional[Tuple[Any, str]]:
        if await self.wait_if_needed():
            return None, None
        
        try:
            last_id = self.state.config_last_msg_id.get(channel, 0)
            
            # دریافت پیام‌ها با نرخ فوق سریع (limit بالاتر و تاخیر ناچیز)
            async for msg in self.user_client.iter_messages(channel, min_id=last_id, limit=30, wait_time=0.05):
                if msg.id <= last_id:
                    continue
                
                if msg.id > last_id:
                    self.state.config_last_msg_id[channel] = msg.id
                
                if msg.text:
                    configs = await self.extract_configs_from_text(msg.text)
                    for config in configs:
                        if self.is_valid_config(config) and await self.is_config_for_iran(config):
                            return msg, config
                
                if msg.file and msg.file.name:
                    file_name = msg.file.name.lower()
                    if file_name.endswith(('.txt', '.conf', '.cfg')):
                        try:
                            file_path = await msg.download_media(file=io.BytesIO())
                            if file_path:
                                configs = await self.extract_configs_from_file(file_path.getvalue())
                                for config in configs:
                                    if self.is_valid_config(config) and await self.is_config_for_iran(config):
                                        return msg, config
                        except Exception as e:
                            logger.error(f"Error reading TXT: {e}")
                
                if msg.file and msg.file.name and msg.file.name.lower().endswith('.zip'):
                    try:
                        file_path = await msg.download_media(file=io.BytesIO())
                        if file_path:
                            with zipfile.ZipFile(file_path) as zip_file:
                                for file_name in zip_file.namelist():
                                    if file_name.lower().endswith(('.txt', '.conf', '.cfg')):
                                        with zip_file.open(file_name) as f:
                                            configs = await self.extract_configs_from_file(f.read())
                                            for config in configs:
                                                if self.is_valid_config(config) and await self.is_config_for_iran(config):
                                                    return msg, config
                    except Exception as e:
                        logger.error(f"Error reading ZIP: {e}")
                
                await asyncio.sleep(0.01)
            
            return None, None
            
        except FloodWaitError as e:
            self.state.flood_wait_until = datetime.now() + timedelta(seconds=e.seconds)
            logger.warning(f"⛔ FLOOD WAIT: {e.seconds}s")
            return None, None
        except Exception as e:
            logger.error(f"Error scanning {channel}: {e}")
            return None, None
    
    async def scan_proxy_channel(self, channel: str) -> Optional[Tuple[Any, str]]:
        if await self.wait_if_needed():
            return None, None
        
        try:
            last_id = self.state.proxy_last_msg_id.get(channel, 0)
            
            async for msg in self.user_client.iter_messages(channel, min_id=last_id, limit=30, wait_time=0.05):
                if msg.id <= last_id:
                    continue
                
                if msg.id > last_id:
                    self.state.proxy_last_msg_id[channel] = msg.id
                
                if msg.buttons:
                    for row in msg.buttons:
                        for btn in row:
                            if btn.url and self.is_proxy_link(btn.url):
                                return msg, btn.url
                
                if msg.text:
                    urls = re.findall(r'https?://[^\s]+', msg.text)
                    for url in urls:
                        if self.is_proxy_link(url):
                            return msg, url
                    
                    markdown_links = re.findall(r'\[.*?\]\((https?://[^\s]+)\)', msg.text)
                    for url in markdown_links:
                        if self.is_proxy_link(url):
                            return msg, url
                    
                    proxy_patterns = [
                        r'(?:proxy|mtproto)\.(?:ir|com|org|net)[^\s]*',
                        r't\.me/proxy\?[^\s]+',
                        r'telegram\.me/proxy\?[^\s]+',
                        r'https?://[^\s]+(?:proxy|mtproto)[^\s]*'
                    ]
                    for pattern in proxy_patterns:
                        matches = re.findall(pattern, msg.text, re.IGNORECASE)
                        for match in matches:
                            url = f"https://{match}" if not match.startswith('http') else match
                            if self.is_proxy_link(url):
                                return msg, url
                    
                    proxy_keywords = ['پروکسی', 'proxy', 'mtproto', 'MTProto']
                    for keyword in proxy_keywords:
                        if keyword in msg.text.lower():
                            all_urls = re.findall(r'https?://[^\s]+', msg.text)
                            for url in all_urls:
                                if self.is_proxy_link(url):
                                    return msg, url
                
                if msg.file and msg.file.name:
                    file_name = msg.file.name.lower()
                    if file_name.endswith(('.txt', '.conf', '.cfg')):
                        try:
                            file_path = await msg.download_media(file=io.BytesIO())
                            if file_path:
                                text = file_path.getvalue().decode('utf-8', errors='ignore')
                                urls = re.findall(r'https?://[^\s]+', text)
                                for url in urls:
                                    if self.is_proxy_link(url):
                                        return msg, url
                        except Exception as e:
                            logger.error(f"Error reading TXT: {e}")
                
                await asyncio.sleep(0.01)
            
            return None, None
            
        except FloodWaitError as e:
            self.state.flood_wait_until = datetime.now() + timedelta(seconds=e.seconds)
            logger.warning(f"⛔ FLOOD WAIT: {e.seconds}s")
            return None, None
        except Exception as e:
            logger.error(f"Error scanning {channel}: {e}")
            return None, None
    
    async def send_config(self, config_text: str, source_channel: str = None) -> bool:
        try:
            host = await self.extract_host(config_text)
            location = await self.get_ip_info(host) if host else {'ip': 'Unknown', 'country': 'Unknown', 'city': 'Unknown'}
            flag = self.get_country_flag(location.get('countryCode', ''))
            
            config_hash = str(abs(hash(config_text.split('#')[0])))
            
            if await self.db.is_config_sent(config_hash):
                return True
            
            if '#' in config_text:
                config_text = config_text.split('#')[0] + '#@v2reya88%20%7C%20%40confinghub2'
            else:
                config_text = config_text + '#@v2reya88%20%7C%20%40confinghub2'
            
            message = f"""```{config_text}```

📍 Location: {flag} {location.get('country', 'Unknown')}
🏙️ City: {location.get('city', 'Unknown')}

@v2reya88 | @confinghub2"""
            
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("CHANNEL 1", url=CHANNEL_1_LINK, style="primary"),
                    InlineKeyboardButton("CHANNEL 2", url=CHANNEL_2_LINK, style="danger")
                ],
                [
                    InlineKeyboardButton("🚀 PAHLAVI VPN", url="https://t.me/iranvpnboldbot", style="success")
                ]
            ])
            
            sent_msg = await self.bot_app.bot.send_message(
                chat_id=CONFIG_TARGET_CHANNEL,
                text=message,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=keyboard
            )
            
            sent_to_topic = False
            
            if self.state.send_to_topic_enabled and self.is_valid_config(config_text):
                try:
                    await self.user_client.forward_messages(
                        entity=GROUP_ID,
                        messages=sent_msg.message_id,
                        from_peer=CONFIG_TARGET_CHANNEL
                    )
                    sent_to_topic = True
                except Exception as e:
                    logger.error(f"Forward to topic error: {e}")
            
            await self.db.add_sent_config(
                config_text=config_text,
                config_hash=config_hash,
                source_channel=source_channel or CONFIG_TARGET_CHANNEL,
                location=location.get('city', 'Unknown'),
                country=location.get('country', 'Unknown'),
                sent_to_topic=sent_to_topic
            )
            
            return True
                
        except Exception as e:
            logger.error(f"Send error: {e}")
            return False
    
    async def send_proxy(self, proxy_url: str, source_channel: str = None) -> bool:
        try:
            info = await self.get_ip_info()
            flag = self.get_country_flag(info.get('countryCode', ''))
            
            proxy_hash = str(abs(hash(proxy_url)))
            
            if await self.db.is_proxy_sent(proxy_hash):
                return True
            
            channel_message = f"""now proxy ⚡️

📍 Location: {flag} {info.get('country', 'Unknown')}

@v2reya88 | @confinghub2"""
            
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("CONNECT", url=proxy_url, style="primary"),
                    InlineKeyboardButton("CHANNEL", url=CHANNEL_2_LINK, style="danger")
                ],
                [
                    InlineKeyboardButton("🚀 PAHLAVI VPN", url="https://t.me/iranvpnboldbot", style="success")
                ]
            ])
            
            sent_msg = await self.bot_app.bot.send_message(
                chat_id=PROXY_TARGET_CHANNEL,
                text=channel_message,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=keyboard
            )
            
            sent_to_topic = False
            
            if self.state.send_to_topic_enabled:
                try:
                    await self.user_client.forward_messages(
                        entity=GROUP_ID,
                        messages=sent_msg.message_id,
                        from_peer=PROXY_TARGET_CHANNEL
                    )
                    sent_to_topic = True
                except Exception as e:
                    logger.error(f"Forward to topic error: {e}")
            
            await self.db.add_sent_proxy(
                proxy_url=proxy_url,
                proxy_hash=proxy_hash,
                source_channel=source_channel or PROXY_TARGET_CHANNEL,
                location=info.get('city', 'Unknown'),
                country=info.get('country', 'Unknown'),
                sent_to_topic=sent_to_topic
            )
            
            return True
                
        except Exception as e:
            logger.error(f"Send error: {e}")
            return False
    
    async def scanner_loop(self):
        logger.info("⚡ Ultra-Fast Scanner Loop Started (Interval: 0.3s)...")
        
        while True:
            try:
                if self.state.config_scanner_running:
                    for channel in SOURCE_CONFIG_CHANNELS:
                        if not self.state.config_scanner_running:
                            break
                        
                        msg, config_text = await self.scan_config_channel(channel)
                        
                        if config_text:
                            config_hash = str(abs(hash(config_text.split('#')[0])))
                            if not await self.db.is_config_sent(config_hash):
                                self.state.add_config_hash(config_hash)
                                await self.send_config(config_text, channel)
                        
                        # مکث بسیار کوتاه (0.1 ثانیه) برای حداکثر سرعت
                        await asyncio.sleep(0.1)
                
                if self.state.proxy_scanner_running:
                    for channel in SOURCE_PROXY_CHANNELS:
                        if not self.state.proxy_scanner_running:
                            break
                        
                        msg, proxy_url = await self.scan_proxy_channel(channel)
                        
                        if proxy_url:
                            proxy_hash = str(abs(hash(proxy_url)))
                            if not await self.db.is_proxy_sent(proxy_hash):
                                self.state.add_proxy_hash(proxy_hash)
                                await self.send_proxy(proxy_url, channel)
                        
                        await asyncio.sleep(0.1)
                
                # تاخیر کل چرخه فقط 0.3 ثانیه برای پاسخ سریع
                await asyncio.sleep(0.3)
                    
            except Exception as e:
                logger.error(f"Scanner loop error: {e}")
                await asyncio.sleep(1)
