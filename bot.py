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
USER_SESSION_STR = "1BJWap1sBu8HDiN1B7OwZ9pgHqBaw9uFIuE4DczmYo4mKidx8idb2rPnceC4akSrPqGoLSvtxMcSP05GvDpt8qBYTYDf732m7IUdKs2u5DCVS2PAkBG0OwRNlGaSPOvYohTtmCR132FN0AuarlUDIq63e4c7vYT2iKbph0eSfxSY999sebGjmMrIwUDuYzJ7q_gkrfSorXvH8uXbBcmXG7Z6ekH94nMReVJt4PLMUpKPyI3NB0R-EbmhzAE3W4fdLAZ1U0tfb9FKDfF4WNni3YNd8t7vSVqvPyhr4C8kPI7RliT-Rn12hqXYrUKq6UpzZ1ZJsi-fTvrA1FW6GyvBouCGQJJOEfHw="
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

# ==================== لیست چنل‌های منبع (فقط ۲ چنل) ====================
SOURCE_CONFIG_CHANNELS = [
    "@FarazV2ray",
    "@ConfigsHUB"
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
        # برای ۲ چنل، هر دو رو با هم برمی‌گردونه
        return SOURCE_CONFIG_CHANNELS

# ==================== کلاس مدیریت هوش مصنوعی ====================
class AIManager:
    def __init__(self, api_key: str, state: BotState):
        self.api_key = api_key
        self.state = state
        self._semaphore = asyncio.Semaphore(3)
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

# ==================== کلاس مدیریت اسکنر ====================
class ChannelScanner:
    def __init__(self, state: BotState, db: Database, user_client: TelegramClient, bot_app: Application):
        self.state = state
        self.db = db
        self.user_client = user_client
        self.bot_app = bot_app
        self.config_regex = re.compile(r"(vless://\S+|vmess://\S+|trojan://\S+|ss://\S+|hy2://\S+|wireguard://\S+)")
        self._flood_lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(2)
    
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
            wait = min(remaining, 30)
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
                async with session.get(url, timeout=5) as response:
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
            if (datetime.now() - self.state.last_scan_reset).seconds > 300:
                self.state.scan_counter = 0
                self.state.last_scan_reset = datetime.now()
            
            if self.state.scan_counter >= 10:
                wait_time = 300 - (datetime.now() - self.state.last_scan_reset).seconds
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                self.state.scan_counter = 0
                self.state.last_scan_reset = datetime.now()
            
            self.state.scan_counter += 1
            last_id = self.state.config_last_msg_id.get(channel, 0)
            
            async for msg in self.user_client.iter_messages(channel, min_id=last_id, limit=20, wait_time=0.5):
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
                                        logger.info(f"📄 Found config in TXT: {msg.file.name}")
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
                                                    logger.info(f"📄 Found config in ZIP: {file_name}")
                                                    return msg, config
                    except Exception as e:
                        logger.error(f"Error reading ZIP: {e}")
                
                await asyncio.sleep(0.2)
            
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
            if (datetime.now() - self.state.last_scan_reset).seconds > 300:
                self.state.scan_counter = 0
                self.state.last_scan_reset = datetime.now()
            
            if self.state.scan_counter >= 10:
                wait_time = 300 - (datetime.now() - self.state.last_scan_reset).seconds
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                self.state.scan_counter = 0
                self.state.last_scan_reset = datetime.now()
            
            self.state.scan_counter += 1
            last_id = self.state.proxy_last_msg_id.get(channel, 0)
            
            async for msg in self.user_client.iter_messages(channel, min_id=last_id, limit=20, wait_time=0.5):
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
                
                if msg.file and msg.file.name and msg.file.name.lower().endswith('.zip'):
                    try:
                        file_path = await msg.download_media(file=io.BytesIO())
                        if file_path:
                            with zipfile.ZipFile(file_path) as zip_file:
                                for file_name in zip_file.namelist():
                                    if file_name.lower().endswith(('.txt', '.conf', '.cfg')):
                                        with zip_file.open(file_name) as f:
                                            text = f.read().decode('utf-8', errors='ignore')
                                            urls = re.findall(r'https?://[^\s]+', text)
                                            for url in urls:
                                                if self.is_proxy_link(url):
                                                    return msg, url
                    except Exception as e:
                        logger.error(f"Error reading ZIP: {e}")
                
                await asyncio.sleep(0.2)
            
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
                logger.info(f"⏭️ Config already sent: {config_hash[:10]}...")
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
            logger.info(f"✅ Config sent to channel: {CONFIG_TARGET_CHANNEL}")
            
            sent_to_topic = False
            
            if self.state.send_to_topic_enabled and self.is_valid_config(config_text):
                try:
                    await self.user_client.forward_messages(
                        entity=GROUP_ID,
                        messages=sent_msg.message_id,
                        from_peer=CONFIG_TARGET_CHANNEL
                    )
                    sent_to_topic = True
                    logger.info(f"✅ Config forwarded to topic")
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
                logger.info(f"⏭️ Proxy already sent: {proxy_hash[:10]}...")
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
            logger.info(f"✅ Proxy sent to channel: {PROXY_TARGET_CHANNEL}")
            
            sent_to_topic = False
            
            if self.state.send_to_topic_enabled:
                try:
                    await self.user_client.forward_messages(
                        entity=GROUP_ID,
                        messages=sent_msg.message_id,
                        from_peer=PROXY_TARGET_CHANNEL
                    )
                    sent_to_topic = True
                    logger.info(f"✅ Proxy forwarded to topic")
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
    
    async def send_log(self, log_text: str):
        try:
            await self.bot_app.bot.send_message(
                chat_id=OWNER_ID,
                text=log_text,
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logger.error(f"Send log error: {e}")
    
    async def scanner_loop(self):
        logger.info("🔄 Scanner loop started (Every 3 seconds)...")
        
        while True:
            try:
                if self.state.config_scanner_running:
                    for channel in SOURCE_CONFIG_CHANNELS:
                        if not self.state.config_scanner_running:
                            break
                        
                        logger.info(f"🔍 Scanning config: {channel}")
                        msg, config_text = await self.scan_config_channel(channel)
                        
                        if config_text:
                            config_hash = str(abs(hash(config_text.split('#')[0])))
                            if not await self.db.is_config_sent(config_hash):
                                self.state.add_config_hash(config_hash)
                                logger.info(f"✅ New config from {channel}")
                                success = await self.send_config(config_text, channel)
                                
                                if success:
                                    await asyncio.sleep(3)
                                else:
                                    await asyncio.sleep(1)
                        
                        # هر ۳ ثانیه یک بار چک کن
                        await asyncio.sleep(3)
                    
                    # بعد از بررسی هر دو چنل، یه مکث کوتاه
                    await asyncio.sleep(1)
                
                if self.state.proxy_scanner_running:
                    for channel in SOURCE_PROXY_CHANNELS:
                        if not self.state.proxy_scanner_running:
                            break
                        
                        logger.info(f"🔍 Scanning proxy: {channel}")
                        msg, proxy_url = await self.scan_proxy_channel(channel)
                        
                        if proxy_url:
                            proxy_hash = str(abs(hash(proxy_url)))
                            if not await self.db.is_proxy_sent(proxy_hash):
                                self.state.add_proxy_hash(proxy_hash)
                                logger.info(f"✅ New proxy from {channel}")
                                success = await self.send_proxy(proxy_url, channel)
                                
                                if success:
                                    await asyncio.sleep(3)
                                else:
                                    await asyncio.sleep(1)
                        
                        await asyncio.sleep(3)
                
                if not self.state.config_scanner_running and not self.state.proxy_scanner_running:
                    await asyncio.sleep(5)
                    
            except Exception as e:
                logger.error(f"Scanner loop error: {e}")
                await asyncio.sleep(5)

# ==================== کلاس مدیریت ربات ====================
class ScannerBot:
    def __init__(self):
        self.db = Database()
        self.state = BotState()
        self.state.db = self.db
        self.ai = AIManager(GROQ_API_KEY, self.state)
        self.application = None
        self.user_client = None
        self.scanner = None
        self._error_handler_registered = False
        
        self.TEXTS = {
            "fa": {
                "welcome": "به ربات اسکنر خوش آمدید!",
                "admin_panel": "پنل مدیریت",
                "user_panel": "پنل کاربری",
                "my_ip": "آیپی من",
                "referral": "رفرال",
                "redeem": "ردیم کد",
                "language": "زبان",
                "back": "بازگشت",
                "help": "راهنما",
                "admin_only": "فقط ادمین‌ها دسترسی دارند.",
                "banned": "شما بن شده‌اید.",
                "language_set": "زبان به {lang} تغییر کرد.",
                "select_language": "زبان خود را انتخاب کنید:",
            },
            "en": {
                "welcome": "Welcome to Scanner Bot!",
                "admin_panel": "Admin Panel",
                "user_panel": "User Panel",
                "my_ip": "My IP",
                "referral": "Referral",
                "redeem": "Redeem Code",
                "language": "Language",
                "back": "Back",
                "help": "Help",
                "admin_only": "Only admins have access.",
                "banned": "You have been banned.",
                "language_set": "Language set to {lang}.",
                "select_language": "Select your language:",
            }
        }
    
    def get_text(self, user_id: int, key: str) -> str:
        lang = self.state.user_language.get(user_id, "fa")
        return self.TEXTS.get(lang, self.TEXTS["fa"]).get(key, key)
    
    # ==================== دکمه‌ها ====================
    def membership_buttons(self):
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("چنل کانفنیگ", url=CHANNEL_1_LINK, style="primary"),
                InlineKeyboardButton("چنل پروکسی", url=CHANNEL_2_LINK, style="danger")
            ],
            [
                InlineKeyboardButton("تایید عضویت", callback_data="check_membership", style="success")
            ]
        ])
    
    def user_panel_buttons(self):
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("خرید کانفنیگ", callback_data="buy_config", style="primary"),
                InlineKeyboardButton("دریافت کانفنیگ", callback_data="get_my_config", style="primary")
            ],
            [
                InlineKeyboardButton("آیپی من", callback_data="my_ip", style="danger"),
                InlineKeyboardButton("رفرال", callback_data="referral", style="danger")
            ],
            [
                InlineKeyboardButton("ردیم کد", callback_data="redeem", style="success"),
                InlineKeyboardButton("زبان", callback_data="language", style="success")
            ],
            [
                InlineKeyboardButton("راهنما", callback_data="help", style="primary"),
                InlineKeyboardButton("AI", callback_data="ai_panel", style="primary")
            ],
            [
                InlineKeyboardButton("گزارش مشکل", callback_data="report_issue", style="danger")
            ]
        ])
    
    def buy_panel_buttons(self):
        buttons = []
        row = []
        
        price_items = [
            ("1gb", "۱ گیگ", "۵,۰۰۰"),
            ("2gb", "۲ گیگ", "۱۰,۰۰۰"),
            ("5gb", "۵ گیگ", "۲۵,۰۰۰"),
            ("10gb", "۱۰ گیگ", "۵۰,۰۰۰"),
            ("15gb", "۱۵ گیگ", "۷۵,۰۰۰"),
            ("20gb", "۲۰ گیگ", "۱۰۰,۰۰۰"),
            ("40gb", "۴۰ گیگ", "۱۶۰,۰۰۰"),
            ("50gb", "۵۰ گیگ", "۲۰۰,۰۰۰"),
            ("60gb", "۶۰ گیگ", "۱۸۰,۰۰۰"),
            ("80gb", "۸۰ گیگ", "۲۴۰,۰۰۰"),
            ("100gb", "۱۰۰ گیگ", "۳۰۰,۰۰۰"),
            ("150gb", "۱۵۰ گیگ", "۴۵۰,۰۰۰"),
            ("200gb", "۲۰۰ گیگ (تخفیف ویژه)", "۵۰۰,۰۰۰"),
            ("unlimited_1m", "نامحدود ۱ ماهه", "۱۰۰,۰۰۰"),
            ("unlimited_3m", "نامحدود ۳ ماهه", "۲۵۰,۰۰۰"),
            ("unlimited_5m", "نامحدود ۵ ماهه", "۴۰۰,۰۰۰")
        ]
        
        for i, (key, name, price) in enumerate(price_items):
            label = f"{name}\n{price} تومان"
            
            if key.startswith("unlimited") or key == "200gb":
                row.append(InlineKeyboardButton(label, callback_data=f"buy_{key}", style="danger"))
            else:
                row.append(InlineKeyboardButton(label, callback_data=f"buy_{key}", style="primary"))
            
            if len(row) == 2:
                buttons.append(row)
                row = []
        
        if row:
            buttons.append(row)
        
        buttons.append([InlineKeyboardButton("بازگشت", callback_data="back_main", style="primary")])
        
        return InlineKeyboardMarkup(buttons)
    
    def plan_info_buttons(self, plan_key: str):
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 پرداخت کارت به کارت", callback_data=f"payment_card_{plan_key}", style="primary")],
            [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_buy", style="danger")]
        ])
    
    def card_info_buttons(self, card_number: str):
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📋 کپی شماره کارت", callback_data=f"copy_card_{card_number}", style="primary"),
                InlineKeyboardButton("📤 ارسال رسید", callback_data="send_receipt", style="success")
            ],
            [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_buy", style="danger")]
        ])
    
    def receipt_back_buttons(self):
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_buy", style="danger")]
        ])
    
    def admin_panel_buttons(self):
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("اسکنر کانفنیگ", callback_data="admin_config_scanner", style="primary"),
                InlineKeyboardButton("اسکنر پروکسی", callback_data="admin_proxy_scanner", style="primary"),
                InlineKeyboardButton("ارسال به تاپیک", callback_data="admin_topic", style="primary")
            ],
            [
                InlineKeyboardButton("📊 آمار کامل", callback_data="admin_stats_full", style="success"),
                InlineKeyboardButton("ساخت ردیم کد", callback_data="admin_gen_redeem", style="success"),
                InlineKeyboardButton("ارسال همگانی", callback_data="admin_broadcast", style="success")
            ],
            [
                InlineKeyboardButton("مدیریت سفارشات", callback_data="admin_orders", style="primary"),
                InlineKeyboardButton("افزودن ادمین", callback_data="admin_add_admin", style="danger"),
                InlineKeyboardButton("حذف ادمین", callback_data="admin_remove_admin", style="danger")
            ],
            [
                InlineKeyboardButton("لیست ادمین‌ها", callback_data="admin_list", style="primary"),
                InlineKeyboardButton("بن/آنبن", callback_data="admin_ban_menu", style="danger"),
                InlineKeyboardButton("لاگ‌ها", callback_data="admin_log_menu", style="primary")
            ],
            [
                InlineKeyboardButton("لیست خریدهای موفق", callback_data="admin_successful_orders", style="success"),
                InlineKeyboardButton("لیست گزارشات", callback_data="admin_report_list", style="success")
            ],
            [
                InlineKeyboardButton("درخواست‌های ردیم کد", callback_data="admin_redeem_requests", style="primary"),
                InlineKeyboardButton("پاسخ مجدد به گزارش", callback_data="admin_reply_again", style="primary"),
                InlineKeyboardButton("بازگشت", callback_data="back_main", style="primary")
            ]
        ])
    
    def admin_orders_buttons(self):
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 لیست سفارشات", callback_data="admin_order_list", style="primary")],
            [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
        ])
    
    def admin_report_buttons(self):
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 لیست گزارشات", callback_data="admin_report_list", style="primary")],
            [InlineKeyboardButton("📝 پاسخ به گزارش", callback_data="admin_reply_report", style="success")],
            [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
        ])
    
    def topic_buttons(self):
        status = "🟢 روشن" if self.state.send_to_topic_enabled else "🔴 خاموش"
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("روشن", callback_data="topic_on", style="success"),
                InlineKeyboardButton("خاموش", callback_data="topic_off", style="danger")
            ],
            [InlineKeyboardButton("بازگشت", callback_data="back_to_admin", style="primary")]
        ])
    
    def main_menu_buttons(self, user_id: int):
        if self.state.is_admin(user_id):
            return InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("پنل ادمین", callback_data="admin_panel", style="primary"),
                    InlineKeyboardButton("پنل کاربری", callback_data="user_panel", style="success")
                ],
                [
                    InlineKeyboardButton("زبان", callback_data="language", style="danger")
                ]
            ])
        else:
            return self.user_panel_buttons()
    
    def ai_panel_buttons(self):
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("💬 چت هوشمند", callback_data="ai_chat", style="success"),
                InlineKeyboardButton("📝 تحلیل کد", callback_data="ai_code", style="danger")
            ],
            [
                InlineKeyboardButton("📖 راهنما", callback_data="ai_help", style="primary"),
                InlineKeyboardButton("🔙 بازگشت", callback_data="back_main", style="danger")
            ]
        ])
    
    def language_buttons(self):
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("English", callback_data="lang_en", style="primary"),
                InlineKeyboardButton("فارسی", callback_data="lang_fa", style="danger")
            ]
        ])
    
    def calculate_expiry(self, created_at: str, duration: str) -> str:
        try:
            if not created_at:
                return "نامشخص"
            
            created_date = datetime.fromisoformat(created_at)
            
            days = 30
            if "روز" in duration:
                try:
                    days = int(re.search(r'(\d+)', duration).group(1))
                except:
                    days = 30
            elif "ماه" in duration:
                try:
                    months = int(re.search(r'(\d+)', duration).group(1))
                    days = months * 30
                except:
                    days = 30
            
            expiry_date = created_date + timedelta(days=days)
            remaining = (expiry_date - datetime.now()).days
            
            if remaining < 0:
                return f"{expiry_date.strftime('%Y-%m-%d')} (منقضی شده)"
            else:
                return f"{expiry_date.strftime('%Y-%m-%d')} ({remaining} روز باقی)"
        except:
            return "نامشخص"
    
    # ==================== متن‌ها ====================
    WELCOME_TEXT = """
به ربات اسکنر خوش آمدید!

برای استفاده از ربات، ابتدا در چنل‌های زیر عضو شوید:

چنل کانفنیگ‌ها
چنل پروکسی‌ها

پس از عضویت، دکمه «تایید عضویت» را بزنید.

━━━━━━━━━━━━━━━━━━━━
@v2reya88 | @confinghub2
━━━━━━━━━━━━━━━━━━━━
"""
    
    AI_WELCOME_TEXT = """
🧠 **هوش مصنوعی - AI**

سلام! چطور می‌تونم کمک‌تون کنم؟

چه کارهایی بلدم؟

💬 **چت هوشمند**
• هر سوالی دارید بپرسید
• برنامه‌نویسی و کدنویسی
• پاسخ‌های دقیق و مفید

📝 **تحلیل کد**
• کدتان را بفرستید تا بررسی کنم
• پیدا کردن باگ‌ها
• پیشنهاد بهبود

فقط سوالتون رو بپرسید!
"""
    
    BUY_TEXT = """
🌩 درود به فروشگاه config هاپ خوش اومدی؛
پرسرعت | بدون قطعی | آیپی ثابت | مخصوص گیم

📱 سازگار با اندروید، ویندوز و آیفون

⚠️ به تبلیغات داخل ربات هم اصلا توجه نکنید
همشون کلاهبردار هستند، مسئولیتش باما نیست

❇️ برای خرید اشتراک
همین الان از طریق دکمه‌های پنل زیر شروع کن !

━━━━━━━━━━━━━━━━━━━━
📢 کانال ما:
@iranvpnboldbot
━━━━━━━━━━━━━━━━━━━━
"""
    
    PLAN_INFO_TEXT = """
📋 **اطلاعات پلن انتخابی شما:**

📦 حجم: {name}
⏳ مدت: {duration}
💰 قیمت: {price:,} تومان

📝 توضیحات: {desc}
⚡ بدون قطعی | پرسرعت | آیپی ثابت

✅ برای ادامه خرید، روی دکمه پرداخت کلیک کنید.
"""
    
    PAYMENT_TEXT = """
📋 کاربر گرامی، فاکتور شما با موفقیت ایجاد شد.

لطفاً مبلغ {price} تومان را به یکی از کارت‌های زیر واریز نمایید:

💳 کارت : 
5047061131076354

👤 نام صاحب کارت‌ها:
منوچهر بیات

پس از واریز، دکمه‌ی ارسال رسید را لمس کرده و رسید خود را ارسال نمایید.

⚠️ در صورت ارسال مجدد رسید با اکانت دیگر بدون اطلاع شما تمام سرویس های هر دو اکانت حذف خواهد شد.
"""
    
    RECEIPT_TEXT = """
📥 فیش واریزی رو بصورت عکس ارسال کنید

📂 بعد از بررسی توسط حساب‌داری ربات
موجودی شما بصورت خودکار افزایش می‌یابد

⚠️ فیش واریزی رو فقط یــکبار ارسال کنید
درصورت ارسال مجدد تراکنش تایید نمی‌شود

⏳ زمان تایید شدن تراکنش چــقدر است؟
بصورت تقریبی ۱۵ الی ۶۰ دقیقه طول می‌کشد
"""
    
    RECEIPT_SENT_TEXT = """
☑️ فیش واریزی شما با موفقیت ارسال شد.
بعد از بررسی توسط مدیریت، تایید خواهد شد.

⚠️ لطفاً از ارسال دوباره فیش خودداری کنید.
ارسال مجدد فیش منجر به رد درخواست خواهد شد.
"""
    
    REPORT_TEXT = """
گزارش مشکل

لطفاً یکی از گزینه‌های زیر را انتخاب کنید:

۱. مشکل کانفنیگ خریداری شده
۲. مشکل کانفنیگ رایگان
۳. مشکل پرداخت
۴. سایر مشکلات

شناسه کانفنیگ خود را وارد کنید و مشکل را توضیح دهید.
"""
    
    REDEEM_REQUEST_TEXT = """
🎫 **درخواست استفاده از کد تخفیف**

کاربر: {user_id}
یوزرنیم: {username}
کد تخفیف: {code}

✅ برای تایید و ارسال سرویس، روی دکمه تایید کلیک کنید.
"""

    REDEEM_APPROVED_TEXT = """
✅ کد تخفیف شما تایید شد!

🙏 ممنون از اعتماد شما
🔄 سرویس شما به زودی ارسال خواهد شد.

لطفاً منتظر باشید...
"""

    # ==================== وب پنل ====================
    async def web_admin_panel(self, request):
        try:
            stats = await self.db.get_all_stats()
            
            html = f"""
            <!DOCTYPE html>
            <html dir="rtl" lang="fa">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>پنل مدیریت ربات اسکنر</title>
                <style>
                    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                    body {{ font-family: 'Vazir', 'Segoe UI', Tahoma, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; padding: 20px; color: #333; }}
                    .container {{ max-width: 1400px; margin: 0 auto; }}
                    .header {{ background: rgba(255,255,255,0.95); border-radius: 20px; padding: 30px; margin-bottom: 30px; box-shadow: 0 10px 40px rgba(0,0,0,0.1); text-align: center; }}
                    .header h1 {{ font-size: 2.5em; background: linear-gradient(135deg, #667eea, #764ba2); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; }}
                    .header p {{ color: #666; font-size: 1.1em; margin-top: 10px; }}
                    .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin-bottom: 30px; }}
                    .stat-card {{ background: rgba(255,255,255,0.95); border-radius: 15px; padding: 15px; text-align: center; box-shadow: 0 5px 20px rgba(0,0,0,0.1); }}
                    .stat-card .number {{ font-size: 2em; font-weight: bold; color: #667eea; }}
                    .stat-card .label {{ font-size: 0.8em; color: #666; margin-top: 5px; }}
                    .section {{ background: rgba(255,255,255,0.95); border-radius: 20px; padding: 20px; margin-bottom: 30px; box-shadow: 0 5px 20px rgba(0,0,0,0.1); }}
                    .section-title {{ font-size: 1.3em; margin-bottom: 15px; color: #333; border-bottom: 3px solid #667eea; padding-bottom: 10px; }}
                    .table-container {{ overflow-x: auto; }}
                    table {{ width: 100%; border-collapse: collapse; font-size: 0.85em; }}
                    table thead {{ background: linear-gradient(135deg, #667eea, #764ba2); color: white; }}
                    table th {{ padding: 10px 12px; text-align: right; }}
                    table td {{ padding: 10px 12px; border-bottom: 1px solid #eee; }}
                    table tbody tr:hover {{ background: #f8f9fa; }}
                    .badge {{ display: inline-block; padding: 3px 10px; border-radius: 20px; font-size: 0.75em; font-weight: bold; }}
                    .badge-success {{ background: #d4edda; color: #155724; }}
                    .badge-danger {{ background: #f8d7da; color: #721c24; }}
                    .badge-warning {{ background: #fff3cd; color: #856404; }}
                    .refresh-btn {{ position: fixed; bottom: 30px; right: 30px; background: rgba(255,255,255,0.95); border: none; border-radius: 50px; padding: 12px 20px; box-shadow: 0 5px 20px rgba(0,0,0,0.2); cursor: pointer; font-size: 0.9em; font-weight: bold; color: #667eea; transition: all 0.3s; z-index: 1000; }}
                    .refresh-btn:hover {{ transform: scale(1.05); background: #667eea; color: white; }}
                    @media (max-width: 768px) {{ .stats-grid {{ grid-template-columns: repeat(2, 1fr); }} table {{ font-size: 0.75em; }} }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>📊 پنل مدیریت ربات اسکنر</h1>
                        <p>داشبورد مدیریت و آمار کامل ربات</p>
                        <p style="font-size: 0.85em; color: #999;">آخرین بروزرسانی: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    </div>
                    
                    <div class="stats-grid">
                        <div class="stat-card"><div class="number">{stats.get('total_users', 0)}</div><div class="label">👥 کاربران کل</div></div>
                        <div class="stat-card"><div class="number">{stats.get('active_users', 0)}</div><div class="label">🟢 کاربران فعال</div></div>
                        <div class="stat-card"><div class="number">{stats.get('total_configs_sold', 0)}</div><div class="label">📦 کانفنیگ فروخته شده</div></div>
                        <div class="stat-card"><div class="number">{stats.get('total_orders', 0)}</div><div class="label">🛒 کل سفارشات</div></div>
                        <div class="stat-card"><div class="number">{stats.get('pending_orders', 0)}</div><div class="label">⏳ سفارشات در انتظار</div></div>
                        <div class="stat-card"><div class="number">{stats.get('total_configs_sent', 0)}</div><div class="label">📤 کانفنیگ ارسال شده</div></div>
                        <div class="stat-card"><div class="number">{stats.get('total_proxies_sent', 0)}</div><div class="label">🔄 پروکسی ارسال شده</div></div>
                        <div class="stat-card"><div class="number">{stats.get('total_reports', 0)}</div><div class="label">📝 گزارشات</div></div>
                        <div class="stat-card"><div class="number">{stats.get('total_referrals', 0)}</div><div class="label">🔗 رفرال‌ها</div></div>
                    </div>
                    
                    <div class="section">
                        <h2 class="section-title">📋 آخرین خریدهای موفق</h2>
                        <div class="table-container">
                            <table>
                                <thead><tr><th>کاربر</th><th>یوزرنیم</th><th>حجم</th><th>مدت</th><th>تاریخ</th></tr></thead>
                                <tbody>
            """
            
            try:
                orders = await self.db.get_successful_orders()
                for order in orders[:10]:
                    html += f"""<tr>
                        <td>{order.get('user_id', '-')}</td>
                        <td>{order.get('username', '-')}</td>
                        <td>{order.get('size', '-')}</td>
                        <td>{order.get('duration', '-')}</td>
                        <td>{order.get('purchase_date', '')[:10] if order.get('purchase_date') else '-'}</td>
                    </tr>"""
                if not orders:
                    html += '<tr><td colspan="5" style="text-align:center;color:#999;">هیچ خریدی ثبت نشده است</td></tr>'
            except:
                html += '<tr><td colspan="5" style="text-align:center;color:#999;">خطا در دریافت داده</td></tr>'
            
            html += """
                                </tbody>
                            </table>
                        </div>
                    </div>
                    
                    <div class="section">
                        <h2 class="section-title">👥 لیست کاربران</h2>
                        <div class="table-container">
                            <table>
                                <thead><tr><th>آیدی</th><th>یوزرنیم</th><th>نام</th><th>تاریخ عضویت</th><th>وضعیت</th></tr></thead>
                                <tbody>
            """
            
            try:
                users = await self.db.get_all_users()
                for user in users[:20]:
                    status_badge = '<span class="badge badge-danger">بن</span>' if user.get('is_banned') else '<span class="badge badge-success">فعال</span>'
                    html += f"""<tr>
                        <td>{user.get('user_id', '-')}</td>
                        <td>{user.get('username', '-')}</td>
                        <td>{user.get('first_name', '')} {user.get('last_name', '')}</td>
                        <td>{user.get('joined_at', '')[:10] if user.get('joined_at') else '-'}</td>
                        <td>{status_badge}</td>
                    </tr>"""
                if not users:
                    html += '<tr><td colspan="5" style="text-align:center;color:#999;">هیچ کاربری ثبت نشده است</td></tr>'
            except:
                html += '<tr><td colspan="5" style="text-align:center;color:#999;">خطا در دریافت داده</td></tr>'
            
            html += """
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
                <button class="refresh-btn" onclick="location.reload()">🔄 بروزرسانی</button>
                <script>setTimeout(function(){{location.reload();}}, 60000);</script>
            </body>
            </html>
            """
            
            return web.Response(text=html, content_type='text/html')
            
        except Exception as e:
            logger.error(f"Web panel error: {e}")
            return web.Response(text=f"Error: {e}", status=500)
    
    # ==================== آمار کامل ====================
    async def admin_stats_full(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند!", show_alert=True)
            return
        
        await query.answer()
        
        try:
            stats = await self.db.get_all_stats()
            
            text = f"""📊 **آمار کامل ربات**

━━━━━━━━━━━━━━━━━━━━━
👥 **کاربران**
└ کل کاربران: {stats.get('total_users', 0)}
└ کاربران فعال: {stats.get('active_users', 0)}
└ کاربران بن: {stats.get('total_users', 0) - stats.get('active_users', 0)}

🛒 **فروش**
└ کل سفارشات: {stats.get('total_orders', 0)}
└ سفارشات در انتظار: {stats.get('pending_orders', 0)}
└ کانفنیگ فروخته شده: {stats.get('total_configs_sold', 0)}

📤 **ارسال‌ها**
└ کانفنیگ ارسال شده به چنل: {stats.get('total_configs_sent', 0)}
└ پروکسی ارسال شده به چنل: {stats.get('total_proxies_sent', 0)}

📝 **گزارشات**
└ کل گزارشات: {stats.get('total_reports', 0)}

🔗 **رفرال**
└ کل رفرال‌ها: {stats.get('total_referrals', 0)}

━━━━━━━━━━━━━━━━━━━━━
📅 آخرین بروزرسانی: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 بروزرسانی", callback_data="admin_stats_full", style="primary")],
                [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
            ])
            
            await query.edit_message_text(
                text,
                reply_markup=keyboard,
                parse_mode=ParseMode.MARKDOWN
            )
            
        except Exception as e:
            logger.error(f"Stats error: {e}")
            await query.edit_message_text(
                f"❌ خطا در دریافت آمار: {e}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
                ])
            )
    
    # ==================== مدیریت تاپیک ====================
    async def admin_topic_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند!", show_alert=True)
            return
        
        await query.answer()
        status = "🟢 روشن" if self.state.send_to_topic_enabled else "🔴 خاموش"
        await query.edit_message_text(
            f"📤 **ارسال به تاپیک**\n\nوضعیت: {status}\n\n"
            f"با روشن بودن این گزینه، کانفنیگ‌ها و پروکسی‌ها به تاپیک گروه نیز ارسال می‌شوند.\n"
            f"⚠️ فقط کانفنیگ‌های معتبر (vless://, vmess://, trojan://, ss://, hy2://, wireguard://) فورارد می‌شوند.",
            reply_markup=self.topic_buttons(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def topic_on(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند!", show_alert=True)
            return
        
        self.state.send_to_topic_enabled = True
        await query.answer("✅ ارسال به تاپیک روشن شد!")
        await self.admin_topic_panel(update, context)
    
    async def topic_off(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند!", show_alert=True)
            return
        
        self.state.send_to_topic_enabled = False
        await query.answer("🔴 ارسال به تاپیک خاموش شد!")
        await self.admin_topic_panel(update, context)
    
    # ==================== مدیریت سفارشات ====================
    async def admin_orders_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند.", show_alert=True)
            return
        
        await query.answer()
        await query.edit_message_text(
            "📋 پنل مدیریت سفارشات",
            reply_markup=self.admin_orders_buttons(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_order_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند!", show_alert=True)
            return
        
        await query.answer()
        
        try:
            orders_with_receipt = []
            for uid, order in self.state.pending_orders.items():
                if order.get('receipt_sent', False):
                    orders_with_receipt.append((uid, order))
            
            if not orders_with_receipt:
                await query.edit_message_text(
                    "📭 هیچ سفارشی در انتظار نیست!",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 بازگشت", callback_data="admin_orders", style="danger")]
                    ])
                )
                return
            
            buttons = []
            for uid, order in orders_with_receipt:
                username_display = ""
                try:
                    user = await self.db.get_user(uid)
                    if user and user.get('username'):
                        username_display = f" (@{user['username']})"
                except:
                    pass
                
                button_text = f"👤 کاربر {uid}{username_display} - {order.get('name', 'نامشخص')}"
                buttons.append([InlineKeyboardButton(button_text, callback_data=f"view_receipt_{uid}", style="primary")])
            
            buttons.append([InlineKeyboardButton("🔙 بازگشت", callback_data="admin_orders", style="danger")])
            
            await query.edit_message_text(
                "📋 لیست سفارشات در انتظار تایید:\n\nبرای مشاهده رسید هر سفارش، روی آن کلیک کنید.",
                reply_markup=InlineKeyboardMarkup(buttons),
                parse_mode=ParseMode.MARKDOWN
            )
            
        except Exception as e:
            logger.error(f"Admin order list error: {e}")
            await query.edit_message_text(
                f"❌ خطا: {e}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 بازگشت", callback_data="admin_orders", style="danger")]
                ])
            )
    
    async def view_receipt(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        admin_id = query.from_user.id
        data = query.data
        
        if not self.state.is_admin(admin_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند!", show_alert=True)
            return
        
        if data.startswith("view_receipt_"):
            user_id = int(data.replace("view_receipt_", ""))
            order = self.state.pending_orders.get(user_id)
            
            if not order or not order.get('receipt_photo'):
                await query.answer("رسیدی برای این سفارش وجود ندارد!", show_alert=True)
                return
            
            await query.answer()
            
            user_info = ""
            try:
                user = await self.db.get_user(user_id)
                if user and user.get('username'):
                    user_info = f"📛 یوزرنیم: @{user['username']}"
                else:
                    user_info = "📛 یوزرنیم: ندارد"
            except:
                user_info = "📛 یوزرنیم: نامشخص"
            
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ تایید", callback_data=f"confirm_order_{user_id}", style="success"),
                    InlineKeyboardButton("❌ عدم تایید", callback_data=f"reject_order_{user_id}", style="danger")
                ],
                [
                    InlineKeyboardButton("🔙 بازگشت", callback_data="admin_order_list", style="primary")
                ]
            ])
            
            caption = f"""📋 **رسید سفارش**

🆔 کاربر: {user_id}
{user_info}
📦 حجم: {order.get('name', 'نامشخص')}
💰 مبلغ: {order.get('price', 0):,} تومان
📅 تاریخ: {order.get('receipt_time', '').split('T')[0] if order.get('receipt_time') else '-'}
"""
            
            try:
                await query.message.reply_photo(
                    photo=order['receipt_photo'],
                    caption=caption,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.MARKDOWN
                )
                try:
                    await query.message.delete()
                except:
                    pass
            except Exception as e:
                logger.error(f"Error sending receipt photo: {e}")
                await query.edit_message_text("❌ خطا در نمایش رسید!")
    
    async def confirm_order(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        admin_id = query.from_user.id
        data = query.data
        
        if not self.state.is_admin(admin_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند.", show_alert=True)
            return
        
        if data.startswith("confirm_order_"):
            user_id = int(data.replace("confirm_order_", ""))
            
            self.state.pending_config_user = user_id
            
            await query.answer("✅ سفارش تایید شد!")
            
            try:
                await query.message.delete()
            except:
                pass
            
            await query.message.reply_text(
                f"✅ **سفارش کاربر {user_id} تایید شد!**\n\n"
                f"📝 لطفاً کانفنیگ را ارسال کنید:",
                parse_mode=ParseMode.MARKDOWN
            )
            context.user_data['waiting_for_config'] = True
    
    async def reject_order(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        admin_id = query.from_user.id
        data = query.data
        
        if not self.state.is_admin(admin_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند.", show_alert=True)
            return
        
        if data.startswith("reject_order_"):
            user_id = int(data.replace("reject_order_", ""))
            
            order = self.state.pending_orders.get(user_id)
            if order and order.get('order_id'):
                await self.db.reject_order(order['order_id'])
            
            if user_id in self.state.pending_orders:
                del self.state.pending_orders[user_id]
            
            await query.answer("❌ سفارش رد شد!")
            await query.edit_message_text(
                f"❌ سفارش کاربر {user_id} رد شد!",
                parse_mode=ParseMode.MARKDOWN
            )
    
    # ==================== لیست خریدهای موفق ====================
    async def admin_successful_orders(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """لیست همه خریدهای موفق با جزئیات"""
        query = update.callback_query
        user_id = query.from_user.id
        
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند!", show_alert=True)
            return
        
        await query.answer()
        
        try:
            orders = await self.db.get_successful_orders()
            
            if not orders:
                await query.edit_message_text(
                    "📭 هیچ خرید موفقی ثبت نشده است!",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            text = "📋 **لیست همه خریدهای موفق:**\n\n"
            
            for i, order in enumerate(orders[:20], 1):
                text += f"{i}. 👤 کاربر: {order.get('user_id')}\n"
                text += f"   📛 یوزرنیم: {order.get('username', 'ندارد')}\n"
                text += f"   📦 حجم: {order.get('size', 'نامشخص')}\n"
                text += f"   ⏳ مدت: {order.get('duration', 'نامشخص')}\n"
                text += f"   🆔 `{order.get('config_id', '')}`\n"
                text += f"   📅 {order.get('purchase_date', '')[:10] if order.get('purchase_date') else 'نامشخص'}\n\n"
            
            if len(orders) > 20:
                text += f"\n📌 {len(orders)} خرید وجود دارد.\nبرای مشاهده کامل، از دکمه زیر استفاده کنید."
            
            keyboard = [
                [InlineKeyboardButton("📥 دانلود لیست کامل", callback_data="admin_export_orders", style="primary")],
                [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
            ]
            
            await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
            
        except Exception as e:
            logger.error(f"Admin successful orders error: {e}")
            await query.edit_message_text(
                f"❌ خطا: {e}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
                ])
            )
    
    async def admin_export_orders(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """خروجی گرفتن از همه خریدها"""
        query = update.callback_query
        user_id = query.from_user.id
        
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند!", show_alert=True)
            return
        
        await query.answer("🔄 در حال آماده‌سازی...")
        
        try:
            orders = await self.db.get_successful_orders()
            
            orders_data = {
                "export_date": datetime.now().isoformat(),
                "total_orders": len(orders),
                "orders": orders
            }
            
            json_data = json.dumps(orders_data, ensure_ascii=False, indent=2, default=str)
            
            file_buffer = io.BytesIO(json_data.encode('utf-8'))
            file_buffer.name = f"orders_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            await query.message.reply_document(
                document=file_buffer,
                caption=f"📋 خروجی خریدها\nتاریخ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nتعداد: {len(orders)}",
                parse_mode=ParseMode.MARKDOWN
            )
            
            await query.edit_message_text(
                "✅ خریدها با موفقیت دانلود شد!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 بازگشت", callback_data="admin_successful_orders", style="primary")]
                ])
            )
            
        except Exception as e:
            logger.error(f"Export orders error: {e}")
            await query.edit_message_text(f"❌ خطا: {e}")
    
    # ==================== لیست گزارشات ====================
    async def admin_report_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """لیست همه گزارشات با وضعیت"""
        query = update.callback_query
        user_id = query.from_user.id
        
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند!", show_alert=True)
            return
        
        await query.answer()
        
        try:
            reports = await self.db.get_reports()
            
            if not reports:
                await query.edit_message_text(
                    "📭 هیچ گزارشی ثبت نشده است!",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
                    ])
                )
                return
            
            pending_reports = [r for r in reports if r.get('status') == 'pending']
            answered_reports = [r for r in reports if r.get('status') == 'answered']
            
            text = "📋 **لیست گزارشات مشکلات:**\n\n"
            
            if pending_reports:
                text += "🟡 **در انتظار پاسخ:**\n"
                for report in pending_reports[:10]:
                    text += f"┌ 🆔 `{report.get('report_id')}`\n"
                    text += f"├ 👤 کاربر: {report.get('user_id')}\n"
                    text += f"├ 📛 یوزرنیم: {report.get('username', 'ندارد')}\n"
                    text += f"├ 📋 نوع: {report.get('report_type', 'نامشخص')}\n"
                    text += f"└ 📌 وضعیت: در انتظار ⏳\n\n"
            
            if answered_reports:
                text += "🟢 **پاسخ داده شده:**\n"
                for report in answered_reports[:10]:
                    text += f"┌ 🆔 `{report.get('report_id')}`\n"
                    text += f"├ 👤 کاربر: {report.get('user_id')}\n"
                    text += f"├ 📛 یوزرنیم: {report.get('username', 'ندارد')}\n"
                    text += f"├ 📋 نوع: {report.get('report_type', 'نامشخص')}\n"
                    text += f"└ 📌 وضعیت: پاسخ داده شده ✅\n\n"
            
            if len(reports) > 20:
                text += f"\n📌 {len(reports)} گزارش وجود دارد."
            
            keyboard = [
                [InlineKeyboardButton("🆕 پاسخ به گزارش جدید", callback_data="admin_reply_report", style="primary")],
                [InlineKeyboardButton("🔄 پاسخ مجدد به گزارش", callback_data="admin_reply_again", style="success")],
                [InlineKeyboardButton("📥 دانلود همه گزارشات", callback_data="admin_export_reports", style="primary")],
                [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
            ]
            
            await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
            
        except Exception as e:
            logger.error(f"Admin report list error: {e}")
            await query.edit_message_text(
                f"❌ خطا: {e}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
                ])
            )
    
    async def admin_export_reports(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """خروجی گرفتن از همه گزارشات"""
        query = update.callback_query
        user_id = query.from_user.id
        
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند!", show_alert=True)
            return
        
        await query.answer("🔄 در حال آماده‌سازی...")
        
        try:
            reports = await self.db.get_reports()
            
            reports_data = {
                "export_date": datetime.now().isoformat(),
                "total_reports": len(reports),
                "reports": reports
            }
            
            json_data = json.dumps(reports_data, ensure_ascii=False, indent=2, default=str)
            
            file_buffer = io.BytesIO(json_data.encode('utf-8'))
            file_buffer.name = f"reports_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            await query.message.reply_document(
                document=file_buffer,
                caption=f"📋 خروجی گزارشات\nتاریخ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nتعداد: {len(reports)}",
                parse_mode=ParseMode.MARKDOWN
            )
            
            await query.edit_message_text(
                "✅ گزارشات با موفقیت دانلود شد!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 بازگشت", callback_data="admin_report_list", style="primary")]
                ])
            )
            
        except Exception as e:
            logger.error(f"Export reports error: {e}")
            await query.edit_message_text(f"❌ خطا: {e}")
    
    async def admin_reply_again(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """پاسخ مجدد به یک گزارش (حتی اگر پاسخ داده شده باشد)"""
        query = update.callback_query
        admin_id = query.from_user.id
        
        if not self.state.is_admin(admin_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند!", show_alert=True)
            return
        
        await query.answer()
        
        try:
            reports = await self.db.get_reports()
            
            if not reports:
                await query.edit_message_text(
                    "📭 هیچ گزارشی ثبت نشده است!",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
                    ])
                )
                return
            
            buttons = []
            for report in reports:
                status_text = "✅" if report.get('status') == 'answered' else "⏳"
                report_id = report.get('report_id', 'نامشخص')
                user_id = report.get('user_id', 'نامشخص')
                buttons.append([
                    InlineKeyboardButton(
                        f"{status_text} {report_id} - کاربر {user_id}",
                        callback_data=f"reply_report_{report_id}",
                        style="primary"
                    )
                ])
            
            buttons.append([InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")])
            
            await query.edit_message_text(
                "📝 **پاسخ مجدد به گزارشات:**\n\n"
                "🟢 پاسخ داده شده | 🟡 در انتظار\n\n"
                "روی هر گزارش کلیک کنید تا پاسخ دهید:",
                reply_markup=InlineKeyboardMarkup(buttons),
                parse_mode=ParseMode.MARKDOWN
            )
            
        except Exception as e:
            logger.error(f"Admin reply again error: {e}")
            await query.edit_message_text(
                f"❌ خطا: {e}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
                ])
            )
    
    # ==================== سیستم خرید و فروش ====================
    async def buy_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        membership = await self.check_membership(user_id)
        if not membership.get('config') or not membership.get('proxy'):
            await query.answer("ابتدا در چنل‌ها عضو شوید!", show_alert=True)
            return
        
        await query.answer()
        await query.edit_message_text(self.BUY_TEXT, reply_markup=self.buy_panel_buttons(), parse_mode=ParseMode.MARKDOWN)
    
    async def buy_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        data = query.data
        
        if not data.startswith("buy_"):
            return
        
        plan_key = data.replace("buy_", "")
        plan_name = PRICE_NAMES.get(plan_key, plan_key)
        plan_price = PRICES.get(plan_key, 0)
        plan_duration = PLAN_DETAILS.get(plan_key, {}).get("duration", "نامشخص")
        plan_desc = PLAN_DETAILS.get(plan_key, {}).get("desc", "بدون توضیحات")
        
        self.state.pending_plan_info = {
            'key': plan_key,
            'name': plan_name,
            'price': plan_price,
            'duration': plan_duration,
            'desc': plan_desc
        }
        
        await query.answer()
        await query.edit_message_text(
            self.PLAN_INFO_TEXT.format(
                name=plan_name,
                duration=plan_duration,
                price=plan_price,
                desc=plan_desc
            ),
            reply_markup=self.plan_info_buttons(plan_key),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def payment_card(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        data = query.data
        
        if data.startswith("payment_card_"):
            plan_key = data.replace("payment_card_", "")
            
            plan_info = self.state.pending_plan_info
            if not plan_info:
                await query.answer("خطا در اطلاعات پلن!", show_alert=True)
                return
            
            user_info = await self.db.get_user(user_id)
            username = user_info['username'] if user_info else None
            
            order_id = await self.db.add_order(
                user_id=user_id,
                username=username,
                plan_key=plan_key,
                plan_name=plan_info['name'],
                price=plan_info['price'],
                duration=plan_info['duration']
            )
            
            self.state.pending_orders[user_id] = {
                'order_id': order_id,
                'plan': plan_key,
                'price': plan_info['price'],
                'name': plan_info['name'],
                'duration': plan_info['duration'],
                'desc': plan_info['desc'],
                'status': 'pending',
                'created_at': datetime.now().isoformat(),
                'receipt_sent': False
            }
            
            await query.answer()
            
            card_number = "5047061131076354"
            
            await query.edit_message_text(
                self.PAYMENT_TEXT.format(price=f"{plan_info['price']:,}"),
                reply_markup=self.card_info_buttons(card_number),
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def copy_card(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        data = query.data
        
        if data.startswith("copy_card_"):
            card_number = data.replace("copy_card_", "")
            await query.answer(f"✅ شماره کارت کپی شد!\n{card_number}", show_alert=True)
            await query.message.reply_text(f"📋 شماره کارت کپی شد:\n\n`{card_number}`", parse_mode=ParseMode.MARKDOWN)
    
    async def send_receipt(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        await query.answer()
        context.user_data['waiting_for_receipt'] = True
        
        await query.edit_message_text(
            self.RECEIPT_TEXT,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_buy", style="danger")]]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def receive_receipt(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        if not context.user_data.get('waiting_for_receipt'):
            return
        
        if not update.message.photo:
            await update.message.reply_text("❌ لطفاً یک تصویر از رسید ارسال کنید.")
            return
        
        order = self.state.pending_orders.get(user_id)
        if not order:
            await update.message.reply_text("❌ سفارشی یافت نشد.\nلطفاً دوباره از بخش خرید اقدام کنید.")
            context.user_data['waiting_for_receipt'] = False
            return
        
        try:
            photo_file_id = update.message.photo[-1].file_id
            order['receipt_sent'] = True
            order['receipt_time'] = datetime.now().isoformat()
            order['receipt_photo'] = photo_file_id
            
            if order.get('order_id'):
                await self.db.update_order_receipt(order['order_id'], photo_file_id)
            
            caption = f"""📦 **سفارش جدید**

👤 کاربر: {user_id}
📦 حجم: {order.get('name', 'نامشخص')}
💰 مبلغ: {order.get('price', 0):,} تومان
🆔 شناسه سفارش: {order.get('order_id', user_id)}

✅ برای تایید سفارش، روی دکمه تایید کلیک کنید.
            """
            
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ تایید", callback_data=f"confirm_order_{user_id}", style="success"),
                    InlineKeyboardButton("❌ عدم تایید", callback_data=f"reject_order_{user_id}", style="danger")
                ]
            ])
            
            sent_to_admins = False
            for admin_id in self.state.admins:
                try:
                    await self.application.bot.send_photo(
                        chat_id=admin_id,
                        photo=photo_file_id,
                        caption=caption,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.MARKDOWN
                    )
                    sent_to_admins = True
                except Exception as e:
                    logger.error(f"Error sending receipt to admin {admin_id}: {e}")
            
            if not sent_to_admins:
                try:
                    await self.application.bot.send_photo(
                        chat_id=OWNER_ID,
                        photo=photo_file_id,
                        caption=caption,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.MARKDOWN
                    )
                except:
                    pass
            
            await update.message.reply_text(
                self.RECEIPT_SENT_TEXT,
                reply_markup=self.receipt_back_buttons(),
                parse_mode=ParseMode.MARKDOWN
            )
            
        except Exception as e:
            logger.error(f"Error in receive_receipt: {e}")
            await update.message.reply_text(
                "❌ خطا در ارسال رسید!\nلطفاً دوباره امتحان کنید.",
                parse_mode=ParseMode.MARKDOWN
            )
        
        context.user_data['waiting_for_receipt'] = False
    
    # ==================== دریافت کانفنیگ از ادمین ====================
    async def receive_config_from_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = self.state.pending_config_user
        
        if not user_id:
            await update.message.reply_text(
                "❌ ابتدا سفارش را تایید کنید!",
                parse_mode=ParseMode.MARKDOWN
            )
            context.user_data['waiting_for_config'] = False
            return
        
        config_text = update.message.text.strip()
        
        if not config_text:
            await update.message.reply_text("❌ کانفنیگ نمی‌تواند خالی باشد!")
            return
        
        order = self.state.pending_orders.get(user_id, {})
        size = order.get('name', 'نامشخص')
        duration = order.get('duration', 'نامشخص')
        
        config_id = f"CFG_{user_id}_{int(time.time())}"
        
        user_info = await self.db.get_user(user_id)
        username = user_info['username'] if user_info else None
        
        self.state.temp_config = {
            'user_id': user_id,
            'username': username,
            'config': config_text,
            'config_id': config_id,
            'size': size,
            'duration': duration,
            'created_at': datetime.now().isoformat()
        }
        
        self.state.pending_config_user = None
        context.user_data['waiting_for_config'] = False
        
        self.state.pending_subscription_user = user_id
        context.user_data['waiting_for_subscription'] = True
        
        await update.message.reply_text(
            f"✅ **کانفنیگ دریافت شد!**\n\n"
            f"📝 لطفاً لینک ساب (Subscription) را ارسال کنید:\n"
            f"(اگر لینک ساب ندارید، عدد 0 را وارد کنید)",
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def receive_subscription_from_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = self.state.pending_subscription_user
        
        if not user_id:
            await update.message.reply_text(
                "❌ ابتدا کانفنیگ را ارسال کنید!",
                parse_mode=ParseMode.MARKDOWN
            )
            context.user_data['waiting_for_subscription'] = False
            return
        
        subscription_link = update.message.text.strip()
        
        if not self.state.temp_config:
            await update.message.reply_text(
                "❌ کانفنیگی برای ارسال یافت نشد!\nلطفاً دوباره کانفنیگ را ارسال کنید.",
                parse_mode=ParseMode.MARKDOWN
            )
            context.user_data['waiting_for_subscription'] = False
            return
        
        temp_config = self.state.temp_config
        user_id = temp_config.get('user_id')
        username = temp_config.get('username')
        config_text = temp_config.get('config')
        config_id = temp_config.get('config_id')
        size = temp_config.get('size', 'نامشخص')
        duration = temp_config.get('duration', 'نامشخص')
        
        service_name = f"proservice{random.randint(10000, 99999)}"
        tracking_code = str(random.randint(1000000000, 9999999999))
        
        await self.db.add_successful_order(
            user_id=user_id,
            username=username,
            config_id=config_id,
            size=size,
            duration=duration,
            service_name=service_name,
            tracking_code=tracking_code,
            subscription_link=subscription_link if subscription_link != "0" else ""
        )
        
        await self.db.add_config(
            config_id=config_id,
            user_id=user_id,
            username=username,
            size=size,
            duration=duration,
            service_name=service_name,
            tracking_code=tracking_code,
            subscription_link=subscription_link if subscription_link != "0" else "",
            config_text=config_text
        )
        
        self.state.config_ids[config_id] = {
            'user_id': user_id,
            'username': username,
            'size': size,
            'duration': duration,
            'service_name': service_name,
            'tracking_code': tracking_code,
            'subscription_link': subscription_link if subscription_link != "0" else "",
            'config': config_text,
            'created_at': datetime.now().isoformat(),
            'status': 'active'
        }
        
        order = self.state.pending_orders.get(user_id)
        if order and order.get('order_id'):
            await self.db.confirm_order(order['order_id'], config_id)
        
        created_at = datetime.now().isoformat()
        expiry_date = self.calculate_expiry(created_at, duration)
        
        if subscription_link == "0" or not subscription_link:
            sub_link_text = "لینک ساب برای این سرویس ثبت نشده است."
        else:
            sub_link_text = subscription_link
        
        message = f"""✅ کانفنیگ شما ارسال شد!

🔍 نام اصلی سرویس: {service_name}
📂 کد پیگیری سرویس: {tracking_code}
💻 وضعیت سرویس: فعال ✅
〰️〰️〰️〰️〰️〰️
📦 حجم سرویس: {size}
📆 تاریخ انقضا: {expiry_date}
〰️〰️〰️〰️〰️〰️
🔗 لینک کانفنیگ : {config_text}

🔗 لینک اتصال (Subscription ) : امن و سریع 
{sub_link_text}"""
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📞 گزارش مشکل", callback_data=f"report_{config_id}", style="danger")],
            [InlineKeyboardButton("🔙 منو اصلی", callback_data="back_main", style="primary")]
        ])
        
        try:
            await self.application.bot.send_message(
                chat_id=user_id,
                text=message,
                reply_markup=keyboard,
                parse_mode=ParseMode.MARKDOWN
            )
            
            if user_id in self.state.pending_orders:
                del self.state.pending_orders[user_id]
            
            self.state.pending_subscription_user = None
            self.state.temp_config = None
            context.user_data['waiting_for_subscription'] = False
            
            await update.message.reply_text(
                f"✅ **کانفنیگ و اطلاعات سرویس با موفقیت به کاربر {user_id} ارسال شد!**\n\n"
                f"🆔 شناسه کانفنیگ: `{config_id}`",
                parse_mode=ParseMode.MARKDOWN
            )
            
        except Exception as e:
            logger.error(f"Error sending config: {e}")
            await update.message.reply_text(f"❌ خطا در ارسال: {e}")
    
    # ==================== سیستم رفرال ====================
    async def referral(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        code = await self.db.get_referral_code(user_id)
        count = await self.db.get_referral_count(user_id)
        
        link = f"https://t.me/{self.application.bot.username}?start=ref_{user_id}"
        
        referrals = await self.db.get_referrals(user_id)
        
        referrals_list = ""
        if referrals:
            for i, ref in enumerate(referrals, 1):
                ref_username = ref.get('referred_username', f"کاربر {ref.get('referred_id')}")
                referrals_list += f"{i}. {ref_username}\n"
        else:
            referrals_list = "هنوز کسی را دعوت نکرده‌اید."
        
        eligible_codes = count // 3
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 کپی لینک", callback_data=f"copy_ref_{user_id}", style="primary")],
            [InlineKeyboardButton("🎁 دریافت کد تخفیف", callback_data="get_referral_reward", style="success")],
            [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_user", style="danger")]
        ])
        
        await query.edit_message_text(
            f"👥 **سیستم رفرال**\n\n"
            f"لینک دعوت شما:\n`{link}`\n\n"
            f"تعداد دعوت‌های شما: {count}\n\n"
            f"📊 **لیست دعوت‌شدگان:**\n{referrals_list}\n\n"
            f"🎁 **پاداش:**\n"
            f"هر 3 دعوت = 1 کد تخفیف\n"
            f"تعداد کدهای قابل دریافت: {eligible_codes}",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def copy_referral_link(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        data = query.data
        
        if data.startswith("copy_ref_"):
            user_id = int(data.replace("copy_ref_", ""))
            link = f"https://t.me/{self.application.bot.username}?start=ref_{user_id}"
            
            await query.answer(f"✅ لینک کپی شد!\n{link}", show_alert=True)
            await query.message.reply_text(
                f"📋 **لینک دعوت شما کپی شد:**\n\n`{link}`\n\n"
                f"این لینک را برای دوستان خود ارسال کنید.",
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def get_referral_reward(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        count = await self.db.get_referral_count(user_id)
        eligible_codes = count // 3
        
        user_redeem_count = 0
        for code, uid in self.state.config_redeem_usage.items():
            if uid == user_id:
                user_redeem_count += 1
        
        if eligible_codes <= user_redeem_count:
            await query.answer("❌ شما به تعداد کافی رفرال برای دریافت کد جدید ندارید!", show_alert=True)
            return
        
        code = await self.db.get_referral_code(user_id)
        self.state.config_redeem_codes[code] = None
        self.state.config_redeem_usage[code] = user_id
        
        await query.answer("🎁 کد تخفیف شما ساخته شد!", show_alert=True)
        await query.edit_message_text(
            f"🎫 **کد تخفیف شما:**\n\n`{code}`\n\n"
            f"✅ این کد را در بخش **«ردیم کد»** وارد کنید.\n"
            f"📌 هر کد فقط یک بار قابل استفاده است.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 بازگشت به رفرال", callback_data="referral", style="primary")]
            ])
        )
    
    # ==================== سیستم ردیم کد ====================
    async def redeem(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        await query.answer()
        await query.edit_message_text(
            f"🎫 **ورود کد تخفیف**\n\n"
            f"لطفاً کد تخفیف خود را وارد کنید:\n"
            f"(کد را از بخش رفرال دریافت کنید)",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_user", style="danger")]
            ])
        )
        context.user_data['waiting_for_redeem'] = True
    
    async def receive_redeem_code(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        code = update.message.text.strip().upper()
        
        if not context.user_data.get('waiting_for_redeem'):
            return
        
        if code in self.state.config_redeem_codes:
            if code in self.state.config_redeem_used:
                await update.message.reply_text("❌ این کد قبلاً استفاده شده است!")
                context.user_data['waiting_for_redeem'] = False
                return
            
            request_id = f"REQ_{user_id}_{int(time.time())}"
            
            username = ""
            user_info = await self.db.get_user(user_id)
            if user_info and user_info.get('username'):
                username = f"@{user_info['username']}"
            
            self.state.redeem_requests[request_id] = {
                'user_id': user_id,
                'username': username,
                'code': code,
                'status': 'pending',
                'created_at': datetime.now().isoformat()
            }
            
            await update.message.reply_text(
                self.REDEEM_APPROVED_TEXT,
                parse_mode=ParseMode.MARKDOWN
            )
            
            for admin_id in self.state.admins:
                try:
                    keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("✅ تایید", callback_data=f"approve_redeem_{request_id}", style="success"),
                            InlineKeyboardButton("❌ رد", callback_data=f"reject_redeem_{request_id}", style="danger")
                        ]
                    ])
                    
                    await self.application.bot.send_message(
                        chat_id=admin_id,
                        text=self.REDEEM_REQUEST_TEXT.format(
                            user_id=user_id,
                            username=username if username else "ندارد",
                            code=code
                        ),
                        reply_markup=keyboard,
                        parse_mode=ParseMode.MARKDOWN
                    )
                except:
                    pass
            
            context.user_data['waiting_for_redeem'] = False
            
        else:
            await update.message.reply_text("❌ کد تخفیف نامعتبر است!")
            context.user_data['waiting_for_redeem'] = False
    
    async def admin_redeem_requests(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند!", show_alert=True)
            return
        
        await query.answer()
        
        if not self.state.redeem_requests:
            await query.edit_message_text(
                "هیچ درخواست ردیم کدی وجود ندارد!",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("بازگشت", callback_data="back_to_admin", style="danger")]
                ])
            )
            return
        
        text = "📋 **درخواست‌های ردیم کد:**\n\n"
        for req_id, req in self.state.redeem_requests.items():
            if req.get('status') == 'pending':
                text += f"🆔 {req_id}\n"
                text += f"👤 کاربر: {req.get('user_id')}\n"
                text += f"📛 یوزرنیم: {req.get('username', 'ندارد')}\n"
                text += f"🎫 کد: {req.get('code')}\n"
                text += f"📌 وضعیت: در انتظار\n\n"
        
        if "در انتظار" not in text:
            text += "✅ هیچ درخواست در انتظاری وجود ندارد."
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("بازگشت", callback_data="back_to_admin", style="danger")]
        ])
        
        await query.edit_message_text(
            text,
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def approve_redeem(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        admin_id = query.from_user.id
        data = query.data
        
        if not self.state.is_admin(admin_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند!", show_alert=True)
            return
        
        if data.startswith("approve_redeem_"):
            request_id = data.replace("approve_redeem_", "")
            request = self.state.redeem_requests.get(request_id)
            
            if not request:
                await query.answer("درخواست یافت نشد!", show_alert=True)
                return
            
            if request.get('status') != 'pending':
                await query.answer("این درخواست قبلاً بررسی شده!", show_alert=True)
                return
            
            user_id = request.get('user_id')
            code = request.get('code')
            
            self.state.config_redeem_used.add(code)
            self.state.redeem_requests[request_id]['status'] = 'approved'
            
            await query.answer("✅ درخواست تایید شد!")
            
            await query.edit_message_text(
                f"✅ **درخواست کاربر {user_id} تایید شد!**\n\n"
                f"🎫 کد: {code}\n"
                f"📝 لطفاً کانفنیگ را ارسال کنید:",
                parse_mode=ParseMode.MARKDOWN
            )
            
            self.state.pending_config_user = user_id
            context.user_data['waiting_for_config'] = True
    
    async def reject_redeem(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        admin_id = query.from_user.id
        data = query.data
        
        if not self.state.is_admin(admin_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند!", show_alert=True)
            return
        
        if data.startswith("reject_redeem_"):
            request_id = data.replace("reject_redeem_", "")
            request = self.state.redeem_requests.get(request_id)
            
            if not request:
                await query.answer("درخواست یافت نشد!", show_alert=True)
                return
            
            if request.get('status') != 'pending':
                await query.answer("این درخواست قبلاً بررسی شده!", show_alert=True)
                return
            
            user_id = request.get('user_id')
            
            self.state.redeem_requests[request_id]['status'] = 'rejected'
            
            await query.answer("❌ درخواست رد شد!")
            
            await query.edit_message_text(
                f"❌ **درخواست کاربر {user_id} رد شد!**",
                parse_mode=ParseMode.MARKDOWN
            )
            
            try:
                await self.application.bot.send_message(
                    chat_id=user_id,
                    text="❌ متاسفانه درخواست استفاده از کد تخفیف شما رد شد.\n\n"
                         "لطفاً با پشتیبانی تماس بگیرید.",
                    parse_mode=ParseMode.MARKDOWN
                )
            except:
                pass
    
    # ==================== سیستم گزارش ====================
    async def report_issue(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("مشکل کانفنیگ خرید", callback_data="report_bought", style="danger")],
            [InlineKeyboardButton("مشکل کانفنیگ رایگان", callback_data="report_free", style="primary")],
            [InlineKeyboardButton("مشکل پرداخت", callback_data="report_payment", style="danger")],
            [InlineKeyboardButton("سایر مشکلات", callback_data="report_other", style="primary")],
            [InlineKeyboardButton("بازگشت", callback_data="back_main", style="primary")]
        ])
        
        await query.edit_message_text(
            self.REPORT_TEXT,
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def report_type_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        data = query.data
        
        if not data.startswith("report_"):
            return
        
        report_type = data.replace("report_", "")
        context.user_data['report_type'] = report_type
        
        await query.answer()
        await query.edit_message_text(
            f"لطفاً شناسه کانفنیگ خود را وارد کنید:\n\nسپس مشکل خود را توضیح دهید.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("بازگشت", callback_data="report_issue", style="danger")]
            ])
        )
        context.user_data['waiting_for_report'] = 'config_id'
    
    async def receive_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        text = update.message.text
        
        if not context.user_data.get('waiting_for_report'):
            return
        
        report_type = context.user_data.get('report_type', 'other')
        waiting_for = context.user_data.get('waiting_for_report')
        
        if waiting_for == 'config_id':
            context.user_data['report_config_id'] = text.strip()
            context.user_data['waiting_for_report'] = 'description'
            await update.message.reply_text(
                "شناسه ثبت شد!\n\nحالا مشکل خود را توضیح دهید:",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        elif waiting_for == 'description':
            config_id = context.user_data.get('report_config_id', 'ندارد')
            description = text
            report_id = f"RPT_{user_id}_{int(time.time())}"
            
            user_info = await self.db.get_user(user_id)
            username = user_info['username'] if user_info else None
            
            await self.db.add_report(
                report_id=report_id,
                user_id=user_id,
                username=username,
                report_type=report_type,
                config_id=config_id,
                description=description
            )
            
            self.state.reports[report_id] = {
                'user_id': user_id,
                'username': username,
                'type': report_type,
                'config_id': config_id,
                'description': description,
                'created_at': datetime.now().isoformat(),
                'status': 'pending'
            }
            
            report_message = f"""📝 **گزارش جدید**

🆔 شناسه: {report_id}
👤 کاربر: {user_id}
📛 یوزرنیم: {username or 'ندارد'}
📋 نوع: {report_type}
🆔 کانفنیگ: {config_id}

📝 توضیحات:
{description}
            """
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("پاسخ", callback_data=f"reply_report_{report_id}", style="primary")]
            ])
            
            for admin_id in self.state.admins:
                try:
                    await self.application.bot.send_message(
                        chat_id=admin_id,
                        text=report_message,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.MARKDOWN
                    )
                except:
                    pass
            
            await update.message.reply_text(
                "✅ گزارش شما ارسال شد!\n\n🔄 منتظر پاسخ ادمین باشید.",
                parse_mode=ParseMode.MARKDOWN
            )
            context.user_data['waiting_for_report'] = None
    
    async def reply_to_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        admin_id = query.from_user.id
        data = query.data
        
        if not self.state.is_admin(admin_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند.", show_alert=True)
            return
        
        if data.startswith("reply_report_"):
            report_id = data.replace("reply_report_", "")
            context.user_data['replying_to_report'] = report_id
            
            report = await self.db.get_reports()
            report = next((r for r in report if r.get('report_id') == report_id), {})
            status = "پاسخ داده شده ✅" if report.get('status') == 'answered' else "در انتظار ⏳"
            
            await query.answer()
            await query.edit_message_text(
                f"📝 **پاسخ به گزارش:** `{report_id}`\n\n"
                f"👤 کاربر: {report.get('user_id', 'نامشخص')}\n"
                f"📛 یوزرنیم: {report.get('username', 'ندارد')}\n"
                f"📋 نوع: {report.get('report_type', 'نامشخص')}\n"
                f"🆔 کانفنیگ: {report.get('config_id', 'ندارد')}\n"
                f"📌 وضعیت: {status}\n\n"
                f"📝 توضیحات گزارش:\n{report.get('description', 'بدون توضیحات')}\n\n"
                f"✏️ لطفاً پاسخ خود را بنویسید:",
                parse_mode=ParseMode.MARKDOWN
            )
            context.user_data['admin_action'] = 'reply_report'
    
    async def send_report_reply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        admin_id = update.effective_user.id
        
        if not self.state.is_admin(admin_id):
            await update.message.reply_text("فقط ادمین‌ها دسترسی دارند.")
            return
        
        report_id = context.user_data.get('replying_to_report')
        if not report_id:
            await update.message.reply_text("گزارشی یافت نشد.")
            return
        
        try:
            report = await self.db.get_reports()
            report = next((r for r in report if r.get('report_id') == report_id), {})
            user_id = report.get('user_id')
        except:
            await update.message.reply_text("شناسه نامعتبر.")
            return
        
        reply_text = update.message.text
        
        try:
            await self.db.reply_report(report_id, reply_text)
            
            if report_id in self.state.reports:
                self.state.reports[report_id]['status'] = 'answered'
            
            await self.application.bot.send_message(
                chat_id=user_id,
                text=f"📝 **پاسخ به گزارش شما:**\n\n{reply_text}\n\n🙏 ممنون از صبر شما",
                parse_mode=ParseMode.MARKDOWN
            )
            
            await update.message.reply_text(
                f"✅ پاسخ ارسال شد!",
                parse_mode=ParseMode.MARKDOWN
            )
            context.user_data['replying_to_report'] = None
            context.user_data['admin_action'] = None
            
        except Exception as e:
            logger.error(f"Error sending reply: {e}")
            await update.message.reply_text(f"❌ خطا: {e}")
    
    # ==================== دریافت مجدد کانفنیگ ====================
    async def get_my_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        user_configs = await self.db.get_user_configs(user_id)
        
        if not user_configs:
            await query.answer("شما کانفنیگ خریداری شده‌ای ندارید!", show_alert=True)
            return
        
        buttons = []
        for config in user_configs:
            size = config.get('size', 'نامشخص')
            duration = config.get('duration', 'نامشخص')
            created_at = config.get('created_at')
            expiry_date = self.calculate_expiry(created_at.isoformat() if created_at else None, duration)
            button_text = f"{size}\n{expiry_date}"
            buttons.append([InlineKeyboardButton(button_text, callback_data=f"view_config_{config.get('config_id')}", style="success")])
        
        buttons.append([InlineKeyboardButton("🔙 بازگشت", callback_data="back_main", style="danger")])
        
        await query.edit_message_text(
            "📋 **کانفنیگ‌های شما:**\n\nلطفاً یکی از گزینه‌های زیر را انتخاب کنید:",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def view_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        data = query.data
        
        if data.startswith("view_config_"):
            config_id = data.replace("view_config_", "")
            config = await self.db.get_config(config_id)
            
            if not config:
                await query.answer("کانفنیگ یافت نشد!", show_alert=True)
                return
            
            size = config.get('size', 'نامشخص')
            duration = config.get('duration', 'نامشخص')
            created_at = config.get('created_at')
            service_name = config.get('service_name', f"proservice{random.randint(10000, 99999)}")
            tracking_code = config.get('tracking_code', str(random.randint(1000000000, 9999999999)))
            subscription_link = config.get('subscription_link', '')
            config_text = config.get('config_text', '')
            
            expiry_date = self.calculate_expiry(created_at.isoformat() if created_at else None, duration)
            
            if subscription_link:
                sub_text = subscription_link
            else:
                sub_text = "لینک ساب برای این سرویس ثبت نشده است."
            
            if config_text:
                config_text_display = config_text
            else:
                config_text_display = "کانفنیگ برای این سرویس ثبت نشده است."
            
            message = f"""🔍 نام اصلی سرویس: {service_name}
📂 کد پیگیری سرویس: {tracking_code}
💻 وضعیت سرویس: فعال ✅
〰️〰️〰️〰️〰️〰️
📦 حجم سرویس: {size}
📆 تاریخ انقضا: {expiry_date}
〰️〰️〰️〰️〰️〰️
🔗 لینک کانفنیگ : {config_text_display}

🔗 لینک اتصال (Subscription ) : امن و سریع 
{sub_text}"""
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📞 گزارش مشکل", callback_data=f"report_{config_id}", style="danger")],
                [InlineKeyboardButton("🔙 بازگشت", callback_data="get_my_config", style="primary")]
            ])
            
            await query.edit_message_text(
                message,
                reply_markup=keyboard,
                parse_mode=ParseMode.MARKDOWN
            )
    
    # ==================== آیپی من ====================
    async def my_ip(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        await query.answer("🔄 در حال دریافت اطلاعات...")
        
        try:
            ip_info = await self.get_real_ip(user_id)
            
            flag = self.scanner.get_country_flag(ip_info.get('countryCode', ''))
            
            message = f"""🌍 **اطلاعات آیپی شما**

{flag} **کشور:** {ip_info.get('country', 'نامشخص')}
🏙️ **شهر:** {ip_info.get('city', 'نامشخص')}
📍 **منطقه:** {ip_info.get('region', 'نامشخص')}
🖥 **آیپی:** `{ip_info.get('ip', 'نامشخص')}`
🏢 **ارائه‌دهنده:** {ip_info.get('isp', 'نامشخص')}
🕐 **منطقه زمانی:** {ip_info.get('timezone', 'نامشخص')}

📌 این اطلاعات بر اساس آیپی شما نمایش داده شده است.
"""
            
            await query.edit_message_text(
                message,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_user", style="danger")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            
        except Exception as e:
            logger.error(f"IP info error: {e}")
            await query.edit_message_text(
                "❌ خطا در دریافت اطلاعات آیپی!\nلطفاً دوباره امتحان کنید.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_user", style="danger")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
    
    # ==================== هندلرهای هوش مصنوعی ====================
    async def ai_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        context.user_data['ai_mode'] = True
        await query.edit_message_text(
            self.AI_WELCOME_TEXT,
            reply_markup=self.ai_panel_buttons(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def ai_chat_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        context.user_data['ai_mode'] = 'chat'
        await query.edit_message_text(
            "💬 **حالت چت هوشمند فعال شد!**\n\nسلام! هر چی دوست دارید بپرسید:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 بازگشت", callback_data="ai_panel", style="danger")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def ai_code_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        context.user_data['ai_mode'] = 'code'
        await query.edit_message_text(
            "📝 **حالت تحلیل کد فعال شد!**\n\nکدتان را بفرستید:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 بازگشت", callback_data="ai_panel", style="danger")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def ai_help_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        help_text = "🧠 **راهنمای هوش مصنوعی**\n\n💬 چت هوشمند\n📝 تحلیل کد"
        await query.edit_message_text(
            help_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 بازگشت", callback_data="ai_panel", style="danger")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def ai_message_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        message = update.message.text.strip()
        
        await update.message.chat.send_action(action="typing")
        
        response = await self.ai.get_chat_response(message)
        await update.message.reply_text(response["text"], parse_mode="Markdown")
    
    # ==================== هندلرهای اصلی ====================
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            user_id = update.effective_user.id
            text = update.message.text
            
            logger.info(f"📩 Start command from user {user_id}")
            
            user = update.effective_user
            username = user.username if user.username else None
            first_name = user.first_name if user.first_name else None
            last_name = user.last_name if user.last_name else None
            
            ref_id = None
            if text and "ref_" in text:
                try:
                    ref_id = int(text.split("ref_")[1].strip())
                    if ref_id == user_id:
                        ref_id = None
                except:
                    pass
            
            await self.db.add_user(user_id, username, first_name, last_name, ref_id)
            
            if self.state.is_admin(user_id):
                keyboard = self.main_menu_buttons(user_id)
                await update.message.reply_text(
                    "به ربات اسکنر خوش آمدید!\n\nشما دسترسی ادمین دارید.",
                    reply_markup=keyboard,
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            await update.message.reply_text(
                self.WELCOME_TEXT,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=self.membership_buttons()
            )
        except Exception as e:
            logger.error(f"❌ Error in start_command: {e}")
            import traceback
            logger.error(f"   {traceback.format_exc()}")
            await update.message.reply_text("❌ خطا در اجرای دستور! لطفاً دوباره تلاش کنید.")
    
    async def admin_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند.", show_alert=True)
            return
        await query.answer()
        await query.edit_message_text(
            "📋 پنل مدیریت ربات",
            reply_markup=self.admin_panel_buttons(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def user_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        await query.edit_message_text(
            "👤 پنل کاربری",
            reply_markup=self.user_panel_buttons(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    # ==================== مدیریت اسکنر ====================
    async def config_scanner_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند.", show_alert=True)
            return
        await query.answer()
        
        status = "🟢 در حال اجرا" if self.state.config_scanner_running else "🔴 متوقف"
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("▶️ استارت", callback_data="config_start", style="success"),
                InlineKeyboardButton("⏹ توقف", callback_data="config_stop", style="danger")
            ],
            [
                InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="primary")
            ]
        ])
        
        sent_count = await self.db.get_sent_configs_count()
        
        await query.edit_message_text(
            f"🎯 **اسکنر کانفنیگ**\n\n"
            f"وضعیت: {status}\n"
            f"چنل‌ها: {len(SOURCE_CONFIG_CHANNELS)}\n"
            f"کانفنیگ ارسال شده: {sent_count}",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def proxy_scanner_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند.", show_alert=True)
            return
        await query.answer()
        
        status = "🟢 در حال اجرا" if self.state.proxy_scanner_running else "🔴 متوقف"
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("▶️ استارت", callback_data="proxy_start", style="success"),
                InlineKeyboardButton("⏹ توقف", callback_data="proxy_stop", style="danger")
            ],
            [
                InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="primary")
            ]
        ])
        
        sent_count = await self.db.get_sent_proxies_count()
        
        await query.edit_message_text(
            f"🔄 **اسکنر پروکسی**\n\n"
            f"وضعیت: {status}\n"
            f"چنل‌ها: {len(SOURCE_PROXY_CHANNELS)}\n"
            f"پروکسی ارسال شده: {sent_count}",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
    
    # ==================== مدیریت ادمین ====================
    async def admin_add_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if user_id != OWNER_ID:
            await query.answer("فقط مالک ربات دسترسی دارد.", show_alert=True)
            return
        
        context.user_data['admin_action'] = 'add_admin'
        await query.answer()
        await query.edit_message_text(
            "🆔 ایدی عددی ادمین جدید را وارد کنید:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
            ])
        )
    
    async def admin_remove_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if user_id != OWNER_ID:
            await query.answer("فقط مالک ربات دسترسی دارد.", show_alert=True)
            return
        
        context.user_data['admin_action'] = 'remove_admin'
        await query.answer()
        await query.edit_message_text(
            "🆔 ایدی عددی ادمین برای حذف را وارد کنید:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
            ])
        )
    
    async def admin_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند.", show_alert=True)
            return
        
        await query.answer()
        
        try:
            admins = await self.db.get_admins()
            if not admins:
                await query.edit_message_text(
                    "📭 هیچ ادمینی وجود ندارد.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
                    ])
                )
                return
            
            admin_list = ""
            for i, admin in enumerate(admins, 1):
                username = admin.get('username', 'بدون یوزرنیم')
                admin_list += f"{i}. {admin.get('user_id')} - @{username if username else 'بدون یوزرنیم'}\n"
            
            await query.edit_message_text(
                f"👥 **لیست ادمین‌ها:**\n\n{admin_list}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
                ])
            )
        except Exception as e:
            logger.error(f"Admin list error: {e}")
            await query.edit_message_text(
                f"❌ خطا: {e}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
                ])
            )
    
    async def admin_ban_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند.", show_alert=True)
            return
        
        await query.answer()
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("⛔ بن کاربر", callback_data="admin_ban", style="danger")],
            [InlineKeyboardButton("✅ آنبن کاربر", callback_data="admin_unban", style="success")],
            [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="primary")]
        ])
        await query.edit_message_text(
            "مدیریت بن کاربران",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_ban(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if user_id != OWNER_ID:
            await query.answer("فقط مالک ربات دسترسی دارد.", show_alert=True)
            return
        
        context.user_data['admin_action'] = 'ban_user'
        await query.answer()
        await query.edit_message_text(
            "🆔 ایدی یا یوزرنیم کاربر برای بن را وارد کنید:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 بازگشت", callback_data="admin_ban_menu", style="danger")]
            ])
        )
    
    async def admin_unban(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if user_id != OWNER_ID:
            await query.answer("فقط مالک ربات دسترسی دارد.", show_alert=True)
            return
        
        context.user_data['admin_action'] = 'unban_user'
        await query.answer()
        await query.edit_message_text(
            "🆔 ایدی یا یوزرنیم کاربر برای آنبن را وارد کنید:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 بازگشت", callback_data="admin_ban_menu", style="danger")]
            ])
        )
    
    async def admin_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if user_id != OWNER_ID:
            await query.answer("فقط مالک ربات دسترسی دارد.", show_alert=True)
            return
        
        context.user_data['admin_action'] = 'broadcast'
        await query.answer()
        await query.edit_message_text(
            "📢 پیام همگانی خود را ارسال کنید:\n\n"
            "⚠️ توجه: این پیام به همه کاربران ارسال خواهد شد.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
            ])
        )
    
    async def admin_gen_redeem(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if user_id != OWNER_ID:
            await query.answer("فقط مالک ربات دسترسی دارد.", show_alert=True)
            return
        
        code = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=10))
        await self.db.add_redeem_code(code, user_id)
        self.state.config_redeem_codes[code] = None
        
        await query.answer()
        await query.edit_message_text(
            f"🎫 **کد تخفیف جدید:**\n\n`{code}`\n\n"
            f"✅ این کد را به کاربران بدهید تا از تخفیف استفاده کنند.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
            ])
        )
    
    # ==================== هندلرهای عضویت ====================
    async def check_membership(self, user_id: int) -> Dict[str, bool]:
        results = {'config': False, 'proxy': False}
        try:
            for channel, key in [(f"@{CHANNEL_1_USERNAME}", 'config'), (f"@{CHANNEL_2_USERNAME}", 'proxy')]:
                try:
                    member = await self.application.bot.get_chat_member(chat_id=channel, user_id=user_id)
                    if member.status in ['member', 'creator', 'administrator', 'restricted']:
                        results[key] = True
                except Exception as e:
                    logger.error(f"Error checking membership for {channel}: {e}")
        except Exception as e:
            logger.error(f"Error in check_membership: {e}")
        return results
    
    async def membership_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if self.state.is_admin(user_id):
            await query.answer("شما ادمین هستید!", show_alert=True)
            keyboard = self.main_menu_buttons(user_id)
            await query.edit_message_text("پنل ادمین", reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
            return
        
        await query.answer("در حال بررسی عضویت...")
        membership = await self.check_membership(user_id)
        
        try:
            await query.message.delete()
        except:
            pass
        
        if membership.get('config') and membership.get('proxy'):
            context.user_data['membership_confirmed'] = True
            keyboard = self.main_menu_buttons(user_id)
            await context.bot.send_message(
                chat_id=user_id,
                text="✅ عضویت شما تایید شد!\n\nبه ربات اسکنر خوش آمدید!",
                reply_markup=keyboard,
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            missing = []
            if not membership.get('config'):
                missing.append("چنل کانفنیگ")
            if not membership.get('proxy'):
                missing.append("چنل پروکسی")
            await context.bot.send_message(
                chat_id=user_id,
                text=f"❌ شما عضو همه چنل‌ها نیستید!\n\nلطفاً در چنل‌های زیر عضو شوید:\n{', '.join(missing)}\n\nسپس دکمه «تایید عضویت» را بزنید.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=self.membership_buttons()
            )
    
    # ==================== هندلرهای زبان ====================
    async def language(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        await query.answer()
        await query.edit_message_text(
            self.get_text(user_id, "select_language"),
            reply_markup=self.language_buttons()
        )
    
    async def lang_en(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        await self.db.update_language(user_id, "en")
        await query.answer()
        await query.edit_message_text(
            "Language set to English!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="back_main", style="primary")]
            ])
        )
    
    async def lang_fa(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        await self.db.update_language(user_id, "fa")
        await query.answer()
        await query.edit_message_text(
            "زبان به فارسی تغییر کرد!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 بازگشت", callback_data="back_main", style="primary")]
            ])
        )
    
    # ==================== لاگ‌ها ====================
    async def admin_log_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = query.from_user.id
        
        if not self.state.is_admin(user_id):
            await query.answer("فقط ادمین‌ها دسترسی دارند.", show_alert=True)
            return
        
        await query.answer()
        await query.edit_message_text(
            "📋 **مدیریت لاگ‌ها**\n\n"
            "لاگ‌ها در Railway قابل مشاهده هستند.\n"
            "برای مشاهده لاگ‌ها به پنل Railway بروید.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_admin", style="danger")]
            ])
        )
    
    # ==================== راهنما ====================
    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        await query.edit_message_text(
            "📖 **راهنمای کامل ربات**\n\n"
            "🛒 **خرید کانفنیگ**\n"
            "برای خرید اشتراک، از دکمه خرید استفاده کنید.\n\n"
            "📥 **دریافت کانفنیگ**\n"
            "کانفنیگ‌های خریداری شده خود را دریافت کنید.\n\n"
            "🌍 **آیپی من**\n"
            "اطلاعات آیپی خود را مشاهده کنید.\n\n"
            "👥 **رفرال**\n"
            "دوستان خود را دعوت کنید و جایزه بگیرید.\n\n"
            "🎫 **ردیم کد**\n"
            "کد تخفیف خود را وارد کنید.\n\n"
            "🌐 **زبان**\n"
            "زبان ربات را تغییر دهید.\n\n"
            "🤖 **AI**\n"
            "از هوش مصنوعی برای چت و تحلیل کد استفاده کنید.\n\n"
            "📞 **گزارش مشکل**\n"
            "مشکلات خود را گزارش دهید.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_user", style="danger")]
            ])
        )
    
    # ==================== Keep-Alive ====================
    async def keep_alive(self):
        logger.info("🔄 Keep-Alive started...")
        last_ping = datetime.now()
        
        while True:
            try:
                now = datetime.now()
                
                if self.application and self.application.bot:
                    try:
                        me = await self.application.bot.get_me()
                        logger.info(f"✅ Bot ping: @{me.username}")
                    except Exception as e:
                        logger.error(f"❌ Bot ping failed: {e}")
                
                if self.user_client:
                    try:
                        await self.user_client.get_me()
                        logger.info(f"✅ Client ping: OK")
                    except Exception as e:
                        logger.error(f"❌ Client disconnected! Reconnecting...")
                        try:
                            await self.user_client.disconnect()
                            await asyncio.sleep(2)
                            await self.user_client.connect()
                            logger.info("✅ Client reconnected!")
                        except Exception as reconnect_error:
                            logger.error(f"❌ Reconnect failed: {reconnect_error}")
                
                await asyncio.sleep(15)
                
            except Exception as e:
                logger.error(f"❌ Keep-Alive error: {e}")
                await asyncio.sleep(5)
    
    # ==================== Webhook ====================
    async def webhook_handler(self, request):
        try:
            data = await request.json()
            update = Update.de_json(data, self.application.bot)
            await self.application.process_update(update)
            return web.Response(text="OK")
        except Exception as e:
            logger.error(f"Webhook error: {e}")
            return web.Response(text="Error", status=500)
    
    async def health_handler(self, request):
        return web.Response(text="Bot is running! ✅")
    
    # ==================== Error Handler ====================
    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """مدیریت خطاها با لاگ دقیق"""
        error = context.error
        logger.error(f"❌ Exception: {error}")
        logger.error(f"   Type: {type(error)}")
        
        import traceback
        logger.error(f"   Traceback: {traceback.format_exc()}")
        
        if update and hasattr(update, 'effective_chat') and update.effective_chat:
            try:
                if "FloodWait" not in str(error) and "flood" not in str(error).lower():
                    await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text="❌ خطایی رخ داد! لطفاً دوباره امتحان کنید."
                    )
            except:
                pass
    
    # ==================== Message Handler ====================
    async def message_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        message = update.message
        text = message.text
        
        logger.info(f"📩 Message from user {user_id}: {text[:50] if text else '[non-text]'}")
        
        if await self.db.is_banned(user_id):
            await message.reply_text("⛔ شما بن شده‌اید.")
            return
        
        user = update.effective_user
        await self.db.add_user(
            user_id,
            user.username if user.username else None,
            user.first_name if user.first_name else None,
            user.last_name if user.last_name else None
        )
        
        # دریافت رسید
        if context.user_data.get('waiting_for_receipt') and message.photo:
            await self.receive_receipt(update, context)
            return
        
        # دریافت گزارش
        if context.user_data.get('waiting_for_report'):
            await self.receive_report(update, context)
            return
        
        # دریافت کد تخفیف
        if context.user_data.get('waiting_for_redeem'):
            await self.receive_redeem_code(update, context)
            return
        
        # دریافت کانفنیگ از ادمین
        if context.user_data.get('waiting_for_config'):
            await self.receive_config_from_admin(update, context)
            return
        
        # دریافت لینک ساب از ادمین
        if context.user_data.get('waiting_for_subscription'):
            await self.receive_subscription_from_admin(update, context)
            return
        
        # اکشن‌های ادمین
        action = context.user_data.get('admin_action')
        
        if action == 'reply_report':
            await self.send_report_reply(update, context)
            return
        
        if action == 'add_admin':
            try:
                admin_id = int(text.strip())
                if admin_id in self.state.admins:
                    await message.reply_text("این کاربر قبلاً ادمین است.")
                else:
                    user_info = await self.db.get_user(admin_id)
                    username = user_info['username'] if user_info else None
                    self.state.admins.add(admin_id)
                    await self.db.add_admin(admin_id, username)
                    await message.reply_text("✅ ادمین با موفقیت اضافه شد!")
            except:
                await message.reply_text("❌ لطفاً یک ایدی عددی معتبر وارد کنید.")
            context.user_data['admin_action'] = None
            return
        
        if action == 'remove_admin':
            try:
                admin_id = int(text.strip())
                if admin_id not in self.state.admins:
                    await message.reply_text("این کاربر ادمین نیست.")
                else:
                    self.state.admins.remove(admin_id)
                    await self.db.remove_admin(admin_id)
                    await message.reply_text("✅ ادمین با موفقیت حذف شد!")
            except:
                await message.reply_text("❌ لطفاً یک ایدی عددی معتبر وارد کنید.")
            context.user_data['admin_action'] = None
            return
        
        if action == 'ban_user':
            target = text.strip()
            try:
                if target.startswith('@'):
                    target = target.replace('@', '')
                    user_info = await self.db.get_user_by_username(target)
                    if user_info:
                        target_id = user_info['user_id']
                    else:
                        await message.reply_text("❌ کاربر پیدا نشد.")
                        context.user_data['admin_action'] = None
                        return
                else:
                    target_id = int(target)
                
                if await self.db.is_banned(target_id):
                    await message.reply_text("این کاربر قبلاً بن است.")
                else:
                    await self.db.ban_user(target_id)
                    self.state.banned_users.add(target_id)
                    await message.reply_text("✅ کاربر با موفقیت بن شد!")
            except:
                await message.reply_text("❌ کاربر پیدا نشد.")
            context.user_data['admin_action'] = None
            return
        
        if action == 'unban_user':
            target = text.strip()
            try:
                if target.startswith('@'):
                    target = target.replace('@', '')
                    user_info = await self.db.get_user_by_username(target)
                    if user_info:
                        target_id = user_info['user_id']
                    else:
                        await message.reply_text("❌ کاربر پیدا نشد.")
                        context.user_data['admin_action'] = None
                        return
                else:
                    target_id = int(target)
                
                if not await self.db.is_banned(target_id):
                    await message.reply_text("این کاربر بن نیست.")
                else:
                    await self.db.unban_user(target_id)
                    self.state.banned_users.remove(target_id)
                    await message.reply_text("✅ کاربر آنبن شد!")
            except:
                await message.reply_text("❌ کاربر پیدا نشد.")
            context.user_data['admin_action'] = None
            return
        
        if action == 'broadcast':
            if user_id != OWNER_ID:
                await message.reply_text("فقط مالک ربات دسترسی دارد.")
                context.user_data['admin_action'] = None
                return
            
            count = 0
            users = await self.db.get_all_users()
            for user_data in users:
                uid = user_data['user_id']
                if uid == OWNER_ID or uid in self.state.admins:
                    continue
                try:
                    if message.photo:
                        await self.application.bot.send_photo(chat_id=uid, photo=message.photo[-1].file_id, caption=message.caption or "")
                    elif message.video:
                        await self.application.bot.send_video(chat_id=uid, video=message.video.file_id, caption=message.caption or "")
                    elif message.document:
                        await self.application.bot.send_document(chat_id=uid, document=message.document.file_id, caption=message.caption or "")
                    else:
                        await self.application.bot.send_message(chat_id=uid, text=text or "")
                    count += 1
                    await asyncio.sleep(0.2)
                except Exception as e:
                    logger.error(f"Broadcast error for {uid}: {e}")
            await message.reply_text(f"✅ پیام به {count} کاربر ارسال شد.")
            context.user_data['admin_action'] = None
            return
        
        # دستور start
        if text and text.startswith("/start"):
            await self.start_command(update, context)
            return
        
        # هوش مصنوعی
        if text and context.user_data.get('ai_mode'):
            await self.ai_message_handler(update, context)
            return
        
        await message.reply_text(
            "❌ دستور نامعتبر!\n\nبرای مشاهده راهنما، دکمه /start را بزنید.",
            parse_mode=ParseMode.MARKDOWN
        )
    
    # ==================== Button Handler ====================
    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            query = update.callback_query
            user_id = query.from_user.id
            data = query.data
            
            logger.info(f"🔄 Button pressed: {data} from user {user_id}")
            
            # ====== AI ======
            if data.startswith("ai_"):
                if data == "ai_panel":
                    await self.ai_panel(update, context)
                elif data == "ai_chat":
                    await self.ai_chat_handler(update, context)
                elif data == "ai_code":
                    await self.ai_code_handler(update, context)
                elif data == "ai_help":
                    await self.ai_help_handler(update, context)
                elif data == "back_main":
                    await self.user_panel(update, context)
                else:
                    await self.ai_button_handler(update, context)
                return
            
            # ====== آمار ======
            if data == "admin_stats_full":
                await self.admin_stats_full(update, context)
                return
            
            # ====== خرید ======
            if data == "buy_config":
                await self.buy_panel(update, context)
                return
            if data.startswith("buy_"):
                await self.buy_selection(update, context)
                return
            if data.startswith("payment_card_"):
                await self.payment_card(update, context)
                return
            if data.startswith("copy_card_"):
                await self.copy_card(update, context)
                return
            if data == "send_receipt":
                await self.send_receipt(update, context)
                return
            if data == "back_to_buy":
                await self.buy_panel(update, context)
                return
            
            # ====== دریافت کانفنیگ ======
            if data == "get_my_config":
                await self.get_my_config(update, context)
                return
            if data.startswith("view_config_"):
                await self.view_config(update, context)
                return
            
            # ====== رفرال ======
            if data == "referral":
                await self.referral(update, context)
                return
            if data.startswith("copy_ref_"):
                await self.copy_referral_link(update, context)
                return
            if data == "get_referral_reward":
                await self.get_referral_reward(update, context)
                return
            
            # ====== ردیم کد ======
            if data == "redeem":
                await self.redeem(update, context)
                return
            
            # ====== گزارش ======
            if data == "report_issue":
                await self.report_issue(update, context)
                return
            if data.startswith("report_") and data != "report_issue":
                await self.report_type_selection(update, context)
                return
            
            # ====== تاپیک ======
            if data == "admin_topic":
                await self.admin_topic_panel(update, context)
                return
            if data == "topic_on":
                await self.topic_on(update, context)
                return
            if data == "topic_off":
                await self.topic_off(update, context)
                return
            
            # ====== سفارشات ======
            if data == "admin_orders":
                await self.admin_orders_panel(update, context)
                return
            if data == "admin_order_list":
                await self.admin_order_list(update, context)
                return
            if data.startswith("confirm_order_"):
                await self.confirm_order(update, context)
                return
            if data.startswith("reject_order_"):
                await self.reject_order(update, context)
                return
            if data.startswith("view_receipt_"):
                await self.view_receipt(update, context)
                return
            
            # ====== ردیم کد ادمین ======
            if data == "admin_redeem_requests":
                await self.admin_redeem_requests(update, context)
                return
            if data.startswith("approve_redeem_"):
                await self.approve_redeem(update, context)
                return
            if data.startswith("reject_redeem_"):
                await self.reject_redeem(update, context)
                return
            
            # ====== گزارشات ادمین ======
            if data == "admin_reports":
                await query.answer()
                await query.edit_message_text(
                    "📋 پنل مدیریت گزارشات",
                    reply_markup=self.admin_report_buttons(),
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            if data == "admin_report_list":
                await self.admin_report_list(update, context)
                return
            if data == "admin_reply_again":
                await self.admin_reply_again(update, context)
                return
            if data == "admin_export_reports":
                await self.admin_export_reports(update, context)
                return
            if data == "admin_export_orders":
                await self.admin_export_orders(update, context)
                return
            if data.startswith("reply_report_"):
                await self.reply_to_report(update, context)
                return
            
            # ====== خریدهای موفق ======
            if data == "admin_successful_orders":
                await self.admin_successful_orders(update, context)
                return
            
            # ====== آیپی من ======
            if data == "my_ip":
                await self.my_ip(update, context)
                return
            
            # ====== بن ======
            if await self.db.is_banned(user_id):
                await query.answer("⛔ شما بن شده‌اید.", show_alert=True)
                return
            
            await self.db.add_user(user_id)
            
            # ====== عضویت ======
            if data == "check_membership":
                await self.membership_callback(update, context)
                return
            
            # ====== زبان ======
            if data == "language":
                await self.language(update, context)
                return
            if data == "lang_en":
                await self.lang_en(update, context)
                return
            if data == "lang_fa":
                await self.lang_fa(update, context)
                return
            
            # ====== بازگشت ======
            if data == "back_main":
                await query.answer()
                keyboard = self.main_menu_buttons(user_id)
                await query.edit_message_text(
                    "به ربات اسکنر خوش آمدید!",
                    reply_markup=keyboard,
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            if data == "back_to_admin":
                await query.answer()
                await self.admin_panel(update, context)
                return
            if data == "back_to_user":
                await query.answer()
                await self.user_panel(update, context)
                return
            
            # ====== پنل‌ها ======
            if data == "admin_panel":
                await self.admin_panel(update, context)
                return
            if data == "admin_config_scanner":
                await self.config_scanner_panel(update, context)
                return
            if data == "admin_proxy_scanner":
                await self.proxy_scanner_panel(update, context)
                return
            if data == "user_panel":
                await self.user_panel(update, context)
                return
            if data == "help":
                await self.help(update, context)
                return
            
            # ====== کنترل اسکنر ======
            if data == "config_start":
                if self.state.config_scanner_running:
                    await query.answer("اسکنر در حال اجراست!", show_alert=True)
                    return
                self.state.config_scanner_running = True
                await self.db.set_scanner_state("config_scanner", "True")
                await query.answer("✅ اسکن کانفنیگ شروع شد!", show_alert=True)
                await self.config_scanner_panel(update, context)
                return
            if data == "config_stop":
                if not self.state.config_scanner_running:
                    await query.answer("اسکنر در حال اجرا نیست!", show_alert=True)
                    return
                self.state.config_scanner_running = False
                await self.db.set_scanner_state("config_scanner", "False")
                await query.answer("⏹ اسکن کانفنیگ متوقف شد!", show_alert=True)
                await self.config_scanner_panel(update, context)
                return
            if data == "proxy_start":
                if self.state.proxy_scanner_running:
                    await query.answer("اسکنر در حال اجراست!", show_alert=True)
                    return
                self.state.proxy_scanner_running = True
                await self.db.set_scanner_state("proxy_scanner", "True")
                await query.answer("✅ اسکن پروکسی شروع شد!", show_alert=True)
                await self.proxy_scanner_panel(update, context)
                return
            if data == "proxy_stop":
                if not self.state.proxy_scanner_running:
                    await query.answer("اسکنر در حال اجرا نیست!", show_alert=True)
                    return
                self.state.proxy_scanner_running = False
                await self.db.set_scanner_state("proxy_scanner", "False")
                await query.answer("⏹ اسکن پروکسی متوقف شد!", show_alert=True)
                await self.proxy_scanner_panel(update, context)
                return
            
            # ====== مدیریت ادمین ======
            if data == "admin_add_admin":
                await self.admin_add_admin(update, context)
                return
            if data == "admin_remove_admin":
                await self.admin_remove_admin(update, context)
                return
            if data == "admin_list":
                await self.admin_list(update, context)
                return
            if data == "admin_ban_menu":
                await self.admin_ban_menu(update, context)
                return
            if data == "admin_ban":
                await self.admin_ban(update, context)
                return
            if data == "admin_unban":
                await self.admin_unban(update, context)
                return
            if data == "admin_gen_redeem":
                await self.admin_gen_redeem(update, context)
                return
            if data == "admin_broadcast":
                await self.admin_broadcast(update, context)
                return
            if data == "admin_log_menu":
                await self.admin_log_menu(update, context)
                return
            
            await query.answer("❌ دکمه نامعتبر!")
        except Exception as e:
            logger.error(f"❌ Error in button_handler: {e}")
            import traceback
            logger.error(f"   {traceback.format_exc()}")
            try:
                await query.answer("❌ خطا!", show_alert=True)
            except:
                pass
    
    # ==================== اجرای اصلی ====================
    async def run(self):
        logger.info("🚀 Starting Bot (Webhook Mode - Port 8080)...")
        logger.info(f"👤 Owner ID: {OWNER_ID}")
        logger.info(f"📡 Config channels: {len(SOURCE_CONFIG_CHANNELS)}")
        logger.info(f"🔄 Proxy channels: {len(SOURCE_PROXY_CHANNELS)}")
        logger.info(f"🧠 AI: {'✅ Enabled' if GROQ_API_KEY else '❌ Disabled'}")
        
        # راه‌اندازی دیتابیس
        await self.db.init()
        logger.info("✅ Database initialized!")
        
        # بارگذاری ادمین‌ها
        try:
            admins = await self.db.get_admins()
            for admin in admins:
                self.state.admins.add(admin['user_id'])
            logger.info(f"👥 Loaded {len(admins)} admins")
        except Exception as e:
            logger.error(f"Error loading admins: {e}")
        
        # بارگذاری وضعیت اسکنر (پیش‌فرض خاموش)
        try:
            config_state = await self.db.get_scanner_state("config_scanner")
            self.state.config_scanner_running = config_state == "True"
            
            proxy_state = await self.db.get_scanner_state("proxy_scanner")
            self.state.proxy_scanner_running = proxy_state == "True"
            
            logger.info(f"📂 Scanner states - Config: {self.state.config_scanner_running}, Proxy: {self.state.proxy_scanner_running}")
        except Exception as e:
            logger.error(f"Error loading scanner states: {e}")
            self.state.config_scanner_running = False
            self.state.proxy_scanner_running = False
        
        # اتصال به اکانت تلگرام
        if not USER_SESSION_STR:
            logger.error("❌ ERROR: USER_SESSION_STR not set!")
            return
        
        try:
            self.user_client = TelegramClient(StringSession(USER_SESSION_STR), API_ID, API_HASH)
            await self.user_client.start()
            me = await self.user_client.get_me()
            logger.info(f"✅ Scanner client connected!")
            logger.info(f"   👤 Username: @{me.username if me.username else 'None'}")
            logger.info(f"   🆔 User ID: {me.id}")
        except Exception as e:
            logger.error(f"❌ Scanner client error: {e}")
            return
        
        # راه‌اندازی ربات
        self.application = Application.builder().token(BOT_TOKEN).build()
        
        # ثبت error handler
        self.application.add_error_handler(self.error_handler)
        
        self.scanner = ChannelScanner(self.state, self.db, self.user_client, self.application)
        
        # ثبت هندلرها
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CallbackQueryHandler(self.button_handler))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.message_handler))
        self.application.add_handler(MessageHandler(filters.PHOTO, self.message_handler))
        self.application.add_handler(MessageHandler(filters.Document.ALL, self.message_handler))
        
        await self.application.initialize()
        await self.application.start()
        logger.info("✅ Bot started!")
        
        # شروع تسک‌های پس‌زمینه
        asyncio.create_task(self.scanner.scanner_loop())
        asyncio.create_task(self.keep_alive())
        
        # ============ راه‌اندازی وب‌سرور با Webhook ============
        if WEBHOOK_URL:
            logger.info(f"🌐 Setting webhook to: {WEBHOOK_URL}")
            
            try:
                await self.application.bot.delete_webhook()
                await self.application.bot.set_webhook(
                    url=WEBHOOK_URL,
                    drop_pending_updates=True,
                    allowed_updates=["message", "callback_query"]
                )
                logger.info("✅ Webhook set successfully!")
            except Exception as e:
                logger.error(f"❌ Webhook error: {e}")
                logger.info("⚠️ Webhook failed! Check your RAILWAY_PUBLIC_DOMAIN")
        else:
            logger.warning("⚠️ WEBHOOK_URL is empty! Set RAILWAY_PUBLIC_DOMAIN environment variable.")
        
        # راه‌اندازی وب‌سرور
        app = web.Application()
        app.router.add_post('/webhook', self.webhook_handler)
        app.router.add_get('/', self.health_handler)
        app.router.add_get('/health', self.health_handler)
        app.router.add_get('/admin', self.web_admin_panel)
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        await site.start()
        logger.info(f"🌐 Web server running on port {PORT}")
        
        if WEBHOOK_URL:
            logger.info(f"📊 Admin panel: {WEBHOOK_URL.replace('/webhook', '/admin')}")
        
        logger.info("✅ Bot is fully operational with Webhook!")
        
        # منتظر موندن تا ابد
        await asyncio.Event().wait()

# ==================== اجرا ====================
if __name__ == "__main__":
    try:
        bot = ScannerBot()
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("👋 Stopped by user")
    except RuntimeError as e:
        if "Cannot close a running event loop" in str(e):
            logger.info("⚠️ Event loop close error (non-critical)")
        else:
            logger.error(f"❌ Runtime error: {e}")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
