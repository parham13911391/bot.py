"""
ابزار مستقل ساخت سشن جدید - وقتی سشن اکانت اسکنر غیرفعال شد.

این اسکریپت جدا از خودِ ربات اجرا می‌شه:
1. شماره تلفن رو می‌گیره
2. کد تاییدی که تلگرام می‌فرسته رو می‌گیره (و اگه لازم بود رمز 2FA)
3. سشن جدید رو می‌سازه
4. مستقیم تو همون دیتابیسِ ربات (جدول scanner_state) ذخیره‌ش می‌کنه -
   یعنی کافیه بعدش ربات رو ری‌استارت کنی (یا حتی صبر کنی خودش دفعهٔ
   بعد بالا میاد از همین سشن استفاده می‌کنه)، بدون نیاز به دست‌کاری کد.

طرز اجرا (رو همون سروری که DATABASE_URL بهش وصله، مثلاً Railway shell،
یا لوکال با ست‌کردن متغیر محیطی DATABASE_URL):

    pip install telethon asyncpg --break-system-packages
    python3 generate_session.py

اگه DATABASE_URL ست نباشه، اسکریپت فقط سشن رو چاپ می‌کنه (می‌تونی خودت
دستی تو USER_SESSION_STR داخل bot.py بذاری) و کاری به دیتابیس نداره.
"""
import asyncio
import os
import sys

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PhoneCodeExpiredError

# همون مقادیری که تو bot.py هست - اگه اونجا عوض کردی، اینجا هم عوض کن
API_ID = 31809598
API_HASH = "9df12f1fa837a291683e8c5802d82e72"

DATABASE_URL = os.environ.get("DATABASE_URL", "")


async def save_to_database(session_string: str) -> bool:
    """سشن جدید رو مستقیم تو جدول scanner_state ربات ذخیره می‌کنه."""
    if not DATABASE_URL:
        print("\n⚠️  DATABASE_URL ست نشده - سشن تو دیتابیس ذخیره نشد.")
        print("    خودت دستی این رشته رو تو USER_SESSION_STR داخل bot.py بذار.")
        return False

    try:
        import asyncpg
    except ImportError:
        print("\n⚠️  پکیج asyncpg نصب نیست (pip install asyncpg --break-system-packages) - ذخیره تو دیتابیس رد شد.")
        return False

    try:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scanner_state (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            """
            INSERT INTO scanner_state (key, value, updated_at)
            VALUES ('user_session_string', $1, NOW())
            ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()
            """,
            session_string,
        )
        await conn.close()
        return True
    except Exception as e:
        print(f"\n❌ خطا در ذخیره تو دیتابیس: {e}")
        return False


async def main():
    print("=" * 60)
    print("🔐 ساخت سشن جدید برای اکانت اسکنر")
    print("=" * 60)

    phone = input("\n📱 شماره تلفن (با کد کشور، مثال +989123456789): ").strip()
    if not phone.startswith("+"):
        print("❌ شماره باید با + شروع بشه.")
        sys.exit(1)

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()

    sent = await client.send_code_request(phone)
    print("\n✅ کد تایید به همون اکانت تلگرام فرستاده شد.")
    code = input("🔢 کد تایید رو وارد کن (فقط عدد، بدون فاصله): ").strip().replace(" ", "")

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=sent.phone_code_hash)
    except SessionPasswordNeededError:
        password = input("\n🔐 این اکانت رمز دومرحله‌ای داره - رمز رو وارد کن: ").strip()
        await client.sign_in(password=password)
    except (PhoneCodeInvalidError, PhoneCodeExpiredError) as e:
        print(f"\n❌ کد اشتباه یا منقضی‌شده: {e}")
        print("دوباره اسکریپت رو اجرا کن (هر بار که کد جدید بخوای، باید از اول اجرا بشه).")
        sys.exit(1)

    me = await client.get_me()
    session_string = client.session.save()
    await client.disconnect()

    print("\n" + "=" * 60)
    print(f"✅ لاگین موفق: @{me.username or me.id}")
    print("=" * 60)
    print("STRING SESSION:")
    print(session_string)
    print("=" * 60)

    saved = await save_to_database(session_string)
    if saved:
        print("\n✅ سشن مستقیم تو دیتابیس ربات ذخیره شد.")
        print("   ربات رو ری‌استارت کن (یا صبر کن دفعهٔ بعد خودکار همینو بخونه).")


if __name__ == "__main__":
    asyncio.run(main())
