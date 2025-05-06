from cinemabot.bot import main
from cinemabot.database import close_connection


async def close():
    await close_connection()


if __name__ == "__main__":
    import asyncio

    try:
        asyncio.run(main())
    finally:
        asyncio.run(close())
