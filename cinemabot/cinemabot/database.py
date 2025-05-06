import json
import typing

import aiosqlite

from .utils import FilmInfo

conn: aiosqlite.Connection | None = None
cursor: aiosqlite.Cursor | None = None


async def open_connection() -> None:
    global conn, cursor
    conn = await aiosqlite.connect('cinemabot.db')
    cursor = await conn.cursor()
    await cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        telegram_id INTEGER UNIQUE,
        history TEXT,
        stats TEXT,
        wishlist TEXT
    )
    ''')
    await conn.commit()


async def close_connection() -> None:
    if conn:
        await conn.close()


async def register_user(telegram_id: int, check: bool = False) -> bool:
    await cursor.execute('SELECT telegram_id FROM users WHERE telegram_id = ?', (telegram_id,))
    a = await cursor.fetchone()
    if check:
        return a is not None
    if a is None:
        await cursor.execute('INSERT INTO users (telegram_id, history, stats, wishlist) VALUES (?, ?, ?, ?)',
                             (telegram_id, json.dumps([]), json.dumps({}), json.dumps([])))
        await conn.commit()
        return True
    return False


async def update_history(telegram_id: int, query: str, film: FilmInfo) -> bool:
    await cursor.execute('SELECT history, stats FROM users WHERE telegram_id = ?', (telegram_id,))
    row = await cursor.fetchone()

    if row is None:
        return False

    else:
        history, stats = json.loads(row[0]), json.loads(row[1])
        history.append((query, film.name))
        if film.name in stats:
            stats[film.name] += 1
        else:
            stats[film.name] = 1
        await cursor.execute('UPDATE users SET history = ?, stats = ? WHERE telegram_id = ?',
                             (json.dumps(history), json.dumps(stats), telegram_id))
    await conn.commit()
    return True


async def get_user_data(telegram_id: int, prompt: str) -> dict | None:
    await cursor.execute(f'SELECT {prompt} FROM users WHERE telegram_id = ?', (telegram_id,))
    row = (await cursor.fetchone())[0]
    if row:
        res = json.loads(row)
        return res
    return None


async def get_user_stats(telegram_id: int) -> dict | None:
    return await get_user_data(telegram_id, 'stats')


async def get_user_history(telegram_id: int) -> dict | None:
    return await get_user_data(telegram_id, 'history')


async def add_or_show_wishlist(telegram_id: int, *, show: bool) -> typing.Any:
    await cursor.execute('SELECT history, wishlist FROM users WHERE telegram_id = ?', (telegram_id,))
    row = (await cursor.fetchone())
    if row is None:
        return ''
    elif show:
        wishlist = json.loads(row[1])
        return wishlist
    else:
        history = json.loads(row[0])
        if not history:
            return ''
        film = history[-1][1]
        wishlist = json.loads(row[1])
        wishlist.append(film)
        await cursor.execute('UPDATE users SET wishlist = ? WHERE telegram_id = ?',
                             (json.dumps(wishlist), telegram_id))
        await conn.commit()
        return film


async def remove_from_wishlist(telegram_id: int, num: int) -> bool:
    await cursor.execute('SELECT wishlist FROM users WHERE telegram_id = ?', (telegram_id,))

    try:
        row = (await cursor.fetchone())
        wishlist = json.loads(row[0])
        _ = wishlist[-num]
    except IndexError:
        return False
    wishlist.pop(-num)
    await cursor.execute('UPDATE users SET wishlist = ? WHERE telegram_id = ?',
                         (json.dumps(wishlist), telegram_id))
    await conn.commit()
    return True


async def clear_user_history(telegram_id: int) -> None:
    await cursor.execute('UPDATE users SET history = ?, stats = ?, wishlist = ? WHERE telegram_id = ?',
                         (json.dumps([]), json.dumps({}), json.dumps([]), telegram_id))
    await conn.commit()
