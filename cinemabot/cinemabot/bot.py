import asyncio
import logging
import os
import re
import sys
import time

import pydantic_core
from aiogram import Bot, types, BaseMiddleware, Dispatcher
from aiogram.enums import ContentType
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup
from dotenv import load_dotenv

from .database import register_user, update_history, get_user_history, open_connection, get_user_stats, \
    clear_user_history, add_or_show_wishlist, remove_from_wishlist
from .utils import get_film_info_from_kinopoisk, make_buttons_on_films

load_dotenv()

bot = Bot(token=os.environ['BOT_TOKEN'])
dp = Dispatcher()


async def send_typing_animation(chat_id: int, msg_id: int):
    for i in range(1, 60):
        await asyncio.sleep(0.1)
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id,
                                        text=f"🔍 Ищу фильм{'.' * (i % 5)}")
        except TelegramBadRequest:  # if message was deleted before the end of the loop
            break


class CheckUserRegistration(BaseMiddleware):
    async def __call__(self, handler, event: types.Message, data: dict):
        if event.content_type == ContentType.TEXT and event.text == '/start':
            return await handler(event, data)
        user_id = event.from_user.id
        if not await register_user(user_id, check=True):
            await event.answer("❌ Ты не зарегистрирован( Введи /start, чтобы зарегистрироваться")
            return
        return await handler(event, data)


dp.message.middleware(CheckUserRegistration())


@dp.message(Command("start"))
async def send_welcome(message: types.Message):
    try:
        if await register_user(message.from_user.id):
            await message.reply(
                "👋 *Привет!* Ты успешно зарегистрировался!\n\nЯ бот, который поможет тебе найти информацию о фильме. "
                "А "
                "также где его можно посмотреть."
                "\nПросто отправь мне его название, название может немного не соответствовать (но тогда поиск может "
                "подвести!)\n"
                "Так же я еще сохраняю статистику и историю запросов, поподробнее можешь узнать в /help",
                parse_mode='Markdown')
        else:
            await message.reply(
                "Ты уже зарегистрирован. Просто отправь мне название фильма, чтобы получить информацию о нем.\n"
                "Или напиши /help, чтобы получить информацию по командам")
    except Exception as e:
        logging.error(f"Error in send_welcome: {e}")
        await message.reply("❌ Произошла ошибка при регистрации. Попробуйте еще раз.")


@dp.message(Command("help"))
async def send_help(message: types.Message):
    await message.reply(
        "Для того чтобы узнать информацию о фильме, просто отправь его название, название может немного не "
        "соответствовать (но тогда поиск может подвести!).\n\n"
        "<b>Список моих команд:</b>\n"
        "/stats - показывает твои самые популярные фильмы\n"
        "/history - показывает историю запросов\n"
        "/clear - очищает историю запросов и статистику, а также вишлист\n"
        "/add_to_wishlist - добавляет последний найденный фильм в список желаемого\n"
        "/remove_from_wishlist num - удаляет фильм из вишлиста под номером num\n"
        "/wishlist - показывает список фильмов, которые ты хочешь посмотреть\n"
        "/help - показывает список команд\n",
        parse_mode='html')


@dp.message(Command("add_to_wishlist"))
async def add_to_wishlist(message: types.Message):
    film = await add_or_show_wishlist(message.from_user.id, show=False)
    if film:
        await message.reply(f"✅ Добавил {film} в список желаемого")
    else:
        await message.reply("❌ Ты еще не искал никакие фильмы, чтобы добавить их в список желаемого")


@dp.message(Command("remove_from_wishlist"))
async def remove_from_wl(message: types.Message):
    try:
        num = int(message.text.split()[1])
    except (ValueError, IndexError):
        await message.reply("❌ Не верно указан номер фильма. Попробуй еще раз")
        return
    if await remove_from_wishlist(message.from_user.id, num):
        await message.reply(f"✅ Фильм под номером {num} удален из списка желаемого")
    else:
        await message.reply("❌ Фильм с таким номером не найден в списке желаемого")


@dp.message(Command("wishlist"))
async def show_wishlist(message: types.Message):
    wishlist = await add_or_show_wishlist(message.from_user.id, show=True)
    answer = "Вот твой список фильмов, которые ты хотел посмотреть:\n"
    if wishlist:
        for num, film in enumerate(wishlist[:-11:-1], start=1):
            answer += f"\t\t*{num})* '{film}'\n"
        await message.reply(answer, parse_mode='Markdown')
    else:
        await message.reply("❌ Список желаемого пуст, пора что-то добавить!")


@dp.message(Command("history"))
async def send_history(message: types.Message):
    history = await get_user_history(message.from_user.id)
    answer = "Вот твоя история запросов:\n"
    if history:
        for num, (query, film) in enumerate(history[:-11:-1], start=1):
            answer += f"\t\t*{num})* '{query}' *->* '{film}'\n"
        await message.reply(answer, parse_mode='Markdown')
    else:
        await message.reply("❌ История запросов пуста")


@dp.message(Command("stats"))
async def send_stats(message: types.Message):
    stats = await get_user_stats(message.from_user.id)
    answer = "Вот твои самые популярные фильмы:\n"
    if stats:
        for film, count in sorted(stats.items(), key=lambda x: x[1], reverse=True)[:6:]:
            answer += f"\t\t*{film}*: {count} раз(а)\n"
        await message.reply(answer, parse_mode='Markdown')
    else:
        await message.reply("❌ Статистика запросов пуста")


@dp.message(Command("clear"))
async def clear_history(message: types.Message):
    await clear_user_history(message.from_user.id)
    await message.reply("✅ История запросов и статистика очищена")


@dp.message(Command("ping"))
async def ping(message: types.Message):
    await message.reply("✅ Pong! Я в сети! ✅")


@dp.message(Command(re.compile(r".*")))
async def command(message: types.Message):
    await message.reply("❌ Не поддерживаю такую команду. Можешь ввести /help, чтобы узнать список команд ❌")


@dp.message()
async def find_film(message: types.Message):
    start = time.time()
    if message.content_type != ContentType.TEXT:
        await message.reply("❌ Я воспринимаю только текстовые сообщения( ❌")
        return
    logging.info(f"User {message.from_user.id} sent message: {message.text}")
    typing_message = await bot.send_message(message.from_user.id, "🔍 Ищу фильм")
    asyncio.create_task(send_typing_animation(message.from_user.id, typing_message.message_id))
    film = await get_film_info_from_kinopoisk(message.text)
    logging.info(f"Time to get film info: {time.time() - start}")
    if film is not None:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[await make_buttons_on_films(film)])
        logging.info(f"Time to make buttons: {time.time() - start}")
        try:
            await update_history(message.from_user.id, message.text, film)
            logging.info(f"Time to update history: {time.time() - start}")
            await bot.send_photo(message.from_user.id, photo=film.poster, caption=str(film), parse_mode='Markdown',
                                 reply_markup=keyboard)
        except (pydantic_core.ValidationError, TelegramBadRequest):
            await bot.send_photo(message.from_user.id, photo='https://imgur.com/R7GjQi9',
                                 caption='❌ Что-то пошло не так. Попробуй другой фильм ❌'),
    else:
        await bot.send_photo(message.from_user.id, photo='https://imgur.com/R7GjQi9',
                             caption="❌ Фильм не найден в базе kinopoisk. Попробуй другой ❌")
    logging.info(f"Time to send message: {time.time() - start}")
    await bot.delete_message(message.from_user.id, typing_message.message_id)


async def main() -> None:
    await open_connection()
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
