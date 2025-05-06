import asyncio
import dataclasses
import json
import logging
import os
from urllib.parse import urlparse

from aiogram.client.session import aiohttp
from aiogram.types import InlineKeyboardButton
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()
HEADERS = json.loads(os.environ['HEADERS'])


@dataclasses.dataclass
class FilmInfo:
    name: str
    alternative_name: str
    length: int
    series_length: int
    year: int
    imdb: float
    kp: float
    description: str
    poster: str
    is_serial: bool
    top250: int

    def _convert_time(self):
        if self.is_serial:
            return f"серии по {self.series_length} мин."
        return f"{self.length // 60} ч. {self.length % 60} мин."

    def __repr__(self):
        return (
            f"*✅ Название:* {self.name}{' | ' + self.alternative_name if self.alternative_name else ''}\n\n"
            f"*⌛ Продолжительность:* {self._convert_time()}\n\n"
            f"*📅 Год выпуска:* {self.year}\n\n"
            f"*📈 Рейтинги:*\n\t\t\t\t\t\t\t\t\tIMBD: "
            f"{self.imdb}\n\t\t\t\t\t\t\t\t\tКинопоиск: {self.kp} "
            f"{f'🏅*Топ {self.top250}*🏅' if self.top250 else ''}\n\n"
            f"*🎬 Описание:* {self.description}")


async def get_film_info_from_kinopoisk(film_name: str) -> FilmInfo | None:
    query = f"https://api.kinopoisk.dev/v1.4/movie/search?page=1&limit=1&query={'+'.join(film_name.split())}"
    async with aiohttp.ClientSession() as session:
        async with session.get(query, headers=HEADERS) as response:
            if response.status == 200:
                data = await response.json()
                try:
                    return FilmInfo(data['docs'][0]['name'], data['docs'][0]['alternativeName'],
                                    data['docs'][0]['movieLength'],
                                    data['docs'][0]['seriesLength'],
                                    data['docs'][0]['year'],
                                    data['docs'][0]['rating']['imdb'], data['docs'][0]['rating']['kp'],
                                    data['docs'][0]['description'], data['docs'][0]['poster']['previewUrl'],
                                    data['docs'][0]['isSeries'],
                                    data['docs'][0]['top250'] if data['docs'][0]['top250'] is not None else 0)
                except (IndexError, TypeError):
                    return None


async def fetch_film_links(session: ClientSession, film_info: FilmInfo, site: str) -> InlineKeyboardButton | None:
    name = f"{film_info.name} {'сериал' if film_info.is_serial else ''} смотреть на {site}"
    url = f"https://www.google.com/search?q={'+'.join(name.split())}&num=3"
    try:
        async with session.get(url, headers=HEADERS, timeout=2) as response:
            data = BeautifulSoup(await response.text(), 'html.parser')
            for film in data.find_all('a', href=True, jsname=lambda jsname: jsname and jsname == "UWckNb"):
                try:
                    async with session.get(film['href'], headers=HEADERS, timeout=2) as r:
                        logging.info(f"Fetching film link: {film['href']} with status {r.status}")
                        if r.status == 200:
                            site_name = urlparse(film['href']).netloc.split('.')
                            parsed_url = site_name[0] if site_name[0] != 'www' else site_name[1]
                            return InlineKeyboardButton(text=f"Смотреть на {parsed_url.capitalize()}", url=film['href'])
                except asyncio.TimeoutError:
                    logging.warning(f"Timeout error fetching film link: {film['href']}")
                except Exception as e:
                    logging.error(f"Error fetching film link: {e}")
    except asyncio.TimeoutError:
        logging.warning(f"Timeout error fetching film link: {film['href']}")
    return None


async def make_buttons_on_films(film_info: FilmInfo) -> list[InlineKeyboardButton]:
    buttons: list[InlineKeyboardButton] = []
    async with ClientSession() as session:
        tasks = [fetch_film_links(session, film_info, site) for site in ['lordfilm', 'rutube']]
        results = await asyncio.gather(*tasks)
        buttons.extend(filter(None, results))
    return buttons
