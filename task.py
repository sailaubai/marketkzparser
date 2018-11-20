import asyncio
import aiohttp
from datetime import date, timedelta
from bs4 import BeautifulSoup as BS
import re
import pickle
import os


"""
Variables
"""
URL = "https://market.kz"
headers = {
    'User-Agent':
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36'
}
"""
For sitemap
"""
categories = (
    ('detyam', 'Детям'),
    ('zhivotnye', 'Животные'),
    ('elektronika', 'Электроника'),
    ('dom-dacha', 'Для дома и дачи'),
    ('uslugi', 'Услуги'),
    ('hobby-otdyh', 'Хобби и отдых'),
    ('lichnye-vezchi', 'Личные вещи'),
    ('rabota', 'Работа'),
    ('biznes', 'Для бизнеса'),
    ('ruchnaya-rabota', 'Ручная работа'),
    ('transport', 'Транспорт'),
    ('nedvizhimost', 'Недвижимость')
)
THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))

hours = [str(i).zfill(2) for i in range(0, 1)]
today = date.today()
dates = [(today - timedelta(days=x)).strftime('%Y/%m/%d') for x in range(0, 30)]

DATEPATTERN = re.compile(r'(\d+)/(\d+)/(\d+)')
NUMERICPATTERN = re.compile(r'\D')

"""
Global var
"""
users_list = set()
"""
user type:
{
    name: username
    phones: [phones]
    location: location
}
"""
users_info = []  # not used


def url_maker(category, dt, hour):
    return "/".join([URL, 'sitemap', category, dt, hour]) + "/"


def url_advert(ad):
    return "".join([URL, ad])


def date_snake(dt):
    d, m, y = DATEPATTERN.match(dt).groups()
    return "{}_{}_{}".format(d, m, y)


async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


async def word_parser(url, words):
    async with aiohttp.ClientSession(headers=headers) as session:
        html = await fetch(session, url)
        advert_soup = BS(html, "html.parser")
        item = advert_soup.find('div', {'id': 'content'})
        item_title = item.find('h1', {'itemprop': 'name'}).text
        item_text = item.find('p', {'itemprop': 'description'})
        item_text = item_text.text if item_text else ''
        if item.has_attr('data-current-category'):
            item_category = item['data-current-category']
        else:
            item_category = None
        item_price_container = item.find('dl', {'class': 'price'})
        item_price = item_price_container.find('dd').text
        price = NUMERICPATTERN.sub('', item_price)
        if len(price) > 0:
            price = int(price)
        else:
            price = None
        user_link = item.find('div', {'class': 'advert-owner__name'}).a['href']
        username = user_link.split('/')[2]
        if username not in users_list:
            users_list.add(username)
            user = await get_user(username)
        else:
            user = username
        words.append({
            'title': item_title,
            'text': item_text,
            'url': url,
            'category': item_category,
            'price': price,
            'user': user
        })


async def get_user(user):
    url = url_advert("/profile/{}/".format(user))
    async with aiohttp.ClientSession(headers=headers) as session:
        html = await fetch(session, url)
        soup = BS(html, "html.parser")
        profile = soup.find('div', {'class': 'profile-info'})
        contact_info = profile.find('div', {'class': 'contact-info'})
        location = contact_info.p.span.text if contact_info.p.span else None
        contacts_box = profile.find('div', {'class': 'contacts'})
        contacts = contacts_box.dl.dd.find_all('span', {'class': 'phones'}) if contacts_box.dl else []
        numbers = []
        for contact in contacts:
            numbers.append(contact.text)
        user = {
            'name': user,
            'phones': numbers,
            'location': location
        }
        return user


async def hours_parser(category, dt, hour, words):
    url = url_maker(category, dt, hour)
    async with aiohttp.ClientSession(headers=headers) as session:
        html = await fetch(session, url)
        soup = BS(html, "html.parser")
        links_container = soup.find('div', {'class': 'inline-links'})
        links = links_container.find_all('a', href=True) if links_container else None
        if links:
            parsers = [asyncio.ensure_future(word_parser(url_advert(link['href']), words)) for link in links]
            await asyncio.wait(parsers)


async def days_parser(category_name, dt):
    words = []
    filename = os.path.join('categories', category_name, "{}.pickle".format(date_snake(dt)))
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    if not os.path.isfile(filename):
        print("FETCHING {} date {}".format(category_name, dt))
        tasks = [asyncio.ensure_future(hours_parser(category_name, dt, i, words)) for i in hours]
        await asyncio.wait(tasks)
        with open(filename, 'wb') as fp:
            pickle.dump(words, fp)
            fp.close()


async def category_task(category_name):
    tasks = [asyncio.ensure_future(days_parser(category_name, dt)) for dt in dates]
    await asyncio.wait(tasks)
    print("COMPLETE {}".format(category_name))


async def run_tasks():
    # подгрузка распарсеных юзеров
    for cat, _ in categories:
        files_list = os.listdir(os.path.join(THIS_FOLDER, 'categories', cat))
        for file in files_list:
            filename = os.path.join(THIS_FOLDER, 'categories', cat, file)
            if os.path.isfile(filename):
                with open(filename, 'rb') as f:
                    adverts = pickle.load(f)
                    for advert in adverts:
                        if advert['user']:
                            if isinstance(advert['user'], dict):
                                users_list.add(advert['user']['name'])
                            else:
                                users_list.add(advert['user'])
    tasks = [asyncio.ensure_future(category_task(i)) for i, n in categories]
    await asyncio.wait(tasks)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_tasks())
    loop.close()
