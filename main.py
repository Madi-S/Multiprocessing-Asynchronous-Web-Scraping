import aiohttp
import asyncio
import string
import sys
import logging
from multiprocessing import Pool
from time import sleep, time
from bs4 import BeautifulSoup
from parse_funcs import parse, get_urls


LETTERS = [letter for letter in string.ascii_lowercase]
URL = 'https://www.healthgrades.com/affiliated-physicians/{letter}-{pageNo}'


scraped_counter = 0

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)





async def loop_through(session, l):
    global scraped_counter

    i = 1
    _urls = []

    while True:

        url = URL.format(letter=l, pageNo=i)
        logger.debug('Crawling %s', url)

        try:

            async with session.get(url, allow_redirects=True) as r:

                if url == str(r.url):

                    html = await r.text()

                    _urls.extend(get_urls(html))
                    scraped_counter += len(_urls)

                    if sys.getsizeof(_urls) > 2000:
                        logger.info(
                            'Memory is depleted. Starting to parse %s URLs', len(_urls))

                        start = time()

                        with Pool(16) as p:
                            p.map(parse, _urls)

                        finish = time()

                        logger.debug('Scraped: %s\tSpeed: %s URLs/second\tTotal: %s', len(
                            _urls), round(len(_urls)/(finish-start), 2), scraped_counter)

                        _urls.clear()

                        logger.debug('URLs list has been cleared: %s', _urls)

                    i += 1

                else:
                    break

        except Exception as e:
            logger.exception('Exception found in loop through: %s', e)


# Loop through all pages with doctors and save them in redis
async def main1():
    for i in range(4):

        left = round(len(LETTERS)/4*i)
        right = round(len(LETTERS)/4*(i+1))
        logger.debug('ROUND NUMBER %s\nPARSING from %s to %s', i, left, right)

        try:
            async with aiohttp.ClientSession() as session:
                await asyncio.gather(*(loop_through(session, letter) for letter in LETTERS[left:right]))

        except Exception as e:
            logger.exception('Exception found in main: %s', e)


if __name__ == '__main__':
    start_time = time()

    with open('data.csv', 'w') as f:
        f.write('')

    asyncio.run(main1())

    finish_time = time()

    print(finish_time - start_time)
