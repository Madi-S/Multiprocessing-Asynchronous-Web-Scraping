import aiohttp
import asyncio
import string
import sys
import logging
from multiprocessing import Pool
from time import sleep, time
from bs4 import BeautifulSoup
from parse_funcs import parse, get_urls


# Structure of URL to scrape - it has a letter and page number, which are not constant
URL = 'https://www.healthgrades.com/affiliated-physicians/{letter}-{pageNo}'

# Since this website has sections for each letter, we need a list of all lowercase letters
LETTERS = [letter for letter in string.ascii_lowercase]

# To show scraping speed (URLs/second) and the overall amount of scraped URLs
scraped_counter = 0

# Logger configuration (set level to DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)



# Function to go through each letter from the above list, which takes session and letter as parameters
async def loop_through(session, l):
    global scraped_counter

    i = 1
    _urls = []

    while True:

        url = URL.format(letter=l, pageNo=i) # Format URL by passing letter and counter i
        logger.debug('Crawling %s', url)

        try:
            
            # Get method to URL
            async with session.get(url, allow_redirects=True) as r:
                
                # Check if such page exists
                if url == str(r.url):
                    
                    # Get html code of current page
                    html = await r.text()

                    # Add urls to the list of urls
                    _urls.extend(get_urls(html))
                    scraped_counter += len(_urls)

                    # If the list of URLs consumes too much memory, the program starts to scrape those URLs
                    if sys.getsizeof(_urls) > 2000:
                        logger.info(
                            'Memory is depleted. Starting to parse %s URLs', len(_urls))

                        start = time()
                        
                        # Starting to scrape the list of URLs using multiprocessing Pool with 16 processors
                        with Pool(16) as p:
                            p.map(parse, _urls)

                        finish = time()
                        
                        # Show the scraping speed and overall counter
                        logger.debug('Scraped: %s\tSpeed: %s URLs/second\tTotal: %s', len(
                            _urls), round(len(_urls)/(finish-start), 2), scraped_counter)

                        # Clear the URLs list because those URLs were scraped
                        _urls.clear()

                        logger.debug('URLs list has been cleared: %s', _urls)
                    
                    # Increment the counter -> to go to the next page (pagination)
                    i += 1
                
                # Else the loop breaks and the next letter goes on
                else:
                    break
    
        except Exception as e:
            logger.exception('Exception found in loop through: %s', e)


# Main function
async def main1():
    # Tasks are divided into 4 sections (gathering all 27 tasks is too much -> may result in an early crush)
    # It is not compulsory to divide tasks and make await asyncio.gather(*(loop_through(session, letter) for letter in LETTERS)) but it did not work for me (scraper just hung on 20000th URL)
    for i in range(4):

        # Left is the index to start with, where right is the index of last letter (needed because tasks are divided into partitions) 
        left = round(len(LETTERS)/4*i)
        right = round(len(LETTERS)/4*(i+1))
        logger.debug('ROUND NUMBER %s\nPARSING from %s to %s', i, left, right)

        try:
            # Creating safe session, which will close
            async with aiohttp.ClientSession() as session:
                await asyncio.gather(*(loop_through(session, letter) for letter in LETTERS[left:right]))
                
            # Give some time to all connections/session to close
            sleep(1.5)
                
        except Exception as e:
            logger.exception('Exception found in main: %s', e)


if __name__ == '__main__':
    start_time = time()

    # Clear the data file
    with open('data.csv', 'w') as f:
        f.write('')
    
    # Run main function asynchronously
    asyncio.run(main1())

    finish_time = time()
    
    # Count taken time to scrape all URLs
    t = finish_time - start_time
    
    # Print 
    logger.debug('Time taken %s for %s URLs', t, scraped_counter)
