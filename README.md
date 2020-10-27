# Multiprocessing-Asynchronous-Web-Scraping
A simple example of asynchronous web scraping with multiprocessing Pool. 

With these methods, web scraper achieves 8-15 (depends on the number of processors specified) parsed URLs per second. There is a lot of data to scrape (8 qualities for each person), thus the speed is not so high, since parsing function takes 90% time, not requests and retrieving HTML source.

Scraped website - www.healthgrades.com
