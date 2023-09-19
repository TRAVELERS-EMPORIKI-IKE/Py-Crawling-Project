import csv
import random
import asyncio
import aiohttp
import configparser
from time import time as current_time
from bs4 import BeautifulSoup
import logging

# Initialize variables
requests_per_second = 2  # Default value

# Clear the previous asynchronous crawling log
with open('async_crawling_log.txt', 'w'):
    pass

# Initialize logging
logging.basicConfig(filename='async_crawling_log.txt', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Read the configuration file
config = configparser.ConfigParser()
try:
    config.read('crawler_config.ini')
    desktop_agents = config['UserAgents']['desktop_agents'].split(',')
    mobile_agents = config['UserAgents']['mobile_agents'].split(',')
    requests_per_second = int(config['RateLimit']['requests_per_second'])
except Exception as e:
    logging.debug(f"Failed to read the configuration file. Exception: {e}")

# Asynchronous crawl_url function
async def crawl_url(url, user_agent):
    headers = {'User-Agent': user_agent}
    start_time = current_time()
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                end_time = current_time()
                duration = end_time - start_time
                text = await response.text()
                soup = BeautifulSoup(text, 'html.parser')
                page_length = len(text)
                num_images = len(soup.find_all('img'))
                num_links = len(soup.find_all('a'))
                logging.info(f'Successfully crawled {url} with {user_agent}. Title: {soup.title.string}, Page Length: {page_length}, Images: {num_images}, Links: {num_links}, Duration: {duration:.2f} seconds')
        except Exception as e:
            end_time = current_time()
            duration = end_time - start_time
            logging.error(f"Failed to crawl {url}. Exception: {e}, Duration: {duration:.2f} seconds")

# Asynchronous main function to crawl all URLs
async def crawl_all_urls():
    with open('sitemap_links.csv', 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        tasks = []
        semaphore = asyncio.Semaphore(requests_per_second)

        async def bounded_crawl(url, user_agent):
            async with semaphore:
                await crawl_url(url, user_agent)

        for row in reader:
            url = row[0]
            for user_agent in desktop_agents + mobile_agents:  # Loop through all user agents
                task = asyncio.ensure_future(bounded_crawl(url, user_agent))
                tasks.append(task)
                await asyncio.sleep(1 / requests_per_second)  # Sleep to maintain the rate limit

        await asyncio.gather(*tasks)

# Entry point
if __name__ == '__main__':
    asyncio.run(crawl_all_urls())
