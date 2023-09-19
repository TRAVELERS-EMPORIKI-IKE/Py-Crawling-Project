import asyncio
import httpx
from bs4 import BeautifulSoup
import csv
import logging
import configparser
from datetime import datetime

# Initialize logging for loop
logging.basicConfig(filename='Loop_Log.txt', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Read the configuration file
config = configparser.ConfigParser()
config.read('crawler_config.ini')

async def read_last_loop_time(filename):
    try:
        with open(filename, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        logging.error(f"File {filename} not found.")
        return None

async def write_last_loop_time(filename, datetime_str):
    try:
        with open(filename, 'w') as f:
            f.write(datetime_str)
    except Exception as e:
        logging.error(f"Failed to write to {filename}. Exception: {e}")

async def get_sitemap_index(url):
    async with httpx.AsyncClient() as client:
        r = await client.get(url)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, 'xml')
        sitemaps = [loc.string for loc in soup.find_all('loc')]
        return sitemaps

async def get_sitemap_links(sitemap_url, current_datetime):
    async with httpx.AsyncClient() as client:
        r = await client.get(sitemap_url)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, 'xml')
        links = [url.find('loc').string for url in soup.find_all('url') if url.find('lastmod').string >= current_datetime]
        return links

async def crawl_url(url, user_agent):
    headers = {'User-Agent': user_agent}
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        logging.info(f'Successfully crawled {url} with {user_agent}. Title: {soup.title.string}')

async def main():
    current_datetime = await read_last_loop_time('datetime.txt')
    if current_datetime is None:
        return

    desktop_agents = config['UserAgents']['desktop_agents'].split(',')
    mobile_agents = config['UserAgents']['mobile_agents'].split(',')
    sitemap_index_urls = config['Sitemaps']['sitemap_index_urls'].split(',')

    async with aiofiles.open('sitemap_links.csv', mode='a', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        for sitemap_index_url in sitemap_index_urls:
            sitemaps = await get_sitemap_index(sitemap_index_url)
            for sitemap in sitemaps:
                links = await get_sitemap_links(sitemap, current_datetime)
                for link in links:
                    writer.writerow([link])
                    await csvfile.flush()

    await write_last_loop_time('datetime.txt', datetime.now().strftime('%Y-%m-%dT%H:%M:%S+02:00'))

if __name__ == '__main__':
    asyncio.run(main())
