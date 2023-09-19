import requests
from bs4 import BeautifulSoup
import csv
import time
import logging
import configparser
import aiofiles
import random
from datetime import datetime
from time import time as current_time

# Initialize logging for loop
loop_log = open('Loop_Log.txt', 'a')

try:
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('crawler_config.ini')
except Exception as e:
    logging.error(f"Failed to read the configuration file. Exception: {e}")

# Function to read the last loop time from a file
def read_last_loop_time(filename):
    try:
        with open(filename, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        logging.error(f"File {filename} not found.")
        return None

# Function to write the last loop time to a file
def write_last_loop_time(filename, datetime_str):
    try:
        with open(filename, 'w') as f:
            f.write(datetime_str)
    except Exception as e:
        logging.error(f"Failed to write to {filename}. Exception: {e}")

# Function to get sitemap index
def get_sitemap_index(url):
    try:
        r = requests.get(url)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, 'xml')
        sitemaps = []
        for loc in soup.find_all('loc'):
            sitemaps.append(loc.string)
        return sitemaps
    except requests.RequestException as e:
        logging.error(f"Failed to fetch sitemap index for {url}. Exception: {e}")
        return []

# Function to get sitemap links
def get_sitemap_links(sitemap_url, current_datetime):
    try:
        r = requests.get(sitemap_url)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, 'xml')
        links = []
        for url in soup.find_all('url'):
            loc = url.find('loc')
            lastmod = url.find('lastmod')
            if loc and lastmod:
                if lastmod.string >= current_datetime:
                    links.append(loc.string)
        return links
    except requests.RequestException as e:
        logging.error(f"Failed to fetch sitemap links for {sitemap_url}. Exception: {e}")
        return []

# Function to crawl a single URL
def crawl_url(url, user_agent):
    headers = {'User-Agent': user_agent}
    start_time = current_time()
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        end_time = current_time()
        duration = end_time - start_time
        soup = BeautifulSoup(response.text, 'html.parser')
        page_length = len(response.text)
        num_images = len(soup.find_all('img'))
        num_links = len(soup.find_all('a'))
        logging.info(f'Successfully crawled {url} with {user_agent}. Title: {soup.title.string}, Page Length: {page_length}, Images: {num_images}, Links: {num_links}, Duration: {duration:.2f} seconds')
    except requests.RequestException as e:
        end_time = current_time()
        duration = end_time - start_time
        logging.error(f"Failed to crawl {url}. Exception: {e}, Duration: {duration:.2f} seconds")

# Main function
def main():
    try:
        # Clear the previous crawling log
        with open('crawling_log.txt', 'w'):
            pass

        # Initialize logging
        logging.basicConfig(filename='crawling_log.txt', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

        # Read user agents and sitemap URLs from the configuration file
        desktop_agents = config['UserAgents']['desktop_agents'].split(',')
        mobile_agents = config['UserAgents']['mobile_agents'].split(',')
        sitemap_index_urls = config['Sitemaps']['sitemap_index_urls'].split(',')

        # Read the last loop time
        current_datetime = read_last_loop_time('datetime.txt')
        if current_datetime is None:
            return

        # Start loop log
        loop_start_datetime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+02:00')
        loop_log.write(f'Start Time: {loop_start_datetime}\n')

        # Open a CSV file to append links
        with open('sitemap_links.csv', 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)

            # Loop through each sitemap index URL
            for sitemap_index_url in sitemap_index_urls:
                # Get individual sitemaps from the current sitemap index
                sitemaps = get_sitemap_index(sitemap_index_url)

                # Loop through individual sitemaps to get all links
                for sitemap in sitemaps:
                    links = get_sitemap_links(sitemap, current_datetime)
                    for link in links:
                        writer.writerow([link])
                        csvfile.flush()

        # Update the last loop time
        write_last_loop_time('datetime.txt', loop_start_datetime)
		
        # After writing all URLs to sitemap_links.csv
        with open('sitemap_links.csv', 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                url = row[0]
                user_agent = random.choice(desktop_agents)  # or random.choice(mobile_agents), based on your needs
                crawl_url(url, user_agent)

        # End loop log
        end_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+02:00')
        start_time_obj = datetime.strptime(loop_start_datetime, '%Y-%m-%dT%H:%M:%S+02:00')
        end_time_obj = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S+02:00')
        duration = end_time_obj - start_time_obj
        loop_log.write(f'End Time: {end_time}\n')
        loop_log.write(f'Duration: {duration}\n\n')
        loop_log.close()

    except Exception as e:
        logging.error(f"An unexpected error occurred in the main function. Exception: {e}")

if __name__ == '__main__':
    main()
