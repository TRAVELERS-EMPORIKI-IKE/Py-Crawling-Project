import requests
from bs4 import BeautifulSoup
import csv
import time
import logging
import configparser
import aiofiles
import aiohttp
import asyncio
import random
import json
import google.auth
import requests_oauth2
import httplib2
import os
import pytz
from datetime import datetime, timedelta, timezone
from time import time as current_time
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from oauth2client.service_account import ServiceAccountCredentials

#athens_dt_pytz = utc_dt.astimezone(athens_tz)  # Convert to Athens time, automatically accounting for DST
#print("Using datetime.timezone:", athens_timezone) # Debug Print
#print("Using pytz.timezone:", athens_dt_pytz) #debug Print

try:
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('crawler_config.ini')
except Exception as e:
    logging.error(f"Failed to read the configuration file. Exception: {e}")

# Pass the configuration for function run
googlesubmit = config.getboolean('FunctionRun', 'googlesubmit', fallback=False)
bingsubmit = config.getboolean('FunctionRun', 'bingsubmit', fallback=False)
sitecrawler = config.getboolean('FunctionRun', 'sitecrawler', fallback=False)

# Get the timezone string from the configuration file
timezone_str = config['Timezone']['timezone']

# Create a timezone offset from the timezone string
program_timezone = pytz.timezone(timezone_str)

# Get the data_folder path from ini file
data_folder = config['Paths']['data_folder']

# Convert it to an absolute path based on the script's location
current_directory = os.path.dirname(os.path.abspath(__file__))
data_folder_absolute = os.path.join(current_directory, data_folder.strip('.\\'))

# Finally, construct the complete path to your csv and txt files
csv_bing_iterations = os.path.join(data_folder_absolute, 'bing_iterations.csv')
csv_Bing_Submission = os.path.join(data_folder_absolute, 'Bing_Submission.csv')
csv_Bing_Submission_Errors = os.path.join(data_folder_absolute, 'Bing_Submission_Errors.csv')
csv_Google_Submission = os.path.join(data_folder_absolute, 'Google_Submission.csv')
csv_sitemap_links = os.path.join(data_folder_absolute, 'sitemap_links.csv')
txt_datetime = os.path.join(data_folder_absolute, 'datetime.txt')
txt_crawling_log = os.path.join(data_folder_absolute, 'crawling_log.txt')
txt_loop_log = os.path.join(data_folder_absolute, 'Loop_Log.txt')

# Initialize logging
logging.basicConfig(filename=txt_crawling_log, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize logging for loop
loop_log = open(txt_loop_log, 'a')

# Function to read the last loop time from a file
def read_last_loop_time(filename):
    try:
        with open(filename, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        logging.error(f"File {filename} not found.")
        return None

# Function to write the last loop time to a file
def write_last_loop_time(filename, datetime_obj):
    try:
        with open(filename, 'w') as f:
            datetime_str = datetime_obj.strftime('%Y-%m-%dT%H:%M:%S%z')
            f.write(datetime_str)
    except Exception as e:
        logging.error(f"Failed to write to {filename}. Exception: {e}")

# Function to get sitemap index
def get_sitemap_index(url):
    logging.debug(f"url def: {url}")
    try:
        r = requests.get(url)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, 'xml')
        sitemaps = []
        lastmod_times = []  # Added to store lastmod times
        for sitemap in soup.find_all('sitemap'):  # Loop through each sitemap element
            loc = sitemap.find('loc')
            lastmod = sitemap.find('lastmod')
            if loc:
                sitemaps.append(loc.string)
            if lastmod:
                lastmod_str = lastmod.string.replace("<![CDATA[", "").replace("]]>", "")  # Remove CDATA
                lastmod_time = datetime.fromisoformat(lastmod_str)  # Parse to datetime object
                lastmod_time = lastmod_time.astimezone(program_timezone)  # Convert to Athens time
                lastmod_time_str = lastmod_time.strftime('%Y-%m-%dT%H:%M:%S+02:00')  # Convert to string in desired format
                lastmod_times.append(lastmod_time_str)
                logging.debug(f"Sitemap def: {sitemaps}, Last Modified Time def: {lastmod_times}")
        return sitemaps, lastmod_times  # Modified to return lastmod times as well
    except requests.RequestException as e:
        logging.error(f"Failed to fetch sitemap index for {url}. Exception: {e}")
        return [], []  # Modified to return empty lists for both

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
import csv

def remove_duplicates_from_csv(filename):
    unique_links = set()
    
    # Read the CSV and store unique links in a set
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                unique_links.add(row[0])
    except FileNotFoundError:
        print(f"File {filename} not found.")
        return
    
    # Write the unique links back to the CSV
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            for link in unique_links:
                writer.writerow([link])
    except Exception as e:
        print(f"Failed to write to {filename}. Exception: {e}")

async def submit_to_bing(url, session):
    try:
        bing_key = config['BingIndexNow']['key']
        # Check if the URL exists in Bing_Submission.csv
        with open(csv_Bing_Submission, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            if url not in [row[0] for row in reader]:
                # Submit the URL to Bing IndexNow
                bing_url = f"https://www.bing.com/indexnow?url={url}&key={bing_key}"
                async with session.get(bing_url) as bing_response:
                    bing_response.raise_for_status()
                    logging.info(f"Successfully submitted {url} to Bing IndexNow.")
                
                # Append the URL to Bing_Submission.csv
                with open(csv_Bing_Submission, 'a', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([url])
    except Exception as e:
        logging.error(f"Failed to submit {url} to Bing IndexNow. Exception: {e}")
        with open(csv_Bing_Submission_Errors, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([url])
            await countdown_timer(300.00)  # call the countdown function and request a 300 seconds delay for rate limiting

async def countdown_timer(seconds):
    print("\n")  # This will move the cursor to a new line
    for i in range(seconds, 0, -1):
        print(f'\rCountdown: {i} seconds remaining', end='', flush=True)
        await asyncio.sleep(1)
    print('\rCountdown complete!                   ', end='', flush=True)
    print("\n")  # This will move the cursor to a new line
					
def submit_to_google(url):
    try:
        service_account_email = config['GoogleIndexAPIjson']['Google_service_account_email']
        google_json_file = config['GoogleIndexAPIjson']['file']
        SCOPES = ["https://www.googleapis.com/auth/indexing"]
        ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"

        # Authenticate using service account credentials
        credentials = ServiceAccountCredentials.from_json_keyfile_name(google_json_file, scopes=SCOPES)

        # Initialize http Variable with the credentials
        http = credentials.authorize(httplib2.Http())

        with open(csv_Google_Submission, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            if url not in [row[0] for row in reader]:
                # Prepare the request data for URL notification
                url_notification_data = {
                    "url": url,
                    "type": "URL_UPDATED"
                }

                # Send the request to the Google Indexing API
                response, content = http.request(ENDPOINT, method="POST", body=json.dumps(url_notification_data))

                # Check if the response contains a status code
                if 'status' in response:
                    status_code = response['status']
                    if status_code == '200':
                        logging.info(f"Successfully submitted {url} to Google Indexing API")

                        # Append the URL to Google_Submission.csv only if successful
                        with open(csv_Google_Submission, 'a', newline='', encoding='utf-8') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow([url])
                    else:
                        # Log the HTTP status code and the response content
                        logging.error(f"Failed to submit {url} to Google Indexing API. HTTP Status Code: {status_code}")
                        logging.error(f"API Response Content: {content}")
                else:
                    # Log a message when the response does not contain a status code
                    logging.error(f"Failed to submit {url} to Google Indexing API. No HTTP status code in the response.")
    except Exception as e:
        # Log any other exceptions that may occur
        logging.error(f"Failed to submit {url} to Google Indexing API. Exception: {e}")

# Asynchronous crawl_url function
async def crawl_url(url, user_agent):
    headers = {'User-Agent': user_agent}
    start_time = current_time()
    # Read the Bing IndexNow key from the configuration file
    bing_key = config['BingIndexNow']['key']
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
            await countdown_timer(300.00)  # call the countdown function and request a 300 seconds delay for rate limiting

async def write_to_csv(counter, current_date):
    with open(csv_bing_iterations, 'w') as f:
        f.write(f"{current_date},{counter}")

async def crawl_all_urls_2(desktop_agents, mobile_agents, delay, crawl_counter, session,sitemap_links_reader):
    try:
        tasks = []
        print("\n")  # This will move the cursor to a new line
        user_agent_index = 0  # Initialize an index variable to keep track of the user agents
        row = next(sitemap_links_reader, None)
        while row:
            url = row[0]
    
            # Cycle through desktop user agents
            selected_desktop_agent = desktop_agents[user_agent_index % len(desktop_agents)]
            await asyncio.sleep(delay)
            task = asyncio.ensure_future(crawl_url(url, selected_desktop_agent))
            tasks.append(task)
            crawl_counter += 1
            print(f'\rCrawled URLs: {crawl_counter}', end='', flush=True)
    
            # Cycle through mobile user agents
            selected_mobile_agent = mobile_agents[user_agent_index % len(mobile_agents)]
            await asyncio.sleep(delay)
            task = asyncio.ensure_future(crawl_url(url, selected_mobile_agent))
            tasks.append(task)
            crawl_counter += 1
            print(f'\rCrawled URLs: {crawl_counter}', end='', flush=True)
    
            # Increment the user agent index
            user_agent_index += 1
    
            row = next(sitemap_links_reader, None)  # Move to the next row
        await asyncio.gather(*tasks)
    except Exception as e:
        logging.error(f"An unexpected error occurred in crawl_all_urls_2. Exception: {e}") 

async def crawl_all_urls(desktop_agents, mobile_agents, rate_limit, bing_rate_limit):
    try:
        counter = 0  # Initialize the counter
        crawl_counter = 0  # Initialize the crawl counter
        tz = timezone(timedelta(hours=3))  # UTC+3 Timezone
        now = datetime.now(tz)  # Moved up
        current_date = now.date()  # Moved up
                		
        # Check if bing_iterations.csv exists and read counter value
        if os.path.exists(csv_bing_iterations):
            with open(csv_bing_iterations, 'r') as f:
                try:
                        last_record = f.readlines()[-1].strip().split(',')
                        last_date = datetime.strptime(last_record[0], '%Y-%m-%d %H:%M:%S.%f%z').date()
                        if last_date == current_date:
                            counter = int(last_record[1])
                        else:
                            counter = 0
                except ValueError:
                        counter = 0
        else:
            counter = 0  # Initialize if the file doesn't exist
		
        delay = 1 / rate_limit  # time to wait between requests     
        bing_delay = 1 / bing_rate_limit  # time to wait between requests to Bing IndexNow

        async with aiohttp.ClientSession() as session:  # Create a session here
            with open(csv_sitemap_links, 'r', newline='', encoding='utf-8') as sitemap_reader_csvfile:
                sitemap_links_reader = csv.reader(sitemap_reader_csvfile)
                #tasks = []
                for row in sitemap_links_reader:
                    url = row[0]
                    now = datetime.now(tz)
                    current_date = now.date()
                    # Check the counter
                    if bingsubmit == True and counter < 10000:
                        await asyncio.sleep(bing_delay)  # Introduce delay for rate limiting
                        await submit_to_bing(url, session)
                        counter += 1  # Increment the counter
                        await write_to_csv(counter, now)
                        #print(counter) #display counter
                        print(f'\rSubmited to Bing URLs: {counter}', end='', flush=True) #display counter
                    elif bingsubmit == True and counter >= 10000:
                        # Pause until 03:00:01 (UTC+3 Time)
                        next_run = datetime(now.year, now.month, now.day, 3, 0, 1, tzinfo=tz)
                        next_run += timedelta(days=1)
                        if now >= next_run:
                            #next_run += timedelta(days=1)
                            await asyncio.sleep(bing_delay)  # Introduce delay for rate limiting
                            await submit_to_bing(url, session)
                            counter = 0  # Reset the counter
                            counter += 1  # Increment the counter
                            await write_to_csv(counter, now)
                            #print(counter) #display counter
                            print(f'\rSubmited to Bing URLs: {counter}', end='', flush=True) #display counter
                        else:
                            break
                # Second loop to crawl URLs
                if sitecrawler == True:
                    await crawl_all_urls_2(desktop_agents, mobile_agents, delay, crawl_counter, session, sitemap_links_reader)
    except Exception as e:
        logging.error(f"An unexpected error occurred in crawl_all_urls. Exception: {e}")

# Main function
def main():
    while True:
        try:
            # Clear the previous crawling log
            with open(txt_crawling_log, 'w'):
                pass

            # Read user agents, sitemap URLs, and rate limit from the configuration file
            desktop_agents = config['UserAgents']['desktop_agents'].split(',')
            mobile_agents = config['UserAgents']['mobile_agents'].split(',')
            sitemap_index_urls = config['Sitemaps']['sitemap_index_urls'].split(',')
            rate_limit = float(config['RateLimit']['requests_per_second'])
            bing_rate_limit = float(config['RateLimit']['bing_requests_per_second'])

            # Read the last loop time
            current_datetime = read_last_loop_time(txt_datetime)
            #print (current_datetime)
            if current_datetime is None:
                return

            # Start loop log
            loop_start_datetime = datetime.now(timezone.utc).astimezone()
            #print(loop_start_datetime) #debug Print
            loop_log.write(f'Start Time: {loop_start_datetime}\n')

            # Open a CSV file to append links
            with open(csv_sitemap_links, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)

            # Timer for 'Get individual sitemaps'
            start_time = current_time()
            #print(start_time) # Debug Print

            for sitemap_index_url in sitemap_index_urls:
                sitemaps, lastmod_times = get_sitemap_index(sitemap_index_url)
                elapsed_time = timedelta(seconds=int(current_time() - start_time))
                print(f"Get individual sitemaps: {elapsed_time}")
	            
                # Timer for 'Get all links'
                start_time = current_time()
                
                # Open a CSV file to append links
                with open(csv_sitemap_links, 'a', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    
                    for sitemap, lastmod_time in zip(sitemaps, lastmod_times):
                        logging.debug(f"Sitemap: {sitemap}, Last Modified Time: {lastmod_time}")
                        if lastmod_time >= current_datetime:
                            links = get_sitemap_links(sitemap, lastmod_time)
                            for link in links:
                                writer.writerow([link])
                                csvfile.flush()  # Now this should work, as csvfile is still open
    
            elapsed_time = timedelta(seconds=int(current_time() - start_time))
            print(f"Get all links: {elapsed_time}")

            # Timer for 'Remove all the duplicate URLs'
            start_time = current_time()
            remove_duplicates_from_csv(csv_sitemap_links)
            remove_duplicates_from_csv(csv_Bing_Submission)
            remove_duplicates_from_csv(csv_Google_Submission)
            elapsed_time = timedelta(seconds=int(current_time() - start_time))
            print(f"Remove all the duplicate URLs: {elapsed_time}")

            # Update the last loop time
            write_last_loop_time(txt_datetime, loop_start_datetime)

            # Timer for 'Asynchronous crawling'
            start_time = current_time()
            asyncio.run(crawl_all_urls(desktop_agents, mobile_agents, rate_limit, bing_rate_limit))
            elapsed_time = timedelta(seconds=int(current_time() - start_time))
            print(f"Asynchronous crawling: {elapsed_time}")

            # End loop log
            end_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+02:00')
            start_time_obj = datetime.strptime(loop_start_datetime, '%Y-%m-%dT%H:%M:%S+02:00')
            end_time_obj = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S+02:00')
            duration = end_time_obj - start_time_obj
            loop_log.write(f'End Time: {end_time}\n')
            loop_log.write(f'Duration: {duration}\n\n')
            loop_log.close()

             # Check if all flags are False
            if not (sitecrawler or bingsubmit or googlesubmit):
                print("All functions are disabled. Exiting.")
                break

            # Sleep for a specified time before the next iteration
            time.sleep(60)
        except Exception as e:
            logging.error(f"An unexpected error occurred in the main function. Exception: {e}")

if __name__ == '__main__':
    main()
