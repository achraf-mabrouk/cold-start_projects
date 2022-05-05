from datetime import datetime
from selenium import webdriver
from selenium.webdriver import ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import time, os
import json
from url_normalize import url_normalize
import argparse
from KafkaOutputProducer import KafkaOutputProducer
from VideoScraper import get_video_data


parser = argparse.ArgumentParser(description='YouTube channel scraper')
parser.add_argument('-i', '--input_file', help='path the JSON file containing a list of YouTube URLs', required=True)
parser.add_argument('-o', '--output_dir', help='output directory to keep track of the processed youtubers', required=True)
parser.add_argument('-c', '--yt_channel_topic', help="Kafka topic for channels data", required=True)
parser.add_argument('-v', '--yt_videos_topic', help="Kafka topic for videos data", required=True)
parser.add_argument('-b', '--kafka_server', help="Kafka server", required=True)
parser.add_argument('-k', '--api_key', help="YouTube API Key", required=True)

args = parser.parse_args()


def initiate_browser():
    print("Initiating browser.")
    global browser
    # Chrome configurations
    options = ChromeOptions()
    options.add_argument("--incognito")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument("--lang=en")
    options.add_experimental_option('useAutomationExtension', False)
    options.headless = True
    # open browser
    browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def time_converter(time_str : str)-> int:
    # equivalent in minutes 
    YEAR = 525600
    MONTH = 43800
    DAY = 1440
    HOUR = 60

    st = time_str.split(" ")
    num = int(st[0])
    duration = st[1]
    if duration in ["year", "years"]:
        return num*YEAR
    elif duration in ['month', 'months']:
        return num*MONTH
    elif duration in ['day', 'days']:
        return num*DAY
    elif duration in ["hour", 'hours']:
        return num*HOUR
    else:
        return num

def normalize_yt_url(url):
    if url[-1] == '/':
        url = url[:url.rfind('/')]
    normalized_url = url_normalize(url+'/videos')
    return normalized_url

def scroll_down():
    # 2 years = 1051200 minutes
    published_time_limit = 1051200
    SCROLL_PAUSE_TIME = 2
    # get initial Height   
    last_height = browser.execute_script("return document.documentElement.scrollHeight")
    while True:
        try:
            # if videos older than 2 years we stops the scrolling
            videos_list = browser.find_elements(by=By.CLASS_NAME, value='style-scope ytd-grid-video-renderer')
            last_video = videos_list[-1]
            uploaded_time = last_video.find_element(by=By.XPATH, value='.//*[@id="metadata-line"]/span[2]').text
            if time_converter(uploaded_time) > published_time_limit:
                break
            # Scroll down to bottom
            browser.execute_script("window.scrollTo(0,document.documentElement.scrollHeight);")
            # Wait to load page
            time.sleep(SCROLL_PAUSE_TIME)
            # Calculate new scroll height and compare with last scroll height
            new_height = browser.execute_script("return document.documentElement.scrollHeight")
            if new_height == last_height :
                break
            last_height = new_height
        except:
               print('This channel has no videos.')
               break
    
'''this takes a YT channel url and returns a list of videos urls'''
def extract_channel_data(channel_link: str) -> dict:
    channel_info = {}
    videos_links = []
    try:
        # Normalize youtube url
        normalized_url = normalize_yt_url(channel_link)
        browser.get(normalized_url)
        # set wait to 5 sec to load page elements
        browser.implicitly_wait(5)
        # get YouTube channel data
        channel_info['channel_name'] = browser.find_element(by=By.XPATH, value='//*[@id="text-container"]/yt-formatted-string').text
        channel_info['channel_link'] = channel_link
        channel_info['subscriber_count'] = browser.find_element(by=By.XPATH, value='//*[@id="subscriber-count"]').text
        # scroll down to load more videos
        scroll_down()
        # getting list of videos (they are in a grid)
        videos = browser.find_elements(by=By.CLASS_NAME, value='style-scope ytd-grid-video-renderer')
        # collecting videos links
        for video in videos:
            videos_links.append(video.find_element(by=By.XPATH, value='.//*[@id="video-title"]').get_attribute('href'))
    except:
        print('Selenium ERROR : invalid YouTube channel URL or no longer exists!')
        channel_info['channel_name'] = "NaN"
        channel_info['channel_link'] = channel_link
        channel_info['subscriber_count'] = "NaN"

    return {
        "channel_info": channel_info,
        "videos_links" : videos_links
    }

# save data on JSON format
def save_data(data: dict, file_path: str):
    with open(file_path + '.json', "w") as f:
        json.dump(data, f, indent=4)

    print(f'Data saved in {file_path}.json successfully!')
  

if __name__ == '__main__':
    start_time = datetime.now()
    # Instance of Kafka producer
    global kafka 
    kafka = KafkaOutputProducer(args.kafka_server)
    # Initializing chrome browser
    initiate_browser()
    # output dir path for channels data
    youtubers_dir = args.output_dir     
    channel_count = 0
    # read influencers file
    with open(args.input_file, 'r') as f:
        data = json.load(f)        
        for item in data:            
            channel_link = item['youtube_url']            
            if channel_link is not None:      
                file_name = (item['username']).lower()
                # get all video links + channel info
                file_path = os.path.join(youtubers_dir, file_name)
                # avoid re-scrape the already scraped channels
                if not os.path.exists(file_path+'.json'):  
                    channel_count += 1                  
                    print(f'Processing channel number {channel_count} : {file_name}')
                    channel_data = extract_channel_data(channel_link)
                    # send channel data to kafka topic
                    kafka.message_writer(args.yt_channel_topic, channel_data)          

                    #VIDEO DATA EXTRACTION
                    videos_links = channel_data['videos_links']
                    # max urls per request is 50, so we pop 50 urls from the list each time.
                    while videos_links is not []:
                        A = videos_links[:50]
                        videos_links = videos_links[50:]
                        get_video_data(A, args.api_key, kafka, args.yt_videos_topic)
                        if A == []:
                            break
                    # save channel data in JSON file
                    save_data(channel_data, file_path)                                        
                
    # close the browser
    browser.quit()
    # calculate execution time
    print(f'{channel_count} channels has been crawled successfully !')
    end_time = datetime.now()
    print("Start time:", start_time)
    print("End time:",end_time)
    print('Duration: {}'.format(end_time - start_time))