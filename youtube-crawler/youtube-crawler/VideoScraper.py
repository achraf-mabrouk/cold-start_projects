from urllib.error import HTTPError
import googleapiclient.discovery
import googleapiclient.errors
import json,os,re
from pytube import extract

# ------------------------------------------------------------------------------------------
#  GENERAL HELPER FUNCTIONS
# ------------------------------------------------------------------------------------------

def save_data(data, file_name):
    path = os.path.join('videos/', file_name)
    with open(path + ".json", "w") as f:
        json.dump(data, f, indent=4)

def is_product_link(url):
    excluded_domains =['youtu.be', "youtube",'twitter', 'facebook', 'instagram',
                        'tiktok', 'spoti.fi', 'goo.gl', 'github', 'medium', "skl.sh"]
    
    for domain in excluded_domains:
        # if the name of the site is exist in the url
        if url.find(domain) != -1:
            return False
    return True

# returns a string of videos ids separated by ,
def get_ids_string(links):
    return ",".join([extract.video_id(x) for x in links])

# ------------------------------------------------------------------------------------------
# YouTube Data API - extracting videos data
# ------------------------------------------------------------------------------------------
video_count = 1
def get_video_data(ids_list, channel_link, api_key, kafka, topic):

    API_SERVICE_NAME = "youtube"
    API_VERSION = "v3"    
    API_KEY = api_key

    ids_string = get_ids_string(ids_list)
        
    global video_count 
    try:
        # create an API client
        youtube = googleapiclient.discovery.build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)
        request = youtube.videos().list(
            part="statistics, snippet",
            id=ids_string
        )
        response = request.execute()
    
        for item in response['items']:
            snippet = item['snippet']
            statistics = item['statistics']
            # Extracting the needed data
            channel_id = snippet['channelId']
            channel_name = snippet['channelTitle']
            title = snippet['title']
            description = snippet['description']
            publishedAt = snippet['publishedAt']
            try:
                likeCount = statistics['likeCount']
            except:
                likeCount = 'Not available'
            try:
                viewCount = statistics['viewCount']
            except:
                viewCount = "Not available"  
            try:              
                commentCount = statistics['commentCount']
            except:
                commentCount = 'Not available'
            
            links_in_description = re.findall("(?P<url>https?://[^\s]+)", description)
            filtred_list = [ x for x in links_in_description if is_product_link(x)]

            video_data = {                    
                    'video_id' : item['id'],
                    'title' : title,
                    'channelId' : channel_id,
                    'channelTitle' : channel_name,
                    'channelURL' : channel_link,
                    'description' : description,
                    'publishedAt' : publishedAt,
                    'likeCount' : likeCount,
                    'viewCount' : viewCount,
                    'commentCount' : commentCount,
                    'links_in_description' : filtred_list
                }
            
            kafka.message_writer(topic, video_data)
            print(f"video number {video_count} : {title}")
            save_data(video_data, f"video {video_count}")
            video_count += 1
            
    except HTTPError as e:
        print('Error response status code : {0}, reason : {1}'.format(e.status_code, e.error_details))
        