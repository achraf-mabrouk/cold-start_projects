docker run -ti -v /root/youtube-crawler/input:/youtube-crawler/input \
            -v /root/youtube-crawler/youtubers:/youtube-crawler/youtubers \
            -v /root/youtube-crawler/logs:/youtube-crawler/logs \
            -e INPUT_FILE=input.json \
            -e OUTPUT_DIR=/youtube-crawler/youtubers \
            -e KAFKA_SERVER=176.9.30.110:9092 \
            -e YT_CHANNELS_TOPIC=coldstart-yt-channels \
            -e YT_VIDEOS_TOPIC=coldstart-yt-videos \
            -e API_KEY=AIzaSyCwKGvKOJ3BgPmr8pasFu4P5Q3jJMcICDg \
            docker.joinhippo.com/coldstart-youtube-crawler:latest /bin/bash