FROM python:3.8

ENV PYTHONUNBUFFERED 1
#Prepare project folder
RUN mkdir /youtube-crawler
#ADD copies folder contents, not the folder itself
ADD youtube-crawler /youtube-crawler/

#Install python packages from requirements
COPY requirements.txt /youtube-crawler/
RUN pip install --upgrade pip
RUN pip install -r /youtube-crawler/requirements.txt

# Install google chrome
RUN apt-get update && \
    apt-get install -y gnupg wget curl unzip --no-install-recommends && \
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list && \
    apt-get update -y && \
    apt-get install -y google-chrome-stable
# chromdriver will be installed automatically when running the scraper thanks to webdriver_manager module

#Make a volume for input
VOLUME ["/youtube-crawler/input"]
ENV INPUT_DIR /youtube-crawler/input
#Make a volume for output data
VOLUME ["/youtube-crawler/youtubers"]
ENV OUTPUT_DIR /youtube-crawler/youtubers
#Make a volume for logs
VOLUME ["/youtube-crawler/logs"]
ENV LOGS_DIR /youtube-crawler/logs

#Copy keys and configurations
COPY main.sh /youtube-crawler/main.sh
RUN chmod +x /youtube-crawler/main.sh


ENTRYPOINT [ "/youtube-crawler/main.sh" ]