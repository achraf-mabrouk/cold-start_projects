import scrapy
import json
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
import argparse
parser = argparse.ArgumentParser(description="YouTube videos data")
parser.add_argument("-i", "--input_path", help="YouTube data input file",required=True)
parser.add_argument("-o", "--output_path", help="output data directory", required=True)
args = parser.parse_args()

def dump_jsonl(json_record, output_path, append=False):
    """
    Write list of objects to a JSON lines file.
    """
    mode = 'a+' if append else 'w'
    with open(output_path, mode, encoding='utf-8') as f:
        json_record = json.dumps(json_record, ensure_ascii=False)
        f.write(json_record + '\n')


def load_jsonl(input_path) -> list:
    """
    Read list of objects from a JSON lines file.
    """
    data = []
    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line.rstrip('\n|\r')))
    return data

class ResolverSpider(scrapy.Spider):
    name = 'resolver'
    record = None
    resolved_urls = []

    def __init__(self, record):
        self.record = record  
        self.urls = iter(self.record['links_in_description'])
        self.resolved_urls = []
                
    def start_requests(self):
        yield scrapy.Request(next(self.urls), 
                                callback=self.parse,
                                # meta={'video' : self.record}
                            )
        
    def parse(self, response):
        # resolving 
        canonical_url = response.xpath('//link[@rel="canonical"]/@href').get()
        if canonical_url is None or "{{SEO.url}}":
            canonical_url = response.url
        
        self.resolved_urls.append(canonical_url)
        
        try:
            url = next(self.urls)
            yield response.follow(url, callback=self.parse) 
        except StopIteration:
            self.record['resolved_urls'] = self.resolved_urls
            dump_jsonl(self.record, args.output_path, append=True)


input_path = args.input_path
output_path = args.output_path
configure_logging()
settings = get_project_settings()
runner = CrawlerRunner(settings)

@defer.inlineCallbacks
def crawl():
    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            record = json.loads(line.rstrip('\n|\r'))
            yield runner.crawl(ResolverSpider, record)
        reactor.stop()

crawl()
reactor.run() # the script will block here until the last crawl call is finished