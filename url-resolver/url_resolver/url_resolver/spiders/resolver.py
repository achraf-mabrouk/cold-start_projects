import scrapy
import json
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import argparse

parser = argparse.ArgumentParser(description="YouTube videos data")
parser.add_argument("-i", "--input_path", help="YouTube data input file", required=True)
parser.add_argument("-o", "--output_path", help="output data directory", required=True)
args = parser.parse_args()
# input_path = "/home/frantic95/cold-start/url-resolver/url_resolver/input/chunk1.jsonl"

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
    data = load_jsonl(args.input_path)

    def start_requests(self):
        for record in self.data:
            # add 'resolved_urls' key:value pair to the record                      
            record['resolved_urls'] = []
            try:
                urls = iter(record['links_in_description'])
                yield scrapy.Request(next(urls), 
                                    callback=self.parse, 
                                    meta={'record' : record,
                                        'urls' : urls                                     
                                        },
                                    errback=self.errback_handler
                                    )
            # if links_in_description is null                                         
            except StopIteration:
                record['resolved_urls'] = None
                dump_jsonl(record, args.output_path, append=True)
                # yield record
       
    def parse(self, response):
        record = response.meta['record']
        urls = response.meta['urls']
        # resolving 
        canonical_url = response.xpath('//link[@rel="canonical"]/@href').get()
        if canonical_url is None or "{{SEO.url}}":
            canonical_url = response.url
        
        record['resolved_urls'].append(canonical_url)
        try:
            url = next(urls)
            yield response.follow(url, 
                                callback=self.parse,  
                                meta={'record' : record,
                                'urls' : urls },
                                errback=self.errback_handler
                                ) 
        except StopIteration:            
            dump_jsonl(record, args.output_path, append=True)
            # yield record
        
    def errback_handler(self, response, failure):
        record = response.meta['record']
        urls = response.meta['urls']
        record['resolved_urls'].append(None)
        try:
            yield response.follow(next(urls), 
                                callback=self.parse,  
                                meta={'record' : record,
                                'urls' : urls },
                                errback=self.errback_handler
                                )
        except StopIteration:            
            dump_jsonl(record, args.output_path, append=True) 
            # yield record
    


settings = get_project_settings()
# # settings={
# #     "FEEDS": {
# #         "items.json": {"format": "json"},

# #     },
# # }
process = CrawlerProcess(settings)

process.crawl(ResolverSpider)

process.start() # the script will block here until the crawling is finished