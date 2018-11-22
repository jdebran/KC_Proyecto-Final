import scrapy
import json
import os
from scrapy.crawler import CrawlerProcess
from google.cloud import storage

filep = open('/tmp/data.txt', 'w')
    
class GuruFocusSpider(scrapy.Spider):
  name = "gurufocus"
  start_urls = [
      'https://www.gurufocus.com/stock_list.php?r=USA,$OTCPK,$GREY,$NAS,$NYSE,$ARCA,$OTCBB,$AMEX,$BATS&n=100'
      ]
  
  def parse(self, response):
    companies = response.xpath('//table[@id="R1"]//tr')
    print("code,symbol,company,price\n", file=filep)
    for company in companies[1:]:
      company_save = {
          "code" : company.xpath('td[1]/a/@href').extract_first().replace("/stock/", ""),
          "symbol" : company.xpath('td[1]//text()').extract_first(),
          "company" : company.xpath('td[2]//text()').extract_first(),
          "price" : company.xpath('td[3]//text()').extract_first()
      }
      # Print a un fichero
      print(f"{company_save['code']},{company_save['symbol']},{company_save['company']},{company_save['price']}\n", file=filep)
    
    
    for href in response.css('div.page_links a'):
      href_img = href.xpath('img').extract_first()
      if href_img is not None:
        img_alt = href.css('img').xpath('./@alt').extract_first()
        if img_alt == 'Next Page':
          next_page = href.xpath('@href').extract_first()
          yield response.follow(next_page, callback=self.parse)
          break

def main(request): 
    client = storage.Client()
    bucket_name = os.environ.get('BUCKET_NAME')
    bucket = client.get_bucket(bucket_name)
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })
    process.crawl(GuruFocusSpider)
    process.start()
    filep.close()
    
    blob2 = bucket.blob('data/companies.csv')
    blob2.upload_from_filename(filename='/tmp/data.txt')
