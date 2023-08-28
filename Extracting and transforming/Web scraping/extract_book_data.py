from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import requests
import pprint
import json
import pandas as pd

class tiki_web_scraping():
    def __init__(self):
        # headers
        self.payload = ""
        self.headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
                            'Accept': 'application/json, text/plain, */*',
                            'Accept-Language': 'vi-VN,vi;q=0.8,en-US;q=0.5,en;q=0.3',
                            'Referer': 'https://tiki.vn/nha-sach-tiki/c8322',
                            'x-guest-token': 'AxGdXDv2EEga1iKNZc9iJEoRmALIBJc2MRs8F65tJ7B68m%2FhUhkt7Dx%2Bh2NZwhG3uzxqD2V8Yyo%3D',
                            'Connection': 'keep-alive',
                            'TE': 'Trailers',
                        }
        # endpoint API
        self.url = "https://tiki.vn/api/personalish/v1/blocks/listings" 
        #Chrome browser
        browser_service = Service(ChromeDriverManager().install())
        self.browser = webdriver.Chrome(service=browser_service)
        # gen coookies
        
        home_url = "https://tiki.vn/"
        self.browser.get(home_url)
        cookies = self.browser.get_cookies()
        self.cookies_dict = {cookie["name"]: cookie["value"] for cookie in cookies}
        #quite browser
        self.browser.quit()
    def scrape_tiki(self, num_pages):
        extracted_data = []
        for page_num in range(1, num_pages + 1):
            querystring = {"code":"10",
                    "limit":"40",
                    "include":"advertisement",
                    "aggregations":"2",
                    "version":"home-persionalized",
                    "trackity_id":"329dea95-4edc-1a13-101f-9047fa4915d6",
                    "category":"8322",
                    "page":str(page_num),
                    "urlKey":"nha-sach-tiki"}
            response = requests.request("GET", self.url, headers= self.headers, params= querystring, cookies = self.cookies_dict ).text
            data = json.loads(response)
            #pprint.pprint(data)

            for product in data['data']:
                id = product['id']
                SKU = product['sku']
                try:
                    brand_name = product['brand_name']
                except KeyError:
                    brand_name = 'NA'
                name = product['name']
                rating_average = product['rating_average']
                price = product['price']
                # append to the list
                extracted_data.append({
                    'id': id,
                    'SKU': SKU,
                    'brand_name': brand_name,
                    'name': name,
                    'rating_average': rating_average,
                    'price': price
                })
            # create a df from list 
            df = pd.DataFrame(extracted_data)
            # save df to a CSV fife
            df.to_csv('extracted_tiki_data.csv')

scraper = tiki_web_scraping()
data = scraper.scrape_tiki(num_pages=5)
