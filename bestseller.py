import concurrent
import json
import time
import traceback

import requests
from lxml import html
from lxml.etree import ParserError
import re
import string
import random


class BestSeller:
    country_name = {'us': 'www.amazon.com', 'uk': 'www.amazon.co.uk'}
    headers = {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
    }
    def create_async_urls(self,url_list,base_url,node_list):
        jsonResults = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_url = {executor.submit(self.parse_best_details, url_list[i],node_list[i],base_url): i for i in range(len(url_list))}
            for future in concurrent.futures.as_completed(future_to_url):
                i = future_to_url[future]
                try:
                    data = future.result()
                    key = node_list[i]
                    jsonResults[key] = data
                except Exception as exc:
                    print('%r generated an exception: %s' % (url_list[i], exc))
                else:
                    print('%r page is %d bytes' % (url_list[i], len(data)))
                #print(json.dumps(jsonResults))
        return json.dumps(jsonResults)

    def parse_best_details(self, url,node,base_url_):
        for retry in range(1):
            try:
                print(retry," time. Downloading and processing page :", url)
                time.sleep(1)
                response = requests.get(url, headers=self.headers, timeout = 5)
                if response.status_code != 200:
                    raise ValueError("Captcha found. Retrying")
                response_text = response.text
                parser = html.fromstring(response_text)
                base_url = base_url_
                parser.make_links_absolute(base_url)
                index = 1
                rank_list = []
                XPATH_BEST_SELLERS_LIST = "//ol[@id ='zg-ordered-list']//li[contains(@class, 'zg-item-immersion')]//span[contains(@class,'aok-inline-block zg-item')]/a/@href"
                best_seller_listing = parser.xpath(XPATH_BEST_SELLERS_LIST)
                best_seller_list = []
                for best_seller in best_seller_listing:
                    if(index>15):
                        break;
                    best_seller_list.append(best_seller.strip())
                    rank_list.append(index)
                    index = index+1;
                return self.async_calls(best_seller_list,rank_list,base_url_,node,random.getrandbits(64))
            except ParserError:
                print("Empty page found")
                break
            except:
                print(traceback.format_exc())
                print("retrying :", url)

    def async_calls(self,url_list,rank_list,base_url,queryId,uniqueCrawledId):
        product_list = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_url = {executor.submit(self.parse_individual_product, url_list[i],rank_list[i], base_url,queryId,uniqueCrawledId): i for i in
                             range(len(url_list))}
            for future in concurrent.futures.as_completed(future_to_url):
                i = future_to_url[future]
                try:
                    data = future.result()
                    index = rank_list[i]
                    product_list.insert(index,data)
                except Exception as exc:
                    print('%r generated an exception: %s' % (url_list[i], exc))
                else:
                    print('%r page is %d bytes' % (url_list[i], len(data)))
        return product_list
    def parse_individual_product(self,url, rank, base_url_,queryId,uniqueCrawledId):
        for retry in range(1):
            try:
                print(retry, " time. Downloading and processing page :", url)
                time.sleep(1)
                response = requests.get(url, headers=self.headers, timeout=30)
                if response.status_code != 200:
                    raise ValueError("Captcha found. Retrying")
                response_text = response.text
                parser = html.fromstring(response_text)
                base_url = base_url_
                parser.make_links_absolute(base_url)
                XPATH_NAME = '//h1[@id="title"]//text()'
                XPATH_SALE_PRICE = '//span[contains(@id,"ourprice") or contains(@id,"saleprice")]/text()'
                XPATH_ORIGINAL_PRICE = '//td[contains(text(),"List Price") or contains(text(),"M.R.P") or contains(text(),"Price")]/following-sibling::td/text()'
                XPATH_CATEGORY = '//div[@id="wayfinding-breadcrumbs_container"]//li[last()]//a/text()'
                XPATH_DESC = '//div[@id="productDescription"]/p/text()'
                XPATH_SPEC ='//div[@id="feature-bullets"]//li//span[contains(@class,"a-list-item")]/text()'
                XPATH_SCRIPT ='//div[@id="turboState"]//script[@type="a-state"]/text()'
                XPATH_SELLER ='//div[@id="merchant-info"]//a/text()'
                XPATH_NUM_PICS ='//div[@id="altImages"]/ul[1]//li'
                XPATH_SELLER_ADD_TO_CART = "//form[@id='addToCart']/input[@name='merchantID']/@value"
                XPATH_RATINGS ="//span[@id='acrPopover']//span[contains(@class, 'a-icon-alt')]//text()"
                XPATH_CHOICE ="//div[@id='acBadge_feature_div']//span[contains(@class,'a-size-small aok-float-left ac-badge-rectangle')]//span//text()"
                seller_id = parser.xpath(XPATH_SELLER_ADD_TO_CART)
                AMAZON_CHOICE =parser.xpath(XPATH_CHOICE)
                AMAZON_CHOICE = ''.join(AMAZON_CHOICE).strip() if(AMAZON_CHOICE) else None
                ratings =parser.xpath(XPATH_RATINGS)
                RAW_NAME = parser.xpath(XPATH_NAME)
                NUM_PICS = parser.xpath(XPATH_NUM_PICS)
                SELLER_NAME = parser.xpath(XPATH_SELLER)
                RAW_SALE_PRICE = parser.xpath(XPATH_SALE_PRICE)
                RAW_CATEGORY = parser.xpath(XPATH_CATEGORY)
                RAW_ORIGINAL_PRICE = parser.xpath(XPATH_ORIGINAL_PRICE)
                PRODUCT_SPEC = parser.xpath(XPATH_SPEC)
                PRODUCT_DESC = parser.xpath(XPATH_DESC)
                SCRIPT_DATA = parser.xpath(XPATH_SCRIPT)
                if(not SELLER_NAME):
                    SELLER_NAME = parser.xpath("//a[@id='bylineInfo']/text()")
                SELLER_NAME = SELLER_NAME[0].strip() if (SELLER_NAME) else None
                p_ratings = ratings[0].split()[0].strip() if ratings else None
                script_json= json.loads(SCRIPT_DATA[0]) if(SCRIPT_DATA) else None
                NAME = ' '.join(''.join(RAW_NAME).split()) if RAW_NAME else None
                SALE_PRICE = ' '.join(''.join(RAW_SALE_PRICE).split()).strip() if RAW_SALE_PRICE else None
                upper_price = None
                if(SALE_PRICE and '-' in SALE_PRICE):
                    data=SALE_PRICE.split('-')
                    upper_price = data[1]
                    SALE_PRICE = data[0]
                CATEGORY = ' > '.join([i.strip() for i in RAW_CATEGORY]) if RAW_CATEGORY else None
                ORIGINAL_PRICE = ''.join(RAW_ORIGINAL_PRICE).strip() if RAW_ORIGINAL_PRICE else None
                PRODUCT_SP = " ".join(str(x).strip() for x in PRODUCT_SPEC) if(PRODUCT_SPEC) else ''
                PRODUCT_DE = " ".join(str(x).strip() for x in PRODUCT_DESC) if(PRODUCT_DESC) else ''
                PIC_NUM = len(NUM_PICS) if(NUM_PICS) else None
                PRODUCT_CONTENTS = PRODUCT_SP +" "+PRODUCT_DE
                script_eligibility = script_json.get('eligibility')if(script_json  and script_json.get('eligibility')) else None
                if not ORIGINAL_PRICE:
                    ORIGINAL_PRICE = SALE_PRICE

                data = {
                    'productTitle': NAME,
                    'salePrice': float(re.sub('[!@#$£,]','',SALE_PRICE)) if SALE_PRICE else None,
                    'productDescription': PRODUCT_CONTENTS.translate(str.maketrans('', '', string.punctuation)),
                    'category': CATEGORY if(CATEGORY) else None,
                    'originalPrice': float(re.sub('[!@#$£,]','',ORIGINAL_PRICE)) if ORIGINAL_PRICE else None,
                    'availability': int(script_eligibility.get('stockOnHand')) if(script_eligibility and script_eligibility.get('stockOnHand')) else None,
                    'url': url,
                    'prime': (1 if(script_eligibility.get('prime') is True) else 0) if(script_eligibility and script_eligibility.get('prime')) else 0,
                    'primeShipping': (1 if(script_eligibility.get('primeShipping') is True) else 0) if(script_eligibility and script_eligibility.get('primeShipping')) else 0,
                    'isAmazonFullfilled': (1 if(script_eligibility.get('isEligible') is True) else 0) if(script_eligibility and script_eligibility.get('isEligible')) else 0,
                    'queryId':queryId,
                    'country': "GBP" if('uk' in base_url_) else "USD",
                    'sellerId': seller_id[0].strip() if(seller_id) else None,
                    'sellerName': SELLER_NAME,
                    'rank': rank,
                    'productReviews': float(p_ratings) if(p_ratings) else None,
                    'pictureAvailable':PIC_NUM-3 if(PIC_NUM) else None,
                    'upperBoundPrice': float(re.sub('[!@#$£,]','',upper_price)) if(upper_price) else None,
                    'asin':(url.split('/ref')[0]).split('dp/')[1].strip() if(url) else None,
                    'queryId': queryId,
                    'isAmazonChoice': 1 if(AMAZON_CHOICE and "Amazon's Choice" in AMAZON_CHOICE) else 0,
                    'uniqueCrawledId':uniqueCrawledId

                }
                return data
            except ParserError:
                print("Empty page found")
            except:
                print(traceback.format_exc())
                print("Retrying :", url)
