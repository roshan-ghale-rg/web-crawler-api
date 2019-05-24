#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import concurrent.futures
import json
import re
import time
import traceback

import requests
from flask import Flask, abort, request, jsonify, make_response
from lxml import html
from lxml.etree import ParserError
import urllib.parse as urlparse

import bestseller as bs
import algorithms as algo

# Belongs to property of Mr.. Roshan Ghale.

app: Flask = Flask(__name__)

country_name = {'us': 'https://www.amazon.com', 'uk': 'https://www.amazon.co.uk'}


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.route('/')
def index():
    # print ("This username"+request.json['username'])
    return "Welcome user. Crawler is running."


@app.route('/testAlgorithm', methods=['GET'])
def test():
    if not request.args.get('text'):
        return json.dumps({"error": "text parameter is missing."})
    query_str = request.args.get('text')
    data_id = request.args.get('amazonDataId') if (request.args.get('amazonDataId')) else 111111
    crawledId = request.args.get('uniqueCrawledId') if (request.args.get('uniqueCrawledId')) else 000000
    type = 'D' if (request.args.get('type') and request.args.get('type')=='D' )  else 'T'
    return json.dumps(algo.Algorithms().process_algorithm(query_str, data_id, crawledId, 0,type))


@app.route('/keywords', methods=['POST'])
def get_keyword():
    if not request.json['title'] and not request.json['description'] and not request.json['uniqueCrawledId']:
        return abort(400, errors="You need to send paramter text with query Id or requestId")
    query_str = request.json['title']
    data_id = request.json['description']
    crawledId = request.json['uniqueCrawledId']
    return json.dumps(algo.Algorithms().initialize(query_str, data_id, crawledId))


@app.route('/bestsellers', methods=['POST'])
def start():
    if not request.json['url'] and not request.json['country']:
        abort(400, errors="Data should be in json format. It should contains country, url")
    country = request.json['country']
    url = request.json['url']
    base_url = country_name.get(country)
    url_list = []
    node_list = []
    if (url):
        my_url = url.split(",")
        for single_url in my_url:
            flag = False
            if ('/ref' in single_url and 'diy/' in single_url):
                node_list.append((single_url.split('/ref')[0]).split('diy/')[1])
                flag = True
            elif ('diy/' in single_url):
                node_list.append(single_url.split('diy/')[1])
                flag = True
            elif ('/ref' in single_url):
                node_list.append(single_url.split('/ref')[0].rsplit('/', 1)[1])
                flag = True
            if (flag):
                url_list.append(single_url)
    return bs.BestSeller().create_async_urls(url_list, base_url, node_list)


@app.route('/crawler', methods=['POST'])
def parse():
    if not request.json['asin'] and not request.json['condition'] and not request.json['shipping']:
        abort(400, errors="Data should be in json format. It should contains asin, condition, shipping")
    asin = request.json['asin']
    print("Asinn...." + asin)
    countries = {'us': 'www.amazon.com', 'uk': 'www.amazon.co.uk'}
    condition = request.json['condition']
    country = request.json['country'] if (request.json['country']) else 'us'
    shipping = request.json['shipping']  # for creating url according to the filter applied
    condition_dict = {'new': 'dp_olp_new_mbc?ie=UTF8&condition=new',
                      'used': '&f_used=true',
                      'all': '&condition=all',
                      'like_new': '&f_usedLikeNew=true',
                      'good': '&f_usedGood=true',
                      'verygood': '&f_usedVeryGood=true',
                      'acceptable': 'f_usedAcceptable=true'
                      }
    shipping_dict = {'prime': '&f_primeEligible=true', 'free': '&f_freeShipping=true', 'all': ''}
    my_asin = asin.split(",")
    url_list = []
    base_url = countries.get(country)
    if my_asin:
        for single_asin in my_asin:
            url_list.append(
                'https://' + base_url + '/gp/offer-listing/' + single_asin.strip() + '/ref=' + condition_dict.get(
                    condition) + shipping_dict.get(shipping))
        return runUrl(url_list, my_asin, base_url)


def runUrl(url_list, asin_list, base_url):
    jsonResults = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        future_to_url = {executor.submit(parse_offer_details, url_list[i], asin_list[i], base_url): i for i in
                         range(len(url_list))}
        for future in concurrent.futures.as_completed(future_to_url):
            i = future_to_url[future]
            try:
                data = future.result()
                key = asin_list[i];
                jsonResults[key] = data
            except Exception as exc:
                print('%r generated an exception: %s' % (asin_list[i], exc))
            else:
                print('%r page is %d bytes' % (asin_list[i], len(data)))
    return json.dumps(jsonResults)


# Retrieve a single page and report the URL and contents
def getBuyBoxSeller(url, base_url_):
    headers = {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
    }
    for retry in range(1):
        try:
            print("::::: Getting the buy box winner seller ID :", url)
            response = requests.get(url, headers=headers, timeout=5)
            if response.status_code != 200:
                raise ValueError("Captcha found. Retrying")
            response_text = response.text
            time.sleep(1)
            parser = html.fromstring(response_text)
            base_url = base_url_
            parser.make_links_absolute(base_url)
            XPATH_SELLER_ADD_TO_CART = "//form[@id='addToCart']/input[@name='merchantID']/@value"
            seller_winner_buy_box = parser.xpath(XPATH_SELLER_ADD_TO_CART)
            additional_info = []
            url_list = []
            XPATH_CATEGORY = '//div[@id="wayfinding-breadcrumbs_container"]//li[last()]//a/text()'
            XPATH_CATEGORY_HREF = '//div[@id="wayfinding-breadcrumbs_container"]//li[last()]//a/@href'
            XPATH_PRODUCT_DETAILS = "//tr[@id ='SalesRank']//li[contains(@class, 'zg_hrsr_item')]//text()"
            XPATH_PRODUCT_HREF = "//tr[@id ='SalesRank']//li[contains(@class, 'zg_hrsr_item')]//@href"
            ranking = parser.xpath(XPATH_PRODUCT_DETAILS)
            category = parser.xpath(XPATH_CATEGORY)
            cat_href_node = parser.xpath(XPATH_CATEGORY_HREF)
            if (category):
                category = category[0].strip()
            if (cat_href_node):
                cat_href_url = "https://" + base_url_ + cat_href_node[0]
                parsed = urlparse.urlparse(cat_href_url)
                cat_href_node = urlparse.parse_qs(parsed.query)['node']
            if not ranking:
                ranking = parser.xpath("//li[@id ='SalesRank']//li[contains(@class, 'zg_hrsr_item')]//text()")
            if not ranking:
                ranking = parser.xpath("//ul[contains(@class,'zg_hrsr')]//li[contains(@class, 'zg_hrsr_item')]//text()")
            href = parser.xpath(XPATH_PRODUCT_HREF)
            if not href:
                href = parser.xpath("//li[@id ='SalesRank']//li[contains(@class, 'zg_hrsr_item')]//@href")
            if not href:
                href = parser.xpath("//ul[contains(@class,'zg_hrsr')]//li[contains(@class, 'zg_hrsr_item')]//@href")
            ranking = list(map(str.strip, ranking))
            ranking = [item for item in ranking if (item and item != 'in')]
            flag = False
            for j in range(0, len(ranking), 2):
                info_seller = {'rank': int(re.sub('[!@#$£in,]', '', ranking[j]).strip()),
                               'category': ranking[j + 1].strip(),
                               'bestSellerUrl': href[j // 2].strip(),
                               'country': 'UK' if "uk" in base_url else 'US',
                               'pageFlag': 1 if (category == ranking[j + 1].strip()) else 0
                               }
                if (info_seller.get('pageFlag') == 1):
                    flag = True
                url_list.append(href[j // 2].strip())
                additional_info.append(info_seller)
            if (flag is False and category and cat_href_node):
                category_info = {'rank': None,
                                 'category': category,
                                 'bestSellerUrl': "https://" + base_url_ + "/gp/bestsellers/diy/" + cat_href_node[
                                     0] if (cat_href_node) else None,
                                 'country': 'UK' if "uk" in base_url else 'US',
                                 'pageFlag': 1
                                 }
                additional_info.append(category_info)
            info = {
                'winner_buybox': seller_winner_buy_box[0] if (seller_winner_buy_box) else None,
                'additional_info': additional_info if (additional_info) else None,
                'best_seller_urls': url_list if (url_list) else None
            }
            return info
        except ParserError:
            print("Empty page found")
        except:
            print(traceback.format_exc())
            print("Retrying :", url)


def parse_offer_details(url, asin, base_url_):
    headers = {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
    }
    offer_list = []
    for retry in range(1):
        try:
            print(retry, " time. Downloading and processing page :", url)
            time.sleep(1)
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code != 200:
                raise ValueError("Captcha found. Retrying")
            response_text = response.text
            parser = html.fromstring(response_text)
            base_url = base_url_
            parser.make_links_absolute(base_url)
            XPATH_PRODUCT_DETAILS = "//a[@id='olpDetailPageLink']/@href"
            detail_url = parser.xpath(XPATH_PRODUCT_DETAILS)
            buybox_winner = getBuyBoxSeller(detail_url[0], base_url)
            if (buybox_winner and buybox_winner.get('additional_info')):
                for info in buybox_winner.get('additional_info'):
                    q_id = None
                    # TODO handled needed
                    print("--Best seller Url--> " + info.get('bestSellerUrl'))
                    if info.get('bestSellerUrl') and 'diy/' in info.get('bestSellerUrl'):
                        q_id = int((info.get('bestSellerUrl').split('/ref')[0]).split('diy/')[1])
                    else:
                        que_id = (info.get('bestSellerUrl').split('/ref')[0]).rsplit('/', 1)
                        q_id = int(que_id[1])
                    offer_details = {
                        'sellerId': buybox_winner.get('winner_buybox') if (
                            buybox_winner.get('winner_buybox')) else None,
                        'asin': asin,
                        'country': info.get('country') if (info.get('country')) else None,
                        'rank': info.get('rank') if info.get('rank') else None,
                        'category': info.get('category') if info.get('category') else None,
                        'bestSellerUrl': info.get('bestSellerUrl') if (info.get('bestSellerUrl')) else None,
                        'pageFlag': info.get('pageFlag') if (info.get('pageFlag')) else None,
                        'queryId': q_id
                    }
                    # rank = rank+1
                    if (q_id):
                        offer_list.append(offer_details)
        except ParserError:
            print("Empty page found")
            break
        except:
            print(traceback.format_exc())
            print("retrying :", url)
    return offer_list


def check_for_total_key(data):
    if ("total" in data):
        return data.split("total")[0]
    else:
        return None


if __name__ == '__main__':
    app.run(debug=True)
