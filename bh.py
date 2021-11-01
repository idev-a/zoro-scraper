from sgrequests import SgRequests
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sgscrape.sgrecord_deduper import SgRecordDeduper
from bs4 import BeautifulSoup as bs
import pandas as pd
import math
from concurrent.futures import ThreadPoolExecutor
import os
import shutil
import re
import logging
from logging.handlers import RotatingFileHandler
import boto3
from io import BytesIO
from util.util import Util
import time
import argparse

# BASE_PATH = os.path.abspath(os.curdir)
BASE_PATH = '/tmp'
BH_PATH = BASE_PATH + '/BH_SCRAPE.xlsx'
# OLD_LIST_PATH = BASE_PATH + '/old_list'
# BREAKER_LIST_PATH = BASE_PATH + '/BREAKER LIST 2021 with new columns.xlsx'

# aws
OLD_LIST_BUCKET = 'oldlist'
OTHER_LIST_BUCKET = 'otherlist'
ZORO_RESULTS_BUCKET = 'zoro-results'
MASTER_BUCKET = 'jo-masters'
ZORO_MASTER = BASE_PATH + '/zoro-master.xlsx'

AWS_ACCESS_KEY_ID = "AKIATLGDYHYDHKCHCF67"
AWS_SECURITY_KEY = "6DVWj1RdfXuTUxeQVamLhuuJCG6JDPW4CF+oUZFV"

s3 = boto3.client('s3',
                    aws_access_key_id= AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECURITY_KEY)


# Create a custom logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# remove log file
LOG_DIR = os.path.abspath(os.curdir) + '/logs/'
LOG_FILE = LOG_DIR + 'processing.log'

if os.path.exists(LOG_DIR):
    shutil.rmtree(LOG_DIR)

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Create handlers
c_handler = logging.StreamHandler()
f_handler = RotatingFileHandler(LOG_FILE, maxBytes=20000, backupCount=10)
c_handler.setLevel(logging.INFO)
f_handler.setLevel(logging.WARNING)

# Create formatters and add it to handlers
c_format = logging.Formatter('[%(levelname)s] %(message)s')
f_format = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)

# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)

base_url = "https://www.zoro.com"
prod_url = "https://www.zoro.com/product/?products={}"
jpeg_url = "https://www.zoro.com/static/cms/product/full/{}"

max_workers = 8

http = SgRequests(verify_ssl=False)

class Script:

    def initialize(self):
        # load old list file
        logger.info("load old list files inside /old_list dir")

        self.old_pds = []
        # for file in s3.list_objects(Bucket=OLD_LIST_BUCKET)['Contents']:
        #     logger.info(f"load old list {file['Key']}")
        #     obj = s3.get_object(Bucket=OLD_LIST_BUCKET, Key=file['Key'])
        #     self.old_pds.append(pd.read_excel(BytesIO(obj.get('Body').read()), sheet_name='RESULTS_FULL', usecols=['Manufacturer Part Number']) )

        # load breaker list
        logger.info("load breaker list")
        # breaker_list_obj = s3.get_object(Bucket=OTHER_LIST_BUCKET, Key='BREAKER LIST 2021 with new columns.xlsx')
        # self.breaker_pd = pd.read_excel(BytesIO(breaker_list_obj.get('Body').read()), usecols=[1])

    def _save_master_excel(self):
        '''
            save master excel file
        '''

        # master excel file containing all the data
        df_list = []

        for file in s3.list_objects(Bucket=ZORO_RESULTS_BUCKET)['Contents']:
            logger.info(f"[master] load old list {file['Key']}")
            obj = s3.get_object(Bucket=ZORO_RESULTS_BUCKET, Key=file['Key'])
            df_list.append(pd.read_excel(BytesIO(obj.get('Body').read()), sheet_name='Sheet'))

        if df_list:
            logger.info('creating master zoro file ...')
            df_concat = pd.concat(df_list, ignore_index=True)
            df_concat.to_excel(ZORO_MASTER)
        logger.info('uploading master zoro file ...')
        s3.upload_file(ZORO_MASTER, MASTER_BUCKET, ZORO_MASTER.split('/')[-1])
        
    def find_missing_or_incomplete_categories(self):
        retry_cats = []
        cats = []
        total_cats = list(range(294))
        for file in s3.list_objects(Bucket=ZORO_RESULTS_BUCKET)['Contents']:
            logger.info(f"load old list {file['Key']}")
            cat = int(os.path.splitext(file['Key'])[0].split('_')[-1])
            cats.append(cat)
            obj = s3.get_object(Bucket=ZORO_RESULTS_BUCKET, Key=file['Key'])
            dd = pd.read_excel(BytesIO(obj.get('Body').read()), sheet_name='Sheet', usecols=['category'])
            if dd.empty or dd.isnull().values.any():
                retry_cats.append(cat)

        retry_cats += list(set(total_cats) - set(cats))

        print(retry_cats)

    def _headers(self):
        return Util().get_zoro_headers()

    def fetchSingle(self, link):
        _url = link['href']
        if not _url.startswith('http'):
            _url = base_url + link['href']
        
        return _url, self.request_with_retries(_url)

    def fetchList(self, list, occurrence=max_workers):
        output = []
        total = len(list)
        reminder = math.floor(total / 50)
        if reminder < occurrence:
            reminder = occurrence

        count = 0
        with ThreadPoolExecutor(
            max_workers=occurrence, thread_name_prefix="fetcher"
        ) as executor:
            for result in executor.map(self.fetchSingle, list):
                if result:
                    count = count + 1
                    if count % reminder == 0:
                        logger.info(f"Concurrent Operation count = {count}")
                    output.append(result)
        return output


    def request_with_retries(self, url):
        res = http.get(url=url, headers=self._headers())
        if res.status_code != 200:
            logger.warning(res.__str__)
        return bs(res.text, 'lxml')


    def _check_old_list(self, mf_number):
        '''
            check if the mf_number is in the old list (1-12)
        '''
        return False
        for old_pd in self.old_pds:
            if len(old_pd[old_pd.eq(mf_number).any(1)]) > 1:
                return True

        return False


    def _check_breaker_list(self, mf_number):
        '''
            check if the mf_number is in the breaker list
        '''
        return False
        return len(self.breaker_pd[self.breaker_pd.eq(mf_number).any(1)]) > 1


    def _u(self, val):
        '''
            split value with its unit
        '''
        unit = [uu.strip() for uu in re.split(r"[\b\W\b]+", val) if uu.strip()]
        if len(unit) > 1:
            return unit[-1]
        return ''


    def _spec(self, sp1, name):
        for li in sp1.select('ul.product-specifications__list li'):
            ss = list(li.stripped_strings)
            if name in ss[0] and len(ss) > 1:
                return ss[1].split(':')[-1].strip()

        return ''


    def _url(self, link):
        url = link['href']
        if not url.startswith('http'):
            url = base_url + link['href']
        return url

    def _d_zoro(self, item):
        prod_id = item['gtm-data-productid']
        item_link = item.select_one('div.product-card__description a')
        item_url = self._url(item_link)
        logger.info(f"[item] {item_url}")
        sp1 = self.request_with_retries(item_url)

        ss = http.get(prod_url.format(prod_id), headers=self._headers()).json()['products']
        if len(ss) > 1:
            logger.warning('there is more than one search result' + item_url)
        ss = ss[0]

        category_path = [ii.text.strip() for ii in sp1.select('nav.zcl-breadcrumb li span[itemprop="name"]')][1:]
        if not category_path:
            category_path = [ii.text.strip() for ii in sp1.select('nav.Breadcrumb li span[itemprop="name"]')]
        mf_number=sp1.select_one('span[data-za="PDPMfrNo"]').text.strip()
        width=self._spec(sp1, 'Width')
        height=self._spec(sp1, 'Height')
        return SgRecord(
            item_url=item_url,
            category=' >> '.join(category_path),
            name=ss['title'],
            company=ss['brand'],
            price=ss['price'],
            description=sp1.select_one('div.product-description__text').text.strip(),
            country_of_origin=self._spec(sp1, 'Country of Origin'),
            mf_number=mf_number,
            shipping_day=ss['leadTime'],
            jpeg=jpeg_url.format(ss['image']),
            width=width,
            width_unit=self._u(width),
            height=height,
            height_unit=self._u(height),
            depth=self._spec(sp1, 'Depth'),
            weight=self._spec(sp1, 'Weight'),
            on_new_breaker_list=self._check_breaker_list(mf_number),
            on_old_list=self._check_old_list(mf_number)
        )

    def list_zoro_cat(self, cat_idx=0):
        categories = self.request_with_retries(base_url).select('div.mega-menu > div.mega-menu__grid div.mega-menu__grid-item a.mega-menu__level1')
        logger.info(f"Total {len(categories)} categories")
        # for cat_url, cat in self.fetchList(categories):
        for x, link in enumerate(categories):
            logger.info(f"[{x}] {self._url(link)}")

    def fetch_zoro_data(self, cat_idx=0, cat_list=[]):
        categories = self.request_with_retries(base_url).select('div.mega-menu > div.mega-menu__grid div.mega-menu__grid-item a.mega-menu__level1')
        logger.info(f"Total {len(categories)} categories")
        # for cat_url, cat in self.fetchList(categories):
        for x, link in enumerate(categories):
            if x not in cat_list:
                continue
            cat_url = self._url(link)
            cat = self.request_with_retries(cat_url)
            sub_categories = cat.select('ul.c-sidebar-nav__list li a')
            logger.warning(f"{cat_url} [sub cat {len(sub_categories)}]")
            if len(sub_categories):
                # for sub_url, sub_cat in self.fetchList(sub_categories):
                for sub_link in sub_categories:
                    try:
                        sub_url = self._url(sub_link)
                        sub_cat = self.request_with_retries(sub_url)
                        pages = sub_cat.select('section.search__results__footer div.v-select-list a')
                        items = sub_cat.select('div.search-results__result div.product-card-container')
                        logger.info(f"[{len(items)}] {sub_url}")
                        for item in items:
                            yield self._d_zoro(item)

                        # page 2 > 
                        if len(pages) > 1:
                            for page in pages:
                                sub_url1 = sub_url+f'?page={page.text.strip()}'
                                sub_cat1 = self.request_with_retries(sub_url1)
                                items1 = sub_cat1.select('div.search-results__result div.product-card-container')
                                logger.info(f"[{len(items1)}] [{page.text.strip()}]")
                                for item1 in items1:
                                    yield self._d_zoro(item1)

                    except Exception as err:
                        time.sleep(1)
                        logger.warning(link['href'])
                        logger.warning(str(err))
                        pass
            else:
                try:
                    pages1 = cat.select('section.search__results__footer div.v-select-list a')
                    items2 = cat.select('div.search-results__result div.product-card-container')
                    logger.info(f"[{len(items2)}] {cat_url}")
                    for item in items2:
                        yield self._d_zoro(item)

                    # page 2 > 
                    if len(pages1) > 1:
                        for page in pages1:
                            sub_url21 = cat_url+f'?page={page.text.strip()}'
                            sub_cat21 = self.request_with_retries(sub_url21)
                            items21 = sub_cat21.select('div.search-results__result div.product-card-container')
                            logger.info(f"[{len(items21)}] [{page.text.strip()}]")
                            for item1 in items21:
                                yield self._d_zoro(item1)
                except Exception as err:
                        time.sleep(1)
                        logger.warning(link['href'])
                        logger.warning(str(err))
                        pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--index', type=str, required=False, help="the name of childscraper. e.g, ganjapreneur from https://www.ganjapreneur.com/businesses/  complete example: python3 dirscraper.py -k ganjapreneur")
    cat_idx = parser.parse_args().index or 0

    cat_list = [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 22, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 27, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99]
    script = Script()
    # script._save_master_excel()
    if cat_idx == 'missing':
        script.find_missing_or_incomplete_categories()
    elif cat_idx != '-1':
        logger.info(f"{cat_idx}st Category scraper")
        ZORO_PATH = BASE_PATH + f'/ZORO_SCRAPE_Category_{cat_idx}.xlsx'
        with SgWriter(SgRecordDeduper(SgRecordID({SgRecord.Headers.ITEM_URL})), data_file=ZORO_PATH, s3=s3) as writer:
            script.initialize()
            results = script.fetch_zoro_data(cat_idx=cat_idx, cat_list=cat_list)
            for rec in results:
                writer.write_row(rec)
    
    elif cat_idx == 'list':
        script.list_zoro_cat()