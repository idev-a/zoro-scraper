from sgrequests import SgRequests
from sgselenium import SgChrome, SgFirefox
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from undetected_chromedriver import ChromeDriverManager
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
import time
import random
import re
import logging
from logging.handlers import RotatingFileHandler
from util.util import Util

BASE_PATH = os.path.abspath(os.curdir)
ZORO_PATH = BASE_PATH + '/ZORO_SCRAPE.xlsx'
BH_PATH = BASE_PATH + '/BH_SCRAPE.xlsx'
OLD_LIST_PATH = BASE_PATH + '/old_list'
BREAKER_LIST_PATH = BASE_PATH + '/BREAKER LIST 2021 with new columns.xlsx'

# Create a custom logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# remove log file
LOG_DIR = BASE_PATH + '/logs/'
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

zoro_base_url = "https://www.zoro.com"
zoro_prod_url = "https://www.zoro.com/product/?products={}"
zoro_jpeg_url = "https://www.zoro.com/static/cms/product/full/{}"

bh_base_url = "https://www.bhphotovideo.com"

max_workers = 8

http = SgRequests(verify_ssl=False)

class Script:
    

    def __init__(self):
        # load old list file
        logger.info("load old list files inside /old_list dir")
        # self.old_pds = []
        # for root, directories, files in os.walk(OLD_LIST_PATH, topdown=False):
        #     for name in files:
        #         logger.info(f"load old list {name}")
        #         old_path = os.path.join(root, name)
        #         self.old_pds.append(pd.read_excel(open(old_path, 'rb'), sheet_name='RESULTS_FULL', usecols=['Manufacturer Part Number']) )

        # load breaker list
        logger.info("load breaker list")
        # self.breaker_pd = pd.read_excel(open(BREAKER_LIST_PATH, 'rb'), usecols=[1])

        STORM_PROXIES = [
            "5.79.73.131:13040",
        ]
        self.options = Options()
        self.options.add_argument("--no-sandbox")
        self.options.add_argument('--headless')
        self.options.add_argument(f"user-agent={Util().get_random_user_agent()}")
        self.options.add_argument('--proxy-server=%s' % random.choice(STORM_PROXIES))
        self.options.add_argument('user-data-dir=/home/hello/.config/google-chrome/Profile 3')
        self.options.add_argument("--disable-dev-shm-usage")

    def _headers(self):
        return Util().get_zoro_headers()

    def get_driver(self, url, driver=None):
        if driver is not None:
            driver.quit()

        x = 0
        while True:
            x = x + 1
            try:
                driver = webdriver.Chrome(
                    executable_path=BASE_PATH + '/mychromedriver',
                    options=self.options
                )
                driver.get(url)
                break
            except Exception:
                driver.quit()
                if x == 10:
                    raise Exception(
                        "Make sure this ran with a Proxy, will fail without one"
                    )
                continue
        return driver

    def fetchSingle(self, link):
        _url = link['href']
        if not _url.startswith('http'):
            _url = zoro_base_url + link['href']
        
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
            url = zoro_base_url + link['href']
        return url

    def _d_zoro(self, item):
        prod_id = item['gtm-data-productid']
        item_link = item.select_one('div.product-card__description a')
        item_url = self._url(item_link)
        logger.info(f"[item] {item_url}")
        sp1 = self.request_with_retries(item_url)

        ss = http.get(zoro_prod_url.format(prod_id), headers=self._headers()).json()['products']
        if len(ss) > 1:
            logger.warning('there is more than one search result' + item_url)
        ss = ss[0]

        category_path = [ii.text.strip() for ii in sp1.select('nav.zcl-breadcrumb li span[itemprop="name"]')][1:]
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
            jpeg=zoro_jpeg_url.format(ss['image']),
            width=width,
            width_unit=self._u(width),
            height=height,
            height_unit=self._u(height),
            depth=self._spec(sp1, 'Depth'),
            weight=self._spec(sp1, 'Weight'),
            on_new_breaker_list=self._check_breaker_list(mf_number),
            on_old_list=self._check_old_list(mf_number)
        )

    def fetch_bh_data(self):
        driver = self.get_driver(bh_base_url)
        import pdb; pdb.set_trace()
        categories = bs(driver.page_source, 'lxml').select('section.main-nav ul.capital > li')[:-2]

        if driver:
            driver.quit()

    def fetch_zoro_data(self):
        categories = self.request_with_retries(zoro_base_url).select('div.mega-menu > div.mega-menu__grid div.mega-menu__grid-item a.mega-menu__level1')
        logger.info(f"Total {len(categories)} categories")
        # for cat_url, cat in self.fetchList(categories):
        for link in categories:
            cat_url = self._url(link)
            cat = self.request_with_retries(cat_url)
            sub_categories = cat.select('ul.c-sidebar-nav__list li a')
            # for sub_url, sub_cat in fetchList(sub_categories):
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
                    print(err)
                    import pdb; pdb.set_trace()

if __name__ == '__main__':
    with SgWriter(SgRecordDeduper(SgRecordID({SgRecord.Headers.ITEM_URL})), data_file=ZORO_PATH) as writer:
        script = Script()
        results = script.fetch_bh_data()
        # results = script.fetch_zoro_data()
        for rec in results:
            writer.write_row(rec)