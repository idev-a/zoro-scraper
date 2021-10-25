from sgselenium import SgChrome
from selenium.webdriver.common.keys import Keys
import pdb

base_url = "https://www.gsaadvantage.gov/advantage/ws/main/start_page?store=ADVANTAGE"

with SgChrome() as driver:
    driver.get(base_url)
    search_input = driver.find_element_by_id("globalSearch")
    search_input.send_keys('APEX 15MM55-D')
    search_input.send_keys(Keys.ENTER)
    rr = driver.wait_for_request("/advantage/rs/search/advantage_search", timeout=20)
    pdb.set_trace()
    pass    