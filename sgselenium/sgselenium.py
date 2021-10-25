import os
from copy import _copy_immutable
import random
from time import time, sleep
from typing import Optional, List

from selenium.webdriver.remote.webdriver import WebDriver
from seleniumwire import webdriver
from selenium.webdriver.firefox.options import Options
import logging

class SgSelenium:
    DEFAULT_PROXY_URL = "http://groups-RESIDENTIAL,country-us:{}@proxy.apify.com:8000/"

    STORM_PROXIES = [
        "http://5.79.73.131:13040",
    ]

    def __init__(self, is_headless: bool = True, seleniumwire_auto_config: bool = True):
        """
        :param is_headless: Run selenium in headless mode.
        :param seleniumwire_auto_config: Auto-configure seleniumwire options to enable local/remote proxies. Setting seleniumwire_auto_config to False will prevent selenium from proxying the browser requests to a local/remote proxy server when PROXY_PASSWORD is not configured. In certain instances, a bot-detection software will try to block requests coming from a proxy server. In those cases, set seleniumwire_auto_config to False.
        """
        self.__is_headless = is_headless
        self.__behind_proxy = True
        self.__seleniumwire_auto_config = seleniumwire_auto_config
        

    def __set_proxy_options(self) -> dict:
        if self.__behind_proxy:
            proxy_url = random.choice(self.STORM_PROXIES)
            return {
                'proxy': {
                    'https': proxy_url,
                    'http': proxy_url,
                },
                'auto_config': True
            }
        else:
            return {
                'auto_config': self.__seleniumwire_auto_config
            }

    @staticmethod
    def __configure_logging(enable_logging):
        if not enable_logging:
            sgselenium_logger = logging.getLogger('seleniumwire')
            sgselenium_logger.addHandler(logging.NullHandler())

    def chrome(self,
               user_agent: Optional[str] = None,
               executable_path: Optional[str] = None,
               enable_logging: bool = False,
               extra_option_args: List[str] = _copy_immutable(list()),
               chrome_options: Optional[webdriver.ChromeOptions] = None):
        """
        :param user_agent: Optionally, pass a custom user agent string.
        :param executable_path: Optionally, pass a custom path to chromedriver
        :param enable_logging: Enable detailed logging [False by default]
        :param extra_option_args: Add extra arguments to the default ones.
        :param chrome_options: Optionally, pass custom ChromeOptions to override all other ones.
        :return: the configured `Chrome` driver instance
        """

        # https://github.com/ultrafunkamsterdam/undetected-chromedriver
        import undetected_chromedriver as uc
        uc.install()

        seleniumwire_options = self.__set_proxy_options()
        if not executable_path:
            executable_path = 'chromedriver'

        if not chrome_options:
            chrome_options = webdriver.ChromeOptions()
            self.__add_chrome_arguments(chrome_options, user_agent)
            for arg in extra_option_args:
                chrome_options.add_argument(arg)

        chrome_driver = webdriver.Chrome(executable_path=executable_path, chrome_options=chrome_options, seleniumwire_options=seleniumwire_options)

        self.__configure_chromedriver(chrome_driver, user_agent)

        self.__configure_logging(enable_logging)
        return chrome_driver

    def firefox(self,
                user_agent=None,
                executable_path=None,
                enable_logging=False,
                extra_option_args: List[str] = _copy_immutable(list()),
                firefox_options: Optional[Options] = None):
        """
        :param user_agent: Optionally, pass a custom user agent string.
        :param executable_path: Optionally, pass a custom path to chromedriver
        :param enable_logging: Enable detailed logging [False by default]
        :param extra_option_args: Add extra arguments to the default ones.
        :param firefox_options: Optionally, pass custom Options to completely override all other ones.
        :return: the configured `Firefox` driver instance
        """

        seleniumwire_options = self.__set_proxy_options()
        if not executable_path:
            executable_path = 'geckodriver'

        if not firefox_options:
            firefox_options = Options()
            self.__add_ffx_arguments(firefox_options, user_agent)
            for arg in extra_option_args:
                firefox_options.add_argument(arg)

        ffx_driver = webdriver.Firefox(executable_path=executable_path,
                                       options=firefox_options,
                                       seleniumwire_options=seleniumwire_options,
                                       firefox_profile=self.__ffx_profile())

        ffx_driver.set_window_size(1920, 1080)

        self.__configure_logging(enable_logging)
        return ffx_driver

    def __add_common_args(self, options, user_agent):
        if self.__is_headless:
            options.add_argument('--headless')

        options.add_argument('--no-sandbox')
        options.add_argument("--disable-gpu")
        options.add_argument('--disable-dev-shm-usage')

        if user_agent:
            options.add_argument(f'--user-agent={user_agent}')
        else:
            options.add_argument(
                '--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36')

    def __add_ffx_arguments(self, options, user_agent):
        self.__add_common_args(options, user_agent)

    def __ffx_profile(self):
        profile = webdriver.FirefoxProfile()
        profile.accept_untrusted_certs = True
        profile.assume_untrusted_cert_issuer = False

        if not self.__behind_proxy:
            profile.set_preference('network.proxy.type', 0)  # http://kb.mozillazine.org/Network.proxy.type

        return profile

    def __add_chrome_arguments(self, options, user_agent):
        self.__add_common_args(options, user_agent)

        options.add_argument('--allow-running-insecure-content')
        options.add_argument('--ignore-certificate-errors')

        options.add_experimental_option('useAutomationExtension', False)
        options.add_experimental_option("excludeSwitches", ["enable-automation"])

        if not self.__behind_proxy:
            options.add_argument('--no-proxy-server')

    def __configure_chromedriver(self, the_driver: webdriver.Chrome, user_agent: Optional[str]):
        if user_agent:
            driver_user_agent = user_agent
        else:
            driver_user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"

        self.__set_proxy_options()

        the_driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
             Object.defineProperty(navigator, 'webdriver', {
               get: () => undefined
             })
           """
        })
        the_driver.execute_cdp_cmd("Network.enable", {})
        the_driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": {"User-Agent": driver_user_agent}})

        # the_driver.set_window_size(1920,1080)

    @staticmethod
    def get_default_headers_for(the_driver: WebDriver, request_url: str) -> dict:
        """
        Navigates to the requested URL _TWICE_ using the driver, so that cookies are also included in the headers.
        Does _not_ invoke `driver.close()` after use.
        """
        the_driver.get(request_url)
        the_driver.get(request_url)
        # returns the headers of the second request to that same url
        return [r.headers for r in driver.requests if request_url in r.url][1]


class SgChrome(SgSelenium):
    def __init__(self,
                 user_agent: Optional[str] = None,
                 executable_path: Optional[str] = None,
                 enable_logging: bool = False,
                 extra_option_args: List[str] = _copy_immutable(list()),
                 chrome_options: Optional[webdriver.ChromeOptions] = None,
                 is_headless: bool = True,
                 seleniumwire_auto_config: bool = True):
        """
        Creates a resource-safe Selenium Chromedriver.
        Equivalent to calling SgSelenium(is_headless).chrome(...) with the provided params.
        """
        super().__init__(is_headless=is_headless, seleniumwire_auto_config=seleniumwire_auto_config)
        self.__driver = self.chrome(user_agent=user_agent,
                                    executable_path=executable_path,
                                    enable_logging=enable_logging,
                                    extra_option_args=extra_option_args,
                                    chrome_options=chrome_options)

    def driver(self) -> WebDriver:
        """
        :return: The underlying driver, for any kind of tweaking.
        """
        return self.__driver

    def __enter__(self):
        return self.__driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__driver.quit()


class SgFirefox(SgSelenium):
    def __init__(self,
                 user_agent: Optional[str] = None,
                 executable_path: Optional[str] = None,
                 enable_logging: bool = False,
                 extra_option_args: List[str] = _copy_immutable(list()),
                 firefox_options: Optional[Options] = None,
                 is_headless: bool = True,
                 seleniumwire_auto_config: bool = True):
        """
        Creates a resource-safe Selenium Firefox Gecko driver.
        Equivalent to calling SgSelenium(is_headless).firefox(...) with the provided params.
        """
        super().__init__(is_headless=is_headless, seleniumwire_auto_config=seleniumwire_auto_config)
        self.__driver = self.firefox(user_agent=user_agent,
                                     executable_path=executable_path,
                                     enable_logging=enable_logging,
                                     extra_option_args=extra_option_args,
                                     firefox_options=firefox_options)

    def driver(self) -> WebDriver:
        """
        :return: The underlying driver, for any kind of tweaking.
        """
        return self.__driver

    def __enter__(self):
        return self.__driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__driver.quit()

if __name__ == "__main__":
    # Sanity checks
    with SgChrome(is_headless=False, extra_option_args=["xyz"]) as driver:
        print("Using SgChrome")
        driver.get("https://google.com")
        print(driver.requests)
        driver.execute_script("""alert("testing");""")
        sleep(5)

    with SgFirefox(is_headless=False, extra_option_args=["xyz"]) as driver:
        print("Using SgFirefox")
        driver.get("https://google.com")
        print(driver.requests)
        driver.execute_script("""alert("testing");""")
        sleep(5)

