from typing import Callable, Any

import sglogging

class MemoCache:
    def __init__(self, record_count_limit: int = 10000):
        """
        A memoizing object cache, backed by a dict.
        Lazily loads the values into the cache, as needed.
        """
        self.__cache = {}
        self.__limit = record_count_limit
        self.__record_count = 0
        self.__log = sglogging.SgLogSetup().get_logger(logger_name="MemoCache")

    def memoize(self, key: str, getter: Callable[[], Any]) -> Any:
        """
        Fetches the value from the cache for the specified key, or memoizes it if it's not there yet.
        """
        try:
            return self.__cache[key]
        except KeyError:
            value = getter()

            self.__record_count += 1
            if self.__record_count <= self.__limit:
                self.__cache[key] = value
            else:
                self.__log.debug(f"MemoCache reached limit of [{self.__limit}], will not store further items.")

            return value

    def view(self) -> dict:
        """
        Get a shallow copy of the inner cache.
        """
        return self.__cache.copy()