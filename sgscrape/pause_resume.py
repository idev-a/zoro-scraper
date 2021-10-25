import csv
import json
import threading
from dataclasses import asdict, dataclass, field
from datetime import timedelta, datetime
from os import path
from threading import Lock
from typing import List, Iterable, Tuple, Optional, Dict, Union, Set, Callable
from ordered_set import OrderedSet
from sglogging import SgLogSetup

from .sgrecord import SgRecord


@dataclass(frozen=True)
class SerializableRequest:
    """
    Consists of fields that define a request (as per sgrequests, or the requests libs),
    plus a `context` field for storing arbitrary context information alongside the request.
    """
    url: str
    method: str = 'GET'
    params: Dict[str, str] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    data: Union[None, Dict[str, str], List[Tuple[str, str]]] = None
    verify: bool = True
    allow_redirects: bool = True
    cookies: Optional[dict] = None
    auth: Optional[Tuple[str, str]] = None
    timeout: Union[None, float, Tuple[float, float]] = None
    stream: bool = False
    cert: Optional[Union[str, Tuple[str, str]]] = None
    json: Optional[str] = None
    context: Optional[dict] = None

    def serialize(self) -> str:
        """
        Known deficiency: will serialize tuples as lists; this may not matter in practice, as tuples have list accessors.
        """
        return json.dumps(asdict(self))

    def __hash__(self):
        return hash((self.url, self.method, json.dumps(self.params), json.dumps(self.headers),
                     json.dumps(self.data), json.dumps(self.cookies), self.json, json.dumps(self.context)))

    def __eq__(self, other):
        return isinstance(other, SerializableRequest) and hash(self) == hash(other)

    @staticmethod
    def deserialize(serialized_json: str) -> 'SerializableRequest':
        as_dict = json.loads(serialized_json)
        return SerializableRequest(
            url=as_dict['url'],
            method=as_dict['method'],
            data=as_dict['data'],
            params=as_dict['params'],
            headers=as_dict['headers'],
            verify=as_dict['verify'],
            allow_redirects=as_dict['allow_redirects'],
            cookies=as_dict['cookies'],
            auth=as_dict['auth'],
            timeout=as_dict['timeout'],
            stream=as_dict['stream'],
            cert=as_dict['cert'],
            json=as_dict['json'],
            context=as_dict['context']
        )

class RequestStack:
    def __init__(self, seed: Optional[OrderedSet[SerializableRequest]], state: 'CrawlState'):
        self.__request_stack = seed
        self.__state = state

    def push_request(self, req: SerializableRequest) -> bool:
        """
        Whenever found a new request to query, push the request to the request queue instead of directly querying it.
        Note that each request has a dictionary `context` field to provide extra context when making it.
        """
        self.__request_stack.add(req)
        self.__state.save()

    def pop_request(self) -> Optional[SerializableRequest]:
        """
        Pop a request from the queue to query it.
        Note that each request has a dictionary `context` field to provide extra context when making it.
        """
        req = self.__request_stack.pop() if self.__request_stack else None
        if req:
            self.__state.save()
        return req

    def serialize_requests(self) -> Iterable[str]:
        return list(map(lambda r: r.serialize(), self.__request_stack))

    def __iter__(self):
        return self

    def __len__(self):
        return len(self.__request_stack)

    def __next__(self):
        req = self.pop_request()
        if not req:
            raise StopIteration
        else:
            return req


class CrawlState:
    """
    Constructs an object that manages crawler state.
    It will read any previous crawl state from a state file, and overwrite the state file when `save_state`
    is called, either if the `minimum_time_between_saves` has elapsed, or else, a "save signal" has been received.
    """

    REQUEST_QUEUE = "___ReqQ"
    SGZIP_VISITED_CENTROIDS = "___VisitedSgzip"
    MISC = "___Misc"
    DUP_STREAK = "___DupStreak"

    SAVE_SIGNAL_FILE = '.save_state_trigger'
    STATE_FILE = 'state.json'
    DEFAULT_DATA_FILE = 'data.csv'

    def increment_visited_coords(self, country_code: str) -> None:
        """
        Mark that another coordinate has been accounted for, for a country code.
        """
        raise NotImplementedError("Use CrawlStateSingleton.get_instance() instead of CrawlState()")

    def decrement_visited_coords(self, country_code: str) -> None:
        """
        Remove accounting of a coordinate, for a country code.
        """
        raise NotImplementedError("Use CrawlStateSingleton.get_instance() instead of CrawlState()")

    def get_visited_coords(self, country_code: str) -> int:
        """
        Retrieves the number of visited coordinates from state, for a country code.
        """
        raise NotImplementedError("Use CrawlStateSingleton.get_instance() instead of CrawlState()")

    def push_request(self, req: SerializableRequest) -> bool:
        """
        Whenever found a new request to query, push the request to the request queue instead of directly querying it.
        Note that each request has a dictionary `context` field to provide extra context when making it.
        """
        raise NotImplementedError("Use CrawlStateSingleton.get_instance() instead of CrawlState()")

    def pop_request(self) -> Optional[SerializableRequest]:
        """
        Pop a request from the queue to query it.
        Note that each request has a dictionary `context` field to provide extra context when making it.
        """
        raise NotImplementedError("Use CrawlStateSingleton.get_instance() instead of CrawlState()")

    def request_stack_iter(self) -> RequestStack:
        """
        Returns the internal `RequestStack`, which can be conveniently used as an iterator
        """
        raise NotImplementedError("Use CrawlStateSingleton.get_instance() instead of CrawlState()")

    def get_duplicate_streak(self) -> int:
        """
        Returns the value of the duplicate streak
        """
        raise NotImplementedError("Use CrawlStateSingleton.get_instance() instead of CrawlState()")

    def increment_and_get_duplicate_streak(self) -> int:
        """
        Increment the duplicate streak by 1, and fetch it
        """
        raise NotImplementedError("Use CrawlStateSingleton.get_instance() instead of CrawlState()")

    def reset_and_get_duplicate_streak(self) -> int:
        """
        Reset the duplicate streak back to 0, and fetch it
        """
        raise NotImplementedError("Use CrawlStateSingleton.get_instance() instead of CrawlState()")

    def set_misc_value(self, key: str, value: Union[str, int, float]) -> None:
        """
        Set an arbitrary key->value in the state. It can be one of str, int, float.
        """
        raise NotImplementedError("Use CrawlStateSingleton.get_instance() instead of CrawlState()")

    def get_misc_value(self, key: str, default_factory: Optional[Callable[[], Union[str, int, float]]] = None) -> Union[str, int, float, None]:
        """
        Retrieves a miscellaneous value from the state, or None if there is none.
        :param key: The key, under which the value is stored.
        :param default_factory: [Optional] If the value isn't found, call the `default_factory()` to generate a value,
                                save it under the `key`, and return it.
        """
        raise NotImplementedError("Use CrawlStateSingleton.get_instance() instead of CrawlState()")

    def save(self, override: bool = False) -> None:
        """
        Saves a (serializable!) dictionary into a JSON file, if a "save" signal has been received, or enough time has
        elapsed since the last save.

        :param override: If set to True, will always save to file.
        """
        raise NotImplementedError("Use CrawlStateSingleton.get_instance() instead of CrawlState()")

    @staticmethod
    def load_data(data_file: str = DEFAULT_DATA_FILE) -> Iterable[SgRecord]:
        """
        Loads the data of an existing data file into a list of dictionaries.
        """
        if path.exists(data_file):
            with open(data_file, mode='r', encoding='utf-8') as dfile:
                reader = csv.DictReader(dfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator='\n')
                for rec in reader:
                    yield SgRecord(raw=rec)


class _CrawlStateImpl(CrawlState):

    __instance_id = 0
    __state_file_lock = Lock()

    def __init__(self,
                 minimum_time_between_saves: timedelta):
        """
        Constructs an object that manages crawler state.
        It will read any previous crawl state from a state file, and overwrite the state file when `save_state`
        is called, either if the `minimum_time_between_saves` has elapsed, or else, a "save signal" has been received.

        :param minimum_time_between_saves: The minimum time period, between which there is no need to save results.
        """
        _CrawlStateImpl.__instance_id += 1
        self.__log = SgLogSetup().get_logger(logger_name=f'CrawlState_{_CrawlStateImpl.__instance_id}')
        state = self.__load_state()
        self.__request_stack: RequestStack = RequestStack(seed=self.__req_q_from_state(state), state=self)
        self.__visited_centroids: Dict[str, int] = self.__visited_centroids_from_state(state)
        self.__dup_streak: int = self.__dup_streak_from_state(state)
        self.__misc: dict = self.__misc_from_state(state)
        self.__minimum_time_between_saves = minimum_time_between_saves
        self.__saved_on_signal_received = False
        self.__update_last_saved()

    def increment_visited_coords(self, country_code: str) -> None:
        """
        Mark that another coordinate has been accounted for.
        """
        incr = self.get_visited_coords(country_code) + 1
        self.__visited_centroids[country_code] = incr
        self.save()

    def decrement_visited_coords(self, country_code: str) -> None:
        """
        Remove accounting of a coordinate, for a country code.
        """
        decr = self.get_visited_coords(country_code) - 1
        self.__visited_centroids[country_code] = decr
        self.save()

    def get_visited_coords(self, country_code: str) -> int:
        """
        Retrieves the number of visited coordinates from state.
        """
        return self.__visited_centroids.get(country_code) or 0

    def push_request(self, req: SerializableRequest) -> bool:
        """
        Whenever found a new request to query, push the request to the request queue instead of directly querying it.
        Note that each request has a dictionary `context` field to provide extra context when making it.
        """
        return self.__request_stack.push_request(req)

    def pop_request(self) -> Optional[SerializableRequest]:
        """
        Pop a request from the queue to query it.
        Note that each request has a dictionary `context` field to provide extra context when making it.
        """
        return self.__request_stack.pop_request()

    def request_stack_iter(self) -> RequestStack:
        """
        Returns the internal `RequestStack`, which can be conveniently used as an iterator
        """
        return self.__request_stack

    def get_duplicate_streak(self) -> int:
        """
        Returns the value of the duplicate streak
        """
        return self.__dup_streak

    def increment_and_get_duplicate_streak(self) -> int:
        """
        Increment the duplicate streak by 1, and fetch it
        """
        self.__dup_streak += 1
        self.save()
        return self.__dup_streak

    def reset_and_get_duplicate_streak(self) -> int:
        """
        Reset the duplicate streak back to 0, and fetch it
        """
        self.__dup_streak = 0
        self.save()
        return self.__dup_streak

    def set_misc_value(self, key: str, value: Union[str, int, float]) -> None:
        """
        Set an arbitrary key->value in the state. It can be one of str, int, float.
        """
        self.__misc[key] = value
        self.save()

    def get_misc_value(self, key: str, default_factory: Optional[Callable[[], Union[str, int, float]]] = None) -> Union[str, int, float, None]:
        """
        Retrieves a miscellaneous value from the state, or None if there is none.
        :param key: The key, under which the value is stored.
        :param default_factory: [Optional] If the value isn't found, call the `default_factory()` to generate a value,
                                save it under the `key`, and return it.
        """
        value = self.__misc.get(key)
        if not value and default_factory:
            value = default_factory()
            self.__misc[key] = value
            self.save()

        return value

    def save(self, override: bool = False) -> None:
        """
        Saves a (serializable!) dictionary into a JSON file, if a "save" signal has been received, or enough time has
        elapsed since the last save.

        :param override: If set to True, will always save to file.
        """
        with _CrawlStateImpl.__state_file_lock:
            save_signal = self.__should_save_on_signal()
            if override or save_signal or self.__is_time_to_save():
                self.__update_last_saved()
                self.__raise_saved_on_signal_flag(save_signal)
                state = {
                    CrawlState.REQUEST_QUEUE: self.__request_stack.serialize_requests(),
                    CrawlState.SGZIP_VISITED_CENTROIDS: self.__visited_centroids,
                    CrawlState.MISC: json.dumps(self.__misc),
                    CrawlState.DUP_STREAK: self.__dup_streak
                }
                with open(file=CrawlState.STATE_FILE, mode='w', encoding='utf-8') as state_file:
                    self.__log.info(f'Saving crawler state to {CrawlState.STATE_FILE}...')
                    t1 = datetime.now()
                    json.dump(state, state_file)
                    t2 = datetime.now()
                    self.__log.info(f'Writing state file took: [{(t2 - t1).total_seconds()}] seconds')

    def __should_save_on_signal(self) -> bool:
        save_signal_received = self.__save_signal_received()
        value = save_signal_received and not self.__saved_on_signal_received
        return value

    def __raise_saved_on_signal_flag(self, signal_received: bool):
        self.__saved_on_signal_received = self.__saved_on_signal_received or signal_received

    def __is_time_to_save(self) -> bool:
        return self.__last_saved + self.__minimum_time_between_saves < datetime.utcnow()

    def __update_last_saved(self):
        self.__last_saved = datetime.utcnow()

    @staticmethod
    def __load_state() -> dict:
        """
        Loads the previously-saved (via `save_state`) state in the form of a dict.
        Returns {} if file did not exist, for the sake of uniformity.
        """
        if path.exists(CrawlState.STATE_FILE):
            with _CrawlStateImpl.__state_file_lock:
                with open(file=CrawlState.STATE_FILE, mode='r', encoding='utf-8') as state_file:
                    return json.load(state_file)
        else:
            return {}

    @staticmethod
    def __req_q_from_state(state: dict) -> OrderedSet[SerializableRequest]:
        return OrderedSet(map(lambda r: SerializableRequest.deserialize(r),
                              state.get(CrawlState.REQUEST_QUEUE) or []))

    @staticmethod
    def __visited_centroids_from_state(state: dict) -> Dict[str, int]:
        return state.get(CrawlState.SGZIP_VISITED_CENTROIDS) or dict()

    @staticmethod
    def __dup_streak_from_state(state: dict) -> int:
        return state.get(CrawlState.DUP_STREAK) or 0

    @staticmethod
    def __misc_from_state(state: dict) -> dict:
        misc = state.get(CrawlState.MISC)
        return json.loads(misc) if misc else dict()

    @staticmethod
    def __save_signal_received() -> bool:
        """
        Tells the current process whether the parent process has asked to save state.
        """
        return path.exists(CrawlState.SAVE_SIGNAL_FILE)

class CrawlStateSingleton:
    __instance: Optional[CrawlState] = None
    __lock = threading.Lock()

    DEFAULT_MIN_TIME_BETWEEN_SAVES = timedelta(seconds=30)
    __minimum_time_between_saves = DEFAULT_MIN_TIME_BETWEEN_SAVES

    @staticmethod
    def set_minimum_time_between_saves(duration: timedelta):
        CrawlStateSingleton.__minimum_time_between_saves = duration

    @staticmethod
    def get_instance() -> CrawlState:
        """
        Returns a singleton instance of CrawlState.
        """
        with CrawlStateSingleton.__lock:
            if not CrawlStateSingleton.__instance:
                CrawlStateSingleton.__instance = _CrawlStateImpl(CrawlStateSingleton.__minimum_time_between_saves)
            return CrawlStateSingleton.__instance

    @staticmethod
    def _delete_instance():
        """
        For test use; Do not use.
        """
        CrawlStateSingleton.__instance = None
