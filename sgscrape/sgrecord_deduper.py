from typing import Set, Tuple

from .pause_resume import CrawlState, CrawlStateSingleton
from .sgrecord import SgRecord
from .sgrecord_id import SgRecordID


class DupCycleDetectedError(Exception):
    def __init__(self, dup_streak: int, uniq_records: int, dup_streak_failure_factor: float):
        self.__dup_streak = dup_streak
        self.__uniq_records = uniq_records
        self.__dup_streak_failure_factor = dup_streak_failure_factor

    def __str__(self):
        return f"Duplicate Cycle suspected. " \
               f"Unique records: {self.__uniq_records}, " \
               f"Duplicate streak: {self.__dup_streak}, " \
               f"Dup streak factor: {self.__dup_streak_failure_factor}"


class SgRecordDeduper:

    DUPLICATE_STREAK_MINIMUM = 1000
    DUPLICATE_STREAK_DEFAULT_FAILURE_FACTOR = 2.0

    def __init__(self,
                 record_id: SgRecordID,
                 duplicate_streak_failure_factor: float = DUPLICATE_STREAK_DEFAULT_FAILURE_FACTOR,
                 data_file_path: str = CrawlState.DEFAULT_DATA_FILE):
        """
        Deduplicates records by holding an internal set of record ids.

        :param record_id: The way by which to deduplicate a record.
        :param duplicate_streak_failure_factor: How much bigger should the duplicate streak be than the set of
                                                unique ids, to trigger a failure? [Defaults to 2]
        :param data_file_path: The path to the existing data file.
        """
        self.__id = record_id
        self.__encountered_ids: Set[str] = self.__read_existing_file(record_id, data_file_path)
        self.__duplicate_streak_failure_factor: float = duplicate_streak_failure_factor
        self.__state = CrawlStateSingleton.get_instance()

    def get_id(self) -> SgRecordID:
        """
        Returns the record ID class that's used
        """
        return self.__id

    def __err_if_cycle_detected(self, is_next_duplicate: bool):
        """
        Errors out if duplicate streak is too long (maximum of either the DUPLICATE_STREAK_MINIMUM, or deduped ids)
        """
        if is_next_duplicate:
            dup_streak = self.__state.increment_and_get_duplicate_streak()
        else:
            dup_streak = self.__state.reset_and_get_duplicate_streak()

        uniq_ids = len(self.__encountered_ids)

        if dup_streak / self.__duplicate_streak_failure_factor > max(SgRecordDeduper.DUPLICATE_STREAK_MINIMUM, uniq_ids):
            raise DupCycleDetectedError(dup_streak=dup_streak,
                                        uniq_records=uniq_ids,
                                        dup_streak_failure_factor=self.__duplicate_streak_failure_factor)

    @staticmethod
    def __read_existing_file(record_id: SgRecordID, data_file_path: str) -> Set[str]:
        ids = set()
        for rec in CrawlState.load_data(data_file_path):
            ids.add(record_id.generate_id(rec))
        return ids

    def dedup(self, record: SgRecord) -> Tuple[bool, str]:
        """
        Returns a tuple signifying: (is_duplicate?, generated_identity)
        """
        rec_id = self.__id.generate_id(record)
        if rec_id in self.__encountered_ids:
            self.__err_if_cycle_detected(True)
            return True, rec_id
        else:
            self.__encountered_ids.add(rec_id)
            self.__err_if_cycle_detected(False)
            return False, rec_id

    def unique_ids(self) -> int:
        return len(self.__encountered_ids)
