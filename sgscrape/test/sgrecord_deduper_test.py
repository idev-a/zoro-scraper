import unittest
from typing import List

from sgscrape.sgrecord import SgRecord
from sgscrape.sgrecord_deduper import SgRecordDeduper, DupCycleDetectedError
from sgscrape.sgrecord_id import RecommendedRecordIds


class SgRecordDeduperTest(unittest.TestCase):

    __uniq_id = RecommendedRecordIds.StoreNumberId

    @staticmethod
    def mk_deduper(factor: float = 1) -> SgRecordDeduper:
        return SgRecordDeduper(record_id=SgRecordDeduperTest.__uniq_id,
                               duplicate_streak_failure_factor=factor)

    @staticmethod
    def __gen_uniq_records(num: int) -> List[SgRecord]:
        recs = []
        for i in range(num):
            recs.append(SgRecord(store_number=str(i)))
        return recs

    def test_no_cycle_error(self):
        deduper = SgRecordDeduperTest.mk_deduper()
        records = SgRecordDeduperTest.__gen_uniq_records(SgRecordDeduper.DUPLICATE_STREAK_MINIMUM * 5)

        for rec in records:
            deduper.dedup(record=rec)

        self.assertEqual(SgRecordDeduper.DUPLICATE_STREAK_MINIMUM * 5, deduper.unique_ids())

    def test_no_cycle_error_off_by_one(self):
        deduper = SgRecordDeduperTest.mk_deduper()
        records = SgRecordDeduperTest.__gen_uniq_records(SgRecordDeduper.DUPLICATE_STREAK_MINIMUM)

        for _ in range(2):
            for rec in records:
                deduper.dedup(record=rec)

        self.assertEqual(SgRecordDeduper.DUPLICATE_STREAK_MINIMUM, deduper.unique_ids())

    def test_cycle_error(self):
        deduper = SgRecordDeduperTest.mk_deduper()
        records = SgRecordDeduperTest.__gen_uniq_records(SgRecordDeduper.DUPLICATE_STREAK_MINIMUM)

        for i in range(2):
            for rec in records:
                deduper.dedup(record=rec)

        with self.assertRaises(DupCycleDetectedError):
            deduper.dedup(record=records[0])  # the straw that broke the camel's back

        self.assertEqual(SgRecordDeduper.DUPLICATE_STREAK_MINIMUM, deduper.unique_ids())

    def test_small_factor_with_error(self):
        deduper = SgRecordDeduperTest.mk_deduper(0.5)
        records = SgRecordDeduperTest.__gen_uniq_records(SgRecordDeduper.DUPLICATE_STREAK_MINIMUM)

        with self.assertRaises(DupCycleDetectedError):
            for i in range(2):
                for rec in records:
                    deduper.dedup(record=rec)

        self.assertEqual(SgRecordDeduper.DUPLICATE_STREAK_MINIMUM, deduper.unique_ids())

    def test_big_factor_with_error(self):
        deduper = SgRecordDeduperTest.mk_deduper(2)
        records = SgRecordDeduperTest.__gen_uniq_records(SgRecordDeduper.DUPLICATE_STREAK_MINIMUM)
        for i in range(3):
            for rec in records:
                deduper.dedup(record=rec)

        with self.assertRaises(DupCycleDetectedError):
            deduper.dedup(record=records[0])  # the straw that broke the camel's back

        self.assertEqual(SgRecordDeduper.DUPLICATE_STREAK_MINIMUM, deduper.unique_ids())


if __name__ == "__main__":
    unittest.main()
