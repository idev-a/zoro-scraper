from dataclasses import dataclass, field
from functools import partial
from typing import Set, Tuple, Dict, Callable, Any

from .sgrecord import SgRecord
from sglogging import SgLogSetup

_log = SgLogSetup().get_logger('SgRecordID')


@dataclass(frozen=True)
class SgRecordID:
    id_fields: Set[SgRecord.Headers.HeaderUnion] = field(default_factory=set)
    fail_on_empty_field: bool = False
    fail_on_empty_id: bool = True
    transformations: Dict[SgRecord.Headers.HeaderUnion, Tuple[str, Callable[[Any], str]]] = field(default_factory=dict)

    def with_truncate(self, header: SgRecord.Headers.HeaderUnion, nums_after_decimal: int) -> 'SgRecordID':
        def truncate_decimal(after_decimal: int, value: Any) -> str:
            str_val = str(value)
            if '.' in str_val:
                i = str_val.index('.')
                return str_val[0:i+1+after_decimal]
            else:
                return value

        return self.__with_transform(header, partial(truncate_decimal, nums_after_decimal), identifier=f".{nums_after_decimal}")

    def __with_transform(self, header: SgRecord.Headers.HeaderUnion, fn: Callable[[Any], str], identifier: str) -> 'SgRecordID':
        if "%" in identifier or ":" in identifier:
            raise ValueError(f"Identifier {identifier} cannot contain the '%' or ':' characters")

        upd_transforms = dict()
        upd_transforms.update(self.transformations.copy(), **{header: (identifier, fn)})
        return SgRecordID(id_fields=self.id_fields, transformations=upd_transforms)

    def generate_id(self, record: SgRecord) -> str:
        """
        Generates a human-readable string identity for a record based on the provided fields.

        Fails with `ValueError` either when one of the fields is empty or marked missing,
        or else when composite identity is empty.
        """
        rec_dict = record.as_dict()
        ident = ""
        for field in self.id_fields:
            value = rec_dict.get(field)
            if value and value != SgRecord.MISSING:
                transform = self.transformations.get(field)
                value = transform[1](value) if transform else value
                ident += f"{field}:{value} "
            elif self.fail_on_empty_field:
                raise ValueError(f"Record identity field '{field}' cannot be empty or {SgRecord.MISSING}. Value: {value}")

        if not ident:
            if self.fail_on_empty_id:
                raise ValueError(f"Composite record identity '{self.__str__()}' is empty. Record: {record}")
            else:
                _log.debug(f"Composite record identity '{self.__str__()}' is empty. Record: {record}")

        return ident

    def __str__(self) -> str:
        reprs = []
        for field in self.id_fields:
            transform = self.transformations.get(field)
            t_id = f"%{transform[0]}" if transform else ""
            reprs.append(f"{field}{t_id}")

        return ":".join(reprs)


class RecommendedRecordIds:
    pass
    # PhoneNumberId = SgRecordID({SgRecord.Headers.PHONE})
    # GeoSpatialId = SgRecordID({SgRecord.Headers.LATITUDE, SgRecord.Headers.LONGITUDE})\
    #                  .with_truncate(SgRecord.Headers.LATITUDE, 3)\
    #                  .with_truncate(SgRecord.Headers.LONGITUDE, 3)
    # StoreNumberId = SgRecordID({SgRecord.Headers.STORE_NUMBER})
    # PageUrlId = SgRecordID({SgRecord.Headers.PAGE_URL})
    # StoreNumAndPageUrlId = SgRecordID({SgRecord.Headers.STORE_NUMBER, SgRecord.Headers.PAGE_URL})


