from dataclasses import dataclass
from functools import partial
from logging import Logger

from .sgrecord_deduper import SgRecordDeduper
from .sgrecord_id import SgRecordID
from .sgwriter import SgWriter
from .simple_utils import *


class _FieldDefBase:
    """
    Field definition for `SimpleScraperPipeline`
    """
    def __init__(self,
                 mapping: Optional[list] = None,
                 constant_value: Optional[str] = None,
                 value_transform: Optional[Callable[[str], str]] = None,
                 raw_value_transform: Optional[Callable[[list], str]] = None,
                 part_of_record_identity: bool = False,
                 is_required: bool = True,
                 multi_mapping_concat_with: str = ' '):
        """
        - DO NOT INSTANTIATE DIRECTLY; INSTEAD, USE THE SUBCLASSES -

        Initializes and validates the field definition. Validation rules are as follows:
        - Field must have one and only one of mapping and constant_value defined.
        - Field cannot both be constant, and have a value_transform.
        - Field cannot both be constant, and be part of the record's identity.
        - Field cannot have both value_transform and raw_value_transform

        :param mapping: -- SIMPLE CASE --
                        Given a raw record in `dict` form, a mapping is the path to the field's value.
                        For example, in { address: { street1: "..." } } the path of `street_address`
                        would be `['address', 'street1']`.
                        -- COMPLEX CASE --
                        In cases where multiple raw fields need to be concatenated to form a single logical field,
                        a list of lists should be provided, such as in: { address: { street1: "...", street2: "... } }
                        and the `street_address` could be `[['address', 'street1'], ['address', 'street2']]`.
                        In this case, the fields will be joined with the value of the `multi_mapping_concat_with` param.

        :param constant_value: If the field's value will always be constant, set this value.
        :param value_transform: If the field's final value needs to be transformed, supply a lambda: str->str
        :param raw_value_transform: Optionally, transforms a list of raw values (dense array with None fillers) into a str.
        :param part_of_record_identity: Is this field part of the record's identity?
        :param is_required: Is this field required to be present in a record, or can it be <MISSING>?
        :param multi_mapping_concat_with: If we're dealing with multi-concat field mapping, this is the delimiter between
                                          the raw concatenated fields.
        """

        if (not mapping and not constant_value) or (mapping and constant_value):
            raise ValueError("Field must have one and only one of mapping and constant_value defined.")

        if constant_value and value_transform:
            raise ValueError("Field cannot both be constant, and have a value_transform.")

        if constant_value and part_of_record_identity:
            raise ValueError("Field cannot both be constant, and be part of the record's identity.")

        if value_transform and raw_value_transform:
            raise ValueError("Field cannot have both value_transform and raw_value_transform")

        self.__mapping = _FieldDefBase.__normalize_mapping(mapping) if mapping else None
        self.__constant_value = constant_value
        self.__is_required = is_required
        self.__part_of_record_identity = part_of_record_identity
        self.__value_transform = value_transform
        self.__raw_value_transform = raw_value_transform
        self.__multi_mapping_concat_with = multi_mapping_concat_with

    def mapping(self) -> Optional[List[List[str]]]:
        return self.__mapping

    def constant_value(self) -> Optional[str]:
        return self.__constant_value

    def is_required(self) -> bool:
        return self.__is_required

    def part_of_record_identity(self) -> bool:
        return self.__part_of_record_identity

    def value_transform(self) -> Optional[Callable[[Any], str]]:
        return self.__value_transform

    def raw_value_transform(self):
        return self.__raw_value_transform

    def multi_mapping_concat_with(self) -> str:
        return self.__multi_mapping_concat_with

    @staticmethod
    def __normalize_mapping(mapping: list) -> List[List[str]]:
        if isinstance(mapping[0], list):
            return mapping
        else:
            # that means we need to wrap
            return [mapping]

##################################################
#
# USE THESE FieldDef SUBCLASSES:
#

class MissingField(_FieldDefBase):
    def __init__(self):
        """
        Field definition for a <MISSING> field.
        Note - this was better achieved via a constant, but a subclass was chosen for consistency.
        """
        super().__init__(constant_value=SimpleScraperPipeline.MISSING)

class ConstantField(_FieldDefBase):
    def __init__(self, value: str):
        """
        Represents a field definition that is constant
        """
        super().__init__(constant_value=value)

class MappingField(_FieldDefBase):
    def __init__(self,
                 mapping: List[str],
                 value_transform: Optional[Callable[[str], str]] = None,
                 raw_value_transform: Optional[Callable[[Any], str]] = None,
                 part_of_record_identity: bool = False,
                 is_required: bool = True):
        """
        Represents a field definition with a single field mapping.

        :param mapping: Given a raw record in `dict` form, a mapping is the path to the field's value.
                        For example, in { address: { street1: "..." } } the path of `street_address`
                        would be `['address', 'street1']`.
        :param value_transform: See documentation in `_FieldDefBase`
        :param raw_value_transform: Accepts a single raw value, and transforms to a string.
                                    See documentation in `_FieldDefBase` for full details.
        :param part_of_record_identity: See documentation in `_FieldDefBase`
        :param is_required: See documentation in `_FieldDefBase`
        """

        super().__init__(
            mapping=mapping,
            raw_value_transform=self.__convert_transformer(raw_value_transform),
            value_transform=value_transform,
            part_of_record_identity=part_of_record_identity,
            is_required=is_required
        )

    @staticmethod
    def __convert_transformer(orig: Optional[Callable[[Any], str]]):
        if not orig:
            return None
        else:
            return partial(MappingField.__list_transformer, orig)

    @staticmethod
    def __list_transformer(doer: Callable[[Any], str], objs: list) -> str:
        return doer(objs[0])


class MultiMappingField(_FieldDefBase):
    def __init__(self,
                 mapping: List[List[str]],
                 part_of_record_identity: bool = False,
                 value_transform: Optional[Callable[[str], str]] = None,
                 raw_value_transform: Optional[Callable[[list], str]] = None,
                 is_required: bool = True,
                 multi_mapping_concat_with: str = ' '):
        """
        Represents a field definition with a multiple field mappings.

        :param mapping: In cases where multiple raw fields need to be concatenated to form a single logical field,
                        a list of lists should be provided, such as in: { address: { street1: "...", street2: "..." } }
                        and the `street_address` could be `[['address', 'street1'], ['address', 'street2']]`.
                        In this case, the fields will be joined with the value of the `multi_mapping_concat_with` param.
        :param value_transform: See documentation in `_FieldDefBase`
        :param raw_value_transform: See documentation in `_FieldDefBase`
        :param part_of_record_identity: See documentation in `_FieldDefBase`
        :param is_required: See documentation in `_FieldDefBase`
        :param multi_mapping_concat_with: See documentation in `_FieldDefBase`
        """
        super().__init__(
            mapping=mapping,
            value_transform=value_transform,
            raw_value_transform=raw_value_transform,
            part_of_record_identity=part_of_record_identity,
            is_required=is_required,
            multi_mapping_concat_with=multi_mapping_concat_with
        )

##################################################
#
# PIPELINE DEFINITION AND IMPLEMENTATION:
#


@dataclass(frozen=True)
class SSPFieldDefinitions:
    locator_domain: _FieldDefBase
    page_url: _FieldDefBase
    location_name: _FieldDefBase
    street_address: _FieldDefBase
    city: _FieldDefBase
    state: _FieldDefBase
    zipcode: _FieldDefBase
    country_code: _FieldDefBase
    store_number: _FieldDefBase
    phone: _FieldDefBase
    location_type: _FieldDefBase
    latitude: _FieldDefBase
    longitude: _FieldDefBase
    hours_of_operation: _FieldDefBase
    raw_address: _FieldDefBase

    def to_dict(self) -> Dict[str, _FieldDefBase]:
        return {
            SgRecord.Headers.LOCATOR_DOMAIN: self.locator_domain,
            SgRecord.Headers.PAGE_URL: self.page_url,
            SgRecord.Headers.LOCATION_NAME: self.location_name,
            SgRecord.Headers.STREET_ADDRESS: self.street_address,
            SgRecord.Headers.CITY: self.city,
            SgRecord.Headers.STATE: self.state,
            SgRecord.Headers.ZIP: self.zipcode,
            SgRecord.Headers.COUNTRY_CODE: self.country_code,
            SgRecord.Headers.STORE_NUMBER: self.store_number,
            SgRecord.Headers.PHONE: self.phone,
            SgRecord.Headers.LOCATION_TYPE: self.location_type,
            SgRecord.Headers.LATITUDE: self.latitude,
            SgRecord.Headers.LONGITUDE: self.longitude,
            SgRecord.Headers.HOURS_OF_OPERATION: self.hours_of_operation,
            SgRecord.Headers.RAW_ADDRESS: self.raw_address
        }


class SimpleScraperPipeline:
    """
    This is a simple scraper framework that aims to be generic enough for most common scenarios.

    It is generic in the sense it doesn't care about the source of data, nor how the fields are transformed.
    """

    MISSING = "<MISSING>"

    MISSING_FIELD_DEF = _FieldDefBase(constant_value = MISSING, is_required=False)

    @staticmethod
    def field_definitions(locator_domain: _FieldDefBase,
                          page_url: _FieldDefBase,
                          location_name: _FieldDefBase,
                          street_address: _FieldDefBase,
                          city: _FieldDefBase,
                          state: _FieldDefBase,
                          zipcode: _FieldDefBase,
                          country_code: _FieldDefBase,
                          store_number: _FieldDefBase,
                          phone: _FieldDefBase,
                          location_type: _FieldDefBase,
                          latitude: _FieldDefBase,
                          longitude: _FieldDefBase,
                          hours_of_operation: _FieldDefBase,
                          raw_address: _FieldDefBase = MISSING_FIELD_DEF) -> SSPFieldDefinitions:
        """
        Use this method to generate the required definitions for all the record's fields.
        """
        return SSPFieldDefinitions(
            locator_domain=locator_domain,
            page_url=page_url,
            location_name=location_name,
            street_address=street_address,
            city=city,
            state=state,
            zipcode=zipcode,
            country_code=country_code,
            store_number=store_number,
            phone=phone,
            location_type=location_type,
            latitude=latitude,
            longitude=longitude,
            hours_of_operation=hours_of_operation,
            raw_address=raw_address
        )

    def __init__(self,
                 scraper_name: str,
                 data_fetcher: Optional[Callable[[], List[dict]]],
                 field_definitions: SSPFieldDefinitions,
                 fail_on_outlier=False,
                 post_process_filter: Optional[Callable[[SgRecord], bool]] = None,
                 duplicate_streak_failure_factor: float = SgRecordDeduper.DUPLICATE_STREAK_DEFAULT_FAILURE_FACTOR,
                 log_stats_interval: Optional[int] = 100):
        """
        Creates a declarative the scraper pipeline.
        When the pipeline is run, it writes `data.csv` to the current dir, and logs out useful info/errors.

        :param scraper_name:
            The name of this scraper, as will appear in log prefixes.
        :param data_fetcher:
            A lambda that fetches a raw list of location data, each record being a dict. Can be deeply nested or flat.
            Preferably, the `data_fetcher` returns a Generator by means of yielding the data. The rest of the pipeline
            is built to stream data all the way to the file, allowing for incremental writes.
        :param field_definitions:
            The full set of record definitions, as produced by the static `field_definitions()`.
        :param fail_on_outlier:
            Should the function except on missing a mandatory field, or just record an absence and skip it?
            Default is False.
        :param post_process_filter:
            Optionally, a lambda that filters records that have been processed. Returns `true` for each record to be kept.
        :param duplicate_streak_failure_factor:
            Pass-through to `SgRecordDeduper` (see for details)
        :param log_stats_interval:
            Optionally, supply an interval (in terms of written records) at which running stats would be logged.
        """

        self.__log = sglog.SgLogSetup().get_logger(logger_name=scraper_name)

        self.__data_fetcher = data_fetcher
        self.__field_definitions = field_definitions.to_dict()
        self.__post_process_filter = post_process_filter
        self.__fail_on_outlier = fail_on_outlier
        self.__log_stats_interval = log_stats_interval

        # record keeping
        record_identity_fields = set()
        self.__record_mapping = {}
        self.__constant_fields = {}
        self.__required_fields = []
        self.__field_transform = {}
        self.__raw_field_transform = {}
        self.__multimap_concat = {}

        for name,definition in self.__field_definitions.items():
            if definition.part_of_record_identity():
                record_identity_fields.add(name)

            if definition.mapping():
                self.__record_mapping[name] = definition.mapping()

            if definition.constant_value():
                self.__constant_fields[name] = definition.constant_value()

            if definition.is_required():
                self.__required_fields.append(name)

            if definition.value_transform():
                self.__field_transform[name] = definition.value_transform()

            if definition.raw_value_transform():
                self.__raw_field_transform[name] = definition.raw_value_transform()

            self.__multimap_concat[name] = definition.multi_mapping_concat_with()

        self.__deduper = SgRecordDeduper(record_id=SgRecordID(record_identity_fields),
                                         duplicate_streak_failure_factor=duplicate_streak_failure_factor)

    def run(self, data_file_path: str = 'data.csv') -> None:
        """
        Runs the framework, writing the contents to the data file.
        """
        if not self.__data_fetcher:
            raise Exception('The `data_fetcher` supplied to the constructor is empty; pipeline is not runnable.')

        start_sec = time.time()
        counter = 0
        no_dups_counter = 0
        dups_counter = 0

        with SgWriter(deduper=self.__deduper, data_file=data_file_path) as writer:
            initial_data = self.__data_fetcher()
            processed_data = self.__parse_data(locations=initial_data)

            # Body
            for row in processed_data:
                counter += 1
                dup_id = writer.write_row(row)
                if dup_id:
                    self.__log.debug(f"Duplicate record found with identity: {dup_id}")
                    dups_counter += 1
                else:
                    no_dups_counter += 1

                if self.__log_stats_interval and counter % self.__log_stats_interval == 0:
                    self.__log_stats(no_dups_counter=no_dups_counter, dups_counter=dups_counter, start_sec=start_sec)

        # log final stats
        self.__log_stats(no_dups_counter=no_dups_counter, dups_counter=dups_counter, start_sec=start_sec)

    def replace_logger(self, logger: Logger) -> None:
        """
        Replaces the default logger with another one.
        """
        self.__log = logger

    def __log_stats(self, no_dups_counter: int, dups_counter: int, start_sec: float) -> None:
        self.__log.debug(f"[Stats] [Seconds elapsed: {int(time.time() - start_sec)}] [Uniq Records: {no_dups_counter}] [Dups: {dups_counter}]")

    @staticmethod
    def __missing_if_empty(field: str) -> str:
        """
        Convenience function that defaults an empty field to `MISSING`
        """
        if not field:
            return SimpleScraperPipeline.MISSING
        else:
            return field

    def __parse_record(self, record: dict) -> Optional[dict]:
        """
        Decodes a record, and returns a list of decoded string values, sorted by the record's own keys
        :param record: The raw record as a dict.
        :return:
        """

        # instantiate with the constant fields
        result = self.__constant_fields
        for field, fieldPaths in self.__record_mapping.items():
            transformer = self.__field_transform.get(field)
            raw_transformer = self.__raw_field_transform.get(field)
            field_value_ar = []
            for fieldPath in fieldPaths:
                try:
                    next_value = drill_down_into(record, fieldPath, True)
                    field_value_ar.append(next_value)
                except KeyError:
                    if self.__fail_on_outlier:
                        self.__log.critical(f"Invalid path: {fieldPath} for field: {field} and record: {record}")
                        raise KeyError(f"Invalid field configuration. Invalid path: {fieldPath} for field: {field} and record: {record}")
                    else:
                        self.__log.warning(f"Invalid path: {fieldPath} for field: {field} and record: {record}")

            if raw_transformer:
                field_value = raw_transformer(field_value_ar)
            else:
                field_value_str_ar = [str(v).strip() for v in field_value_ar if str(v).strip()]
                field_value = self.__multimap_concat[field].join(field_value_str_ar)

            if transformer:
                field_value = transformer(field_value)

            if not field_value:
                try:
                    self.__required_fields.index(field)  # will only succeed if it's there
                    if self.__fail_on_outlier:
                        raise Exception(f"Required field: {field} was missing from record: {record}")
                    else:
                        self.__log.warning(f"Skipping record. Required field: {field} was missing from record: {record}")
                        return None
                except ValueError:
                    # it's not in the required_field list, ergo it's defaultable.
                    field_value = SimpleScraperPipeline.MISSING

            result[field] = self.__missing_if_empty(field_value)

        return result

    def parse_and_filter_raw_record(self, raw: dict) -> Optional[SgRecord]:
        """
        Parse and filter a raw datum. Returns `SgRecord` if it wasn't filtered, and logs the error if it has been.
        """
        normalised_dict = self.__parse_record(raw)
        if normalised_dict:
            record = SgRecord(raw=normalised_dict)

            if not self.__post_process_filter or self.__post_process_filter(record):
                return record
            else:
                self.__log.info(f"Record failed post-processing filter: {record}")

        return None

    def __parse_data(self, locations: Iterable[dict]) -> Generator[SgRecord, None, None]:
        """
        Parses the raw location data

        :param locations:
            A list of raw location data (as dict).
        :return:
            A list of location data, each raw_rec being a list of parsed string values, sorted by key.
        """
        for raw_rec in locations:
            parsed_rec = self.parse_and_filter_raw_record(raw_rec)
            if parsed_rec:
                yield parsed_rec
