from builtins import Exception
from collections import defaultdict
from dataclasses import dataclass
from functools import partial
from typing import List, Optional, Union

from postal.parser import parse_address as parse_address_native
from scourgify import normalize_address_record

from sgscrape.simple_utils import or_else

@dataclass(frozen=True)
class SgAddress:
    """
    Canonical representation of address.
    """
    country: Optional[str] = None
    state: Optional[str] = None
    postcode: Optional[str] = None
    city: Optional[str] = None
    street_address_1: Optional[str] = None
    street_address_2: Optional[str] = None


class ChainedException(Exception):
    def __init__(self, exceptions: List[Exception]):
        self.__exceptions = exceptions

    def __str__(self) -> str:
        return str([str(e) for e in self.__exceptions])

    def exceptions(self) -> List[Exception]:
        return self.__exceptions


class EmptyValueException(Exception): pass


class AddressParser:
    """
    The base class for parser functionality
    """
    def parse(self,
              raw_address: Optional[str] = None,
              house_name: Optional[str] = None,
              street_address: Optional[str] = None,
              postcode: Optional[str] = None,
              city: Optional[str] = None,
              state: Optional[str] = None,
              country: Optional[str] = None) -> SgAddress:
        """
        Attempts to parse and normalize addresses.

        :param raw_address: A raw address string, if it exists. (Note: DO NOT create your own using concatenation!)
        :param house_name: Is there a already-available name to the place?
        :param street_address: Is there an already-available street address?
        :param postcode: Is there an already-available postcode?
        :param city: Is there an already-available city?
        :param state: Is there an already-available state?
        :param country: Is there an already-available country?

        :return an SgAddress
        """
        raise Exception("Parser unimplemented.")


class USA_Fast_Parser(AddressParser):
    """
    Specialized for US addresses. Uses usaddress under the hood.
    """
    def parse(self,
              raw_address: Optional[str] = None,
              house_name: Optional[str] = None,
              street_address: Optional[str] = None,
              postcode: Optional[str] = None,
              city: Optional[str] = None,
              state: Optional[str] = None,
              country: Optional[str] = None) -> SgAddress:
        """
        Attempts to parse and normalize addresses.

        :param raw_address: A raw address string, if it exists. (Note: DO NOT create your own using concatenation!)
        :param house_name: Is there a already-available name to the place?
        :param street_address: Is there an already-available street address?
        :param postcode: Is there an already-available postcode?
        :param city: Is there an already-available city?
        :param state: Is there an already-available state?
        :param country: Is there an already-available country?

        :return an SgAddress
        """
        structured = normalize_address_record(raw_address)

        return SgAddress(country='US',
                         state=_uppercase_if_abbreviated_else_titlecase_or_none(or_else(state, structured['state'])),
                         postcode=or_else(or_else(postcode, or_else(structured['postal_code'], '')).upper(), None),
                         city=or_else(or_else(city, or_else(structured['city'], '')).title(), None),
                         street_address_1=or_else(or_else(street_address, or_else(structured['address_line_1'], '')).title(), None),
                         street_address_2=or_else(or_else(structured['address_line_2'], '').title(), None))


class International_Parser(AddressParser):
    def parse(self,
              raw_address: Optional[str] = None,
              house_name: Optional[str] = None,
              street_address: Optional[str] = None,
              postcode: Optional[str] = None,
              city: Optional[str] = None,
              state: Optional[str] = None,
              country: Optional[str] = None) -> Union[SgAddress, Exception]:
        """
        Attempts to parse and normalize addresses.

        :param raw_address: A raw address string, if it exists. (Note: DO NOT create your own using concatenation!)
        :param house_name: Is there a already-available name to the place?
        :param street_address: Is there an already-available street address?
        :param postcode: Is there an already-available postcode?
        :param city: Is there an already-available city?
        :param state: Is there an already-available state?
        :param country: Is there an already-available country?

        :return an SgAddress
        """

        parsed = parse_address_native(raw_address)
        structured = defaultdict(str)
        for value, key in parsed:
            structured[key] = value

        if house_name:  structured['house'] = house_name
        if postcode:    structured['postcode'] = postcode
        if city:        structured['city'] = city
        if state:       structured['state'] = state
        if country:     structured['country'] = country

        house = or_else(structured['house_number'], structured['house'])
        street_address = f"{house} {structured['road']}".strip().title()

        return SgAddress(country=_uppercase_if_abbreviated_else_titlecase_or_none(structured['country']),
                         state=_uppercase_if_abbreviated_else_titlecase_or_none(structured['state']),
                         postcode=or_else(structured['postcode'].upper(), None),
                         city=or_else(structured['city'].title(), None),
                         street_address_1=or_else(or_else(street_address, structured['city_district'].title()), None),
                         street_address_2=or_else(f"{structured['unit']} {structured['level']} {structured['entrance']}".strip().title(), None))


class ChainParser(AddressParser):
    def __init__(self, chained_parsers: List[AddressParser]):
        """
        Chains parser invocations one after the other, s.t. their values will replace any `default_feild_value` that
        wasn't filled in previously. Will short-circuit after all values have been filled in.
        """
        assert len(chained_parsers) > 0

        self.__parsers = chained_parsers

    def parse(self,
              raw_address: Optional[str] = None,
              house_name: Optional[str] = None,
              street_address: Optional[str] = None,
              postcode: Optional[str] = None,
              city: Optional[str] = None,
              state: Optional[str] = None,
              country: Optional[str] = None) -> Union[SgAddress, Exception]:
        """
        Attempts to parse and normalize addresses.

        :param raw_address: A raw address string, if it exists. (Note: DO NOT create your own using concatenation!)
        :param house_name: Is there a already-available name to the place?
        :param street_address: Is there an already-available street address?
        :param postcode: Is there an already-available postcode?
        :param city: Is there an already-available city?
        :param state: Is there an already-available state?
        :param country: Is there an already-available country?

        :return an SgAddress
        """

        addr = None
        exceptions = None

        for p in self.__parsers:
            if not addr or not (addr.country or addr.state or addr.postcode or addr.street_address_1 or addr.street_address_2):
                try:
                    addr_prime = p.parse(raw_address=raw_address,
                                         house_name=house_name,
                                         street_address=street_address,
                                         postcode=postcode,
                                         city=city,
                                         state=state,
                                         country=country)

                    if addr:
                        addr = SgAddress(
                            country=or_else(addr.country, addr_prime.country),
                            state=or_else(addr.state, addr_prime.state),
                            postcode=or_else(addr.postcode, addr_prime.postcode),
                            city=or_else(addr.city, addr_prime.city),
                            street_address_1=or_else(addr.street_address_1, addr_prime.street_address_1),
                            street_address_2=or_else(addr.street_address_2, addr_prime.street_address_2)
                        )
                    else:
                        addr = addr_prime
                except Exception as e:
                    exceptions = ChainedException((exceptions.exceptions() if exceptions else []) + [e])
        if addr:
            return addr
        elif exceptions:
            raise exceptions
        else:
            raise EmptyValueException()


class USA_Best_Parser(ChainParser):
    def __init__(self):
        """
        Chains parsing attempts from `USA_Fast_Parser` and `International_Parser`, so it benefits from both parsers.
        """
        super().__init__(chained_parsers=[USA_Fast_Parser(), International_Parser()])


def _uppercase_if_abbreviated_else_titlecase_or_none(name: Optional[str]) -> Optional[str]:
    """
    Returns None if name is empty ('' or None)
    Uppercases if len == 2
    Title-cases otherwise.
    """
    if not name:
        return None
    elif len(name) == 2:
        return name.upper()
    else:
        return name.title()


def parse_address(parser: AddressParser,
                  raw_address: Optional[str] = None,
                  house_name: Optional[str] = None,
                  street_address: Optional[str] = None,
                  postcode: Optional[str] = None,
                  city: Optional[str] = None,
                  state: Optional[str] = None,
                  country: Optional[str] = None) -> SgAddress:
    """
    Attempts to parse and normalize addresses.

    Depending on the parser used, it can fail with an exception.

    :param parser: Which parser to use. See `USA_Fast_Parser()`, `USA_Best_Parser()`, `International_Parser()`.
                   `ChainParser(..)` lets you combine parsers, and extending from `AddressParser()` allows you to create
                   custom parsers.
    :param raw_address: A raw address string, if it exists. (Note: DO NOT create your own using concatenation!)
    :param house_name: Is there a already-available name to the place?
    :param street_address: Is there an already-available street address?
    :param postcode: Is there an already-available postcode?
    :param city: Is there an already-available city?
    :param state: Is there an already-available state?
    :param country: Is there an already-available country?

    :return an SgAddress
    """
    return parser.parse(raw_address=raw_address,
                        house_name=house_name,
                        street_address=street_address,
                        postcode=postcode,
                        city=city,
                        state=state,
                        country=country)

"""
A shortcut for using `USA_Best_Parser()`
"""
parse_address_usa = partial(parse_address, USA_Best_Parser())

"""
A shortcut for using `International_Parser()`
"""
parse_address_intl = partial(parse_address, International_Parser())
