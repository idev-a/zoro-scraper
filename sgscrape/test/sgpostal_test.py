import unittest
from sgscrape.sgpostal import *

class SgPostalTest(unittest.TestCase):
    some_us_addr = '836 Prudential Drive, Jacksonville, Florida 12345'

    def test_usa_best_parser_equivalence(self):
        self.assertEqual(parse_address_usa(self.some_us_addr),
                         parse_address(USA_Best_Parser(), self.some_us_addr),
                         parse_address(ChainParser([USA_Fast_Parser(), International_Parser()]), self.some_us_addr))

    def test_usa_address_parsing(self):
        us_addr_fast = parse_address(USA_Fast_Parser(), self.some_us_addr)
        us_addr_best = parse_address_usa(self.some_us_addr)
        us_addr_generic = parse_address_intl(self.some_us_addr)

        self.assertEqual(us_addr_fast, SgAddress(country='US',
                                                 state='FL',  # <-- note the shortening
                                                 postcode='12345',
                                                 city='Jacksonville',
                                                 street_address_1='836 Prudential Dr',
                                                 street_address_2=None))

        # USA_Best_Parser() first tries to take values from USA_Fast_Parser(), and only then looks at International_Parser()
        self.assertEqual(us_addr_fast, us_addr_best)

        # Different than the US-specific parsers.
        self.assertEqual(us_addr_generic, SgAddress(country=None,   # <- note the lack of country; not added automatically
                                                    state='Florida',
                                                    postcode='12345',
                                                    city='Jacksonville',
                                                    street_address_1='836 Prudential Drive',
                                                    street_address_2=None))

    def test_usa_field_substitutions(self):
        us_addr_fast = parse_address(USA_Fast_Parser(), self.some_us_addr, street_address='something else', postcode='54321')
        us_addr_best = parse_address_usa(self.some_us_addr, street_address='something else', postcode='54321')

        self.assertEqual(us_addr_fast, SgAddress(country='US',
                                                 state='FL',  # <-- note the shortening
                                                 postcode='54321',
                                                 city='Jacksonville',
                                                 street_address_1='Something Else',  # <-- note the capitalization
                                                 street_address_2=None))

        # USA_Best_Parser() first tries to take values from USA_Fast_Parser(), and only then looks at International_Parser()
        self.assertEqual(us_addr_fast, us_addr_best)

    def test_international_address_parsing(self):
        # Parliament hill in Ottawa
        cdn_parliament = parse_address_intl("Wellington St, Ottawa, on K1A 0A9 Canada")
        self.assertEqual(cdn_parliament, SgAddress(country='Canada',
                                                   state='ON',  # <-- note the uppercasing
                                                   postcode='K1A 0A9',
                                                   city='Ottawa',
                                                   street_address_1='Wellington St',
                                                   street_address_2=None))

    def test_country_code_uppercasing(self):
        # Sherlock Holmes' house
        sherlock_holmes_home = parse_address_intl("221B Baker Street, London, NW1 6XE, uk")
        self.assertEqual(sherlock_holmes_home, SgAddress(country='UK',  # <-- two-letter codes uppercased
                                                         state=None,
                                                         postcode='NW1 6XE',
                                                         city='London',
                                                         street_address_1='221B Baker Street',
                                                         street_address_2=None))

    def test_district_in_place_of_street_address_in_intnl(self):
        # Buckingham palace - city district in place of street address, if former is present and latter missing
        buckingham_palace = parse_address_intl("Westminster, London SW1A 1AA, United Kingdom")
        self.assertEqual(buckingham_palace.street_address_1, 'Westminster')

    def test_intnl_addr_field_substitutions(self):
        # CN Tower address - adding a country and changing the postal code
        cn_tower = parse_address_intl("290 Bremner Blvd Toronto, Ontario M5V 3L9",
                                      postcode='XXX XXX',
                                      country='Wakanda')

        self.assertEqual(cn_tower, SgAddress(country='Wakanda',  # <-- added
                                             state='Ontario',
                                             postcode='XXX XXX',  # <-- changed
                                             city='Toronto',
                                             street_address_1='290 Bremner Blvd',
                                             street_address_2=None))

    def test_field_no_substitutions_for_none_of_empty_str(self):
        """
        You can only substitute fields for non-empty strings (not None or '')
        """

        us_addr_fast = parse_address(USA_Fast_Parser(), self.some_us_addr, street_address=None,postcode='')
        us_addr_best = parse_address_usa(self.some_us_addr, street_address=None, postcode='')
        us_addr_intl = parse_address_intl(self.some_us_addr, street_address=None, postcode='')

        self.assertIsNotNone(us_addr_fast.street_address_1)
        self.assertIsNotNone(us_addr_best.street_address_1)
        self.assertIsNotNone(us_addr_intl.street_address_1)

        self.assertIsNot(us_addr_fast.postcode, '')
        self.assertIsNot(us_addr_best.postcode, '')
        self.assertIsNot(us_addr_intl.postcode, '')


    def test_failed_us_parser(self):
        bad_us_addr = 'Legacy Salmon Creek Medical Center campus, Medical Office Building A,'
        with self.assertRaises(Exception):
            parse_address(USA_Fast_Parser(), bad_us_addr)

    def test_fall_back_in_chained_parser(self):
        bad_us_addr = 'Legacy Salmon Creek Medical Center campus, Medical Office Building A,'
        result_chained = parse_address_usa(bad_us_addr)
        result_libpostal = parse_address_intl(bad_us_addr)

        self.assertTrue(isinstance(result_chained, SgAddress))
        self.assertEqual(result_chained, result_libpostal)

if __name__ == '__main__':
    unittest.main()
