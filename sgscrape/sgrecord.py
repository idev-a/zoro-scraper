from dataclasses import MISSING, dataclass
from os import PRIO_PROCESS, defpath
from typing import List, Dict, Optional, Literal, Union


class SgRecord:
    """
    A canonical representation of a SafeGraph record
    """

    # MISSING = "<MISSING>"
    MISSING = ""

    @dataclass(frozen=True)
    class Headers:
        # PAGE_URL            = "page_url"
        # LOCATION_NAME       = "location_name"
        # STREET_ADDRESS      = "street_address"
        # CITY                = "city"
        # STATE               = "state"
        # ZIP                 = "zip"
        # COUNTRY_CODE        = "country_code"
        # STORE_NUMBER        = "store_number"
        # PHONE               = "phone"
        # LOCATION_TYPE       = "location_type"
        # LATITUDE            = "latitude"
        # LONGITUDE           = "longitude"
        # LOCATOR_DOMAIN      = "locator_domain"
        # HOURS_OF_OPERATION  = "hours_of_operation"
        # RAW_ADDRESS         = "raw_address"
        RECORD_ID           = "record_id"

        # Customization (ZORO)
        ITEM_URL                            = "item_url"
        CATEGORY                            = "category"
        NAME                                = "name"
        # TITLE                               = "title",
        COMPANY                             = "company"
        PRICE                               = "price"
        DESCRIPTION                         = "description"
        COUNTRY_OF_ORIGIN                   = "country_of_origin"
        MF_NUMBER                           = "mf_number"
        MF_NUMBER_DUP                       = "mf_number_dup"
        SHIPPING_DAY                        = "shipping_day"
        JPEG                                = "jpeg"
        JPEG_FILE_NAME                      = "jpeg_file_name"
        WIDTH                               = "width"
        WIDTH_UNIT                          = "width_unit"
        HEIGHT                              = "height"
        HEIGHT_UNIT                         = "height_unit"
        DEPTH                               = "depth"
        WEIGHT                              = "weight"
        WEIGHT_UOM                          = "weight_uom"
        GSA_LOWEST_PRICE                    = "gsa_lowest_price"
        GSA_LOWEST_SELLER_NAME              = "gsa_lowest_seller_name"
        COST_PERCENT_COMPARED_TO_LOWEST     = "cost_percent_compared_to_lowest"
        ON_CPO                              = "on_cpo"
        GSA_PRICE                           = "gsa_price"
        COM_PRICE                           = "com_price"
        CURRENT_LIST_PRICE                  = "current_list_price"
        GSA_2ND_LOWEST_SELLER_PRICE         = "gsa_2nd_lowest_seller_price"
        GSA_2ND_LOWEST_SELLER_NAME          = "gsa_2nd_lowest_seller_name"
        GSA_2ND_HIGHEST_PRICE               = "gsa_2nd_highest_price"
        GSA_2ND_HIGHEST_PRICE_SELLER_NAME   = "gsa_2nd_highest_price_seller_name"
        ON_NEW_BREAKER_LIST                 = "on_new_breaker_list"
        ON_OLD_LIST                         = "on_old_list"


        # HEADER_ROW = [PAGE_URL,
        #               LOCATION_NAME,
        #               STREET_ADDRESS,
        #               CITY,
        #               STATE,
        #               ZIP,
        #               COUNTRY_CODE,
        #               STORE_NUMBER,
        #               PHONE,
        #               LOCATION_TYPE,
        #               LATITUDE,
        #               LONGITUDE,
        #               LOCATOR_DOMAIN,
        #               HOURS_OF_OPERATION,
        #               RAW_ADDRESS]

        ZORO_ROW = [
            ITEM_URL,
            CATEGORY,
            NAME,
            # TITLE,
            COMPANY,
            PRICE,
            DESCRIPTION,
            COUNTRY_OF_ORIGIN,
            MF_NUMBER,
            MF_NUMBER_DUP,
            SHIPPING_DAY,
            JPEG,
            JPEG_FILE_NAME,
            WIDTH,
            WIDTH_UNIT,
            HEIGHT,
            HEIGHT_UNIT,
            DEPTH,
            WEIGHT,
            WEIGHT_UOM,
            GSA_LOWEST_PRICE,
            GSA_LOWEST_SELLER_NAME,
            COST_PERCENT_COMPARED_TO_LOWEST,
            ON_CPO,
            GSA_PRICE,
            COM_PRICE,
            CURRENT_LIST_PRICE,
            GSA_2ND_LOWEST_SELLER_PRICE,
            GSA_2ND_LOWEST_SELLER_NAME,
            GSA_2ND_HIGHEST_PRICE,
            GSA_2ND_HIGHEST_PRICE_SELLER_NAME,
            ON_NEW_BREAKER_LIST,
            ON_OLD_LIST
        ]

        BH_ROW = []

        HEADER_ROW = ZORO_ROW + BH_ROW

        HEADER_ROW_WITH_REC_ID = HEADER_ROW + [RECORD_ID]

        # HeaderUnion = Literal[PAGE_URL,         # noqa: these will never change
        #                       LOCATION_NAME,
        #                       STREET_ADDRESS,
        #                       CITY,
        #                       STATE,
        #                       ZIP,
        #                       COUNTRY_CODE,
        #                       STORE_NUMBER,
        #                       PHONE,
        #                       LOCATION_TYPE,
        #                       LATITUDE,
        #                       LONGITUDE,
        #                       LOCATOR_DOMAIN,
        #                       HOURS_OF_OPERATION,
        #                       RAW_ADDRESS]

        HeaderUnion = Literal[
            ITEM_URL,
            CATEGORY,
            NAME,
            # TITLE,
            COMPANY,
            PRICE,
            DESCRIPTION,
            COUNTRY_OF_ORIGIN,
            MF_NUMBER,
            MF_NUMBER_DUP,
            SHIPPING_DAY,
            JPEG,
            JPEG_FILE_NAME,
            WIDTH,
            WIDTH_UNIT,
            HEIGHT,
            HEIGHT_UNIT,
            DEPTH,
            WEIGHT,
            WEIGHT_UOM,
            GSA_LOWEST_PRICE,
            GSA_LOWEST_SELLER_NAME,
            COST_PERCENT_COMPARED_TO_LOWEST,
            ON_CPO,
            GSA_PRICE,
            COM_PRICE,
            CURRENT_LIST_PRICE,
            GSA_2ND_LOWEST_SELLER_PRICE,
            GSA_2ND_LOWEST_SELLER_NAME,
            GSA_2ND_HIGHEST_PRICE,
            GSA_2ND_HIGHEST_PRICE_SELLER_NAME,
            ON_NEW_BREAKER_LIST,
            ON_OLD_LIST
        ]

    def __init__(self,
                 raw: Optional[Dict[str, str]] = None,
                 item_url=MISSING,
                 category=MISSING,
                 name=MISSING,
                 company=MISSING,
                 price=MISSING,
                 description=MISSING,
                 country_of_origin=MISSING,
                 mf_number=MISSING,
                 mf_number_dup=MISSING,
                 shipping_day=MISSING,
                 jpeg=MISSING,
                 jpeg_file_name=MISSING,
                 width=MISSING,
                 width_unit=MISSING,
                 height=MISSING,
                 height_unit=MISSING,
                 depth=MISSING,
                 weight=MISSING,
                 weight_uom=MISSING,
                 gsa_lowest_price=MISSING,
                 gsa_lowest_seller_name=MISSING,
                 cost_percent_compared_to_lowest=MISSING,
                 on_cpo=MISSING,
                 gsa_price=MISSING,
                 com_price=MISSING,
                 current_list_price=MISSING,
                 gsa_2nd_lowest_seller_price=MISSING,
                 gsa_2nd_lowest_seller_name=MISSING,
                 gsa_2nd_highest_price=MISSING,
                 gsa_2nd_highest_price_seller_name=MISSING,
                 on_new_breaker_list=MISSING,
                 on_old_list=MISSING
                ):
        """
        Constructs an SgRecord either from a dictionary (`raw`), or else from the by-name parameters.

        if `raw` is not `None`, fields will be populated using `raw`;
        otherwise, they will be populated using the by-name params.

        The dict keys in `raw` are keyed on `SgRecord.Headers`
        """

        if raw is not None:
            self.__from_dict(raw)
        else:
            # self.__page_url = SgRecord.__normalize_or_missing(page_url)
            # self.__location_name = SgRecord.__normalize_or_missing(location_name)
            # self.__street_address = SgRecord.__normalize_or_missing(street_address)
            # self.__city = SgRecord.__normalize_or_missing(city)
            # self.__state = SgRecord.__normalize_or_missing(state)
            # self.__zip_postal = SgRecord.__normalize_or_missing(zip_postal)
            # self.__country_code = SgRecord.__normalize_or_missing(country_code)
            # self.__store_number = SgRecord.__normalize_or_missing(store_number)
            # self.__phone = SgRecord.__normalize_or_missing(phone)
            # self.__location_type = SgRecord.__normalize_or_missing(location_type)
            # self.__latitude = SgRecord.__normalize_or_missing(latitude)
            # self.__longitude = SgRecord.__normalize_or_missing(longitude)
            # self.__locator_domain = SgRecord.__normalize_or_missing(locator_domain)
            # self.__hours_of_operation = SgRecord.__normalize_or_missing(hours_of_operation)
            # self.__raw_address = SgRecord.__normalize_or_missing(raw_address)

            self.__item_url = SgRecord.__normalize_or_missing(item_url)
            self.__category  = SgRecord.__normalize_or_missing(category)
            # self.__title = SgRecord.__normalize_or_missing(title)
            self.__name = SgRecord.__normalize_or_missing(name)
            self.__company = SgRecord.__normalize_or_missing(company)
            self.__price = SgRecord.__normalize_or_missing(price)
            self.__description = SgRecord.__normalize_or_missing(description)
            self.__country_of_origin = SgRecord.__normalize_or_missing(country_of_origin)
            self.__mf_number = SgRecord.__normalize_or_missing(mf_number)
            self.__mf_number_dup = SgRecord.__normalize_or_missing(mf_number_dup)
            self.__shipping_day = SgRecord.__normalize_or_missing(shipping_day)
            self.__jpeg = SgRecord.__normalize_or_missing(jpeg)
            self.__jpeg_file_name = SgRecord.__normalize_or_missing(jpeg_file_name)
            self.__width = SgRecord.__normalize_or_missing(width)
            self.__width_unit = SgRecord.__normalize_or_missing(width_unit)
            self.__height = SgRecord.__normalize_or_missing(height)
            self.__height_unit = SgRecord.__normalize_or_missing(height_unit)
            self.__depth = SgRecord.__normalize_or_missing(depth)
            self.__weight = SgRecord.__normalize_or_missing(weight)
            self.__weight_uom = SgRecord.__normalize_or_missing(weight_uom)
            self.__gsa_lowest_price = SgRecord.__normalize_or_missing(gsa_lowest_price)
            self.__gsa_lowest_seller_name = SgRecord.__normalize_or_missing(gsa_lowest_seller_name)
            self.__cost_percent_compared_to_lowest = SgRecord.__normalize_or_missing(cost_percent_compared_to_lowest)
            self.__on_cpo = SgRecord.__normalize_or_missing(on_cpo)
            self.__gsa_price = SgRecord.__normalize_or_missing(gsa_price)
            self.__com_price = SgRecord.__normalize_or_missing(weight_uom)
            self.__current_list_price = SgRecord.__normalize_or_missing(current_list_price)
            self.__gsa_2nd_lowest_seller_price = SgRecord.__normalize_or_missing(gsa_2nd_lowest_seller_price)
            self.__gsa_2nd_lowest_seller_name = SgRecord.__normalize_or_missing(gsa_2nd_lowest_seller_name)
            self.__gsa_2nd_highest_price = SgRecord.__normalize_or_missing(gsa_2nd_highest_price)
            self.__gsa_2nd_highest_price_seller_name = SgRecord.__normalize_or_missing(gsa_2nd_highest_price_seller_name)
            self.__on_new_breaker_list = SgRecord.__normalize_or_missing(on_new_breaker_list)
            self.__on_old_list = SgRecord.__normalize_or_missing(on_old_list)

        self.__as_row = self.__to_row()
        self.__as_dict = self.__to_dict()

    def __from_dict(self, raw: Dict[str, str]):
        """
        Constructs an SgRecord from a str->str dictionary that has the canonical names as keys.
        """
        # self.__page_url = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.PAGE_URL))
        # self.__location_name = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.LOCATION_NAME))
        # self.__street_address = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.STREET_ADDRESS))
        # self.__city = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.CITY))
        # self.__state = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.STATE))
        # self.__zip_postal = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.ZIP))
        # self.__country_code = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.COUNTRY_CODE))
        # self.__store_number = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.STORE_NUMBER))
        # self.__phone = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.PHONE))
        # self.__location_type = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.LOCATION_TYPE))
        # self.__latitude = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.LATITUDE))
        # self.__longitude = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.LONGITUDE))
        # self.__locator_domain = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.LOCATOR_DOMAIN))
        # self.__hours_of_operation = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.HOURS_OF_OPERATION))
        # self.__raw_address = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.RAW_ADDRESS))

        self.__item_url = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.ITEM_URL))
        self.__category  = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.CATEGORY))
        # self.__title = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.TITLE))
        self.__name = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.NAME))
        self.__company = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.COMPANY))
        self.__price = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.PRICE))
        self.__description = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.DESCRIPTION))
        self.__country_of_origin = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.COUNTRY_OF_ORIGIN))
        self.__mf_number = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.MF_NUMBER))
        self.__mf_number_dup = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.MF_NUMBER_DUP))
        self.__shipping_day = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.SHIPPING_DAY))
        self.__jpeg = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.JPEG))
        self.__jpeg_file_name = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.JPEG_FILE_NAME))
        self.__width = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.WIDTH))
        self.__width_unit = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.WIDTH_UNIT))
        self.__height = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.HEIGHT))
        self.__height_unit = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.HEIGHT_UNIT))
        self.__depth = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.DEPTH))
        self.__weight = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.WEIGHT))
        self.__weight_uom = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.WEIGHT_UOM))
        self.__gsa_lowest_price = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.GSA_LOWEST_PRICE))
        self.__gsa_lowest_seller_name = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.GSA_LOWEST_SELLER_NAME))
        self.__cost_percent_compared_to_lowest = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.COST_PERCENT_COMPARED_TO_LOWEST))
        self.__on_cpo = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.ON_CPO))
        self.__gsa_price = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.GSA_PRICE))
        self.__com_price = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.COM_PRICE))
        self.__current_list_price = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.CURRENT_LIST_PRICE))
        self.__gsa_2nd_lowest_seller_price = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.GSA_2ND_LOWEST_SELLER_PRICE))
        self.__gsa_2nd_lowest_seller_name = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.GSA_2ND_LOWEST_SELLER_NAME))
        self.__gsa_2nd_highest_price = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.GSA_2ND_HIGHEST_PRICE))
        self.__gsa_2nd_highest_price_seller_name = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.GSA_2ND_HIGHEST_PRICE_SELLER_NAME))
        self.__on_new_breaker_list = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.ON_NEW_BREAKER_LIST))
        self.__on_old_list = SgRecord.__normalize_or_missing(raw.get(SgRecord.Headers.ON_OLD_LIST))

    def item_url(self):    
        return self.__item_url
    
    def category(self):
        return self.__category

    # def title(self):
    #     return self.__title

    def name(self):    
        return self.__name

    def company(self):
        return self.__company

    def price(self):
        return self.__price

    def description(self):
        return self.__description

    def country_of_origin(self):
        return self.__country_of_origin

    def mf_number(self):
        return self.__mf_number

    def mf_number_dup(self):
        return self.__mf_number_dup

    def shipping_day(self):
        return self.__shipping_day

    def jpeg(self):
        return self.__jpeg

    def jpeg_file_name(self):
        return self.__jpeg_file_name

    def width(self):
        return self.__width

    def width_unit(self):
        return self.__width_unit

    def height(self):
        return self.__height

    def height_unit(self):
        return self.__height_unit

    def depth(self):
        return self.__depth

    def weight(self):
        return self.__weight

    def weight_uom(self):
        return self.__weight_uom

    def gsa_lowest_price(self):
        return self.__gsa_lowest_price

    def gsa_lowest_seller_name(self):
        return self.__gsa_lowest_seller_name

    def cost_percent_compared_to_lowest(self):
        return self.__cost_percent_compared_to_lowest

    def on_cpo(self):
        return self.__on_cpo

    def gsa_price(self):
        return self.__gsa_price

    def com_price(self):
        return self.__com_price

    def current_list_price(self):
        return self.__current_list_price

    def gsa_2nd_lowest_seller_price(self):
        return self.__gsa_2nd_lowest_seller_price

    def gsa_2nd_lowest_seller_name(self):
        return self.__gsa_2nd_lowest_seller_name

    def gsa_2nd_highest_price(self):
        return self.__gsa_2nd_highest_price

    def gsa_2nd_highest_price_seller_name(self):
        return self.__gsa_2nd_highest_price_seller_name

    def on_new_breaker_list(self):
        return self.__on_new_breaker_list

    def on_old_list(self):
        return self.__on_old_list

    # def page_url(self) -> str:
    #     return self.__page_url

    # def location_name(self) -> str:
    #     return self.__location_name

    # def street_address(self) -> str:
    #     return self.__street_address

    # def city(self) -> str:
    #     return self.__city

    # def state(self) -> str:
    #     return self.__state

    # def zip_postal(self) -> str:
    #     return self.__zip_postal

    # def country_code(self) -> str:
    #     return self.__country_code

    # def store_number(self) -> str:
    #     return self.__store_number

    # def phone(self) -> str:
    #     return self.__phone

    # def location_type(self) -> str:
    #     return self.__location_type

    # def latitude(self) -> str:
    #     return self.__latitude

    # def longitude(self) -> str:
    #     return self.__longitude

    # def locator_domain(self) -> str:
    #     return self.__locator_domain

    # def hours_of_operation(self) -> str:
    #     return self.__hours_of_operation

    # def raw_address(self) -> str:
    #     return self.__raw_address

    def as_row(self) -> List[str]:
        return self.__as_row

    def as_dict(self) -> Dict[str, Optional[str]]:
        return self.__as_dict

    def __to_dict(self) -> Dict[str, Optional[str]]:
        return dict(zip(SgRecord.Headers.HEADER_ROW, self.as_row()))

    def __to_row(self) -> List[str]:
        # return [self.__page_url,
        #         self.__location_name,
        #         self.__street_address,
        #         self.__city,
        #         self.__state,
        #         self.__zip_postal,
        #         self.__country_code,
        #         self.__store_number,
        #         self.__phone,
        #         self.__location_type,
        #         self.__latitude,
        #         self.__longitude,
        #         self.__locator_domain,
        #         self.__hours_of_operation,
        #         self.__raw_address]

        return [
            self.__item_url,
            self.__category,
            # self.__title,
            self.__name,
            self.__company,
            self.__price,
            self.__description,
            self.__country_of_origin,
            self.__mf_number,
            self.__mf_number_dup,
            self.__shipping_day,
            self.__jpeg,
            self.__jpeg_file_name,
            self.__width,
            self.__width_unit,
            self.__height,
            self.__height_unit,
            self.__depth,
            self.__weight,
            self.__weight_uom,
            self.__gsa_lowest_price,
            self.__gsa_lowest_seller_name,
            self.__cost_percent_compared_to_lowest,
            self.__on_cpo,
            self.__gsa_price,
            self.__com_price,
            self.__current_list_price,
            self.__gsa_2nd_lowest_seller_price,
            self.__gsa_2nd_lowest_seller_name,
            self.__gsa_2nd_highest_price,
            self.__gsa_2nd_highest_price_seller_name,
            self.__on_new_breaker_list,
            self.__on_old_list,
        ]

    def __str__(self) -> str:
        return str(self.__to_dict())

    @staticmethod
    def __normalize_or_missing(field_value: Optional[object]) -> str:
        if isinstance(field_value, float):
            field_value = round(field_value, 5)

        if field_value is not None:
            stripped = str(field_value).strip()
            return stripped or SgRecord.MISSING
        else:
            return SgRecord.MISSING
