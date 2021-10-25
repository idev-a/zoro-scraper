import sys

from .sgrequests import SgRequests, SgRequestsAsync, SgRequestError

if sys.version_info[0] >= 3:
    from .sghttpclient import SgHttpClient
