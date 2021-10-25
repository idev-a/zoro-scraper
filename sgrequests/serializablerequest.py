import json
from dataclasses import dataclass, asdict
from typing import Dict, Optional, Tuple, Union, List

from frozendict import frozendict

@dataclass(frozen=True)
class SerializableRequest:
    url: str
    method: str = 'GET'
    params: Dict[str, str] = frozendict()
    headers: Dict[str, str] = frozendict()
    data: Union[None, Dict[str, str], List[Tuple[str, str]]] = None
    verify: bool = True
    allow_redirects: bool = True
    cookies: Optional[dict] = None
    auth: Optional[Tuple[str, str]] = None
    timeout: Union[None, float, Tuple[float, float]] = None
    stream: bool = False
    cert: Optional[Union[str, Tuple[str, str]]] = None
    json: Optional[str] = None

    def serialize(self) -> str:
        """
        Known deficiency: will serialize tuples as lists; this may not matter in practice, as tuples have list accessors.
        """
        return json.dumps(asdict(self))

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
        )


if __name__ == "__main__":
    r = SerializableRequest(
        url= 'example.com',
        method='PUT',
        data={'a':'a'},
        params={'b':'b'},
        headers={'c':'c'},
        verify=True,
        allow_redirects=True,
        cookies={'d':'d'},
        auth=('basic','123:abc'),
        timeout=(1,2),
        stream=False,
        cert=('cert','123abc'),
        json='{}',
    )
    print(SerializableRequest.deserialize(r.serialize()))
