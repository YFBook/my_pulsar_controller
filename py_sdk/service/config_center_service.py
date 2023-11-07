import time
from async_lru import alru_cache
from typing import List, Optional, Union, Any

from pydantic import BaseModel
import aiohttp
from py_sdk.util.cypt import decrypt
from urllib.parse import quote
from enum import Enum
from py_sdk.util.decorators import retry


class DBType(int, Enum):
    TIDB = 0
    AUTH_CENTER = 1
    SP_API_SYNC = 2


class DbInfo(BaseModel):
    # belongCode: str
    dbCode: str
    dbName: str
    envKey: str
    host: str
    pwd: str
    serviceType: str
    username: str
    port: int

    def decrypt_pwd(self, key: str) -> None:
        self.pwd = decrypt(string=self.pwd, key=key)


class RedisInfo(BaseModel):
    host: str
    port: int
    pwd: str
    redisCode: str
    redisSelect: int


class MasterSlave(BaseModel):
    master: List[DbInfo]
    slave: List[DbInfo]


class ResourceResponseData(BaseModel):
    code: str
    db: Optional[MasterSlave]
    redis: Optional[MasterSlave]


class ResourceResponse(BaseModel):
    code: int
    # data: ResourceResponseData
    # data: Union[list, dict]
    data: Any
    msg: str
    traceId: str


class GetDomainsData(BaseModel):
    companyId: str
    customerId: str
    domain: str
    envKey: str
    envMark: str
    zid: int


class ConfigCenterService(object):
    def __init__(
        self,
        host,
        app_id="",
        key="",
        buss_code="",
        ex=3600,
        connector_limit=100,
    ):
        """
        :param host: 域名 如： 'http://gateway-test.ak.xyz' 末尾不要斜杠
        :param app_id: app_id
        :param key: 解密密码的key
        :param ex: 本地缓存过期的时间
        如果用于请求db配置, 那么key、app_id、buss_code都不能为空
        """
        if host is None or app_id is None or key is None:
            raise Exception("host或者app_id或者key为None")
        self.host = host
        self.key = key
        self.ex = ex
        self.app_id = app_id
        self.buss_code = buss_code
        self.http_session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=connector_limit)
        )

    async def close(self):
        await self.http_session.close()

    async def __get(self, url, params) -> ResourceResponse:
        """
        通过配置中心api获取加密后的数据库连接信息
        :return:
        """
        res = await self.http_session.request(
            method="GET", url=url, timeout=30, params=params
        )
        content = await res.json()
        res.close()
        resp_obj = ResourceResponse(**content)
        if resp_obj.code != 200:
            raise Exception(
                f"请求配置中心失败, url= {url}, params = {params}, code={resp_obj.code}, msg={resp_obj.msg}"
            )
        return resp_obj

    async def __post(self, url, params):
        res = await self.http_session.request(
            method="POST", url=url, timeout=30, json=params
        )
        content = await res.json()
        res.close()
        resp_obj = ResourceResponse(**content)
        if resp_obj.code != 200:
            raise Exception(
                f"请求配置中心失败, url= {url}, params = {params}, code={resp_obj.code}, msg={resp_obj.msg}"
            )
        return resp_obj.data

    # -------------------------------获取db配置---------------------------------
    def __pasrse_db_info(self, resp_obj, db_name=""):
        if not resp_obj.data:
            return

        data = ResourceResponseData(**resp_obj.data)
        if not data.db:
            return
        db = data.db.master[0]
        db.decrypt_pwd(self.key)
        db_info_dict = db.dict()
        if db_name:
            db_info_dict["db"] = db_name
        else:
            db_info_dict["db"] = db.dbName
        db_info_dict["user"] = db.username
        # 转义“@”等特殊字符串
        db_info_dict["password"] = quote(db.pwd)

        return db_info_dict

    @alru_cache(maxsize=1000)
    async def __get_db_info(
        self, _, *, db_name: str = None, company_id: str = None
    ) -> Optional[DbInfo]:
        url = f"{self.host}/resource/get"
        params = {"buss_code": self.buss_code, "app_id": self.app_id}
        if company_id:
            params["company_id"] = company_id
        resp_obj = await self.__get(url=url, params=params)
        return self.__pasrse_db_info(resp_obj=resp_obj, db_name=db_name)

    async def get_domains(self, list_company_id: list, status: int = 1) -> list:
        url = f"{self.host}/resource/get_domains"
        # 分批查询，怕接口服务有限制
        step = 100
        result = []
        for i in range(0, len(list_company_id), step):
            company_ids = list_company_id[i : i + step]
            params = dict(
                companyIds=company_ids,
                status=status,
            )
            list_data = await self.__post(url, params=params)
            if isinstance(list_data, list):
                for data in list_data:
                    result.append(data)
        return result

    async def get_domain(self, company_id) -> list:
        url = f"{self.host}/resource/get_domain"

        params = dict(
            company_id=f"{company_id}",
        )

        result = await self.__get(url=url, params=params)

        return result

    @retry(retry_time=3)
    async def get_db_with_company_id(self, company_id) -> list:
        return await self.__get_db_info(
            int(time.time() // self.ex), company_id=company_id
        )

    @retry(retry_time=3)
    async def get_db_with_db_name(self, db_name) -> list:
        return await self.__get_db_info(int(time.time() // self.ex), db_name=db_name)
