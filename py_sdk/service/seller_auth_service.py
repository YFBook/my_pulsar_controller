from typing import Literal, Union


import aiohttp


class SellerAuthService:
    def __init__(self, host, limit=100, app_name="", request_timeout=40) -> None:
        self.http_session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=limit)
        )
        self.app_name = app_name
        self.timeout = request_timeout
        self.host = host

    async def close(self):
        await self.http_session.close()

    async def post(self, url, params):
        res = await self.http_session.post(
            url,
            json=params,
            timeout=self.timeout,
            headers={
                "app_name": self.app_name,
                "X-Application-Name": self.app_name,
            },
        )
        await res.read()
        res.close()
        return res

    async def get(self, url, params):
        res = await self.http_session.get(
            url,
            params=params,
            timeout=self.timeout,
            headers={
                "app_name": self.app_name,
                "X-Application-Name": self.app_name,
            },
        )
        await res.read()
        res.close()
        return res

    async def get_seller_auth_marketplce_with_seller_marketplace(
        self,
        seller_id,
        marketplace_id,
        extra_field="seller_id,data_sync_status,region,marketplace_id,company_id",
    ):
        path = f"{self.host}/seller_auth_marketplace/by_seller_marketplace"
        res = await self.get(
            url=path,
            params={
                "seller_id": seller_id,
                "marketplace_id": marketplace_id,
                "extra_field": extra_field,
            },
        )
        return await res.json()

    # list_seller_id 不要超过50（缓存服务限制）
    async def get_seller_auth_marketplce_with_seller_ids(
        self,
        list_seller_id: list,
        extra_field="zid,sid,seller_id,mws_status,marketplace_id,company_id,env_key,data_sync_status,region,country_code",
    ):
        list_seller_auth_marketplace = []
        step = 50
        for i in range(0, len(list_seller_id), step):
            seller_ids = list_seller_id[i : i + step]
            res = await self.post(
                url=f"{self.host}/seller_auth_marketplace/by_many_seller_marketplace",
                params={
                    "seller_ids": seller_ids,
                    "extra_field": extra_field,
                },
            )
            res_json = await res.json()
            list_seller_auth_marketplace.extend(res_json)
        return list_seller_auth_marketplace
