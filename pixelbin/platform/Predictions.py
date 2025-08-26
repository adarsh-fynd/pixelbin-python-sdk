import asyncio
import ujson
from typing import Any, Dict, List, Optional

from .PlatformAPIClient import APIClient
from .PixelbinConfig import PixelbinConfig
from ..common.exceptions import (
    PixelbinIllegalArgumentError,
    PixelbinServerResponseError,
)


class Predictions:
    def __init__(self, config: PixelbinConfig):
        self.config = config

    async def createAsync(
        self,
        name: str,
        input: Dict[str, Any] = None,
        webhook: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not isinstance(name, str) or not name:
            raise PixelbinIllegalArgumentError("name (string) is required")

        parts = name.split("_")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise PixelbinIllegalArgumentError(
                "name must be in 'plugin_operation' format, e.g. 'erase_bg'"
            )
        plugin, operation = parts[0], parts[1]

        input = input or {}

        body: Dict[str, Any] = {}
        if webhook:
            body["webhook"] = webhook

        def is_url(value: str) -> bool:
            return isinstance(value, str) and value.lower().startswith(
                ("http://", "https://")
            )

        for key, value in input.items():
            if value is None:
                continue
            field_name = f"input.{key}"
            if isinstance(value, list):
                processed_list: List[Any] = []
                for v in value:
                    if isinstance(v, dict):
                        processed_list.append(
                            ujson.dumps(v, escape_forward_slashes=False)
                        )
                    else:
                        processed_list.append(v)
                body[field_name] = processed_list
            elif isinstance(value, dict):
                body[field_name] = ujson.dumps(value, escape_forward_slashes=False)
            else:
                # Do not treat strings as local file paths. Expect bytes or file-like for files.
                body[field_name] = value

        response = await APIClient.execute(
            conf=self.config,
            method="post",
            url=f"/service/platform/transformation/v1.0/predictions/{plugin}/{operation}",
            query={},
            body=body,
            contentType="multipart/form-data",
        )
        if response["status_code"] != 200:
            raise PixelbinServerResponseError(
                str(response["content"]), response["status_code"]
            )
        return ujson.loads(response["content"])

    def create(
        self,
        name: str,
        input: Dict[str, Any] = None,
        webhook: Optional[str] = None,
    ) -> Dict[str, Any]:
        return asyncio.get_event_loop().run_until_complete(
            self.createAsync(name=name, input=input or {}, webhook=webhook)
        )

    async def getAsync(self, request_id: str) -> Dict[str, Any]:
        if not isinstance(request_id, str) or not request_id:
            raise PixelbinIllegalArgumentError("requestId (string) is required")
        path = f"/service/platform/transformation/v1.0/predictions/{request_id}"
        response = await APIClient.execute(
            conf=self.config,
            method="get",
            url=path,
            query={},
            body=None,
            contentType="",
        )
        if response["status_code"] != 200:
            raise PixelbinServerResponseError(
                str(response["content"]), response["status_code"]
            )
        return ujson.loads(response["content"])

    def get(self, request_id: str) -> Dict[str, Any]:
        return asyncio.get_event_loop().run_until_complete(self.getAsync(request_id))

    async def waitAsync(self, request_id: str) -> Dict[str, Any]:
        if not request_id:
            raise PixelbinIllegalArgumentError("requestId is required")
        DEFAULT_MIN_TIMEOUT = 4.0
        DEFAULT_RETRIES = 150
        interval = DEFAULT_MIN_TIMEOUT
        attempts = DEFAULT_RETRIES

        last_status: Dict[str, Any] = {}
        for _ in range(attempts):
            s = await self.getAsync(request_id)
            last_status = s
            if s and s.get("status") in ("SUCCESS", "FAILURE"):
                return s
            await asyncio.sleep(interval)
        return last_status

    def wait(self, request_id: str) -> Dict[str, Any]:
        return asyncio.get_event_loop().run_until_complete(self.waitAsync(request_id))

    async def create_and_waitAsync(
        self,
        name: str,
        input: Dict[str, Any] = None,
        webhook: Optional[str] = None,
    ) -> Dict[str, Any]:
        job = await self.createAsync(name=name, input=input or {}, webhook=webhook)
        return await self.waitAsync(job["_id"])

    def create_and_wait(
        self,
        name: str,
        input: Dict[str, Any] = None,
        webhook: Optional[str] = None,
    ) -> Dict[str, Any]:
        return asyncio.get_event_loop().run_until_complete(
            self.create_and_waitAsync(name=name, input=input or {}, webhook=webhook)
        )

    async def listAsync(self) -> List[Dict[str, Any]]:
        response = await APIClient.execute(
            conf=self.config,
            method="get",
            url=f"/service/public/transformation/v1.0/predictions",
            query={},
            body={},
            contentType="",
        )
        if response["status_code"] != 200:
            raise PixelbinServerResponseError(
                str(response["content"]), response["status_code"]
            )
        return ujson.loads(response["content"])

    def list(self) -> List[Dict[str, Any]]:
        return asyncio.get_event_loop().run_until_complete(self.listAsync())

    async def get_schemaAsync(self, name: str) -> Dict[str, Any]:
        if not isinstance(name, str) or not name:
            raise PixelbinIllegalArgumentError("name (string) is required")
        response = await APIClient.execute(
            conf=self.config,
            method="get",
            url=f"/service/public/transformation/v1.0/predictions/schema/{name}",
            query={},
            body={},
            contentType="",
        )
        if response["status_code"] != 200:
            raise PixelbinServerResponseError(
                str(response["content"]), response["status_code"]
            )
        return ujson.loads(response["content"])

    def get_schema(self, name: str) -> Dict[str, Any]:
        return asyncio.get_event_loop().run_until_complete(self.get_schemaAsync(name))
