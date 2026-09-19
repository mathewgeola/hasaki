from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple, Self

import anyio
import httpx

__all__ = ["DownloadJob", "DownloadResult", "FileDownloader"]


@dataclass(slots=True)
class DownloadJob:
    url: str
    path: Path


class DownloadResult(NamedTuple):
    path: Path
    error: Exception | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


class FileDownloader:
    _RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

    def __init__(
        self,
        *,
        concurrency: int = 10,
        timeout: float = 60.0,
        retries: int = 3,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        if concurrency <= 0:
            raise ValueError("concurrency 必须大于 0")
        if retries <= 0:
            raise ValueError("retries 必须大于 0")

        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            transport=transport,
        )
        self._limiter = anyio.CapacityLimiter(concurrency)
        self._retries = retries

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    async def download(self, *jobs: DownloadJob) -> list[DownloadResult]:
        return list(await anyio.gather(*(self._download(job) for job in jobs)))

    async def _download(self, job: DownloadJob) -> DownloadResult:
        attempt = 0
        while True:
            try:
                return await self._fetch(job)
            except Exception as exc:
                if attempt == self._retries - 1 or not self._is_retryable(exc):
                    return DownloadResult(job.path, exc)
                await anyio.sleep(0.2 * 2**attempt)
                attempt += 1

    @staticmethod
    def _is_retryable(exc: Exception) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code in FileDownloader._RETRYABLE_STATUS
        return isinstance(exc, httpx.TransportError)

    async def _fetch(self, job: DownloadJob) -> DownloadResult:
        path = job.path
        part = Path(f"{path}.part")

        try:
            async with self._limiter:
                if path.exists():
                    return DownloadResult(path)

                path.parent.mkdir(parents=True, exist_ok=True)

                async with self._client.stream("GET", job.url) as response:
                    response.raise_for_status()

                    async with await anyio.open_file(part, "wb") as f:
                        async for chunk in response.aiter_bytes():
                            await f.write(chunk)

                part.replace(path)

                return DownloadResult(path)
        except Exception:
            part.unlink(missing_ok=True)
            raise
