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
    def __init__(
        self,
        *,
        concurrency: int = 10,
        timeout: float = 60.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        if concurrency <= 0:
            raise ValueError("concurrency 必须大于 0")

        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            transport=transport,
        )
        self._limiter = anyio.CapacityLimiter(concurrency)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    async def download(self, *jobs: DownloadJob) -> list[DownloadResult]:
        return list(await anyio.gather(*(self._download(job) for job in jobs)))

    async def _download(self, job: DownloadJob) -> DownloadResult:
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
        except Exception as exc:
            part.unlink(missing_ok=True)
            return DownloadResult(path, exc)
