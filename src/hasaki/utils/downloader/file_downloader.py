from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

import httpx


@dataclass(slots=True)
class Task:
    url: str
    file_path: str | Path | None = None
    overwrite: bool = False


class FileDownloader:

    def __init__(
        self,
        *,
        max_concurrency: int = 10,
        chunk_size: int = 1024 * 1024,
        timeout: httpx.Timeout | None = None,
    ):
        if max_concurrency <= 0:
            raise ValueError("max_concurrency 必须大于 0")

        if chunk_size <= 0:
            raise ValueError("chunk_size 必须大于 0")

        self.max_concurrency = max_concurrency
        self.chunk_size = chunk_size

        self.timeout = timeout or httpx.Timeout(
            connect=10.0,
            read=60.0,
            write=60.0,
            pool=10.0,
        )

        self._client: httpx.AsyncClient | None = None
        self._semaphore = asyncio.Semaphore(max_concurrency)

    def open(self):
        """打开 HTTP Client。"""
        if self._client is not None:
            return

        self._client = httpx.AsyncClient(
            timeout=self.timeout,
            limits=httpx.Limits(
                max_connections=self.max_concurrency,
                max_keepalive_connections=self.max_concurrency,
            ),
            follow_redirects=True,
        )

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("FileDownloader 尚未 open()")

        return self._client

    async def download(self, *tasks: Task):
        """
        下载一个或多个任务。

        多个任务会并发执行。
        """
        if not tasks:
            return []

        return await asyncio.gather(*(self._download(task) for task in tasks))

    async def _download(self, task: Task) -> Path | None:
        async with self._semaphore:
            file_path = self._resolve_file_path(task)

            # 已存在，并且不允许覆盖
            if file_path.exists() and not task.overwrite:
                return None

            file_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            # 临时文件
            part_path = file_path.with_name(file_path.name + ".part")

            request = self.client.build_request(
                "GET",
                task.url,
            )

            response = await self.client.send(
                request,
                stream=True,
            )

            try:
                response.raise_for_status()

                with part_path.open("wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=self.chunk_size):
                        f.write(chunk)

                # 下载完成后，再替换正式文件
                if task.overwrite:
                    part_path.replace(file_path)
                else:
                    # 理论上这里已经检查过不存在，
                    # 但为了避免并发/外部程序导致覆盖，进行二次判断
                    if file_path.exists():
                        part_path.unlink(missing_ok=True)
                        return None

                    part_path.replace(file_path)

                return file_path

            finally:
                await response.aclose()

    @staticmethod
    def _resolve_file_path(task: Task) -> Path:
        if task.file_path is not None:
            return Path(task.file_path)

        filename = FileDownloader._get_filename(task.url)

        return Path.cwd() / filename

    @staticmethod
    def _get_filename(url: str) -> str:
        path = urlparse(url).path
        filename = Path(unquote(path)).name

        if not filename:
            raise ValueError(f"无法从 URL 获取文件名: {url}")

        return filename

    async def close(self):
        """关闭 HTTP Client。"""
        if self._client is None:
            return

        await self._client.aclose()
        self._client = None


if __name__ == "__main__":

    async def run():
        downloader = FileDownloader()
        downloader.open()

        try:
            path = await downloader.download(
                Task(
                    url="https://www.ickey.cn/static-pf/proimg/e8/33/3d31/5e572c0e9ca083ca3026f92bd1ea6d7a.jpg",
                ),
                Task(
                    url="https://www.ickey.cn/static-pf/proimg/e8/33/3d31/5e572c0e9ca083ca3026f92bd1ea6d7a.jpg",
                ),
                Task(
                    url="https://www.ickey.cn/static-pf/proimg/e8/33/3d31/5e572c0e9ca083ca3026f92bd1ea6d7a.jpg",
                ),
            )

            print(path)

        finally:
            await downloader.close()

    asyncio.run(run())
