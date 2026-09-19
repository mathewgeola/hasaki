from __future__ import annotations

import httpx

from hasaki.utils.downloader import DownloadJob, FileDownloader


def transport(body: bytes = b"hello") -> httpx.MockTransport:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body)

    return httpx.MockTransport(handle)


async def test_file_downloader(tmp_path):
    jobs = [DownloadJob(f"http://x/{i}", tmp_path / f"{i}.txt") for i in range(3)]

    async with FileDownloader(transport=transport()) as downloader:
        results = await downloader.download(*jobs)

    assert [r.ok for r in results] == [True, True, True]
    assert [job.path.read_bytes() for job in jobs] == [b"hello"] * 3
