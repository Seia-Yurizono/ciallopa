"""Single outstanding read, closed 150 ms after dispatch. Wait for its worker's release."""
import asyncio
import json
import os
from pathlib import Path
import sys
import time

import httpx


async def main():
    trace = Path(sys.argv[1]) / "queries.jsonl"
    async with httpx.AsyncClient(base_url="http://127.0.0.1:18380", trust_env=False,
                                 headers={"Authorization": "Bearer " + os.environ["KEY"]}) as client:
        for i in range(20):
            offset = trace.stat().st_size
            path = "/usage/v1/quota" + ("/history?days=7" if i % 2 else "")
            job = asyncio.create_task(client.get(path))
            await asyncio.sleep(0.15)
            cancelled = time.monotonic() * 1000
            job.cancel()
            try:
                await job
                raise RuntimeError("read already finished")
            except asyncio.CancelledError:
                pass
            async with asyncio.timeout(15):
                while True:
                    with trace.open() as f:
                        f.seek(offset)
                        events = [json.loads(line)["payload"] for line in f if line.endswith("\n")]
                    releases = [e[3] for e in events if e[0] == "release"]
                    if releases:
                        assert len(releases) == 1 and len([e for e in events if e[1] == "end"]) == 2, events
                        interrupts = [e[3] for e in events if e[0] == "interrupt"]
                        print(json.dumps([i, path, releases[0] - cancelled,
                                          releases[0] - interrupts[0] if interrupts else None]), flush=True)
                        break
                    await asyncio.sleep(.02)


asyncio.run(main())
