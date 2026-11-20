import asyncio, json, os, sys, time
import httpx

async def main():
    mode, seconds = sys.argv[1], int(sys.argv[2])
    opts = dict(base_url="http://127.0.0.1:18380",
                headers={"Authorization": "Bearer " + os.environ["KEY"]},
                trust_env=False, timeout=20,
                limits=httpx.Limits(max_connections=128, max_keepalive_connections=64))
    async with httpx.AsyncClient(**opts) as writes, httpx.AsyncClient(**opts) as reads:
        async def one(kind, i, due):
            start = time.monotonic()
            first, status, outcome = None, None, "ok"
            try:
                if kind == "sse":
                    body = {"model":"gpt-5.5","stream":True,"input":"hi"}
                    async with writes.stream("POST", "/v1/responses", json=body, headers={"x-codex-turn-metadata": json.dumps({"turn_id": f"sse-{i}"})}) as r:
                        status, terminal = r.status_code, False
                        async for line in r.aiter_lines():
                            if line.startswith("data:"):
                                ev = json.loads(line[5:])["type"]
                                if ev == "response.output_text.delta" and first is None:
                                    first = round(1000 * (time.monotonic() - start), 2)
                                terminal |= ev == "response.completed"
                        if status != 200 or not terminal:
                            outcome = "no_success_terminal"
                else:
                    path = "/usage/v1/quota"
                    if mode == "mixed" and i % 2:
                        path += "/history?days=" + str(1 + i % 7)
                    # 每四个读留一个能正常完成的观察样本，其余模拟切走页面。
                    async with asyncio.timeout(20 if i % 4 == 0 else 0.15):
                        r = await reads.get(path)
                        status = r.status_code
                        if status != 200:
                            outcome = "http_error"
            except Exception as e:
                outcome = type(e).__name__
            print(json.dumps([kind, i, round(1000*(start-due),2), status, outcome,
                              first, round(1000*(time.monotonic()-start),2), start]), flush=True)

        t0 = time.monotonic()
        async with asyncio.TaskGroup() as tasks:
            for i in range(20 * seconds):
                due = t0 + i / 20
                await asyncio.sleep(max(0, due - time.monotonic()))
                tasks.create_task(one("sse", i, due))
                if mode != "base" and i % 5 == 0:
                    tasks.create_task(one("read", i // 5, due))
asyncio.run(main())
