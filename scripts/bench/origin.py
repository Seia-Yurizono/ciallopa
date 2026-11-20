import asyncio, base64, itertools, json, os, random, time
from pathlib import Path
from hypercorn.asyncio import serve
from hypercorn.asyncio.tcp_server import TCPServer
from hypercorn.config import Config

lab = Path(os.environ["LAB"])
delta_interval = float(os.getenv("DELTA_INTERVAL", "0.008"))
accept_ids = itertools.count(1)
real_run = TCPServer.run

async def tagged_run(self):
    self.state = dict(self.state, bench_conn=(next(accept_ids), itertools.count(1)))
    await real_run(self)

TCPServer.run = tagged_run
payload = base64.b64encode(random.Random(42).randbytes(48 * 1024)).decode()

def events(large):
    yield {"type": "response.created", "response": {"id": "r"}}
    for i in range(256 if large else 4):
        yield {"type": "response.output_text.delta", "delta": payload if large else str(i)}
    yield {"type": "response.completed", "response": {"id": "r", "usage": {"output_tokens": 4}}}

async def app(scope, recv, send):
    if scope["type"] == "lifespan":
        while True:
            msg = await recv()
            startup = msg["type"] == "lifespan.startup"
            await send({"type": msg["type"] + ".complete"})
            if not startup:
                return
    cid, counter = scope["state"]["bench_conn"]
    seq = next(counter)
    print(json.dumps([cid, seq, scope["type"], scope["http_version"]]), flush=True)
    if scope["type"] == "http":
        while True:
            request = await recv()
            if request["type"] == "http.disconnect":
                return
            if not request.get("more_body", False):
                break
        await send({"type": "http.response.start", "status": 200, "headers": [
            (b"content-type", b"text/event-stream"), (b"x-bench-conn", f"{cid}.{seq}".encode())]})
        for ev in events(False):
            await send({"type": "http.response.body", "body":
                        ("data: " + json.dumps(ev) + "\n\n").encode(), "more_body": True})
        await send({"type": "http.response.body", "body": b""})
        return
    await recv()
    await send({"type": "websocket.accept"})
    while True:
        msg = await recv()
        if msg["type"] == "websocket.disconnect":
            return
        request = json.loads(msg["text"])
        for ev in events(request.get("model") == "bench-large"):
            if request.get("model") == "bench-long":
                await asyncio.sleep(2)
            if request.get("model") == "bench-large" and delta_interval:
                await asyncio.sleep(delta_interval)
            await send({"type": "websocket.send", "text": json.dumps(ev, separators=(",", ":"))})

cfg = Config()
cfg.bind = ["127.0.0.1:9443"]
cfg.certfile, cfg.keyfile = str(lab / "server.pem"), str(lab / "server.key")
async def monitor():
    loop = asyncio.get_running_loop()
    with (lab / "origin-health.jsonl").open("a") as out:
        while True:
            samples = []
            for _ in range(50):
                due = loop.time() + 0.02
                await asyncio.sleep(0.02)
                samples.append(1000 * (loop.time() - due))
            print(json.dumps([time.time(), max(samples)]), file=out, flush=True)


async def main():
    sampler = asyncio.create_task(monitor())
    try:
        await serve(app, cfg)
    finally:
        sampler.cancel()


asyncio.run(main())
