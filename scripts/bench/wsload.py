import asyncio, json, os, socket, sys, time
from websockets.asyncio.client import connect

addr = ("127.0.0.1", 18380)
url = "ws://127.0.0.1:18380/v1/responses"

async def worker(mode, worker_id):
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32 * 1024)
    sock.setblocking(False)
    seq = 0
    started = time.monotonic()
    try:
        await asyncio.get_running_loop().sock_connect(sock, addr)
        async with connect(url, sock=sock, proxy=None,
                           additional_headers={"Authorization":"Bearer " + os.environ["KEY"]},
                           compression="deflate", max_queue=1, max_size=20 << 20,
                           ping_interval=None, close_timeout=2) as ws:
            port = sock.getsockname()[1]
            count = int(os.getenv("PROBES", "60")) if mode == "probe" else 1
            for seq in range(count):
                if mode == "probe":
                    await asyncio.sleep(max(0, started + seq - time.monotonic()))
                large_out = mode in {"fast", "slow", "pause", "stop"}
                model = "bench-large" if large_out else ("bench-long" if mode == "long" else "gpt-5.5")
                size = (int(os.getenv("INPUT_MIB", "12")) << 20) if mode == "biginput" else 20
                req = json.dumps({"type":"response.create","model":model,"input":"x"*size,
                                  "client_metadata":{"x-codex-turn-metadata":json.dumps({"turn_id":f"{mode}-{worker_id}-{seq}"})}})
                t = time.monotonic()
                await ws.send(req)
                if mode in {"pause", "stop"}:
                    await asyncio.sleep(12)
                    if mode == "stop":
                        ws.transport.abort()
                        print(json.dumps([worker_id, seq, mode, "aborted", None, time.monotonic()-t, port, t]))
                        return
                n, first = 0, None
                async with asyncio.timeout(75):
                    async for raw in ws:
                        ev = json.loads(raw)
                        if ev["type"] == "response.output_text.delta":
                            first = first if first is not None else time.monotonic()-t
                            n += 1
                            if mode == "slow":
                                await asyncio.sleep(0.04)
                        elif ev["type"] == "response.completed":
                            assert n == (256 if large_out else 4), n
                            print(json.dumps([worker_id, seq, mode, "ok", first, time.monotonic()-t, port, t]), flush=True)
                            break
                        elif ev["type"] == "error":
                            raise ValueError(ev.get("status", "error"))
                    else:
                        raise EOFError("missing terminal")
    except Exception as e:
        print(json.dumps([worker_id, seq, mode, type(e).__name__, str(e)[:160]]), flush=True)
    finally:
        sock.close()

async def cpu_samples(mode):
    start = time.monotonic()
    cpu = time.process_time()
    while True:
        await asyncio.sleep(1)
        now, used = time.monotonic(), time.process_time()
        print(json.dumps(["ws_cpu", os.getpid(), mode, time.time(), now-start,
                          100*(used-cpu)/(now-start)]), file=sys.stderr, flush=True)
        start, cpu = now, used


async def main():
    mode = sys.argv[1]
    sampler = asyncio.create_task(cpu_samples(mode))
    try:
        await asyncio.gather(*(worker(mode, i) for i in range(int(sys.argv[2]))))
    finally:
        sampler.cancel()
asyncio.run(main())
