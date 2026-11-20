"""Launch one benchmark child and count entries into SQLite quota/history read bodies."""
import json
import os
from pathlib import Path
import subprocess
import sys

import frida

binary, config, case = sys.argv[1:]
case = Path(case)
names = {"ciallopa_usage::quota::quota": "quota", "ciallopa_usage::history::history": "history"}
diagnostic = os.environ.get("PERMIT_PROBE") == "1"
if diagnostic:
    names["tokio::sync::batch_semaphore::Semaphore::add_permits_locked"] = "release"
    names["sqlite3_interrupt"] = "interrupt"
symbols = {}
for line in subprocess.check_output(["nm", "-C", binary], text=True).splitlines():
    cols = line.split(maxsplit=2)
    if len(cols) == 3 and cols[2] in names:
        symbols[names[cols[2]]] = int(cols[0], 16)
assert len(symbols) == len(names), symbols
with (case / "gateway.log").open("w") as log, (case / "queries.jsonl").open("w") as queries:
    child = subprocess.Popen([binary, "-c", config, "serve"], stdout=log, stderr=log)
    try:
        session = frida.attach(child.pid)
        script = session.create_script("""
            const workers = new Set();
            const clock = new NativeFunction(Module.getGlobalExportByName("clock_gettime"),
                                             "int", ["int", "pointer"]);
            function monotonicMs() {
                const ts = Memory.alloc(16);
                clock(1, ts);
                return ts.readS64().toNumber() * 1000 + ts.add(8).readS64().toNumber() / 1e6;
            }
            for (const [name, offset] of Object.entries(%s)) {
                if (name === "release") {
                    Interceptor.attach(Process.mainModule.base.add(offset), {
                        onEnter(args) {
                            this.read = workers.delete(this.threadId);
                            if (this.read) this.semaphore = args[0].toString();
                        },
                        onLeave() { if (this.read) send([name, "end", this.threadId, monotonicMs(), this.semaphore]); }
                    });
                    continue;
                }
                if (name === "interrupt") {
                    Interceptor.attach(Process.mainModule.base.add(offset), {
                        onEnter() { send([name, "start", this.threadId, monotonicMs()]); }
                    });
                    continue;
                }
                Interceptor.attach(Process.mainModule.base.add(offset), {
                    onEnter() { send([name, "start", this.threadId, Date.now()]); },
                    onLeave() {
                        if (%s) workers.add(this.threadId);
                        send([name, "end", this.threadId, Date.now()]);
                    }
                });
            }
        """ % (json.dumps(symbols), json.dumps(diagnostic)))
        def record(msg, data):
            print(json.dumps(msg), file=queries, flush=True)
        script.on("message", record)
        script.load()
        (case / "pid").write_text(str(child.pid))
        sys.exit(child.wait())
    finally:
        if child.poll() is None:
            child.terminate()
            child.wait()
        frida.shutdown()
