# e2e helpers. Env: CODEX_BIN = Codex CLI binary (required, used by run.sh);
# CIALLOPA_BIN_DIR = dir with built ciallopa (default: <repo>/target/debug).
D=/tmp/cpa-e2e; source $D/run.sh; L=$D/upstream.log; T=${CIALLOPA_BIN_DIR:-$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)/target/debug}
show(){ grep '"t":"\(sse\|ws\)"' $L | python3 -c "import sys,json,collections;r=[json.loads(l)['acct'] for l in sys.stdin];print('  ',r,dict(collections.Counter(r)))"; : > $L; }
restart(){ pkill -x ciallopa; sleep 0.3; (cd $D && $T/ciallopa -c ${1:-config.toml} >> server.log 2>&1 &); sleep 0.7; }
