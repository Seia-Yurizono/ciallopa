D=/tmp/cpa-e2e; source $D/run.sh; L=$D/upstream.log; T=<local-path>
show(){ grep '"t":"\(sse\|ws\)"' $L | python3 -c "import sys,json,collections;r=[json.loads(l)['acct'] for l in sys.stdin];print('  ',r,dict(collections.Counter(r)))"; : > $L; }
restart(){ pkill -x cpa-lite; sleep 0.3; (cd $D && $T/cpa-lite -c ${1:-config.toml} >> server.log 2>&1 &); sleep 0.7; }
