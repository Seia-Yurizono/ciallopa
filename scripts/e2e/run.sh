# Env: CODEX_BIN = Codex CLI binary (required); E2E_WORKDIR = cwd for codex exec (default /tmp).
run () 
{ 
    cd "${E2E_WORKDIR:-/tmp}" && CODEX_HOME=/tmp/cpa-e2e/home-$1 CPA_KEY=${KEY:-sk-cpa-e2e-0123456789abcdef} timeout 60 $CODEX exec --skip-git-repo-check $2 "$3" < /dev/null 2>&1 | grep -E "ok from|ERROR|error" | tail -3
}
CODEX=${CODEX_BIN:?set to Codex CLI binary}
