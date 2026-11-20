#!/usr/bin/env bash
set -eu
umask 077
: "${LAB:?new private ext4 directory}"
cd "$LAB"
uv venv py
uv pip install --python py/bin/python hypercorn==0.17.3 websockets==15.0.1 httpx==0.28.1 frida==17.27.4
openssl req -x509 -newkey rsa:2048 -nodes -days 3 -subj /CN=bench-ca \
  -keyout "$LAB/ca.key" -out "$LAB/ca.pem" -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign
openssl req -new -newkey rsa:2048 -nodes -subj /CN=localhost \
  -keyout "$LAB/server.key" -out "$LAB/server.csr"
echo 'subjectAltName=DNS:localhost,IP:127.0.0.1' > "$LAB/san"
openssl x509 -req -in "$LAB/server.csr" -CA "$LAB/ca.pem" -CAkey "$LAB/ca.key" \
  -CAcreateserial -days 3 -extfile "$LAB/san" -out "$LAB/server.pem"
mkdir -m 700 "$LAB/auth"
echo '{"type":"codex","email":"bench@invalid","account_id":"bench","access_token":"fake-local","expired":"2099-01-01T00:00:00Z"}' > "$LAB/auth/bench.json"
chmod 600 "$LAB/auth/bench.json"
