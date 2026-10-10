#!/bin/bash
# TDE certificate spike on buildcomp. Does not touch mongo-rs, mongo-rs9, or mysql-ce.
# Binds 127.0.0.1 only. Passwords come from the environment or from local/lab.env
# beside this script (not committed; see lab.env.example). Certificate files
# stay in out/.
set -eu
image="mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04"
name="ce-stream-mssql-spike"
here="$(cd "$(dirname "$0")" && pwd)"
# Lab values are not in the repo: the environment, or local/lab.env.
if [ -f "$here/local/lab.env" ]; then
  # shellcheck disable=SC1090
  . <(tr -d '\r' < "$here/local/lab.env")
fi
sa_password="${MSSQL_SA_PASSWORD:?set MSSQL_SA_PASSWORD, or copy lab.env.example to local/lab.env}"
dmk_password="${MSSQL_DMK_PASSWORD:?set MSSQL_DMK_PASSWORD, or copy lab.env.example to local/lab.env}"
pvk_password="${TDE_PVK_PASSWORD:?set TDE_PVK_PASSWORD, or copy lab.env.example to local/lab.env}"
out="$here/out"
mkdir -p "$out"

if ! docker ps --format '{{.Names}}' | grep -qx "$name"; then
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --name "$name" --memory=4g \
    -e ACCEPT_EULA=Y \
    -e "MSSQL_SA_PASSWORD=$sa_password" \
    -e MSSQL_PID=Developer \
    -p "127.0.0.1:14333:1433" \
    "$image"
fi

ready=0
for _ in $(seq 1 60); do
  if docker exec "$name" /opt/mssql-tools18/bin/sqlcmd \
      -S localhost -U sa -P "$sa_password" -C -Q "SELECT 1" -b >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 3
done
if [ "$ready" -ne 1 ]; then
  docker logs "$name" | tail -n 40
  echo "SQL Server did not accept connections" >&2
  exit 1
fi

docker cp "$here/tde.sql" "${name}:/tmp/tde.sql"
docker exec "$name" /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$sa_password" -C \
  -v "MSSQL_DMK_PASSWORD=$dmk_password" "TDE_PVK_PASSWORD=$pvk_password" \
  -i /tmp/tde.sql -y 0 -Y 200 -s "$(printf '\t')" -o /tmp/tde.txt -b
docker cp "${name}:/tmp/tde.txt" "$out/tde.txt"
docker cp "${name}:/var/opt/mssql/data/ce_stream_tde.cer" "$out/ce_stream_tde.cer"
docker cp "${name}:/var/opt/mssql/data/ce_stream_tde.pvk" "$out/ce_stream_tde.pvk"
docker cp "${name}:/var/opt/mssql/data/ce_stream_tde.mdf" "$out/ce_stream_tde.mdf"
docker cp "${name}:/var/opt/mssql/data/ce_stream_tde_log.ldf" "$out/ce_stream_tde_log.ldf"
echo "wrote $out"
