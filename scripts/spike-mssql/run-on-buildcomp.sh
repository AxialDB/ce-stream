#!/bin/bash
# SQL Server log spike on buildcomp. CDC stays off.
# Does not touch mongo-rs, mongo-rs9, or mysql-ce.
# Binds 127.0.0.1 only. The sa password comes from MSSQL_SA_PASSWORD or from
# local/lab.env beside this script (not committed; see lab.env.example).
set -eu
image="mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04"
name="ce-stream-mssql-spike"
port=14333
here="$(cd "$(dirname "$0")" && pwd)"
# Lab values are not in the repo: the environment, or local/lab.env.
if [ -f "$here/local/lab.env" ]; then
  # shellcheck disable=SC1090
  . <(tr -d '\r' < "$here/local/lab.env")
fi
sa_password="${MSSQL_SA_PASSWORD:?set MSSQL_SA_PASSWORD, or copy lab.env.example to local/lab.env}"
sql="$here/fixture.sql"
mkdir -p "$here/out"

docker rm -f "$name" >/dev/null 2>&1 || true
docker run -d --name "$name" --memory=4g \
  -e ACCEPT_EULA=Y \
  -e "MSSQL_SA_PASSWORD=$sa_password" \
  -e MSSQL_PID=Developer \
  -p "127.0.0.1:${port}:1433" \
  "$image"

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
  docker logs "$name"
  echo "SQL Server did not accept connections" >&2
  exit 1
fi

docker cp "$sql" "${name}:/tmp/fixture.sql"
docker exec "$name" /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$sa_password" -C \
  -i /tmp/fixture.sql -y 0 -Y 80 -s "$(printf '\t')" -o /tmp/fixture.txt -b \
  >"$here/out/fixture.txt" || {
    docker exec "$name" cat /tmp/fixture.txt || true
    exit 1
  }
docker cp "${name}:/tmp/fixture.txt" "$here/out/fixture.txt"

# Second process, while sqlservr holds the live log. The container's
# default user is mssql. Root is a separate open. A user outside the
# mssql group is expected to be denied by mode 0640.
docker exec "$name" bash -lc '
set -eu
log=$(ls /var/opt/mssql/data/*_log.ldf | head -n 1)
echo "log=$log"
echo "reader=$(id)"
ls -l "$log"
dd if="$log" of=/tmp/ldf-head.bin bs=512 count=16 status=none
echo "mssql_bytes=$(wc -c < /tmp/ldf-head.bin)"
echo -n "mssql_head="
od -An -tx1 -N 64 /tmp/ldf-head.bin | head -n 4
' >"$here/out/log-read.txt"
docker exec -u 0 -i "$name" bash -s >>"$here/out/log-read.txt" <<'EOS'
set -eu
log=$(ls /var/opt/mssql/data/*_log.ldf | head -n 1)
echo "root_id=$(id)"
dd if="$log" of=/tmp/root-head.bin bs=512 count=1 status=none
echo "root_read=ok bytes=$(wc -c < /tmp/root-head.bin)"
id -u ce_reader >/dev/null 2>&1 || useradd --no-create-home --shell /usr/sbin/nologin ce_reader
if su -s /bin/bash ce_reader -c "dd if=$log of=/tmp/ce-reader-head.bin bs=512 count=1 status=none"; then
  echo "ce_reader=ok"
else
  echo "ce_reader=denied"
fi
EOS

echo "Wrote $here/out/fixture.txt"
echo "Wrote $here/out/log-read.txt"
