#!/usr/bin/env bash
# A failover on the Docker Compose cluster, checked step by step. Needs curl and jq.
#
#   docker compose up -d --build
#   scripts/cluster_test.sh
#
# Loads vectors through node1, kills node1, promotes node2 with node3 as its peer, checks that every
# write node1 answered is on node2, then brings node1 back: it still believes it leads, cannot get a
# write confirmed, and once pointed at node2 it ends with the same snapshot bytes as the others.
set -euo pipefail

n1=http://localhost:8081
n2=http://localhost:8082
n3=http://localhost:8083

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

wait_healthy() {
    for url in "$@"; do
        for attempt in $(seq 1 120); do
            if curl -sf "$url/health" > /dev/null 2>&1; then
                break
            fi
            [ "$attempt" = 120 ] && fail "no answer from $url/health after 60 seconds"
            sleep 0.5
        done
    done
}

status() {
    curl -sf "$1/replication/status"
}

# Sequence number and term of the "demo" collection on a node, or "none" while it cannot say.
position() {
    status "$1" 2> /dev/null | jq -c '[.collections[] | select(.name == "demo") | .sequence, .term]' || echo none
}

# Waits until every node given is at the same position as the first one.
wait_same_position() {
    local target
    for attempt in $(seq 1 240); do
        target=$(position "$1")
        local same=1
        for url in "${@:2}"; do
            [ "$(position "$url")" = "$target" ] || same=0
        done
        [ "$same" = 1 ] && return 0
        sleep 0.25
    done
    fail "the nodes did not reach the same position as $1"
}

post() {
    curl -sf -X POST "$1" -H "Content-Type: application/json" -d "$2"
}

wait_healthy "$n1" "$n2" "$n3"
echo "== node1 leads; node2 and node3 follow it"

curl -sf -X PUT "$n1/collections/demo" -d '{"dimension": 4}' > /dev/null
for batch in $(seq 0 29); do
    body=$(jq -nc --argjson b "$batch" '{vectors: [range(0; 10) as $i | {id: ($b * 10 + $i), vector: [($b * 10 + $i), 1, 0, 0]}]}')
    post "$n1/collections/demo/vectors" "$body" > /dev/null
done
echo "300 vectors written through node1; each answer means node2 or node3 held it too"

docker compose kill node1
echo "== node1 killed"

promotion=$(post "$n2/admin/promote" '{"peers": ["http://node3:8080", "http://node1:8080"]}')
echo "promote node2: $promotion"
echo "$promotion" | jq -e '.term == 1' > /dev/null || fail "node2 is not leading in term 1"
post "$n3/admin/follow" '{"leader": "http://node2:8080"}' > /dev/null
echo "node3 now follows node2"

for id in $(seq 0 299); do
    stored=$(curl -sf "$n2/collections/demo/vectors/$id") || fail "answered write $id is missing on node2"
    # Compared as numbers: the server writes 1.0, and jq versions differ in whether they print it as 1.
    echo "$stored" | jq -e --argjson id "$id" '.vector == [$id, 1, 0, 0]' > /dev/null         || fail "id $id holds $(echo "$stored" | jq -c .vector) on node2"
done
echo "all 300 answered writes are on node2"

post "$n2/collections/demo/vectors" '{"vectors": [{"id": 1000, "vector": [1000, 1, 0, 0]}]}' > /dev/null
echo "node2 accepts writes, confirmed by node3"

docker compose start node1
wait_healthy "$n1"
status "$n1" | jq -e '.role == "leader" and .term == 0' > /dev/null || fail "node1 should come back believing it leads, in term 0"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$n1/collections/demo/vectors" -d '{"vectors": [{"id": 2000, "vector": [2000, 1, 0, 0]}]}')
[ "$code" = 503 ] || fail "a write on the old leader answered $code; nobody should confirm it"
echo "== node1 is back, still believing it leads; its write got 503 because no follower confirms it"

post "$n1/admin/follow" '{"leader": "http://node2:8080"}' > /dev/null
wait_same_position "$n2" "$n1" "$n3"
code=$(curl -s -o /dev/null -w '%{http_code}' "$n1/collections/demo/vectors/2000")
[ "$code" = 404 ] || fail "node1 still holds the write nobody confirmed"
echo "node1 follows node2; the unconfirmed write is gone from it"

incarnation=$(status "$n2" | jq -r '.collections[] | select(.name == "demo") | .incarnation')
digests=$(for url in "$n1" "$n2" "$n3"; do
    curl -sf "$url/replication/collections/demo/snapshot?peer=true&incarnation=$incarnation" | sha256sum
done | sort -u)
[ "$(echo "$digests" | wc -l)" = 1 ] || fail "the snapshots differ: $digests"
echo "all three nodes hold byte-identical snapshots at $(position "$n2"): $digests"

echo "== status of node2"
status "$n2" | jq .
echo "PASS"
