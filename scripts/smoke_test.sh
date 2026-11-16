#!/usr/bin/env bash
# Talks to a running server with curl and checks the answers.
#
#   scripts/smoke_test.sh load  [URL]      create the "demo" collection, add 3 vectors, search
#   scripts/smoke_test.sh add   [URL]      add a 4th vector and replace the 2nd
#   scripts/smoke_test.sh check [URL] N    expect N vectors and the same search answer as before
#
# CI runs these around "docker stop" and "docker kill" to show the data survives both.
set -euo pipefail

command="${1:?usage: smoke_test.sh load|add|check [URL] [N]}"
url="${2:-http://localhost:8080}"

# Prints the response body; fails if the status is not the expected one.
request() {
    local expected="$1" method="$2" path="$3" body="${4:-}"
    local output status
    if [ -n "$body" ]; then
        output=$(curl -sS -X "$method" "$url$path" -H "Content-Type: application/json" -d "$body" -w $'\n%{http_code}')
    else
        output=$(curl -sS -X "$method" "$url$path" -w $'\n%{http_code}')
    fi
    status="${output##*$'\n'}"
    output="${output%$'\n'*}"
    echo "$method $path -> $status $output" >&2
    if [ "$status" != "$expected" ]; then
        echo "FAIL: expected status $expected" >&2
        exit 1
    fi
    printf '%s' "$output"
}

expect() {
    local actual="$1" wanted="$2"
    if [ "$actual" != "$wanted" ]; then
        echo "FAIL: expected $wanted" >&2
        exit 1
    fi
}

for attempt in $(seq 1 60); do
    if curl -sf "$url/health" > /dev/null 2>&1; then
        break
    fi
    if [ "$attempt" = 60 ]; then
        echo "FAIL: no answer from $url/health after 30 seconds" >&2
        exit 1
    fi
    sleep 0.5
done

case "$command" in
    load)
        request 201 PUT /collections/demo '{"dimension": 4, "distance": "l2"}' > /dev/null
        request 200 POST /collections/demo/vectors '{"vectors": [
            {"id": 1, "vector": [1, 0, 0, 0]},
            {"id": 2, "vector": [0, 1, 0, 0]},
            {"id": 3, "vector": [0, 0, 1, 0]}]}' > /dev/null
        expect "$(request 200 POST /collections/demo/search '{"vector": [0.5, 1, 0, 0], "k": 2}')" \
            '{"results":[{"id":2,"distance":0.25},{"id":1,"distance":1.25}]}'
        ;;
    add)
        expect "$(request 200 POST /collections/demo/vectors '{"vectors": [
            {"id": 4, "vector": [0, 0, 0, 1]},
            {"id": 2, "vector": [0, 1, 0, 0]}]}')" '{"upserted":2,"replaced":1}'
        ;;
    check)
        size="${3:?check needs the expected number of vectors}"
        expect "$(request 200 GET /collections/demo)" \
            "{\"name\":\"demo\",\"dimension\":4,\"distance\":\"l2\",\"m\":16,\"efConstruction\":200,\"efSearch\":50,\"size\":$size}"
        expect "$(request 200 POST /collections/demo/search '{"vector": [0.5, 1, 0, 0], "k": 2}')" \
            '{"results":[{"id":2,"distance":0.25},{"id":1,"distance":1.25}]}'
        expect "$(request 200 GET /collections/demo/vectors/3)" '{"id":3,"vector":[0.0,0.0,1.0,0.0]}'
        ;;
    *)
        echo "unknown command: $command" >&2
        exit 2
        ;;
esac
echo "PASS: $command"
