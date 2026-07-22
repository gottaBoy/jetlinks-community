#!/usr/bin/env bash
set -euo pipefail

# Cluster Integration Test Suite
# Prerequisites: docker-compose-cluster.yml running with 3 nodes

BASE_URL="${BASE_URL:-http://localhost:8848}"
ADMIN_USER="${ADMIN_USER:-admin}"
ADMIN_PASS="${ADMIN_PASS:-JetLinks.C0mmVn1ty}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0

log_pass() { echo -e "${GREEN}[PASS]${NC} $1"; ((PASS++)); }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; ((FAIL++)); }
log_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

# Get auth token
get_token() {
    curl -sf -X POST "${BASE_URL}/authorize/login" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"${ADMIN_USER}\",\"password\":\"${ADMIN_PASS}\"}" \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('result',{}).get('token',''))" 2>/dev/null
}

api() {
    local method=$1
    local path=$2
    local data=${3:-}
    local token=$(get_token)
    if [[ -n "$data" ]]; then
        curl -sf -X "$method" "${BASE_URL}${path}" \
            -H "Content-Type: application/json" \
            -H "X-Access-Token: ${token}" \
            -d "$data" 2>/dev/null
    else
        curl -sf -X "$method" "${BASE_URL}${path}" \
            -H "X-Access-Token: ${token}" 2>/dev/null
    fi
}

echo "============================================"
echo "  Cluster Integration Test Suite"
echo "============================================"
echo ""

# Test 1: All 3 nodes healthy
log_info "Test 1: Checking all nodes are healthy..."
for node in jetlinks-node-1 jetlinks-node-2 jetlinks-node-3; do
    status=$(docker inspect --format='{{.State.Health.Status}}' "$node" 2>/dev/null || echo "unhealthy")
    if [[ "$status" == "healthy" ]]; then
        log_pass "Node $node is healthy"
    else
        state=$(docker inspect --format='{{.State.Running}}' "$node" 2>/dev/null || echo "false")
        if [[ "$state" == "true" ]]; then
            log_pass "Node $node is running (no healthcheck configured)"
        else
            log_fail "Node $node is not healthy: $status"
        fi
    fi
done

# Test 2: Scalecube cluster membership
log_info "Test 2: Checking Scalecube cluster membership..."
for node in jetlinks-node-1 jetlinks-node-2 jetlinks-node-3; do
    members=$(docker logs "$node" 2>&1 | grep -c "onMemberAdded\|member.*joined\|cluster.*member" || echo "0")
    if [[ "$members" -gt 0 ]]; then
        log_pass "Node $node has cluster membership logs"
    else
        log_fail "Node $node: no cluster membership logs found"
    fi
done

# Test 3: API accessible through Nginx LB
log_info "Test 3: Checking API through load balancer..."
response=$(curl -sf -o /dev/null -w "%{http_code}" "${BASE_URL}/actuator/health" 2>/dev/null || echo "000")
if [[ "$response" == "200" ]]; then
    log_pass "API accessible through LB (HTTP $response)"
else
    log_fail "API not accessible through LB (HTTP $response)"
fi

# Test 4: Login works
log_info "Test 4: Checking authentication..."
token=$(get_token)
if [[ -n "$token" ]]; then
    log_pass "Authentication works (token obtained)"
else
    log_fail "Authentication failed"
fi

# Test 5: Redis connectivity
log_info "Test 5: Checking Redis..."
redis_ping=$(docker exec cluster-redis redis-cli -a "JetLinks@redis" ping 2>/dev/null)
if [[ "$redis_ping" == "PONG" ]]; then
    log_pass "Redis is accessible"
else
    log_fail "Redis is not accessible"
fi

# Test 6: Room creation (distributed)
log_info "Test 6: Testing distributed room operations..."
# This requires actual device registrations - print the manual test steps
log_info "  Manual verification needed:"
log_info "  1. Register cockpit device on Node-1"
log_info "  2. Register vehicle device on Node-2"
log_info "  3. Perform takeover via API"
log_info "  4. Verify Redis keys: pd:room:info:*, pd:room:idx:*"
log_info "  5. Verify message routing across nodes"

# Test 7: Redis room keys
log_info "Test 7: Checking Redis room keys..."
room_keys=$(docker exec cluster-redis redis-cli -a "JetLinks@redis" keys "pd:room:*" 2>/dev/null | wc -l)
log_info "  Found $room_keys room-related Redis keys"

# Test 8: Distributed lock
log_info "Test 8: Testing distributed lock..."
lock_result=$(docker exec cluster-redis redis-cli -a "JetLinks@redis" SET "pd:lock:vehicle:test-lock" "test" NX EX 10 2>/dev/null)
if [[ "$lock_result" == "OK" ]]; then
    log_pass "Distributed lock SETNX works"
    docker exec cluster-redis redis-cli -a "JetLinks@redis" DEL "pd:lock:vehicle:test-lock" >/dev/null 2>&1
else
    log_fail "Distributed lock SETNX failed"
fi

# Test 9: WebSocket connectivity
log_info "Test 9: Testing WebSocket endpoint..."
ws_check=$(curl -sf -o /dev/null -w "%{http_code}" \
    -H "Upgrade: websocket" -H "Connection: upgrade" \
    "${BASE_URL}/parallel-driving/ws?vehicleId=test" 2>/dev/null || echo "000")
log_info "  WebSocket endpoint returned HTTP $ws_check"

echo ""
echo "============================================"
echo "  Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}"
echo "============================================"

# Cleanup test data
docker exec cluster-redis redis-cli -a "JetLinks@redis" DEL "pd:lock:vehicle:test-lock" >/dev/null 2>&1 || true

exit $FAIL
