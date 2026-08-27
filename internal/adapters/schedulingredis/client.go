package schedulingredis

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/uu999/evalfrog/internal/platform/config"
	"github.com/uu999/evalfrog/internal/scheduling"
)

type Client struct {
	client *redis.Client
	prefix string
}

func Open(value config.RedisEndpointConfig) *Client {
	return &Client{client: redis.NewClient(&redis.Options{
		Addr: value.Address, Password: value.Password, DB: value.DB,
		DialTimeout: value.OperationTimeout.Duration(), ReadTimeout: value.OperationTimeout.Duration(),
		WriteTimeout: value.OperationTimeout.Duration(), PoolTimeout: value.OperationTimeout.Duration(),
		DialerRetries: 1, DialerRetryTimeout: value.OperationTimeout.Duration(),
		ContextTimeoutEnabled: true, MaxRetries: value.MaxRetries,
	}), prefix: value.KeyPrefix}
}

func (client *Client) Check(ctx context.Context) error {
	if err := client.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("Scheduling Redis ping: %w", err)
	}
	policyConfiguration, err := client.client.ConfigGet(ctx, "maxmemory-policy").Result()
	if err != nil {
		return fmt.Errorf("Scheduling Redis memory configuration: %w", err)
	}
	if policyConfiguration["maxmemory-policy"] != "noeviction" {
		return fmt.Errorf("Scheduling Redis maxmemory-policy must be noeviction")
	}
	memoryConfiguration, err := client.client.ConfigGet(ctx, "maxmemory").Result()
	if err != nil {
		return fmt.Errorf("Scheduling Redis memory limit: %w", err)
	}
	maximum, err := strconv.ParseInt(memoryConfiguration["maxmemory"], 10, 64)
	if err != nil || maximum <= 0 {
		return fmt.Errorf("Scheduling Redis maxmemory must be explicitly positive")
	}
	return nil
}

func (client *Client) Close() error { return client.client.Close() }

var registerWorkerScript = redis.NewScript(`
local now = redis.call('TIME')
local expires = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000) + tonumber(ARGV[3])
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[4])
redis.call('ZADD', KEYS[3], expires, ARGV[1])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]) * 2)
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[3]) * 2)
redis.call('PEXPIRE', KEYS[3], tonumber(ARGV[3]) * 2)
return expires
`)

func (client *Client) RegisterWorker(ctx context.Context, registration scheduling.WorkerRegistration) error {
	if err := registration.Validate(); err != nil {
		return err
	}
	tag := "{workers:" + string(registration.ResourceClass) + "}"
	_, err := registerWorkerScript.Run(ctx, client.client,
		[]string{client.prefix + tag + ":slots", client.prefix + tag + ":capabilities", client.prefix + tag + ":expiry"},
		registration.WorkerID, registration.Slots, registration.TTL.Milliseconds(),
		scheduling.CapabilityFingerprint(registration.ResourceClass)).Result()
	return err
}

var capacityScript = redis.NewScript(`
local now = redis.call('TIME')
local millis = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', millis)
if #expired > 0 then
  redis.call('ZREM', KEYS[3], unpack(expired))
  redis.call('HDEL', KEYS[1], unpack(expired))
  redis.call('HDEL', KEYS[2], unpack(expired))
end
local active = redis.call('ZRANGEBYSCORE', KEYS[3], '(' .. millis, '+inf')
local total = 0
for _, worker in ipairs(active) do
  if redis.call('HGET', KEYS[2], worker) == ARGV[1] then
    total = total + tonumber(redis.call('HGET', KEYS[1], worker) or '0')
  end
end
return total
`)

func (client *Client) HealthyCapacity(ctx context.Context) (scheduling.Capacity, error) {
	result := scheduling.Capacity{Pools: make(map[scheduling.ResourceClass]int, 2)}
	for _, class := range scheduling.ResourceClasses() {
		tag := "{workers:" + string(class) + "}"
		value, err := capacityScript.Run(ctx, client.client,
			[]string{client.prefix + tag + ":slots", client.prefix + tag + ":capabilities", client.prefix + tag + ":expiry"},
			scheduling.CapabilityFingerprint(class)).Int()
		if err != nil {
			return scheduling.Capacity{}, err
		}
		result.Pools[class] = value
	}
	return result, nil
}

var _ scheduling.CapacityRegistry = (*Client)(nil)

func (client *Client) schedulerBase() string { return client.prefix + "{scheduler}:" }

// ClearDerivedState intentionally leaves worker registrations intact.
func (client *Client) ClearDerivedState(ctx context.Context) error {
	var cursor uint64
	for {
		keys, next, err := client.client.Scan(ctx, cursor, client.schedulerBase()+"*", 256).Result()
		if err != nil {
			return err
		}
		if len(keys) != 0 {
			if err = client.client.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

var acquireLeaseScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current and string.sub(current, 1, string.len(ARGV[1]) + 1) ~= ARGV[1] .. '|' then
  return {0, 0}
end
local now = redis.call('TIME')
local wallFence = tonumber(now[1]) * 1000000 + tonumber(now[2])
local previousFence = tonumber(redis.call('GET', KEYS[2]) or '0')
local fence = math.max(previousFence + 1, wallFence)
redis.call('SET', KEYS[2], string.format('%.0f', fence))
local expires = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000) + tonumber(ARGV[3])
redis.call('PSETEX', KEYS[1], ARGV[3], ARGV[1] .. '|' .. ARGV[2])
return {fence, expires}
`)

func (client *Client) AcquireReconcileLease(ctx context.Context, owner string, duration time.Duration) (scheduling.ReconcileLease, error) {
	if owner == "" || duration <= 0 {
		return scheduling.ReconcileLease{}, fmt.Errorf("reconciliation owner and positive lease are required")
	}
	token, err := uuid.NewV7()
	if err != nil {
		return scheduling.ReconcileLease{}, err
	}
	base := client.schedulerBase()
	values, err := acquireLeaseScript.Run(ctx, client.client, []string{base + "lease", base + "fence"}, owner, token.String(), duration.Milliseconds()).Slice()
	if err != nil {
		return scheduling.ReconcileLease{}, err
	}
	fence, err := integer(values[0])
	if err != nil {
		return scheduling.ReconcileLease{}, err
	}
	if fence == 0 {
		return scheduling.ReconcileLease{}, scheduling.ErrLeaseLost
	}
	expires, err := integer(values[1])
	if err != nil {
		return scheduling.ReconcileLease{}, err
	}
	return scheduling.ReconcileLease{Owner: owner, Token: token.String(), FencingToken: uint64(fence), ExpiresAt: time.UnixMilli(expires).UTC()}, nil
}

func (client *Client) RefreshMemoryPressure(ctx context.Context, policy scheduling.MemoryPolicy) (bool, error) {
	if err := policy.Validate(); err != nil {
		return false, err
	}
	base := client.schedulerBase()
	if err := cleanupReservationsScript.Run(ctx, client.client,
		[]string{base + "meta", base + "reservations", base + "reservation-expiry", base + "topics"}, base).Err(); err != nil {
		return false, err
	}
	info, err := client.client.Info(ctx, "memory").Result()
	if err != nil {
		return false, err
	}
	values := parseInfo(info)
	used, usedErr := strconv.ParseFloat(values["used_memory"], 64)
	maximum, maximumErr := strconv.ParseFloat(values["maxmemory"], 64)
	if usedErr != nil || maximumErr != nil {
		return false, fmt.Errorf("Scheduling Redis memory INFO is incomplete")
	}
	meta := client.schedulerBase() + "meta"
	if maximum <= 0 {
		return true, client.client.HSet(ctx, meta, "growth_paused", 0).Err()
	}
	paused, err := client.client.HGet(ctx, meta, "growth_paused").Bool()
	if err != nil && err != redis.Nil {
		return false, err
	}
	ratio := used / maximum
	if paused {
		paused = ratio > policy.ResumeWatermark
	} else {
		paused = ratio >= policy.HighWatermark
	}
	flag := 0
	if paused {
		flag = 1
	}
	if err = client.client.HSet(ctx, meta, "growth_paused", flag).Err(); err != nil {
		return false, err
	}
	return !paused, nil
}

var registerReadyScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'paused') ~= '0' or redis.call('HGET', KEYS[1], 'growth_paused') == '1' then
  return 0
end
local generation = redis.call('HGET', KEYS[1], 'generation')
if not generation then return 0 end
local base = ARGV[1]
local count = tonumber(ARGV[2])
local index = 3
local inserted = 0
for item = 1, count do
  local raw = ARGV[index]; index = index + 1
  local candidate = cjson.decode(raw)
  local candidateIndex = base .. 'gen:' .. generation .. ':candidate-index'
  if redis.call('HSETNX', candidateIndex, candidate.node_run_id, candidate.resource_class .. '|' .. candidate.project_id) == 1 then
    local ready = base .. 'gen:' .. generation .. ':ready:' .. candidate.resource_class
    local active = base .. 'gen:' .. generation .. ':active:' .. candidate.resource_class
    local rawQueue = redis.call('HGET', ready, candidate.project_id)
    local queue = {}
    if rawQueue then queue = cjson.decode(rawQueue) end
    table.insert(queue, candidate)
    table.sort(queue, function(a, b)
      if tonumber(a.ready_bucket) ~= tonumber(b.ready_bucket) then return tonumber(a.ready_bucket) < tonumber(b.ready_bucket) end
      if tonumber(a.priority) ~= tonumber(b.priority) then return tonumber(a.priority) > tonumber(b.priority) end
      if a.ready_order_key ~= b.ready_order_key then return a.ready_order_key < b.ready_order_key end
      return a.node_run_id < b.node_run_id
    end)
    redis.call('HSET', ready, candidate.project_id, cjson.encode(queue))
    redis.call('ZADD', active, tonumber(queue[1].ready_bucket), candidate.project_id)
    inserted = inserted + 1
  end
end
return inserted
`)

func (client *Client) RegisterReady(ctx context.Context, candidates []scheduling.Candidate) error {
	if len(candidates) == 0 {
		return nil
	}
	sortCandidatesForRedis(candidates)
	args := []any{client.schedulerBase(), len(candidates)}
	for _, candidate := range candidates {
		candidate = candidate.Normalized()
		if err := candidate.Validate(); err != nil {
			return err
		}
		encoded, err := json.Marshal(candidate)
		if err != nil {
			return err
		}
		args = append(args, encoded)
	}
	return registerReadyScript.Run(ctx, client.client, []string{client.schedulerBase() + "meta"}, args...).Err()
}

var cleanupReservationsScript = redis.NewScript(`
local now = redis.call('TIME')
local nowms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local generation = redis.call('HGET', KEYS[1], 'generation')
local function removeReservation(attemptID)
  local raw = redis.call('HGET', KEYS[2], attemptID)
  if raw and generation then
    local reservation = cjson.decode(raw)
    local load = ARGV[1] .. 'gen:' .. generation .. ':project-load'
    local projectLoad = tonumber(redis.call('HGET', load, reservation.project_id) or '0')
    if projectLoad <= 1 then
      redis.call('HDEL', load, reservation.project_id)
    else
      redis.call('HINCRBY', load, reservation.project_id, -1)
    end
    local occupancy = tonumber(redis.call('HGET', KEYS[4], 'occupancy:' .. reservation.resource_class) or '0')
    redis.call('HSET', KEYS[4], 'occupancy:' .. reservation.resource_class, math.max(0, occupancy - 1))
  end
  redis.call('HDEL', KEYS[2], attemptID)
  redis.call('ZREM', KEYS[3], attemptID)
end
local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', nowms)
if #expired > 0 then
  for _, attemptID in ipairs(expired) do removeReservation(attemptID) end
end
local reservationIDs = redis.call('HKEYS', KEYS[2])
for _, attemptID in ipairs(reservationIDs) do
  if not redis.call('ZSCORE', KEYS[3], attemptID) then removeReservation(attemptID) end
end
return redis.call('HVALS', KEYS[2])
`)

func (client *Client) ListReservations(ctx context.Context) ([]scheduling.Reservation, error) {
	base := client.schedulerBase()
	rawValues, err := cleanupReservationsScript.Run(ctx, client.client,
		[]string{base + "meta", base + "reservations", base + "reservation-expiry", base + "topics"}, base).StringSlice()
	if err != nil {
		return nil, err
	}
	result := make([]scheduling.Reservation, 0, len(rawValues))
	for _, raw := range rawValues {
		var reservation scheduling.Reservation
		if err = json.Unmarshal([]byte(raw), &reservation); err != nil {
			return nil, fmt.Errorf("decode scheduling reservation: %w", err)
		}
		result = append(result, reservation)
	}
	return result, nil
}

var rebuildScript = redis.NewScript(`
if redis.call('GET', KEYS[2]) ~= ARGV[1] .. '|' .. ARGV[2] then return {} end
if redis.call('HGET', KEYS[1], 'growth_paused') == '1' then return {'__MEMORY__'} end
local generation = ARGV[3]
local base = ARGV[4]
local oldGeneration = redis.call('HGET', KEYS[1], 'generation')
local load = base .. 'gen:' .. generation .. ':project-load'
local last = base .. 'gen:' .. generation .. ':project-last-grant'
local inflightKey = base .. 'gen:' .. generation .. ':inflight'
local candidateIndex = base .. 'gen:' .. generation .. ':candidate-index'
local readyBuiltin = base .. 'gen:' .. generation .. ':ready:builtin'
local readySandbox = base .. 'gen:' .. generation .. ':ready:sandbox'
local activeBuiltin = base .. 'gen:' .. generation .. ':active:builtin'
local activeSandbox = base .. 'gen:' .. generation .. ':active:sandbox'
redis.call('DEL', load, last, inflightKey, candidateIndex, readyBuiltin, readySandbox, activeBuiltin, activeSandbox)
local index = 5
local minimumBuiltin = tonumber(ARGV[index]); index = index + 1
local maximumBuiltin = tonumber(ARGV[index]); index = index + 1
local minimumSandbox = tonumber(ARGV[index]); index = index + 1
local maximumSandbox = tonumber(ARGV[index]); index = index + 1
local seen = {}
local projectCount = 0
local topicBuiltin = 0
local topicSandbox = 0
local inflightCount = tonumber(ARGV[index]); index = index + 1
for item = 1, inflightCount do
  local raw = ARGV[index]; index = index + 1
  local value = cjson.decode(raw)
  if not seen[value.attempt_id] then
    seen[value.attempt_id] = true
    redis.call('HSET', inflightKey, value.attempt_id, raw)
    redis.call('HINCRBY', load, value.project_id, 1)
    if value.queue_occupied then
      if value.resource_class == 'builtin' then topicBuiltin = topicBuiltin + 1 else topicSandbox = topicSandbox + 1 end
    end
    projectCount = projectCount + 1
  end
end
local reservations = redis.call('HGETALL', KEYS[3])
for reservationIndex = 1, #reservations, 2 do
  local attemptID = reservations[reservationIndex]
  local raw = reservations[reservationIndex + 1]
  if seen[attemptID] then
    redis.call('HDEL', KEYS[3], attemptID)
    redis.call('ZREM', KEYS[4], attemptID)
  else
    local reservation = cjson.decode(raw)
    redis.call('HINCRBY', load, reservation.project_id, 1)
    if reservation.resource_class == 'builtin' then topicBuiltin = topicBuiltin + 1 else topicSandbox = topicSandbox + 1 end
  end
end
local groupCount = tonumber(ARGV[index]); index = index + 1
local candidateCount = 0
for groupIndex = 1, groupCount do
  local class = ARGV[index]; index = index + 1
  local project = ARGV[index]; index = index + 1
  local rawQueue = ARGV[index]; index = index + 1
  local queue = cjson.decode(rawQueue)
  local ready = base .. 'gen:' .. generation .. ':ready:' .. class
  local active = base .. 'gen:' .. generation .. ':active:' .. class
  redis.call('HSET', ready, project, rawQueue)
  redis.call('ZADD', active, tonumber(queue[1].ready_bucket), project)
  for _, candidate in ipairs(queue) do
    redis.call('HSET', candidateIndex, candidate.node_run_id, class .. '|' .. project)
    candidateCount = candidateCount + 1
  end
end
local existingBuiltin = tonumber(redis.call('HGET', KEYS[5], 'window:builtin') or minimumBuiltin)
local existingSandbox = tonumber(redis.call('HGET', KEYS[5], 'window:sandbox') or minimumSandbox)
redis.call('HSET', KEYS[5], 'window:builtin', math.max(minimumBuiltin, math.min(maximumBuiltin, existingBuiltin)), 'window:sandbox', math.max(minimumSandbox, math.min(maximumSandbox, existingSandbox)))
redis.call('HSET', KEYS[5], 'occupancy:builtin', topicBuiltin, 'occupancy:sandbox', topicSandbox)
redis.call('HSET', KEYS[1], 'generation', generation, 'paused', 0, 'grant_seq', 0)
if oldGeneration and oldGeneration ~= generation then
  local old = base .. 'gen:' .. oldGeneration .. ':'
  redis.call('DEL', old .. 'project-load', old .. 'project-last-grant', old .. 'inflight', old .. 'candidate-index', old .. 'ready:builtin', old .. 'ready:sandbox', old .. 'active:builtin', old .. 'active:sandbox')
end
return {generation, candidateCount, inflightCount, redis.call('HGET', KEYS[5], 'window:builtin'), topicBuiltin, redis.call('HGET', KEYS[5], 'ewma:builtin') or '0', redis.call('HGET', KEYS[5], 'window:sandbox'), topicSandbox, redis.call('HGET', KEYS[5], 'ewma:sandbox') or '0'}
`)

func (client *Client) Rebuild(ctx context.Context, lease scheduling.ReconcileLease, snapshot scheduling.AuthoritySnapshot, policy scheduling.TopicWindowPolicy) (scheduling.ReconcileResult, error) {
	if err := policy.Validate(); err != nil {
		return scheduling.ReconcileResult{}, err
	}
	groups := make(map[scheduling.ResourceClass]map[string][]scheduling.Candidate, 2)
	for _, class := range scheduling.ResourceClasses() {
		groups[class] = map[string][]scheduling.Candidate{}
	}
	for _, candidate := range snapshot.Candidates {
		candidate = candidate.Normalized()
		if err := candidate.Validate(); err != nil {
			return scheduling.ReconcileResult{}, err
		}
		groups[candidate.ResourceClass][candidate.ProjectID] = append(groups[candidate.ResourceClass][candidate.ProjectID], candidate)
	}
	args := []any{
		lease.Owner,
		lease.Token,
		lease.FencingToken,
		client.schedulerBase(),
		policy.Minimum[scheduling.ResourceBuiltin],
		policy.Maximum[scheduling.ResourceBuiltin],
		policy.Minimum[scheduling.ResourceSandbox],
		policy.Maximum[scheduling.ResourceSandbox],
		len(snapshot.Inflight),
	}
	for _, value := range snapshot.Inflight {
		if value.AttemptID == "" || value.ProjectID == "" || !value.ResourceClass.Valid() {
			return scheduling.ReconcileResult{}, fmt.Errorf("inflight scheduling record is invalid")
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return scheduling.ReconcileResult{}, err
		}
		args = append(args, encoded)
	}
	type group struct {
		class   scheduling.ResourceClass
		project string
		values  []scheduling.Candidate
	}
	ordered := make([]group, 0)
	for _, class := range scheduling.ResourceClasses() {
		for project, values := range groups[class] {
			sortCandidatesForRedis(values)
			ordered = append(ordered, group{class: class, project: project, values: values})
		}
	}
	sort.Slice(ordered, func(left, right int) bool {
		if ordered[left].class != ordered[right].class {
			return ordered[left].class < ordered[right].class
		}
		return ordered[left].project < ordered[right].project
	})
	args = append(args, len(ordered))
	for _, value := range ordered {
		encoded, err := json.Marshal(value.values)
		if err != nil {
			return scheduling.ReconcileResult{}, err
		}
		args = append(args, string(value.class), value.project, encoded)
	}
	base := client.schedulerBase()
	values, err := rebuildScript.Run(ctx, client.client, []string{base + "meta", base + "lease", base + "reservations", base + "reservation-expiry", base + "topics"}, args...).Slice()
	if err != nil {
		return scheduling.ReconcileResult{}, err
	}
	if len(values) == 0 {
		return scheduling.ReconcileResult{}, scheduling.ErrLeaseLost
	}
	if fmt.Sprint(values[0]) == "__MEMORY__" {
		return scheduling.ReconcileResult{}, scheduling.ErrMemoryPressure
	}
	return decodeReconcileResult(values)
}

var calibrateScript = redis.NewScript(`
if redis.call('GET', KEYS[2]) ~= ARGV[1] .. '|' .. ARGV[2] then return {} end
local alpha = tonumber(ARGV[3])
local interval = tonumber(ARGV[4])
local buffer = tonumber(ARGV[5])
local result = {}
local index = 6
for item = 1, 2 do
  local class = ARGV[index]; index = index + 1
  local minimum = tonumber(ARGV[index]); index = index + 1
  local maximum = tonumber(ARGV[index]); index = index + 1
  local completed = tonumber(redis.call('HGET', KEYS[3], class) or '0')
  redis.call('HSET', KEYS[3], class, 0)
  local rate = completed / interval
  local previous = redis.call('HGET', KEYS[1], 'ewma:' .. class)
  local ewma = rate
  if previous then ewma = alpha * rate + (1 - alpha) * tonumber(previous) end
  local window = math.ceil(ewma * buffer)
  window = math.max(minimum, math.min(maximum, window))
  redis.call('HSET', KEYS[1], 'ewma:' .. class, ewma, 'window:' .. class, window)
  table.insert(result, class)
  table.insert(result, window)
  table.insert(result, tonumber(redis.call('HGET', KEYS[1], 'occupancy:' .. class) or '0'))
  table.insert(result, ewma)
end
return result
`)

func (client *Client) CalibrateTopicWindows(ctx context.Context, lease scheduling.ReconcileLease, policy scheduling.TopicWindowPolicy) (map[scheduling.ResourceClass]scheduling.TopicState, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	base := client.schedulerBase()
	args := []any{lease.Owner, lease.Token, policy.EWMAAlpha, policy.SampleInterval.Seconds(), policy.BufferDuration.Seconds()}
	for _, class := range scheduling.ResourceClasses() {
		args = append(args, string(class), policy.Minimum[class], policy.Maximum[class])
	}
	values, err := calibrateScript.Run(ctx, client.client, []string{base + "topics", base + "lease", base + "completions"}, args...).Slice()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, scheduling.ErrLeaseLost
	}
	result := make(map[scheduling.ResourceClass]scheduling.TopicState, 2)
	for index := 0; index+3 < len(values); index += 4 {
		window, err := integer(values[index+1])
		if err != nil {
			return nil, err
		}
		occupancy, err := integer(values[index+2])
		if err != nil {
			return nil, err
		}
		ewma, err := number(values[index+3])
		if err != nil {
			return nil, err
		}
		result[scheduling.ResourceClass(fmt.Sprint(values[index]))] = scheduling.TopicState{Window: int(window), Occupancy: int(occupancy), EWMA: ewma}
	}
	return result, nil
}

var reserveScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'paused') ~= '0' then return {'__PAUSED__'} end
local generation = redis.call('HGET', KEYS[1], 'generation')
if not generation then return {'__PAUSED__'} end
local existing = redis.call('HGET', KEYS[2], ARGV[3])
if existing then return {existing} end
local class = ARGV[2]
local window = tonumber(redis.call('HGET', KEYS[4], 'window:' .. class) or '0')
local occupancy = tonumber(redis.call('HGET', KEYS[4], 'occupancy:' .. class) or '0')
if occupancy >= window then return {} end
local base = ARGV[1]
local ready = base .. 'gen:' .. generation .. ':ready:' .. class
local active = base .. 'gen:' .. generation .. ':active:' .. class
local load = base .. 'gen:' .. generation .. ':project-load'
local last = base .. 'gen:' .. generation .. ':project-last-grant'
local candidateIndex = base .. 'gen:' .. generation .. ':candidate-index'
local oldest = redis.call('ZRANGE', active, 0, 0, 'WITHSCORES')
if #oldest == 0 then return {} end
local bucket = oldest[2]
local projects = redis.call('ZRANGEBYSCORE', active, bucket, bucket)
local selected = nil
local selectedLoad = nil
local selectedLast = nil
for _, project in ipairs(projects) do
  local projectLoad = tonumber(redis.call('HGET', load, project) or '0')
  local projectLast = tonumber(redis.call('HGET', last, project) or '0')
  if not selected or projectLoad < selectedLoad or (projectLoad == selectedLoad and projectLast < selectedLast) or (projectLoad == selectedLoad and projectLast == selectedLast and project < selected) then
    selected = project
    selectedLoad = projectLoad
    selectedLast = projectLast
  end
end
if not selected then return {} end
local rawQueue = redis.call('HGET', ready, selected)
if not rawQueue then
  redis.call('ZREM', active, selected)
  return {}
end
local queue = cjson.decode(rawQueue)
local candidate = table.remove(queue, 1)
if #queue == 0 then
  redis.call('HDEL', ready, selected)
  redis.call('ZREM', active, selected)
else
  redis.call('HSET', ready, selected, cjson.encode(queue))
  redis.call('ZADD', active, tonumber(queue[1].ready_bucket), selected)
end
redis.call('HDEL', candidateIndex, candidate.node_run_id)
redis.call('HINCRBY', load, selected, 1)
redis.call('HINCRBY', KEYS[4], 'occupancy:' .. class, 1)
local sequence = redis.call('HINCRBY', KEYS[1], 'grant_seq', 1)
redis.call('HSET', last, selected, sequence)
local reservation = cjson.encode({attempt_id=ARGV[3], project_id=selected, resource_class=class, candidate=candidate})
redis.call('HSET', KEYS[2], ARGV[3], reservation)
local now = redis.call('TIME')
local nowms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
redis.call('ZADD', KEYS[3], nowms + tonumber(ARGV[4]), ARGV[3])
return {reservation}
`)

func (client *Client) ReserveNext(ctx context.Context, class scheduling.ResourceClass, attemptID string, ttl time.Duration) (scheduling.Reservation, bool, error) {
	if !class.Valid() || attemptID == "" || ttl <= 0 {
		return scheduling.Reservation{}, false, fmt.Errorf("reservation class, attempt and TTL are required")
	}
	base := client.schedulerBase()
	values, err := reserveScript.Run(ctx, client.client, []string{base + "meta", base + "reservations", base + "reservation-expiry", base + "topics"}, base, string(class), attemptID, ttl.Milliseconds()).Slice()
	if err != nil {
		return scheduling.Reservation{}, false, err
	}
	if len(values) == 0 {
		return scheduling.Reservation{}, false, nil
	}
	if fmt.Sprint(values[0]) == "__PAUSED__" {
		return scheduling.Reservation{}, false, scheduling.ErrAdmissionPaused
	}
	var reservation scheduling.Reservation
	if err = json.Unmarshal([]byte(fmt.Sprint(values[0])), &reservation); err != nil {
		return scheduling.Reservation{}, false, err
	}
	return reservation, true, nil
}

var confirmScript = redis.NewScript(`
local raw = redis.call('HGET', KEYS[2], ARGV[2])
local generation = redis.call('HGET', KEYS[1], 'generation')
if not generation then return 0 end
local inflight = ARGV[1] .. 'gen:' .. generation .. ':inflight'
if not raw then
  if redis.call('HEXISTS', inflight, ARGV[2]) == 1 then return 1 end
  return 0
end
local reservation = cjson.decode(raw)
local compact = cjson.encode({attempt_id=reservation.attempt_id, project_id=reservation.project_id, resource_class=reservation.resource_class, queue_occupied=true})
redis.call('HSET', inflight, ARGV[2], compact)
redis.call('HDEL', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[2])
return 1
`)

func (client *Client) ConfirmReservation(ctx context.Context, reservation scheduling.Reservation) error {
	base := client.schedulerBase()
	return confirmScript.Run(ctx, client.client, []string{base + "meta", base + "reservations", base + "reservation-expiry"}, base, reservation.AttemptID).Err()
}

var abortScript = redis.NewScript(`
local raw = redis.call('HGET', KEYS[2], ARGV[2])
if not raw then
  redis.call('ZREM', KEYS[3], ARGV[2])
  return 0
end
local reservation = cjson.decode(raw)
local generation = redis.call('HGET', KEYS[1], 'generation')
if ARGV[3] == '1' and generation then
  local ready = ARGV[1] .. 'gen:' .. generation .. ':ready:' .. reservation.resource_class
  local active = ARGV[1] .. 'gen:' .. generation .. ':active:' .. reservation.resource_class
  local candidateIndex = ARGV[1] .. 'gen:' .. generation .. ':candidate-index'
  local rawQueue = redis.call('HGET', ready, reservation.project_id)
  local queue = {}
  if rawQueue then queue = cjson.decode(rawQueue) end
  redis.call('HSET', candidateIndex, reservation.candidate.node_run_id, reservation.resource_class .. '|' .. reservation.project_id)
  table.insert(queue, reservation.candidate)
  table.sort(queue, function(a, b)
    if tonumber(a.ready_bucket) ~= tonumber(b.ready_bucket) then return tonumber(a.ready_bucket) < tonumber(b.ready_bucket) end
    if tonumber(a.priority) ~= tonumber(b.priority) then return tonumber(a.priority) > tonumber(b.priority) end
    if a.ready_order_key ~= b.ready_order_key then return a.ready_order_key < b.ready_order_key end
    return a.node_run_id < b.node_run_id
  end)
  redis.call('ZADD', active, tonumber(queue[1].ready_bucket), reservation.project_id)
  redis.call('HSET', ready, reservation.project_id, cjson.encode(queue))
end
redis.call('HDEL', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[2])
local load = ARGV[1] .. 'gen:' .. generation .. ':project-load'
local value = tonumber(redis.call('HGET', load, reservation.project_id) or '0')
if value <= 1 then redis.call('HDEL', load, reservation.project_id) else redis.call('HINCRBY', load, reservation.project_id, -1) end
local occupancy = tonumber(redis.call('HGET', KEYS[4], 'occupancy:' .. reservation.resource_class) or '0')
redis.call('HSET', KEYS[4], 'occupancy:' .. reservation.resource_class, math.max(0, occupancy - 1))
return 1
`)

func (client *Client) AbortReservation(ctx context.Context, reservation scheduling.Reservation, restore bool) error {
	base := client.schedulerBase()
	flag := 0
	if restore {
		flag = 1
	}
	return abortScript.Run(ctx, client.client, []string{base + "meta", base + "reservations", base + "reservation-expiry", base + "topics"}, base, reservation.AttemptID, flag).Err()
}

var markClaimedScript = redis.NewScript(`
local generation = redis.call('HGET', KEYS[1], 'generation')
if not generation then return 0 end
local inflightKey = ARGV[1] .. 'gen:' .. generation .. ':inflight'
local raw = redis.call('HGET', inflightKey, ARGV[2])
if raw then
  local value = cjson.decode(raw)
  if not value.queue_occupied then return 0 end
  value.queue_occupied = false
  redis.call('HSET', inflightKey, ARGV[2], cjson.encode(value))
  local occupancy = tonumber(redis.call('HGET', KEYS[4], 'occupancy:' .. value.resource_class) or '0')
  redis.call('HSET', KEYS[4], 'occupancy:' .. value.resource_class, math.max(0, occupancy - 1))
  return 1
end
raw = redis.call('HGET', KEYS[2], ARGV[2])
if not raw then return 0 end
local reservation = cjson.decode(raw)
local compact = cjson.encode({attempt_id=reservation.attempt_id, project_id=reservation.project_id, resource_class=reservation.resource_class, queue_occupied=false})
redis.call('HSET', inflightKey, ARGV[2], compact)
redis.call('HDEL', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[2])
local occupancy = tonumber(redis.call('HGET', KEYS[4], 'occupancy:' .. reservation.resource_class) or '0')
redis.call('HSET', KEYS[4], 'occupancy:' .. reservation.resource_class, math.max(0, occupancy - 1))
return 1
`)

func (client *Client) MarkClaimed(ctx context.Context, attemptID string) error {
	if attemptID == "" {
		return fmt.Errorf("claimed attempt is required")
	}
	base := client.schedulerBase()
	return markClaimedScript.Run(ctx, client.client, []string{base + "meta", base + "reservations", base + "reservation-expiry", base + "topics"}, base, attemptID).Err()
}

var markTerminalScript = redis.NewScript(`
local generation = redis.call('HGET', KEYS[1], 'generation')
if not generation then return 0 end
local inflightKey = ARGV[1] .. 'gen:' .. generation .. ':inflight'
local raw = redis.call('HGET', inflightKey, ARGV[2])
local fromReservation = false
if not raw then
  raw = redis.call('HGET', KEYS[2], ARGV[2])
  fromReservation = true
end
if not raw then return 0 end
local value = cjson.decode(raw)
local queueOccupied = value.queue_occupied
if queueOccupied == nil then queueOccupied = true end
if fromReservation then
  redis.call('HDEL', KEYS[2], ARGV[2])
  redis.call('ZREM', KEYS[3], ARGV[2])
else
  redis.call('HDEL', inflightKey, ARGV[2])
end
local load = ARGV[1] .. 'gen:' .. generation .. ':project-load'
local last = ARGV[1] .. 'gen:' .. generation .. ':project-last-grant'
local projectLoad = tonumber(redis.call('HGET', load, value.project_id) or '0')
if projectLoad <= 1 then
  redis.call('HDEL', load, value.project_id)
  local activeBuiltin = ARGV[1] .. 'gen:' .. generation .. ':active:builtin'
  local activeSandbox = ARGV[1] .. 'gen:' .. generation .. ':active:sandbox'
  if not redis.call('ZSCORE', activeBuiltin, value.project_id) and not redis.call('ZSCORE', activeSandbox, value.project_id) then
    redis.call('HDEL', last, value.project_id)
  end
else
  redis.call('HINCRBY', load, value.project_id, -1)
end
if queueOccupied then
  local occupancy = tonumber(redis.call('HGET', KEYS[4], 'occupancy:' .. value.resource_class) or '0')
  redis.call('HSET', KEYS[4], 'occupancy:' .. value.resource_class, math.max(0, occupancy - 1))
end
if ARGV[3] == '1' then redis.call('HINCRBY', KEYS[5], value.resource_class, 1) end
return 1
`)

func (client *Client) MarkTerminal(ctx context.Context, attemptID string, completed bool) error {
	if attemptID == "" {
		return fmt.Errorf("terminal attempt is required")
	}
	flag := 0
	if completed {
		flag = 1
	}
	base := client.schedulerBase()
	return markTerminalScript.Run(ctx, client.client, []string{base + "meta", base + "reservations", base + "reservation-expiry", base + "topics", base + "completions"}, base, attemptID, flag).Err()
}

func decodeReconcileResult(values []any) (scheduling.ReconcileResult, error) {
	generation, err := integer(values[0])
	if err != nil {
		return scheduling.ReconcileResult{}, err
	}
	candidates, err := integer(values[1])
	if err != nil {
		return scheduling.ReconcileResult{}, err
	}
	inflight, err := integer(values[2])
	if err != nil {
		return scheduling.ReconcileResult{}, err
	}
	result := scheduling.ReconcileResult{Generation: uint64(generation), CandidateCount: int(candidates), InflightCount: int(inflight), Topics: map[scheduling.ResourceClass]scheduling.TopicState{}}
	for index, class := range scheduling.ResourceClasses() {
		offset := 3 + index*3
		window, valueErr := integer(values[offset])
		if valueErr != nil {
			return scheduling.ReconcileResult{}, valueErr
		}
		occupancy, valueErr := integer(values[offset+1])
		if valueErr != nil {
			return scheduling.ReconcileResult{}, valueErr
		}
		ewma, valueErr := number(values[offset+2])
		if valueErr != nil {
			return scheduling.ReconcileResult{}, valueErr
		}
		result.Topics[class] = scheduling.TopicState{Window: int(window), Occupancy: int(occupancy), EWMA: ewma}
	}
	return result, nil
}

func sortCandidatesForRedis(values []scheduling.Candidate) {
	for index := range values {
		values[index] = values[index].Normalized()
	}
	sort.Slice(values, func(left, right int) bool {
		if values[left].ReadyBucket != values[right].ReadyBucket {
			return values[left].ReadyBucket < values[right].ReadyBucket
		}
		if values[left].Priority != values[right].Priority {
			return values[left].Priority > values[right].Priority
		}
		if values[left].ReadyOrderKey != values[right].ReadyOrderKey {
			return values[left].ReadyOrderKey < values[right].ReadyOrderKey
		}
		return values[left].NodeRunID < values[right].NodeRunID
	})
}

func parseInfo(value string) map[string]string {
	result := map[string]string{}
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, raw, found := strings.Cut(line, ":")
		if found {
			result[key] = raw
		}
	}
	return result
}

func integer(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case float64:
		return int64(typed), nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected Redis integer %T", value)
	}
}

func number(value any) (float64, error) {
	switch typed := value.(type) {
	case float64:
		return typed, nil
	case int64:
		return float64(typed), nil
	case string:
		return strconv.ParseFloat(typed, 64)
	case []byte:
		return strconv.ParseFloat(string(typed), 64)
	default:
		return math.NaN(), fmt.Errorf("unexpected Redis number %T", value)
	}
}

var _ scheduling.CoordinationStore = (*Client)(nil)
