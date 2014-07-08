-------------------------------------------------------------------------------
-- Resource Class
--
-- Returns an object that represents a resource with the provided RID
-------------------------------------------------------------------------------

----
-- This gets all the data associated with the resource with the provided id. If the
-- job is not found, it returns nil. If found, it returns an object with the
-- appropriate properties
function QlessResource:data(...)
  local res = redis.call(
      'hmget', QlessResource.ns .. self.rid, 'rid', 'max')

  -- Return nil if we haven't found it
  if not res[1] then
    return nil
  end

  local data = {
    rid          = res[1],
    max          = tonumber(res[2] or 0),
    pending      = self:pending(),
    locks        = self:locks(),
  }

  return data
end

function QlessResource:get(...)
  local res = redis.call(
      'hmget', QlessResource.ns .. self.rid, 'rid', 'max')

  -- Return nil if we haven't found it
  if not res[1] then
    return nil
  end

  return tonumber(res[2] or 0)
end

function QlessResource:set(now, max)
  local max = assert(tonumber(max), 'Set(): Arg "max" not a number: ' .. tostring(max))

  local current_max = self:get()
  if current_max == nil then
    current_max = max
  end

  local keyLocks = self:prefix('locks')
  local current_locks = redis.pcall('scard', keyLocks)
  -- get the max of the current limit and the current locks
  -- this is just in case the limit was decreased immediately before and the locks have not come down to the limit yet.
  local confirm_limit = math.max(current_max,current_locks)
  local max_change = max - confirm_limit
  local keyPending = self:prefix('pending')

  redis.call('hmset', QlessResource.ns .. self.rid, 'rid', self.rid, 'max', max);

  if max_change > 0 then
    local jids = redis.call('zrevrange', keyPending, 0, max_change - 1, 'withscores')
    local jid_count = #jids
    if jid_count == 0 then
      return self.rid
    end

    for i = 1, jid_count, 2 do

      local newJid = jids[i]
      local score = jids[i + 1]

      -- we know there is capacity to get this released resource, need to check all resources in case waiting on multiple
      if Qless.job(newJid):acquire_resources(now) then
        local data = Qless.job(newJid):data()
        local queue = Qless.queue(data['queue'])
        queue.work.add(score, 0, newJid)
      end
    end
  end

  return self.rid
end

function QlessResource:unset()
  return redis.call('del', QlessResource.ns .. self.rid);
end

function QlessResource:prefix(group)
  if group then
    return QlessResource.ns..self.rid..'-'..group
  end

  return QlessResource.ns..self.rid
end

function QlessResource:acquire(now, priority, jid)
  local keyLocks = self:prefix('locks')
  local max = self:get()
  if max == nil then
    error({code=1, msg='Acquire(): resource ' .. self.rid .. ' does not exist'})
  end

  if type(jid) ~= 'string' then
    error({code=2, msg='Acquire(): invalid jid; expected string, got \'' .. type(jid) .. '\''})
  end


  -- check if already has a lock, then just return.  This is used for when multiple resources are needed.
  if redis.call('sismember', self:prefix('locks'), jid) == 1 then
    return true
  end

  local remaining = max - redis.pcall('scard', keyLocks)

  if remaining > 0 then
    -- acquire a lock and release it from the pending queue
    redis.call('sadd', keyLocks, jid)
    redis.call('zrem', self:prefix('pending'), jid)

    return true
  end

  -- check if already pending, then don't update its priority.
  if redis.call('zscore', self:prefix('pending'), jid) == false then
    redis.call('zadd', self:prefix('pending'), priority - (now / 10000000000), jid)
  end

  return false
end

--- Releases the resource for the specified job identifier and assigns it to the next waiting job
-- @param now
-- @param jid
--
function QlessResource:release(now, jid)
  local keyLocks = self:prefix('locks')
  local keyPending = self:prefix('pending')

  redis.call('srem', keyLocks, jid)
  redis.call('zrem', keyPending, jid)

  local jids = redis.call('zrevrange', keyPending, 0, 0, 'withscores')
  if #jids == 0 then
    return false
  end

  local newJid = jids[1]
  local score = jids[2]

  -- we know there is capacity to get this released resource but need to check all resources in case multiple.
  if Qless.job(newJid):acquire_resources(now) then
    local data = Qless.job(newJid):data()
    local queue = Qless.queue(data['queue'])
    queue.work.add(score, 0, newJid)
  end

  return newJid
end

--- Return the list of job IDs with locks for this resource
--
function QlessResource:locks()
  return redis.call('smembers', self:prefix('locks'))
end

--- Return the number of active locks for this resource
--
function QlessResource:lock_count()
  return redis.call('scard', self:prefix('locks'))
end

--- Return the list of job identifiers waiting for this resource
--
function QlessResource:pending()
  return redis.call('zrevrange', self:prefix('pending'), 0, -1)
end

--- Return the number of jobs waiting for this resource
--
function QlessResource:pending_count()
  return redis.call('zcard', self:prefix('pending'))
end

function QlessResource:exists()
  return redis.call('exists', self:prefix()) == 1
end

---- Return true if all resources exist
--
function QlessResource.all_exist(resources)
  for _, res in ipairs(resources) do
    if redis.call('exists', QlessResource.ns .. res) == 0 then
      return false
    end
  end
  return true
end

-- Return resources pending
--  [
--      {
--          'name': 'res',
--          'count': 5
--      }, {
--          'name': 'res',
--          'count': 2
--      }
--  ]
function QlessResource.pending_counts(now)
  local search = QlessResource.ns..'*pending'
  local reply = redis.call('keys', search)
  local response = {}
  for index, rname in ipairs(reply) do
    local count = redis.call('zcard', rname)
    local resStat = {name = rname, count = count}
    table.insert(response,resStat)
  end
  return response
end

-- Return resources locks
--  [
--      {
--          'name': 'res',
--          'count': 5
--      }, {
--          'name': 'res',
--          'count': 2
--      }
--  ]
function QlessResource.locks_counts(now)
  local search = QlessResource.ns..'*locks'
  local reply = redis.call('keys', search)
  local response = {}
  for index, rname in ipairs(reply) do
    local count = redis.call('scard', rname)
    local resStat = {name = rname, count = count}
    table.insert(response,resStat)
  end
  return response
end