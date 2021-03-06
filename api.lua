-------------------------------------------------------------------------------
-- This is the analog of the 'main' function when invoking qless directly, as
-- apposed to for use within another library
-------------------------------------------------------------------------------
local QlessAPI = {}

--- Converts an empty string to nil - redis client can't pass nulls
-- @param value
--
local function tonil(value)
  if (value == '') then return nil else return value end
end

-- Return json for the job identified by the provided jid. If the job is not
-- present, then `nil` is returned
function QlessAPI.get(now, jid)
  local data = Qless.job(jid):data()
  if not data then
    return nil
  end
  return cjson.encode(data)
end

-- Return json blob of data or nil for each jid provided
function QlessAPI.multiget(now, ...)
  local results = {}
  for i, jid in ipairs(arg) do
    table.insert(results, Qless.job(jid):data())
  end
  return cjson.encode(results)
end

-- Public access
QlessAPI['config.get'] = function(now, key)
  key = tonil(key)
  if not key then
    return cjson.encode(Qless.config.get(key))
  else
    return Qless.config.get(key)
  end
end

QlessAPI['config.set'] = function(now, key, value)
  key = tonil(key)
  return Qless.config.set(key, value)
end

-- Unset a configuration option
QlessAPI['config.unset'] = function(now, key)
  key = tonil(key)
  return Qless.config.unset(key)
end

-- Get information about a queue or queues
QlessAPI.queues = function(now, queue)
  return cjson.encode(QlessQueue.counts(now, queue))
end

QlessAPI.complete = function(now, jid, worker, queue, data, ...)
  data = tonil(data)
  return Qless.job(jid):complete(now, worker, queue, data, unpack(arg))
end

QlessAPI.failed = function(now, group, start, limit)
  group = tonil(group)
  return cjson.encode(Qless.failed(group, start, limit))
end

QlessAPI.fail = function(now, jid, worker, group, message, data)
  data = tonil(data)
  return Qless.job(jid):fail(now, worker, group, message, data)
end

QlessAPI.jobs = function(now, state, ...)
  return Qless.jobs(now, state, unpack(arg))
end

QlessAPI.retry = function(now, jid, queue, worker, delay, group, message)
  return Qless.job(jid):retry(now, queue, worker, delay, group, message)
end

QlessAPI.depends = function(now, jid, command, ...)
  return Qless.job(jid):depends(now, command, unpack(arg))
end

QlessAPI.heartbeat = function(now, jid, worker, data)
  data = tonil(data)
  return Qless.job(jid):heartbeat(now, worker, data)
end

QlessAPI.workers = function(now, worker)
  return cjson.encode(QlessWorker.counts(now, worker))
end

QlessAPI.track = function(now, command, jid)
  return cjson.encode(Qless.track(now, command, jid))
end

QlessAPI.tag = function(now, command, ...)
  return cjson.encode(Qless.tag(now, command, unpack(arg)))
end

QlessAPI.stats = function(now, queue, date)
  return cjson.encode(Qless.queue(queue):stats(now, date))
end

QlessAPI.priority = function(now, jid, priority)
  return Qless.job(jid):priority(priority)
end

-- Add logging to a particular jid
QlessAPI.log = function(now, jid, message, data)
  assert(jid, "Log(): Argument 'jid' missing")
  assert(message, "Log(): Argument 'message' missing")
  if data then
    data = assert(cjson.decode(data),
      "Log(): Argument 'data' not cjson: " .. tostring(data))
  end

  local job = Qless.job(jid)
  assert(job:exists(), 'Log(): Job ' .. jid .. ' does not exist')
  job:history(now, message, data)
end

QlessAPI.peek = function(now, queue, count)
  local jids = Qless.queue(queue):peek(now, count)
  local response = {}
  for i, jid in ipairs(jids) do
    table.insert(response, Qless.job(jid):data())
  end
  return cjson.encode(response)
end

QlessAPI.pop = function(now, queue, worker, count)
  local jids = Qless.queue(queue):pop(now, worker, count)
  local response = {}
  for i, jid in ipairs(jids) do
    table.insert(response, Qless.job(jid):data())
  end
  return cjson.encode(response)
end

QlessAPI.pause = function(now, ...)
  return QlessQueue.pause(now, unpack(arg))
end

QlessAPI.unpause = function(now, ...)
  return QlessQueue.unpause(unpack(arg))
end

QlessAPI.paused = function(now, queue)
  return Qless.queue(queue):paused()
end

QlessAPI.cancel = function(now, ...)
  return Qless.cancel(now, unpack(arg))
end

QlessAPI.timeout = function(now, ...)
  for _, jid in ipairs(arg) do
    Qless.job(jid):timeout(now)
  end
end

QlessAPI.put = function(now, me, queue, jid, klass, data, delay, ...)
  data = tonil(data)
  return Qless.queue(queue):put(now, me, jid, klass, data, delay, unpack(arg))
end

QlessAPI.requeue = function(now, me, queue, jid, ...)
  local job = Qless.job(jid)
  assert(job:exists(), 'Requeue(): Job ' .. jid .. ' does not exist')
  return QlessAPI.put(now, me, queue, jid, unpack(arg))
end

QlessAPI.unfail = function(now, queue, group, count)
  return Qless.queue(queue):unfail(now, group, count)
end

-- Recurring job stuff
QlessAPI.recur = function(now, queue, jid, klass, data, spec, ...)
  data = tonil(data)
  return Qless.queue(queue):recur(now, jid, klass, data, spec, unpack(arg))
end

QlessAPI.unrecur = function(now, jid)
  return Qless.recurring(jid):unrecur()
end

QlessAPI['recur.get'] = function(now, jid)
  local data = Qless.recurring(jid):data()
  if not data then
    return nil
  end
  return cjson.encode(data)
end

QlessAPI['recur.update'] = function(now, jid, ...)
  return Qless.recurring(jid):update(now, unpack(arg))
end

QlessAPI['recur.tag'] = function(now, jid, ...)
  return Qless.recurring(jid):tag(unpack(arg))
end

QlessAPI['recur.untag'] = function(now, jid, ...)
  return Qless.recurring(jid):untag(unpack(arg))
end

QlessAPI.length = function(now, queue)
  return Qless.queue(queue):length()
end

QlessAPI['worker.deregister'] = function(now, ...)
  return QlessWorker.deregister(unpack(arg))
end

QlessAPI['queue.forget'] = function(now, ...)
  QlessQueue.deregister(unpack(arg))
end

-- Resource apis
QlessAPI['resource.set'] = function(now, rid, max)
  return Qless.resource(rid):set(now, max)
end

QlessAPI['resource.get'] = function(now, rid)
  return Qless.resource(rid):get()
end

QlessAPI['resource.data'] = function(now, rid)
  local data = Qless.resource(rid):data()
  if not data then
    return nil
  end

  return cjson.encode(data)
end

QlessAPI['resource.exists'] = function(now, rid)
  return Qless.resource(rid):exists()
end

QlessAPI['resource.unset'] = function(now, rid)
  return Qless.resource(rid):unset()
end

QlessAPI['resource.locks'] = function(now, rid)
  local data = Qless.resource(rid):locks()
  if not data then
    return nil
  end

  return cjson.encode(data)
end

QlessAPI['resource.lock_count'] = function(now, rid)
  return Qless.resource(rid):lock_count()
end

QlessAPI['resource.pending'] = function(now, rid)
  local data = Qless.resource(rid):pending()
  if not data then
    return nil
  end

  return cjson.encode(data)

end

QlessAPI['resource.pending_count'] = function(now, rid)
  return Qless.resource(rid):pending_count()
end

QlessAPI['resource.stats_pending'] = function(now)
  return cjson.encode(QlessResource.pending_counts(now))
end

QlessAPI['resource.stats_locks'] = function(now)
  return cjson.encode(QlessResource.locks_counts(now))
end

-------------------------------------------------------------------------------
-- Function lookup
-------------------------------------------------------------------------------

-- None of the qless function calls accept keys
if #KEYS > 0 then error('No Keys should be provided') end

-- The first argument must be the function that we intend to call, and it must
-- exist
local command_name = assert(table.remove(ARGV, 1), 'Must provide a command')
local command      = assert(
  QlessAPI[command_name], 'Unknown command ' .. command_name)

-- The second argument should be the current time from the requesting client
local now          = tonumber(table.remove(ARGV, 1))
local now          = assert(
  now, 'Arg "now" missing or not a number: ' .. (now or 'nil'))

return command(now, unpack(ARGV))
