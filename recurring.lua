-- Recur(0, 'on', queue, jid, klass, data, now, 'interval', second, offset, [priority p], [tags t], [retries r])
-- Recur(0, 'off', jid)
-- Recur(0, 'get', jid)
-- Recur(0, 'update', jid, ['priority', priority], ['interval', interval], ['retries', retries], ['data', data], ['klass', klass], ['queue', queue])
-- Recur(0, 'tag', jid, tag, [tag, [...]])
-- Recur(0, 'untag', jid, tag, [tag, [...]])
-- -------------------------------------------------------------------------------------------------------
-- This script takes the name of a queue, and then the info
-- info about the work item, and makes sure that jobs matching
-- its criteria are regularly made available.
function QlessRecurringJob:data()
    local job = redis.call(
        'hmget', 'ql:r:' .. self.jid, 'jid', 'klass', 'state', 'queue',
        'priority', 'interval', 'retries', 'count', 'data', 'tags')
    
    if not job[1] then
        return false
    end
    
    return {
        jid          = job[1],
        klass        = job[2],
        state        = job[3],
        queue        = job[4],
        priority     = tonumber(job[5]),
        interval     = tonumber(job[6]),
        retries      = tonumber(job[7]),
        count        = tonumber(job[8]),
        data         = cjson.decode(job[9]),
        tags         = cjson.decode(job[10])
    }
end

-- Update the recurring job data
function QlessRecurringJob:update(...)
    local options = {}
    -- Make sure that the job exists
    if redis.call('exists', 'ql:r:' .. self.jid) ~= 0 then
        for i = 1, #arg, 2 do
            local key = arg[i]
            local value = arg[i+1]
            if key == 'priority' or key == 'interval' or key == 'retries' then
                value = assert(tonumber(value), 'Recur(): Arg "' .. key .. '" must be a number: ' .. tostring(value))
                -- If the command is 'interval', then we need to update the time
                -- when it should next be scheduled
                if key == 'interval' then
                    local queue, interval = unpack(redis.call('hmget', 'ql:r:' .. self.jid, 'queue', 'interval'))
                    redis.call('zincrby', 'ql:q:' .. queue .. '-recur', value - tonumber(interval), self.jid)
                end
                redis.call('hset', 'ql:r:' .. self.jid, key, value)
            elseif key == 'data' then
                value = assert(cjson.decode(value), 'Recur(): Arg "data" is not JSON-encoded: ' .. tostring(value))
                redis.call('hset', 'ql:r:' .. self.jid, 'data', cjson.encode(value))
            elseif key == 'klass' then
                redis.call('hset', 'ql:r:' .. self.jid, 'klass', value)
            elseif key == 'queue' then
                local queue = redis.call('hget', 'ql:r:' .. self.jid, 'queue')
                local score = redis.call('zscore', 'ql:q:' .. queue .. '-recur', self.jid)
                redis.call('zrem', 'ql:q:' .. queue .. '-recur', self.jid)
                redis.call('zadd', 'ql:q:' .. value .. '-recur', score, self.jid)
                redis.call('hset', 'ql:r:' .. self.jid, 'queue', value)
            else
                error('Recur(): Unrecognized option "' .. key .. '"')
            end
        end
        return true
    else
        error('Recur(): No recurring job ' .. self.jid)
    end
end

function QlessRecurringJob:tag(...)
    local tags = redis.call('hget', 'ql:r:' .. self.jid, 'tags')
    -- If the job has been canceled / deleted, then return false
    if tags then
        -- Decode the json blob, convert to dictionary
        tags = cjson.decode(tags)
        local _tags = {}
        for i,v in ipairs(tags) do _tags[v] = true end
        
        -- Otherwise, add the job to the sorted set with that tags
        for i=1,#arg do if _tags[arg[i]] == nil then table.insert(tags, arg[i]) end end
        
        tags = cjson.encode(tags)
        redis.call('hset', 'ql:r:' .. self.jid, 'tags', tags)
        return tags
    else
        return false
    end
end

function QlessRecurringJob:untag(...)
    -- Get the existing tags
    local tags = redis.call('hget', 'ql:r:' .. self.jid, 'tags')
    -- If the job has been canceled / deleted, then return false
    if tags then
        -- Decode the json blob, convert to dictionary
        tags = cjson.decode(tags)
        local _tags    = {}
        -- Make a hash
        for i,v in ipairs(tags) do _tags[v] = true end
        -- Delete these from the hash
        for i = 1,#arg do _tags[arg[i]] = nil end
        -- Back into a list
        local results = {}
        for i, tag in ipairs(tags) do if _tags[tag] then table.insert(results, tag) end end
        -- json encode them, set, and return
        tags = cjson.encode(results)
        redis.call('hset', 'ql:r:' .. self.jid, 'tags', tags)
        return tags
    else
        return false
    end
end

function QlessRecurringJob:unrecur()
    -- First, find out what queue it was attached to
    local queue = redis.call('hget', 'ql:r:' .. self.jid, 'queue')
    if queue then
        -- Now, delete it from the queue it was attached to, and delete the thing itself
        redis.call('zrem', 'ql:q:' .. queue .. '-recur', self.jid)
        redis.call('del', 'ql:r:' .. self.jid)
        return true
    else
        return true
    end
end