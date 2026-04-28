
-- rediscluster_dialplan.lua
-- FreeSWITCH dialplan 中使用 Redis Cluster 示例
-- 放置于 /usr/share/freeswitch/scripts/ 目录下
-- dialplan 中调用: lua rediscluster_dialplan.lua

-- 加载模块（确保 rediscluster.lua 在 Lua 路径中）
local rediscluster = require "rediscluster"

-- 获取当前 session（dialplan 模式下可用）
local session = session or {}

-- 日志函数（兼容 dialplan 和命令行模式）
local function log(level, msg)
    if freeswitch then
        freeswitch.consoleLog(level, msg)
    else
        print("[" .. level .. "] " .. msg)
    end
end

-- Redis Cluster 配置
local config = {
    serv_list = {
        { ip = "192.168.21.126", port = 6380 },
        { ip = "192.168.21.127", port = 6380 },
        { ip = "192.168.21.128", port = 6380 },
    },
    timeout = 5000,
    connect_timeout = 1000,
    read_timeout = 1000,
    max_redirection = 5,
}

-- 创建集群连接
local cluster, err = rediscluster.new(config)
if not cluster then
    log("ERR", "Redis Cluster init failed: " .. tostring(err))
    -- 可以选择继续执行或挂断
    if session.hangup then session:hangup() end
    return
end

-- ========== 示例 1: 根据 Redis 中的路由规则转接 ==========
local function route_by_redis(caller_number)
    -- 从 Redis 获取路由目标
    local route_key = "route:" .. caller_number
    local destination, err = cluster:get(route_key)

    if destination then
        log("INFO", "Route found for " .. caller_number .. " -> " .. destination)
        return destination
    else
        log("INFO", "No route found for " .. caller_number .. ", using default")
        return "default_destination"
    end
end

-- ========== 示例 2: 呼叫计数限流 ==========
local function check_rate_limit(caller_number)
    local limit_key = "rate_limit:" .. caller_number
    local count, err = cluster:incr(limit_key)

    if not count then
        log("ERR", "Rate limit check failed: " .. tostring(err))
        return true  -- 失败时允许通过
    end

    -- 设置过期时间（首次设置）
    if count == 1 then
        cluster:expire(limit_key, 60)  -- 60秒窗口
    end

    if count > 100 then
        log("WARNING", "Rate limit exceeded for " .. caller_number)
        return false
    end

    log("INFO", "Rate limit OK for " .. caller_number .. ": " .. count .. "/100")
    return true
end

-- ========== 示例 3: 通话计数器 ==========
local function increment_call_counter(caller_number)
    local daily_key = "calls:daily:" .. os.date("%Y%m%d") .. ":" .. caller_number
    local total, err = cluster:incr(daily_key)
    if total then
        cluster:expire(daily_key, 86400)  -- 24小时过期
        log("INFO", "Daily calls for " .. caller_number .. ": " .. total)
    end
    return total
end

-- ========== 示例 4: 使用 Lua 脚本原子操作 ==========
local function atomic_counter_update(key, increment)
    local script = [[
        local current = redis.call('get', KEYS[1])
        if not current then
            current = 0
        end
        local new_val = tonumber(current) + tonumber(ARGV[1])
        redis.call('set', KEYS[1], new_val)
        return new_val
    ]]
    local res, err = cluster:eval(script, 1, key, increment)
    if res then
        log("INFO", "Atomic update " .. key .. " = " .. tostring(res))
    else
        log("ERR", "Atomic update failed: " .. tostring(err))
    end
    return res
end

-- ========== dialplan 主逻辑 ==========
if session and session.ready then
    local caller = session:getVariable("caller_id_number") or "unknown"
    local callee = session:getVariable("destination_number") or "unknown"

    log("INFO", "=== Call started ===")
    log("INFO", "Caller: " .. caller .. ", Callee: " .. callee)

    -- 1. 检查限流
    if not check_rate_limit(caller) then
        log("WARNING", "Call rejected due to rate limit")
        session:execute("playback", "/usr/share/freeswitch/sounds/rate_limit_exceeded.wav")
        session:hangup()
        return
    end

    -- 2. 增加计数器
    increment_call_counter(caller)

    -- 3. 路由查找
    local route = route_by_redis(caller)

    -- 4. 设置通道变量供后续使用
    session:setVariable("redis_route", route)

    -- 5. 桥接到目标
    log("INFO", "Bridging to: " .. route)
    session:execute("bridge", "sofia/internal/" .. route .. "@192.168.21.100")

    log("INFO", "=== Call ended ===")
else
    -- 命令行测试模式
    print("=== Running in test mode ===")
    print("Route test: " .. tostring(route_by_redis("1001")))
    print("Rate limit test: " .. tostring(check_rate_limit("1001")))
    print("Counter test: " .. tostring(increment_call_counter("1001")))
    print("Atomic update test: " .. tostring(atomic_counter_update("test_counter{user:1001}", 5)))
end
