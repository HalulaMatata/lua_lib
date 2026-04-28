-- test_rediscluster.lua
-- FreeSWITCH Redis Cluster 测试脚本

local rediscluster = require "rediscluster"

local config = {
    serv_list = {
        { ip = "192.168.21.126", port = 6380 },
        { ip = "192.168.21.127", port = 6380 },
        { ip = "192.168.21.128", port = 6380 },
    },
    timeout = 5000,           -- 连接超时(ms)
    connect_timeout = 1000,   -- 连接超时(ms)
    read_timeout = 1000,      -- 读取超时(ms)
    max_redirection = 5,      -- 最大重定向次数
}

local cluster, err = rediscluster.new(config)
if not cluster then
    print("ERR: failed to create cluster: " .. tostring(err))
    return
end

-- 测试 SET/GET
print("=== Test SET ===")
local ok, err = cluster:set("testkey{user:1001}", "hello")
if not ok then
    print("SET ERR: " .. tostring(err))
else
    print("SET OK: " .. tostring(ok))
end

print("=== Test GET ===")
local res, err = cluster:get("testkey{user:1001}")
if not res then
    print("GET ERR: " .. tostring(err))
else
    print("GET OK: " .. tostring(res))
end

-- 测试 INCRBY
print("=== Test INCRBY ===")
local res, err = cluster:incrby("counter{user:1001}", 2)
if not res then
    print("INCRBY ERR: " .. tostring(err))
else
    print("INCRBY OK: " .. tostring(res))
end

-- 测试 EVAL (Lua 脚本)
print("=== Test EVAL ===")
local script = "return redis.call('incrby', KEYS[1], ARGV[1])"
local res, err = cluster:eval(script, 1, "counter{user:1001}", 2)
if not res then
    print("EVAL ERR: " .. tostring(err))
else
    print("EVAL OK: " .. tostring(res))
end

-- 测试 HSET/HGET (单个字段)
print("=== Test HSET/HGET ===")
cluster:hset("myhash{user:1001}", "field1", "value1")
local res, err = cluster:hget("myhash{user:1001}", "field1")
if not res then
    print("HGET ERR: " .. tostring(err))
else
    print("HGET OK: " .. tostring(res))
end

-- 新增测试 HMSET/HMGET (批量字段)
print("=== Test HMSET/HMGET ===")
-- 批量设置多个字段
local ok, err = cluster:hmset("myhash{user:1001}", "field2", "value2", "field3", "value3")
if not ok then
    print("HMSET ERR: " .. tostring(err))
else
    print("HMSET OK: " .. tostring(ok))
end

-- 批量获取多个字段
local res, err = cluster:hmget("myhash{user:1001}", "field1", "field2", "field3")
if not res then
    print("HMGET ERR: " .. tostring(err))
else
    print("HMGET OK: " .. tostring(res))
    -- 输出结果表内容
    if type(res) == "table" then
        for i, v in ipairs(res) do
            print(string.format("  field%d = %s", i, tostring(v)))
        end
    else
        print("  result: " .. tostring(res))
    end
end

-- 获取所有字段
local res, err = cluster:hgetall("myhash{user:1001}")
if not res then
    print("HGETALL ERR: " .. tostring(err))
else
    print("HGETALL OK: " .. tostring(res))
    -- 输出结果表内容
    if type(res) == "table" then
        for i, v in ipairs(res) do
            print(string.format("  field%d = %s", i, tostring(v)))
        end
    else
        print("  result: " .. tostring(res))
    end
end

print("=== All tests completed ===")