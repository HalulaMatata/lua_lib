-- rediscluster.lua
-- 基于 redis-lua 协议，扩展 Redis Cluster 支持
-- 纯 Lua 实现，兼容 FreeSWITCH 标准 Lua 5.1/5.2/5.4
-- 依赖: LuaSocket (luasocket)

local socket = require "socket"
local string = string
local table = table
local pairs = pairs
local ipairs = ipairs
local tonumber = tonumber
local tostring = tostring
local type = type
local math = math
local unpack = unpack or table.unpack
local setmetatable = setmetatable

local _M = {}
_M._VERSION = "1.0.5"

-- 调试开关（生产环境设为 false）
local _DEBUG = false
local function debug_print(...)
    if _DEBUG then
        print("[rediscluster]", ...)
    end
end

-- ========== CRC16 for Redis Cluster Hash Slot ==========
-- 兼容 Lua 5.1+ 的位运算模拟（无位运算符版本）

local crc16tab = {
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50A5, 0x60C6, 0x70E7,
    0x8108, 0x9129, 0xA14A, 0xB16B, 0xC18C, 0xD1AD, 0xE1CE, 0xF1EF,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52B5, 0x4294, 0x72F7, 0x62D6,
    0x9339, 0x8318, 0xB37B, 0xA35A, 0xD3BD, 0xC39C, 0xF3FF, 0xE3DE,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64E6, 0x74C7, 0x44A4, 0x5485,
    0xA56A, 0xB54B, 0x8528, 0x9509, 0xE5EE, 0xF5CF, 0xC5AC, 0xD58D,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76D7, 0x66F6, 0x5695, 0x46B4,
    0xB75B, 0xA77A, 0x9719, 0x8738, 0xF7DF, 0xE7FE, 0xD79D, 0xC7BC,
    0x48C4, 0x58E5, 0x6886, 0x78A7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xC9CC, 0xD9ED, 0xE98E, 0xF9AF, 0x8948, 0x9969, 0xA90A, 0xB92B,
    0x5AF5, 0x4AD4, 0x7AB7, 0x6A96, 0x1A71, 0x0A50, 0x3A33, 0x2A12,
    0xDBFD, 0xCBDC, 0xFBBF, 0xEB9E, 0x9B79, 0x8B58, 0xBB3B, 0xAB1A,
    0x6CA6, 0x7C87, 0x4CE4, 0x5CC5, 0x2C22, 0x3C03, 0x0C60, 0x1C41,
    0xEDAE, 0xFD8F, 0xCDEC, 0xDDCD, 0xAD2A, 0xBD0B, 0x8D68, 0x9D49,
    0x7E97, 0x6EB6, 0x5ED5, 0x4EF4, 0x3E13, 0x2E32, 0x1E51, 0x0E70,
    0xFF9F, 0xEFBE, 0xDFDD, 0xCFFC, 0xBF1B, 0xAF3A, 0x9F59, 0x8F78,
    0x9188, 0x81A9, 0xB1CA, 0xA1EB, 0xD10C, 0xC12D, 0xF14E, 0xE16F,
    0x1080, 0x00A1, 0x30C2, 0x20E3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83B9, 0x9398, 0xA3FB, 0xB3DA, 0xC33D, 0xD31C, 0xE37F, 0xF35E,
    0x02B1, 0x1290, 0x22F3, 0x32D2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xB5EA, 0xA5CB, 0x95A8, 0x8589, 0xF56E, 0xE54F, 0xD52C, 0xC50D,
    0x34E2, 0x24C3, 0x14A0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xA7DB, 0xB7FA, 0x8799, 0x97B8, 0xE75F, 0xF77E, 0xC71D, 0xD73C,
    0x26D3, 0x36F2, 0x0691, 0x16B0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xD94C, 0xC96D, 0xF90E, 0xE92F, 0x99C8, 0x89E9, 0xB98A, 0xA9AB,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18C0, 0x08E1, 0x3882, 0x28A3,
    0xCB7D, 0xDB5C, 0xEB3F, 0xFB1E, 0x8BF9, 0x9BD8, 0xABBB, 0xBB9A,
    0x4A75, 0x5A54, 0x6A37, 0x7A16, 0x0AF1, 0x1AD0, 0x2AB3, 0x3A92,
    0xFD2E, 0xED0F, 0xDD6C, 0xCD4D, 0xBDAA, 0xAD8B, 0x9DE8, 0x8DC9,
    0x7C26, 0x6C07, 0x5C64, 0x4C45, 0x3CA2, 0x2C83, 0x1CE0, 0x0CC1,
    0xEF1F, 0xFF3E, 0xCF5D, 0xDF7C, 0xAF9B, 0xBFBA, 0x8FD9, 0x9FF8,
    0x6E17, 0x7E36, 0x4E55, 0x5E74, 0x2E93, 0x3EB2, 0x0ED1, 0x1EF0,
}

-- 兼容的位运算（纯 Lua 实现）
local function bxor(a, b)
    a = a or 0
    b = b or 0
    local result = 0
    local bitval = 1
    while a > 0 or b > 0 do
        local abit = a % 2
        local bbit = b % 2
        if abit ~= bbit then result = result + bitval end
        bitval = bitval * 2
        a = math.floor(a / 2)
        b = math.floor(b / 2)
    end
    return result
end

local function lshift(a, n)
    a = a or 0
    return a * (2 ^ n)
end

local function rshift(a, n)
    a = a or 0
    return math.floor(a / (2 ^ n))
end

local function crc16(str)
    local crc = 0
    for i = 1, #str do
        local b = string.byte(str, i)
        local idx = bxor(rshift(crc, 8), b) + 1
        if idx < 1 then idx = 1 end
        if idx > 256 then idx = 256 end
        crc = bxor(lshift(crc, 8), crc16tab[idx])
        -- 确保 crc 是 16 位无符号整数
        crc = crc % 65536
    end
    return crc
end

local function get_slot(key)
    local s, e = string.find(key, "{(.-)}")
    if s then
        key = string.sub(key, s + 1, e - 1)
    end
    return (crc16(key) % 16384)
end

-- ========== Redis Protocol Parser ==========

local function _gen_req(args)
    local nargs = #args
    local lines = {}
    table.insert(lines, "*" .. nargs .. "\r\n")
    for i = 1, nargs do
        local arg = tostring(args[i])
        table.insert(lines, "$" .. #arg .. "\r\n")
        table.insert(lines, arg .. "\r\n")
    end
    return table.concat(lines)
end

local function _read_line(sock)
    local line, err = sock:receive("*l")
    if not line then
        return nil, err or "connection closed"
    end
    if string.sub(line, -1) == "\r" then
        line = string.sub(line, 1, -2)
    end
    return line
end

local function _read_reply(sock)
    local line, err = _read_line(sock)
    if not line then
        return nil, err
    end

    local prefix = string.byte(line, 1)

    if prefix == 36 then -- '$'
        local size = tonumber(string.sub(line, 2))
        if not size then
            return nil, "invalid bulk reply size"
        end
        if size < 0 then
            return nil
        end
        local data, err = sock:receive(size)
        if not data then
            return nil, "failed to read bulk data: " .. (err or "unknown")
        end
        local crlf, err = sock:receive(2)
        if not crlf then
            return nil, "failed to read trailing CRLF: " .. (err or "unknown")
        end
        return data

    elseif prefix == 43 then -- '+'
        return string.sub(line, 2)

    elseif prefix == 45 then -- '-'
        return nil, string.sub(line, 2)

    elseif prefix == 58 then -- ':'
        local num = tonumber(string.sub(line, 2))
        if not num then
            return nil, "invalid integer reply"
        end
        return num

    elseif prefix == 42 then -- '*'
        local n = tonumber(string.sub(line, 2))
        if not n then
            return nil, "invalid multi-bulk length"
        end
        if n < 0 then
            return nil
        end
        local vals = {}
        for i = 1, n do
            local res, err = _read_reply(sock)
            if err then
                return nil, "failed to read element " .. i .. ": " .. err
            end
            vals[i] = res
        end
        return vals
    else
        return nil, "unknown reply prefix: " .. string.char(prefix)
    end
end

-- ========== Single Redis Node Client ==========

local RedisNode = {}
RedisNode.__index = RedisNode

function RedisNode.new(host, port, timeout)
    local self = setmetatable({}, RedisNode)
    self.host = host
    self.port = port or 6379
    self.timeout = timeout or 5000  -- milliseconds
    self.sock = nil
    return self
end

function RedisNode:connect()
    if self.sock then
        return true
    end
    local sock = socket.tcp()
    if not sock then
        return nil, "failed to create socket"
    end
    sock:settimeout(self.timeout / 1000)  -- seconds
    local ok, err = sock:connect(self.host, self.port)
    if not ok then
        return nil, string.format("failed to connect to %s:%d: %s", self.host, self.port, err or "unknown")
    end
    self.sock = sock
    debug_print("connected to", self.host, self.port)
    return true
end

function RedisNode:close()
    if self.sock then
        self.sock:close()
        self.sock = nil
    end
    return true
end

function RedisNode:cmd(...)
    local args = {...}
    if not self.sock then
        local ok, err = self:connect()
        if not ok then
            return nil, err
        end
    end

    local req = _gen_req(args)
    if _DEBUG then
        local log_args = {}
        for i, v in ipairs(args) do
            if type(v) == "function" then
                log_args[i] = "function"
            else
                log_args[i] = tostring(v)
            end
        end
        debug_print("->", req:gsub("\r\n", "\\r\\n"), "args:", table.concat(log_args, ","))
    end
    local bytes, err = self.sock:send(req)
    if not bytes then
        self:close()
        return nil, "send failed: " .. (err or "unknown")
    end

    local res, err = _read_reply(self.sock)
    if err then
        self:close()
        return nil, err
    end
    if _DEBUG then
        debug_print("<-", type(res) == "table" and "table" or tostring(res))
    end
    return res
end

-- ========== Redis Cluster Client ==========

local RedisCluster = {}
RedisCluster.__index = RedisCluster

-- 安全获取密码字符串
local function get_auth_string(auth_val)
    if auth_val == nil then
        return nil
    end
    local t = type(auth_val)
    if t == "string" then
        if auth_val == "" then
            return nil
        end
        return auth_val
    elseif t == "function" then
        local ok, res = pcall(auth_val)
        if ok and type(res) == "string" and res ~= "" then
            return res
        end
        return nil
    else
        return nil
    end
end

function _M.new(config)
    local self = setmetatable({}, RedisCluster)

    self.serv_list = config.serv_list or {}
    self.timeout = config.timeout or 5000
    self.connect_timeout = config.connect_timeout or 1000
    self.read_timeout = config.read_timeout or 1000
    self.max_redirection = config.max_redirection or 5
    self._password = get_auth_string(config.password)
    self._slots = {}
    self._nodes = {}
    self._startup_nodes = {}

    for _, node in ipairs(self.serv_list) do
        table.insert(self._startup_nodes, {ip = node.ip, port = node.port})
    end

    return self
end

function RedisCluster:_fetch_slots()
    debug_print("fetching cluster slots...")
    local success = false
    for _, node_info in ipairs(self._startup_nodes) do
        debug_print("trying startup node", node_info.ip, node_info.port)
        local node = RedisNode.new(node_info.ip, node_info.port, self.timeout)
        local ok, err = node:connect()
        if not ok then
            debug_print("connect failed:", err)
            node:close()
        else
            local auth_ok = true
            if self._password then
                local res, err = node:cmd("AUTH", self._password)
                if not res then
                    debug_print("auth failed on", node_info.ip, node_info.port, ":", err)
                    auth_ok = false
                end
            end

            if auth_ok then
                local slots_info, err = node:cmd("CLUSTER", "SLOTS")
                if err then
                    debug_print("CLUSTER SLOTS failed on", node_info.ip, node_info.port, ":", err)
                elseif type(slots_info) ~= "table" or #slots_info == 0 then
                    debug_print("CLUSTER SLOTS returned empty or invalid on", node_info.ip, node_info.port)
                else
                    self._slots = {}
                    local slot_count = 0
                    for _, slot_range in ipairs(slots_info) do
                        if type(slot_range) == "table" and #slot_range >= 3 then
                            local start_slot = tonumber(slot_range[1])
                            local end_slot = tonumber(slot_range[2])
                            local master = slot_range[3]
                            if master and type(master) == "table" and #master >= 2 then
                                local master_ip = tostring(master[1])
                                local master_port = tonumber(master[2])
                                if master_ip and master_port then
                                    for i = start_slot, end_slot do
                                        self._slots[i] = {ip = master_ip, port = master_port}
                                        slot_count = slot_count + 1
                                    end
                                end
                            end
                        end
                    end
                    node:close()
                    if slot_count > 0 then
                        debug_print("successfully fetched", slot_count, "slots from", node_info.ip, node_info.port)
                        success = true
                        break
                    else
                        debug_print("no slots parsed from", node_info.ip, node_info.port)
                    end
                end
            end
            node:close()
        end
    end

    if success then
        return true
    else
        return nil, "failed to fetch cluster slots from all startup nodes (check network, cluster mode, and auth)"
    end
end

function RedisCluster:_get_node(slot)
    local node_info = self._slots[slot]
    if not node_info then
        debug_print("slot", slot, "not cached, refreshing slots...")
        local ok, err = self:_fetch_slots()
        if not ok then
            return nil, err
        end
        node_info = self._slots[slot]
        if not node_info then
            return nil, string.format("slot %d not found after refresh", slot)
        end
    end

    local node_key = node_info.ip .. ":" .. node_info.port
    local node = self._nodes[node_key]
    if not node then
        node = RedisNode.new(node_info.ip, node_info.port, self.timeout)
        self._nodes[node_key] = node
        debug_print("created new node for", node_key)
    end
    return node
end

function RedisCluster:_execute(cmd, ...)
    local args = {...}
    local key = args[1]
    if not key then
        return nil, "no key provided"
    end

    local slot = get_slot(key)
    local redirections = 0

    while redirections < self.max_redirection do
        local node, err = self:_get_node(slot)
        if not node then
            return nil, err
        end

        local ok, err = node:connect()
        if not ok then
            debug_print("connect failed, refreshing slots...")
            self:_fetch_slots()
            redirections = redirections + 1
        else
            if self._password then
                local res, err = node:cmd("AUTH", self._password)
                if not res then
                    node:close()
                    return nil, "auth failed: " .. (err or "unknown")
                end
            end

            local res, err = node:cmd(cmd, unpack(args))
            if not res and err then
                node:close()
                debug_print("command error:", err)

                if string.find(err, "^MOVED") then
                    redirections = redirections + 1
                    -- 解析 MOVED 错误，获取新的 slot 和目标节点
                    local new_slot, target_ip, target_port = string.match(err, "MOVED (%d+) ([^:]+):(%d+)")
                    if new_slot then
                        slot = tonumber(new_slot)
                        debug_print("MOVED redirection", redirections, "update slot to", slot)
                    else
                        debug_print("MOVED redirection but failed to parse slot")
                    end
                    -- 刷新槽位缓存
                    local ok, fetch_err = self:_fetch_slots()
                    if not ok then
                        return nil, fetch_err
                    end
                elseif string.find(err, "^ASK") then
                    redirections = redirections + 1
                    local ask_ip, ask_port = string.match(err, "ASK %d+ ([^:]+):(%d+)")
                    if ask_ip and ask_port then
                        debug_print("ASK redirection to", ask_ip, ask_port)
                        local ask_node = RedisNode.new(ask_ip, tonumber(ask_port), self.timeout)
                        local ok, conn_err = ask_node:connect()
                        if ok then
                            if self._password then
                                ask_node:cmd("AUTH", self._password)
                            end
                            ask_node:cmd("ASKING")
                            res, err = ask_node:cmd(cmd, unpack(args))
                            ask_node:close()
                            if res then
                                return res
                            end
                        end
                    end
                    return nil, err
                else
                    return nil, err
                end
            else
                return res
            end
        end
    end

    return nil, "too many redirections"
end

function RedisCluster:close_all()
    for node_key, node in pairs(self._nodes) do
        if node and node.close then
            node:close()
            debug_print("closed node", node_key)
        end
    end
    self._nodes = {}
    return true
end

function RedisCluster:close_node(host, port)
    local node_key = host .. ":" .. tostring(port)
    local node = self._nodes[node_key]
    if node then
        node:close()
        self._nodes[node_key] = nil
        debug_print("closed node", node_key)
        return true
    end
    return false, "node not found"
end

-- 注册常用命令
local commands = {
    "get", "set", "del", "exists", "expire", "ttl",
    "incr", "decr", "incrby", "decrby",
    "hget", "hset", "hdel", "hgetall", "hexists", "hincrby", "hmset", "hmget",
    "lpush", "rpush", "lpop", "rpop", "lrange", "llen", "lindex",
    "sadd", "srem", "smembers", "sismember", "scard", "spop",
    "zadd", "zrange", "zrem", "zcard", "zscore", "zrank", "zrevrank",
    "mget", "mset",
    "eval", "evalsha",
    "ping", "auth", "select",
}

for _, cmd in ipairs(commands) do
    RedisCluster[cmd] = function(self, ...)
        return self:_execute(string.upper(cmd), ...)
    end
end

function RedisCluster:script(subcmd, ...)
    return self:_execute("SCRIPT", string.upper(subcmd), ...)
end

function RedisCluster:cluster(subcmd, ...)
    return self:_execute("CLUSTER", string.upper(subcmd), ...)
end

RedisCluster.__gc = function(self)
    if self.close_all then
        self:close_all()
    end
end

return _M
