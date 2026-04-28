# FreeSWITCH Redis Cluster 模块

基于 redis-lua 协议扩展的 Redis Cluster 客户端，纯 Lua 实现，兼容 FreeSWITCH 标准 Lua 环境。

## 依赖

- Lua 5.1 / 5.2 / 5.4
- LuaSocket (`luasocket`)

### 安装 LuaSocket

```bash
# CentOS/RHEL
yum install -y lua-socket

# 或使用 luarocks
luarocks install luasocket
```

## 文件说明

| 文件 | 说明 |
|------|------|
| `rediscluster.lua` | Redis Cluster 客户端主模块 |
| `test_rediscluster.lua` | 命令行测试脚本 |
| `rediscluster_dialplan.lua` | FreeSWITCH dialplan 使用示例 |

## 安装

1. 将 `rediscluster.lua` 复制到 FreeSWITCH 的 Lua 路径中：

```bash
# 查看 FreeSWITCH 的 Lua 路径
fs_cli -x "lua -e 'print(package.path)'"

# 通常路径为
# /usr/share/freeswitch/scripts/
# /usr/local/freeswitch/scripts/

cp rediscluster.lua /usr/share/freeswitch/scripts/
```

2. 确保 LuaSocket 可用：

```bash
fs_cli -x "lua -e 'local s = require(\"socket\"); print(\"LuaSocket OK\")'"
```

## 使用示例

### 命令行测试

```bash
# 使用 FreeSWITCH 的 lua 命令
fs_cli -x "lua test_rediscluster.lua"

# 或标准 lua（需要确保 rediscluster.lua 在 package.path 中）
lua test_rediscluster.lua
```

### Dialplan 中使用

在 FreeSWITCH dialplan XML 中：

```xml
<extension name="redis_test">
    <condition field="destination_number" expression="^1001$">
        <action application="lua" data="rediscluster_dialplan.lua"/>
    </condition>
</extension>
```

### Lua 脚本中使用

```lua
local rediscluster = require "rediscluster"

local config = {
    serv_list = {
        { ip = "192.168.21.126", port = 6380 },
        { ip = "192.168.21.127", port = 6380 },
        { ip = "192.168.21.128", port = 6380 },
    },
    timeout = 5000,
    max_redirection = 5,
}

local cluster = rediscluster.new(config)

-- 基本操作
cluster:set("mykey{user:1001}", "hello")
local val = cluster:get("mykey{user:1001}")

-- 计数器
local count = cluster:incrby("counter{user:1001}", 1)

-- Lua 脚本
local script = "return redis.call('incrby', KEYS[1], ARGV[1])"
local res = cluster:eval(script, 1, "counter{user:1001}", 2)

-- Hash 操作
cluster:hset("myhash{user:1001}", "field1", "value1")
local field = cluster:hget("myhash{user:1001}", "field1")
```

## 配置参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `serv_list` | table | 必填 | 集群节点列表 |
| `timeout` | number | 5000 | 连接超时(ms) |
| `connect_timeout` | number | 1000 | 连接超时(ms) |
| `read_timeout` | number | 1000 | 读取超时(ms) |
| `max_redirection` | number | 5 | 最大重定向次数 |
| `auth` | string | nil | Redis 密码 |

## Hash Tag 说明

Redis Cluster 使用 key 的 hash slot 来决定数据存储在哪个节点。为保证相关 key 存储在同一节点，使用 Hash Tag：

```lua
-- 以下 key 会存储在同一 slot
cluster:set("user:1001:profile", "...")
cluster:set("user:1001:settings", "...")
cluster:set("user:1001:history", "...")

-- 使用显式 Hash Tag
cluster:set("data{user:1001}:a", "...")
cluster:set("data{user:1001}:b", "...")
```

## 支持的命令

- **String**: `get`, `set`, `del`, `exists`, `expire`, `ttl`, `incr`, `decr`, `incrby`, `decrby`
- **Hash**: `hget`, `hset`, `hdel`, `hgetall`, `hexists`, `hincrby`, `hmset`, `hmget`
- **List**: `lpush`, `rpush`, `lpop`, `rpop`, `lrange`, `llen`, `lindex`
- **Set**: `sadd`, `srem`, `smembers`, `sismember`, `scard`, `spop`
- **Sorted Set**: `zadd`, `zrange`, `zrem`, `zcard`, `zscore`, `zrank`, `zrevrank`
- **Script**: `eval`, `evalsha`
- **Other**: `mget`, `mset`, `ping`, `script`

## 注意事项

1. **连接池**: 当前版本每次命令后关闭连接，未实现连接池。如需连接池，建议结合 `mod_redis` 或外部服务。
2. **性能**: 高频场景建议使用 `redis-cli -c` 或外部服务方案。
3. **错误处理**: 所有命令返回 `nil, err` 表示失败，需做好错误处理。

## License

BSD License (与 lua-resty-redis 一致)
