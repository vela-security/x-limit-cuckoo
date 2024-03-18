--name: x-limit-cuckoo
--Label: 通过cuckoo过滤器解析二维频率

local shared = ngx.shared
local tonumber = tonumber
local tostring = tostring
local log = ngx.log
local ERR = ngx.ERR
local fmt = string.format
local ceil = math.ceil
local NOTICE = ngx.NOTICE

local cuckoo = require("cuckoo")
local function ffi_cuckoo_new(size , now)
    if size < 32 then
    	size = 32    
    end
    
	local cdata = cuckoo.new(size)    
    cdata:total(0)
    cdata:exdata(now)
    return cdata
end

local function ffi_cuckoo_cast(chunk , size , now , ttl) 
    if not chunk then
       	return ffi_cuckoo_new(size , now) , ttl 
    end
    
    local cdata , err = cuckoo.decode(chunk , true)
    if not cdata then
        log(ERR , "cuckoo cast fail " , err)
       	return ffi_cuckoo_new(size , now) , ttl
    end
    
    local last = cdata:exdata()
    if last <= 0 then
       	return ffi_cuckoo_new(size , now) , ttl
    end
   	
    local expire = now - last
    if expire > ttl then
       	return ffi_cuckoo_new(size , now) , ttl 
    end
    
    return cdata , expire
end

local function ffi_cuckoo_put(shm, key , element ,size , ttl)
    local now = ceil(ngx.now())
    local chunk = shm:get(key)
    local cdata , expire = ffi_cuckoo_cast(chunk , size , now , ttl)
    cdata:add(element)
    shm:set(key , cdata:encode(true) , expire)
    return cdata
end

local _M = {}

function _M.access(shm_name, client_name ,second , limit , count ,time, lock, tag)
    local var , ctx = ngx.var , ngx.ctx
    local shm = shared[shm_name]
    local black = shared["black"]
    local client = var[client_name]
    local limit = tonumber(limit)
    local count = tonumber(count)
    local time = tonumber(time)
    local lock = tonumber(lock)
    local tag = tostring(tag)
    
    if  not shm
        or not client
        or not limit 
        or not count
        or not time
        or not lock
        or not tag 
        or not black then 
        --log(ERR ,  "client:", client ," limit:" , limit ," count:", count, " time:", time, " lock:", lock, " tag:", tag)
        return false end
   	
    local key = fmt("%s-%s" , client , tag)
    
    local bl = black:get(key)
    if bl then
    	black:set(key , bl + 1 , lock)
        return true
    end
    
    local elem = var[second] or "N"
    local ud = ffi_cuckoo_put(shm , key , tostring(elem) , count , time)
    if not ud then
    	return false    
    end
	
    local cnt = ud:count()
    --log(ERR , "cuckoo elements:" , cnt , " value:" , elem)
    if cnt >= count then -- 种类判断
    	black:set(key , cnt , lock)
        return true
    end
    
    local total = ud:total()
    --log(ERR , "cuckoo total:" , total , " key:" , key , " total > 10000" , total > 10000 )
    if total >= limit then  -- 频率判断
    	black:set(key , total , lock)
        return true
    end
    return false
end
    
return _M