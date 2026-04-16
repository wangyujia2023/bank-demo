/**
 * API 请求缓存工具
 * 减少重复请求，提高加载速度
 */

const cache = new Map()
const pendingRequests = new Map()

// 缓存过期时间（毫秒）
const CACHE_TTL = {
  dashboard: 5 * 60 * 1000,      // 5分钟
  default: 3 * 60 * 1000,        // 3分钟
  short: 1 * 60 * 1000,          // 1分钟
}

/**
 * 生成缓存键
 */
function getCacheKey(url, params = {}) {
  const paramStr = Object.entries(params)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}=${JSON.stringify(v)}`)
    .join('&')
  return `${url}?${paramStr}`
}

/**
 * 获取缓存的数据
 */
export function getCache(url, params = {}) {
  const key = getCacheKey(url, params)
  const entry = cache.get(key)

  if (!entry) return null

  // 检查是否过期
  if (Date.now() - entry.time > entry.ttl) {
    cache.delete(key)
    return null
  }

  return entry.data
}

/**
 * 设置缓存
 */
export function setCache(url, data, params = {}, ttl = CACHE_TTL.default) {
  const key = getCacheKey(url, params)
  cache.set(key, {
    data,
    time: Date.now(),
    ttl
  })
}

/**
 * 清除所有缓存
 */
export function clearCache() {
  cache.clear()
}

/**
 * 去重请求 - 如果相同的请求已在进行中，返回已有的 Promise
 */
export function deduplicateRequest(url, params = {}, fn) {
  const key = getCacheKey(url, params)

  // 如果相同请求已在进行中，返回已有的 Promise
  if (pendingRequests.has(key)) {
    return pendingRequests.get(key)
  }

  // 创建新的请求 Promise
  const promise = fn()
    .finally(() => pendingRequests.delete(key))

  pendingRequests.set(key, promise)
  return promise
}

/**
 * 包装 HTTP 请求，自动处理缓存和去重
 */
export async function cachedRequest(url, params = {}, fn, options = {}) {
  const { ttl = CACHE_TTL.default, forceRefresh = false } = options

  // 如果不是强制刷新，先检查缓存
  if (!forceRefresh) {
    const cached = getCache(url, params)
    if (cached) return cached
  }

  // 使用去重机制发起请求
  const data = await deduplicateRequest(url, params, fn)

  // 缓存结果
  setCache(url, data, params, ttl)

  return data
}
