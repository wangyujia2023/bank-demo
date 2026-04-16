import axios from 'axios'
import { ElMessage } from 'element-plus'

const http = axios.create({
  baseURL: '/api',
  timeout: 30000,
})

http.interceptors.response.use(
  res => res.data,
  err => {
    ElMessage.error(err.response?.data?.detail || err.message || '请求失败')
    return Promise.reject(err)
  }
)

// 长超时实例（向量初始化等耗时操作，20分钟）
const httpLong = axios.create({
  baseURL: '/api',
  timeout: 1200000,
})
httpLong.interceptors.response.use(
  res => res.data,
  err => {
    ElMessage.error(err.response?.data?.detail || err.message || '请求失败')
    return Promise.reject(err)
  }
)

// ── 用户宽表 ─────────────────────────────────────────────────────
export const userApi = {
  queryWide: (params) => http.get('/user/wide', { params }),
  getDetail: (userId) => http.get(`/user/${userId}`),
}

// ── 人群圈选 ─────────────────────────────────────────────────────
export const segmentApi = {
  count: (data) => http.post('/segment/count', data),
  create: (data) => http.post('/segment/create', data),
  list: () => http.get('/segment/list'),
  delete: (id) => http.delete(`/segment/${id}`),
  users: (id, page, size) => http.get(`/segment/${id}/users`, { params: { page, size } }),
  stats: (id) => http.get(`/segment/${id}/stats`),
}

// ── 行为分析 ─────────────────────────────────────────────────────
export const behaviorApi = {
  funnel: (data) => http.post('/behavior/funnel', data),
  retention: (data) => http.post('/behavior/retention', data),
  transaction: (params) => http.get('/behavior/transaction', { params }),
  rfm: () => http.get('/behavior/rfm'),
}

// ── 大盘 ─────────────────────────────────────────────────────────
export const dashboardApi = {
  overview: () => http.get('/dashboard'),
}

// ── 经营管理大屏 ──────────────────────────────────────────────────
export const managementApi = {
  overview: () => http.get('/management'),
}

// ── AI 日志标签分析 ──────────────────────────────────────────────
export const tagAnalysisApi = {
  overview: () => http.get('/tag-analysis/overview'),
  users: (params) => http.get('/tag-analysis/users', { params }),
  risk: () => http.get('/tag-analysis/risk'),
  cross: () => http.get('/tag-analysis/cross'),
  cooccurrence: () => http.get('/tag-analysis/cooccurrence'),
  runClassify: () => http.post('/tag-analysis/run-classify'),
}

// ── 银行报表 ──────────────────────────────────────────────────────
export const reportApi = {
  overview:    () => http.get('/report/overview'),
  transaction: () => http.get('/report/transaction'),
  risk:        () => http.get('/report/risk'),
}

// ── 指标平台 ──────────────────────────────────────────────────────
export const metricsApi = {
  definitions: () => http.get('/metrics/definitions'),
  query:       (data) => http.post('/metrics/query', data),
}

// ── 日志可观测性 ───────────────────────────────────────────────────
export const observeApi = {
  logs:  (params) => http.get('/observe/logs', { params }),
  stats: () => http.get('/observe/stats'),
}

// ── 链路追踪 ──────────────────────────────────────────────────────
export const traceApi = {
  list:   (params) => http.get('/trace/list', { params }),
  detail: (id) => http.get(`/trace/${id}`),
}

// ── 高并发压测 ────────────────────────────────────────────────────
const httpBench = axios.create({ baseURL: '/api', timeout: 120000 })
httpBench.interceptors.response.use(res => res.data, err => { ElMessage.error(err.response?.data?.detail || err.message || '请求失败'); return Promise.reject(err) })
export const benchmarkApi = {
  run: (data) => httpBench.post('/benchmark/run', data),
}

// ── 系统 ─────────────────────────────────────────────────────────
export const systemApi = {
  health: () => http.get('/system/health'),
  config: () => http.get('/system/config'),
}

// ── HASP 向量检索 ────────────────────────────────────────────────
export const vectorApi = {
  init: () => httpLong.post('/vector/init'),   // 20 分钟超时
  users: () => http.get('/vector/users'),
  labels: () => http.get('/vector/labels'),
  dimLabels: () => http.get('/vector/dim-labels'),
  searchUsers: (data) => http.post('/vector/search/users', data),
  searchLabels: (data) => http.post('/vector/search/labels', data),
  searchHybrid: (data) => http.post('/vector/search/hybrid', data),
  searchByPhoto: (formData) => http.post('/vector/search/by-photo', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  }),
  uploadUser: (formData) => http.post('/vector/users/upload', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  }),
}
