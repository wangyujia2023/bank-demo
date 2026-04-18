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
  definitions: ()     => http.get('/metrics/definitions'),
  templates:   ()     => http.get('/metrics/templates'),
  query:       (data) => http.post('/metrics/query', data),
  compare:     (data) => http.post('/metrics/compare', data),
  drilldown:   (data) => http.post('/metrics/drilldown', data),
  saveQuery:   (data) => http.post('/metrics/saved', data),
  listSaved:   ()     => http.get('/metrics/saved'),
  deleteSaved: (id)   => http.delete(`/metrics/saved/${id}`),
  history:     ()     => http.get('/metrics/history'),
}

// ── 日志可观测性 ───────────────────────────────────────────────────
export const observeApi = {
  logs:     (params) => http.get('/observe/logs', { params }),
  stats:    () => http.get('/observe/stats'),
  classify: () => http.post('/observe/classify'),
  tagStats: () => http.get('/observe/tag-stats'),
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
  run:        (data) => httpBench.post('/benchmark/run', data),
  auditStats: (limit = 300) => http.get('/benchmark/audit-stats', { params: { limit } }),
}

// ── 卫星数据采集 ──────────────────────────────────────────────────
export const satelliteApi = {
  init:     () => http.post('/satellite/init'),
  overview: () => http.get('/satellite/overview'),
  charts:   () => http.get('/satellite/charts'),
  query:    (params) => http.get('/satellite/query', { params }),
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


// ── 基金资讯 AI Function ──────────────────────────────────────────
export const newsApi = {
  init:        ()         => http.post('/news/init'),
  import:      ()         => http.post('/news/import'),
  addManual:   (title, content, source, sector) => http.post('/news/add-manual', { title, content, source, sector }),
  list:        (params)   => http.get('/news/list', { params }),
  detail:      (id)       => http.get(`/news/detail/${id}`),
  stats:       ()         => http.get('/news/stats'),
  summarize:   (ids)      => http.post('/news/summarize',  { article_ids: ids || null }),
  sentiment:   (ids)      => http.post('/news/sentiment',  { article_ids: ids || null }),
  extract:     (ids)      => http.post('/news/extract',    { article_ids: ids || null }),
  runAllAI:    ()         => http.post('/news/run-all-ai'),  // 3合1：同时执行 SUMMARIZE + SENTIMENT + EXTRACT
  tagAnalysis:    ()      => http.get('/news/tag-analysis'),
  sectorMetrics:  ()      => http.get('/news/sector-metrics'),
  signals:        ()      => http.get('/news/signals'),
  hotCompanies:   ()      => http.get('/news/hot-companies'),
}

// ── 基金数字沙盘 ──────────────────────────────────────────────────
export const fundApi = {
  init:        ()         => http.post('/fund/init'),
  generate:    ()         => http.post('/fund/generate'),
  batch:       (days)     => http.post(`/fund/batch?days=${days}`),
  reset:       ()         => http.post('/fund/reset'),
  overview:    ()         => http.get('/fund/overview'),
  list:        (params)   => http.get('/fund/list', { params }),
  detail:      (id)       => http.get(`/fund/detail/${id}`),
  nav:         (id, days) => http.get(`/fund/nav/${id}`, { params: { days } }),
  position:    (id)       => http.get(`/fund/position/${id}`),
  manager:     (mgr_id)   => http.get(`/fund/manager/${mgr_id}`),
  peers:       (id)       => http.get(`/fund/peers/${id}`),
  sectorStats: ()         => http.get('/fund/sector-stats'),
}

// ── 智能制造 ─────────────────────────────────────────────────────
export const mfgApi = {
  init:            ()         => http.post('/mfg/init'),
  generate:        ()         => http.post('/mfg/generate'),
  batch:           (steps)    => http.post(`/mfg/batch?steps=${steps}`),
  overview:        ()         => http.get('/mfg/overview'),
  oeeTrend:        ()         => http.get('/mfg/oee-trend'),
  machineStatus:   ()         => http.get('/mfg/machine-status'),
  machineTrend:    (machine_id) => http.get('/mfg/machine-trend', { params: { machine_id } }),
  causal:          ()         => http.get('/mfg/causal'),
  detail:          (limit=60) => http.get('/mfg/detail', { params: { limit } }),
  qualityStats:    ()         => http.get('/mfg/quality-stats'),
  energyStats:     ()         => http.get('/mfg/energy-stats'),
  maintenanceStats:()         => http.get('/mfg/maintenance-stats'),
  processTrend:    ()         => http.get('/mfg/process-trend'),
  reset:           ()         => http.post('/mfg/reset'),
}
