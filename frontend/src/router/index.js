import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  { path: '/',           redirect: '/dashboard' },
  { path: '/dashboard',  component: () => import('@/views/Dashboard.vue'),   meta: { title: '首页大盘' } },
  { path: '/management', component: () => import('@/views/ManagementDashboard.vue'), meta: { title: '经营管理大屏' } },
  { path: '/user',       component: () => import('@/views/UserWide.vue'),     meta: { title: '用户宽表' } },
  { path: '/segment',    component: () => import('@/views/Segment.vue'),      meta: { title: '人群圈选' } },
  { path: '/behavior',   component: () => import('@/views/Behavior.vue'),     meta: { title: '行为分析' } },
  { path: '/user-tag',   component: () => import('@/views/UserTagAnalysis.vue'), meta: { title: '用户行为分析' } },
  { path: '/log-classify',  component: () => import('@/views/LogClassifyAnalysis.vue'), meta: { title: 'AI 日志标签分析' } },
  { path: '/vector',        component: () => import('@/views/VectorSearch.vue'),        meta: { title: 'HASP 向量检索' } },
  { path: '/report',        component: () => import('@/views/BankReport.vue'),          meta: { title: '银行报表' } },
  { path: '/metrics',       component: () => import('@/views/MetricsPlatform.vue'),     meta: { title: '指标平台' } },
  { path: '/observe',       component: () => import('@/views/LogObserve.vue'),          meta: { title: '日志可观测性' } },
  { path: '/log-tag-stats', component: () => import('@/views/LogTagStats.vue'),         meta: { title: '日志标签分析' } },
  { path: '/trace',         component: () => import('@/views/TraceView.vue'),           meta: { title: '链路追踪' } },
  { path: '/satellite',     component: () => import('@/views/Satellite.vue'),           meta: { title: '卫星数据分析' } },
  { path: '/benchmark',     component: () => import('@/views/Benchmark.vue'),           meta: { title: '高并发点查' } },
  { path: '/config',        component: () => import('@/views/SysConfig.vue'),           meta: { title: '系统配置' } },
]

export default createRouter({
  history: createWebHistory(),
  routes,
})
