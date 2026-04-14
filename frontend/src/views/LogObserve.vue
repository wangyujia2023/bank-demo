<template>
  <div class="lo-wrap">

    <!-- 顶部搜索栏 -->
    <div class="card toolbar">
      <el-input v-model="filters.search" placeholder="搜索日志内容、事件类型、渠道..." clearable style="flex:1" @keyup.enter="loadLogs">
        <template #prefix><el-icon><Search /></el-icon></template>
      </el-input>
      <el-select v-model="filters.level" clearable placeholder="日志级别" style="width:120px" @change="loadLogs">
        <el-option v-for="lv in ['DEBUG','INFO','WARN','ERROR']" :key="lv" :value="lv">
          <el-tag :type="levelType(lv)" size="small" effect="dark">{{ lv }}</el-tag>
        </el-option>
      </el-select>
      <el-select v-model="filters.service" clearable placeholder="服务" style="width:150px" @change="loadLogs">
        <el-option v-for="s in stats.services" :key="s" :value="s" :label="s" />
      </el-select>
      <el-button type="primary" :loading="loading" @click="loadLogs"><el-icon><Refresh /></el-icon> 刷新</el-button>
    </div>

    <div class="lo-body">

      <!-- 左侧 Facets -->
      <div class="facets">
        <div class="facet-section">
          <div class="facet-title">日志级别</div>
          <div v-for="lv in stats.level_counts" :key="lv.level"
            class="facet-item" :class="{active: filters.level===lv.level}"
            @click="toggleFilter('level', lv.level)"
          >
            <el-tag :type="levelType(lv.level)" size="small" effect="dark" style="min-width:48px;text-align:center">{{ lv.level }}</el-tag>
            <span class="facet-cnt">{{ lv.cnt.toLocaleString() }}</span>
          </div>
        </div>
        <div class="facet-divider" />
        <div class="facet-section">
          <div class="facet-title">服务</div>
          <div v-for="s in stats.svc_counts" :key="s.service"
            class="facet-item" :class="{active: filters.service===s.service}"
            @click="toggleFilter('service', s.service)"
          >
            <span class="svc-dot" :style="{background: svcColor(s.service)}" />
            <span class="facet-label">{{ s.service }}</span>
            <span class="facet-cnt">{{ s.cnt.toLocaleString() }}</span>
          </div>
        </div>
      </div>

      <!-- 主体 -->
      <div class="log-main">

        <!-- 日志直方图 -->
        <div class="card hist-card">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
            <span class="card-title" style="margin:0">事件分布</span>
            <el-tag size="small" effect="plain">共 {{ totalHist.toLocaleString() }} 条</el-tag>
          </div>
          <v-chart :option="histOpt" style="height:120px" autoresize />
        </div>

        <!-- 日志列表 -->
        <div class="card log-card" v-loading="loading">
          <div v-if="logs.length === 0" style="text-align:center;padding:40px;color:#c0c4cc">暂无日志</div>
          <div v-for="(log, idx) in logs" :key="idx">
            <div class="log-row" :class="log.level.toLowerCase()" @click="toggleExpand(idx)">
              <span class="log-level-badge" :class="log.level.toLowerCase()">{{ log.level }}</span>
              <span class="log-ts">{{ log.timestamp.slice(0,19) }}</span>
              <span class="log-svc" :style="{color: svcColor(log.service)}">{{ log.service }}</span>
              <span class="log-msg">{{ log.message }}</span>
              <el-icon size="12" style="margin-left:auto;color:#c0c4cc;flex-shrink:0"><ArrowDown /></el-icon>
            </div>
            <div v-if="expanded.has(idx)" class="log-detail">
              <div class="log-detail-grid">
                <div class="ld-row"><span class="ld-key">trace_id</span><span class="ld-val mono">{{ log.trace_id }}</span></div>
                <div class="ld-row"><span class="ld-key">user_id</span><span class="ld-val">{{ log.user_id }}</span></div>
                <div class="ld-row"><span class="ld-key">channel</span><span class="ld-val">{{ log.channel }}</span></div>
                <div class="ld-row"><span class="ld-key">amount</span><span class="ld-val">{{ log.amount }}</span></div>
                <div class="ld-row"><span class="ld-key">event</span><span class="ld-val"><el-tag size="small">{{ log.event }}</el-tag></span></div>
                <div class="ld-row"><span class="ld-key">service</span><span class="ld-val" :style="{color:svcColor(log.service)}">{{ log.service }}</span></div>
              </div>
            </div>
          </div>
        </div>

        <!-- 翻页 -->
        <div style="text-align:center;padding:12px 0">
          <el-pagination
            v-model:current-page="page"
            :page-size="50"
            layout="prev, pager, next"
            :total="10000"
            @current-change="loadLogs"
          />
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, reactive, onMounted } from 'vue'
import { Search, Refresh, ArrowDown } from '@element-plus/icons-vue'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { BarChart } from 'echarts/charts'
import { GridComponent, TooltipComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import { observeApi } from '@/api'

use([CanvasRenderer, BarChart, GridComponent, TooltipComponent])

const filters  = reactive({ search: '', level: '', service: '' })
const logs     = ref([])
const stats    = ref({ level_counts: [], svc_counts: [], histogram: [], services: [] })
const loading  = ref(false)
const page     = ref(1)
const expanded = ref(new Set())

const SVC_COLORS = { 'auth-service': '#409eff', 'payment-service': '#67c23a', 'account-service': '#e6a23c', 'risk-engine': '#f56c6c', 'portal-service': '#9b59b6', 'user-service': '#1abc9c', 'api-gateway': '#e67e22' }
const svcColor = s => SVC_COLORS[s] || '#909399'

const levelType = lv => ({ DEBUG: 'info', INFO: '', WARN: 'warning', ERROR: 'danger' }[lv] || '')

function toggleFilter(key, val) {
  filters[key] = filters[key] === val ? '' : val
  loadLogs()
}
function toggleExpand(idx) {
  const s = new Set(expanded.value)
  if (s.has(idx)) s.delete(idx); else s.add(idx)
  expanded.value = s
}

const totalHist = computed(() => stats.value.histogram.reduce((s, r) => s + (r.cnt || 0), 0))

const histOpt = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: 10, right: 10, top: 5, bottom: 20 },
  xAxis: { type: 'category', data: stats.value.histogram.map(r => r.event_type), axisLabel: { fontSize: 10 } },
  yAxis: { type: 'value', show: false },
  series: [{
    type: 'bar',
    data: stats.value.histogram.map(r => ({
      value: r.cnt,
      itemStyle: { color: SVC_COLORS[{ ANOMALY:'risk-engine',TRANSFER:'payment-service',LOGIN:'auth-service',PAYMENT:'payment-service',DEPOSIT:'account-service',WITHDRAW:'account-service' }[r.event_type]] || '#409eff' }
    })),
    label: { show: true, position: 'top', fontSize: 10 }
  }]
}))

async function loadLogs() {
  loading.value = true
  expanded.value = new Set()
  try {
    const params = { page: page.value, size: 50 }
    if (filters.search)  params.search  = filters.search
    if (filters.level)   params.level   = filters.level
    if (filters.service) params.service = filters.service
    logs.value = await observeApi.logs(params)
  } finally { loading.value = false }
}

onMounted(async () => {
  stats.value = await observeApi.stats()
  await loadLogs()
})
</script>

<style scoped>
.lo-wrap { display: flex; flex-direction: column; gap: 12px; }
.toolbar { display: flex; align-items: center; gap: 10px; padding: 12px 16px; }
.lo-body { display: grid; grid-template-columns: 200px 1fr; gap: 12px; align-items: start; }

/* Facets */
.facets { background: #fff; border-radius: 8px; padding: 12px 0; box-shadow: 0 1px 4px rgba(0,0,0,0.06); }
.facet-section { padding: 4px 0; }
.facet-title { font-size: 11px; font-weight: 600; color: #909399; text-transform: uppercase; padding: 4px 14px 8px; }
.facet-divider { height: 1px; background: #f0f0f0; margin: 8px 14px; }
.facet-item { display: flex; align-items: center; gap: 8px; padding: 5px 14px; cursor: pointer; border-radius: 0; transition: background 0.1s; }
.facet-item:hover { background: #f5f7fa; }
.facet-item.active { background: #ecf5ff; }
.facet-label { flex: 1; font-size: 12px; color: #303133; }
.facet-cnt { font-size: 11px; color: #909399; font-weight: 600; }
.svc-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }

/* Main */
.log-main { display: flex; flex-direction: column; gap: 12px; }
.hist-card { padding: 12px 16px; }

.log-card { padding: 0; overflow: hidden; }
.log-row {
  display: flex; align-items: center; gap: 10px; padding: 7px 14px;
  font-family: 'JetBrains Mono', Consolas, monospace; font-size: 12px;
  border-bottom: 1px solid #f5f5f5; cursor: pointer; transition: background 0.1s;
  min-height: 36px;
}
.log-row:hover { background: #f8f9fa; }
.log-row.error { background: #fff5f5; }
.log-row.warn  { background: #fffbe6; }
.log-row.debug { color: #909399; }

.log-level-badge {
  font-size: 10px; font-weight: 700; padding: 1px 6px; border-radius: 3px;
  flex-shrink: 0; min-width: 44px; text-align: center;
}
.log-level-badge.error { background: #fde2e2; color: #f56c6c; }
.log-level-badge.warn  { background: #fdf6ec; color: #e6a23c; }
.log-level-badge.info  { background: #ecf5ff; color: #409eff; }
.log-level-badge.debug { background: #f4f4f5; color: #909399; }

.log-ts  { color: #909399; font-size: 11px; flex-shrink: 0; }
.log-svc { font-size: 11px; flex-shrink: 0; min-width: 120px; }
.log-msg { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: #303133; }

.log-detail { background: #1e1e2e; padding: 12px 16px; border-bottom: 1px solid #333; }
.log-detail-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; }
.ld-row { display: flex; gap: 8px; }
.ld-key { font-size: 11px; color: #6e7c9a; min-width: 70px; flex-shrink: 0; }
.ld-val { font-size: 12px; color: #a8b3cf; font-family: monospace; }
.ld-val.mono { font-family: monospace; color: #7ec8e3; }
</style>
