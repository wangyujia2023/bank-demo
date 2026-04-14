<template>
  <div class="tv-wrap">

    <!-- 工具栏 -->
    <div class="card toolbar">
      <el-select v-model="filters.service" clearable placeholder="过滤服务" style="width:160px" @change="loadTraces">
        <el-option v-for="s in SERVICES" :key="s" :value="s" :label="s">
          <span class="svc-dot" :style="{background:svcColor(s),display:'inline-block',width:'8px',height:'8px',borderRadius:'50%',marginRight:'6px'}" />{{ s }}
        </el-option>
      </el-select>
      <el-select v-model="filters.status" clearable placeholder="状态" style="width:110px" @change="loadTraces">
        <el-option value="OK" label="OK" />
        <el-option value="ERROR" label="ERROR" />
      </el-select>
      <el-button type="primary" :loading="loading" @click="loadTraces"><el-icon><Refresh /></el-icon> 刷新</el-button>
      <div style="margin-left:auto;font-size:13px;color:#909399">Doris user_behavior → 微服务链路模拟</div>
    </div>

    <div class="tv-body">

      <!-- 左：Trace 列表 -->
      <div class="card trace-list-panel">
        <div class="card-title">Trace 列表 <el-tag size="small" style="margin-left:6px">{{ traces.length }}</el-tag></div>
        <div v-loading="loading">
          <div
            v-for="t in traces" :key="t.trace_id"
            class="trace-row" :class="{selected: selected?.trace_id===t.trace_id, error: t.status==='ERROR'}"
            @click="selectTrace(t)"
          >
            <div class="tr-header">
              <el-tag :type="t.status==='ERROR'?'danger':'success'" size="small" effect="dark">{{ t.status }}</el-tag>
              <span class="tr-op">{{ t.operation }}</span>
              <span class="tr-dur" :style="{color: t.duration_ms>500?'#f56c6c':t.duration_ms>200?'#e6a23c':'#67c23a'}">{{ t.duration_ms }}ms</span>
            </div>
            <div class="tr-meta">
              <span class="mono" style="color:#909399;font-size:11px">{{ t.trace_id }}</span>
              <span style="margin-left:auto;font-size:11px;color:#909399">{{ t.span_count }} spans</span>
            </div>
            <div class="tr-svc" :style="{color:svcColor(t.root_service)}">{{ t.root_service }}</div>
            <div class="tr-time">{{ t.start_time?.slice(0,19) }}</div>
          </div>
        </div>
        <div style="text-align:center;padding:10px">
          <el-pagination v-model:current-page="page" :page-size="20" layout="prev, pager, next" :total="2000" @current-change="loadTraces" small />
        </div>
      </div>

      <!-- 右：Trace 详情 Gantt -->
      <div class="card detail-panel">
        <div v-if="!selected" class="empty-hint">← 点击左侧 Trace 查看详细链路</div>
        <div v-else v-loading="detailLoading">
          <div class="detail-header">
            <div>
              <span class="d-trace-id mono">{{ detail.trace_id }}</span>
              <el-tag :type="detail.spans?.some(s=>s.status==='ERROR')?'danger':'success'" size="small" style="margin-left:8px" effect="dark">
                {{ detail.spans?.some(s=>s.status==='ERROR') ? 'ERROR' : 'OK' }}
              </el-tag>
            </div>
            <div style="margin-top:4px;font-size:13px;color:#606266">
              总耗时 <b style="color:#409eff">{{ detail.total_duration_ms }}ms</b>
              &nbsp;·&nbsp; {{ detail.spans?.length }} 个 Span
              &nbsp;·&nbsp; {{ detail.services?.length }} 个服务
            </div>
          </div>

          <!-- 服务图例 -->
          <div class="svc-legend">
            <div v-for="s in detail.services" :key="s" class="svc-legend-item">
              <span class="svc-dot" :style="{background:svcColor(s)}" />
              <span style="font-size:12px">{{ s }}</span>
            </div>
          </div>

          <!-- Gantt 时间轴 -->
          <div class="gantt-wrap">
            <div class="gantt-header">
              <div class="gantt-label-col">服务 / 操作</div>
              <div class="gantt-bar-col">
                <div class="gantt-ruler">
                  <span v-for="tick in timeTicks" :key="tick" class="tick-label" :style="{left: (tick/detail.total_duration_ms*100)+'%'}">{{ tick }}ms</span>
                </div>
              </div>
            </div>
            <div v-for="span in detail.spans" :key="span.span_id" class="gantt-row">
              <div class="gantt-label-col">
                <span class="svc-dot" :style="{background:svcColor(span.service)}" />
                <div class="span-label">
                  <div class="span-svc">{{ span.service }}</div>
                  <div class="span-op">{{ span.operation }}</div>
                </div>
                <el-tag v-if="span.status==='ERROR'" type="danger" size="small" effect="dark" style="flex-shrink:0">ERR</el-tag>
                <el-tag v-if="span.db" type="primary" size="small" effect="plain" style="flex-shrink:0;font-size:10px">Doris</el-tag>
              </div>
              <div class="gantt-bar-col">
                <div class="gantt-track">
                  <div
                    class="gantt-bar"
                    :class="{error: span.status==='ERROR', db: span.db}"
                    :style="{
                      left: (span.offset_ms / detail.total_duration_ms * 100) + '%',
                      width: Math.max(span.duration_ms / detail.total_duration_ms * 100, 0.8) + '%',
                      background: span.status==='ERROR' ? '#f56c6c' : span.db ? '#67c23a' : svcColor(span.service)
                    }"
                  >
                    <span class="bar-label">{{ span.duration_ms }}ms</span>
                  </div>
                </div>
              </div>
            </div>
          </div>

        </div>
      </div>
    </div>

  </div>
</template>

<script setup>
import { ref, computed, onMounted, reactive } from 'vue'
import { Refresh } from '@element-plus/icons-vue'
import { traceApi } from '@/api'

const SERVICES = ['api-gateway','auth-service','payment-service','account-service','risk-engine','portal-service','user-service']
const SVC_COLORS = { 'api-gateway':'#e67e22','auth-service':'#409eff','payment-service':'#67c23a','account-service':'#1abc9c','risk-engine':'#f56c6c','portal-service':'#9b59b6','user-service':'#3498db','doris-connector':'#27ae60','cache-layer':'#f39c12' }
const svcColor = s => SVC_COLORS[s] || '#909399'

const filters = reactive({ service: '', status: '' })
const traces  = ref([])
const loading = ref(false)
const page    = ref(1)
const selected = ref(null)
const detail   = ref({})
const detailLoading = ref(false)

const timeTicks = computed(() => {
  const total = detail.value.total_duration_ms || 1
  const step  = Math.ceil(total / 4 / 50) * 50 || 50
  const ticks = []
  for (let t = 0; t <= total; t += step) ticks.push(t)
  return ticks
})

async function loadTraces() {
  loading.value = true
  try {
    const params = { page: page.value, size: 20 }
    if (filters.service) params.service = filters.service
    if (filters.status)  params.status  = filters.status
    traces.value = await traceApi.list(params)
  } finally { loading.value = false }
}

async function selectTrace(t) {
  selected.value = t
  detailLoading.value = true
  try { detail.value = await traceApi.detail(t.trace_id) }
  finally { detailLoading.value = false }
}

onMounted(loadTraces)
</script>

<style scoped>
.tv-wrap { display: flex; flex-direction: column; gap: 12px; }
.toolbar { display: flex; align-items: center; gap: 10px; padding: 12px 16px; }
.tv-body { display: grid; grid-template-columns: 340px 1fr; gap: 12px; align-items: start; }

/* Trace list */
.trace-list-panel { max-height: calc(100vh - 170px); overflow-y: auto; padding: 0; }
.trace-row {
  padding: 10px 14px; border-bottom: 1px solid #f5f5f5;
  cursor: pointer; transition: background 0.15s;
}
.trace-row:hover { background: #f8f9fa; }
.trace-row.selected { background: #ecf5ff; border-left: 3px solid #409eff; }
.trace-row.error { border-left: 3px solid #f56c6c; }
.tr-header { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; }
.tr-op  { font-size: 13px; font-weight: 600; color: #303133; flex: 1; }
.tr-dur { font-size: 13px; font-weight: 700; }
.tr-meta { display: flex; align-items: center; margin-bottom: 2px; }
.tr-svc  { font-size: 11px; font-weight: 600; margin-bottom: 2px; }
.tr-time { font-size: 11px; color: #c0c4cc; }
.mono { font-family: monospace; }

/* Detail panel */
.detail-panel { min-height: 400px; }
.empty-hint { text-align: center; padding: 80px 0; color: #c0c4cc; font-size: 14px; }
.detail-header { margin-bottom: 12px; }
.d-trace-id { font-family: monospace; font-size: 14px; color: #409eff; font-weight: 600; }

.svc-legend { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 16px; padding-bottom: 12px; border-bottom: 1px solid #f0f0f0; }
.svc-legend-item { display: flex; align-items: center; gap: 5px; }
.svc-dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; flex-shrink: 0; }

/* Gantt */
.gantt-wrap { overflow-x: auto; }
.gantt-header { display: flex; margin-bottom: 4px; }
.gantt-label-col { width: 260px; flex-shrink: 0; font-size: 11px; color: #909399; font-weight: 600; padding: 0 8px; }
.gantt-bar-col { flex: 1; position: relative; min-width: 0; }
.gantt-ruler { position: relative; height: 18px; border-bottom: 1px solid #f0f0f0; }
.tick-label { position: absolute; transform: translateX(-50%); font-size: 10px; color: #c0c4cc; }

.gantt-row { display: flex; align-items: center; margin-bottom: 6px; }
.gantt-label-col { display: flex; align-items: center; gap: 6px; width: 260px; flex-shrink: 0; padding-right: 12px; }
.span-label { flex: 1; min-width: 0; }
.span-svc  { font-size: 11px; font-weight: 600; color: #303133; }
.span-op   { font-size: 10px; color: #909399; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

.gantt-track { position: relative; height: 24px; background: #f5f7fa; border-radius: 4px; overflow: hidden; }
.gantt-bar {
  position: absolute; height: 100%; border-radius: 4px;
  display: flex; align-items: center; overflow: hidden;
  opacity: 0.85; transition: opacity 0.2s; min-width: 3px;
}
.gantt-bar:hover { opacity: 1; }
.bar-label { font-size: 10px; color: #fff; padding: 0 4px; white-space: nowrap; overflow: hidden; font-weight: 600; }
</style>
