<template>
  <div class="tv-wrap">

    <!-- 工具栏 -->
    <div class="card toolbar">
      <el-select v-model="filters.status" clearable placeholder="状态" style="width:110px" @change="loadTraces">
        <el-option value="OK" label="✅ OK"/>
        <el-option value="ERROR" label="❌ ERROR"/>
      </el-select>
      <el-button type="primary" :loading="loading" @click="loadTraces">
        <el-icon><Refresh /></el-icon> 刷新
      </el-button>
      <div style="margin-left:auto;font-size:12px;color:#909399">
        基于内存 sys_logs 实时采集 · Doris 链路还原
      </div>
    </div>

    <div class="tv-body">

      <!-- 左：Trace 列表 -->
      <div class="card trace-panel">
        <div class="panel-title">
          Trace 列表
          <el-tag size="small" style="margin-left:6px">{{ traces.length }}</el-tag>
        </div>
        <div v-loading="loading">
          <div v-if="!loading && !traces.length"
            style="text-align:center;padding:40px 0;color:#c0c4cc;font-size:13px">
            暂无链路数据
          </div>
          <div v-for="t in traces" :key="t.trace_id"
            class="trace-row"
            :class="{ selected: selected?.trace_id === t.trace_id, error: t.status === 'ERROR' }"
            @click="selectTrace(t)">
            <div class="tr-hd">
              <el-tag :type="t.status === 'ERROR' ? 'danger' : 'success'" size="small" effect="dark">
                {{ t.status }}
              </el-tag>
              <span class="tr-op">{{ t.operation }}</span>
              <span class="tr-dur" :style="durStyle(t.duration_ms)">{{ fmtMs(t.duration_ms) }}</span>
            </div>
            <div class="tr-meta">
              <span class="mono">{{ t.trace_id?.slice(0, 16) }}…</span>
              <span style="margin-left:auto">{{ t.span_count }} spans</span>
            </div>
            <div style="font-size:11px;color:#67c23a">{{ t.service }}</div>
            <div style="font-size:10px;color:#c0c4cc">{{ t.start_time?.toString().slice(0, 19) }}</div>
          </div>
        </div>
        <div style="padding:8px;text-align:center">
          <el-pagination v-model:current-page="page" :page-size="20"
            layout="prev,pager,next" :total="total" @current-change="loadTraces" small/>
        </div>
      </div>

      <!-- 右：Trace 详情 瀑布图 -->
      <div class="card detail-panel">
        <div v-if="!selected" class="empty-hint">← 点击左侧 Trace 查看执行链路</div>

        <div v-else v-loading="detailLoading">

          <!-- 头部摘要 -->
          <div class="detail-hd">
            <div class="d-tid mono">{{ detail.trace_id }}</div>
            <div class="summary-row">
              <el-tag :type="hasError ? 'danger' : 'success'" effect="dark" size="small">
                {{ hasError ? 'ERROR' : 'OK' }}
              </el-tag>
              <span class="s-item">总耗时 <b style="color:#409eff">{{ fmtMs(detail.total_duration_ms) }}</b></span>
              <span class="s-item">{{ detail.spans?.length }} spans</span>
              <span class="s-item">{{ (detail.services || []).join(' → ') }}</span>
            </div>
          </div>

          <!-- 服务图例 -->
          <div class="svc-legend">
            <div v-for="s in detail.services" :key="s" class="svc-item">
              <span class="dot" :style="{ background: svcColor(s) }"/>
              <span>{{ s }}</span>
            </div>
          </div>

          <!-- 瀑布图 -->
          <div class="waterfall" ref="wfRef">

            <!-- 时间轴标尺 -->
            <div class="wf-ruler">
              <div class="wf-label-col"><!-- 占位 --></div>
              <div class="wf-bar-col ruler-bar">
                <div v-for="tick in timeTicks" :key="tick"
                  class="tick-mark"
                  :style="{ left: pct(tick) }">
                  <span class="tick-lbl">{{ tick }}ms</span>
                </div>
                <!-- 竖线 -->
                <div v-for="tick in timeTicks" :key="'l'+tick"
                  class="tick-line"
                  :style="{ left: pct(tick) }"/>
              </div>
            </div>

            <!-- Span 行 -->
            <div v-for="(span, idx) in detail.spans" :key="span.span_id"
              class="wf-row"
              :class="{ 'wf-row-db': span.db, 'wf-row-err': span.status === 'ERROR' }"
              @mouseenter="hovered = span.span_id"
              @mouseleave="hovered = null">

              <!-- 左侧标签 -->
              <div class="wf-label-col">
                <span class="idx-num">{{ idx + 1 }}</span>
                <span class="dot" :style="{ background: svcColor(span.service) }"/>
                <div class="span-info">
                  <div class="span-svc">{{ span.service }}</div>
                  <div class="span-op" :title="span.operation">{{ span.operation }}</div>
                </div>
                <el-tag v-if="span.status === 'ERROR'" type="danger" size="small" effect="dark" style="flex-shrink:0">ERR</el-tag>
                <el-tag v-if="span.db" type="success" size="small" effect="plain" style="flex-shrink:0;font-size:10px">SQL</el-tag>
              </div>

              <!-- 右侧甘特条 -->
              <div class="wf-bar-col">
                <!-- 背景参考线 -->
                <div v-for="tick in timeTicks" :key="'bg'+tick"
                  class="bg-line" :style="{ left: pct(tick) }"/>

                <!-- 甘特条 -->
                <div class="bar-track">
                  <div class="bar"
                    :style="{
                      left:  pct(span.offset_ms),
                      width: barWidth(span.duration_ms),
                      background: span.status === 'ERROR' ? '#f56c6c' : svcColor(span.service),
                    }">
                    <span v-if="barWidthNum(span.duration_ms) > 6" class="bar-lbl">
                      {{ fmtMs(span.duration_ms) }}
                    </span>
                  </div>

                  <!-- 悬浮时长标注（条太窄时） -->
                  <div v-if="hovered === span.span_id && barWidthNum(span.duration_ms) <= 6"
                    class="bar-tip"
                    :style="{ left: 'calc(' + pct(span.offset_ms) + ' + 2px)' }">
                    {{ fmtMs(span.duration_ms) }}
                  </div>
                </div>

                <!-- 耗时占比 -->
                <div class="bar-pct">{{ spanPct(span.duration_ms) }}%</div>
              </div>
            </div>

            <!-- SQL 展开区 -->
            <div v-for="span in detail.spans?.filter(s => s.db && s.detail)" :key="'sql'+span.span_id"
              class="sql-row">
              <div class="wf-label-col" style="padding-left:36px">
                <span style="font-size:10px;color:#909399">SQL</span>
              </div>
              <div class="wf-bar-col">
                <div class="sql-text">{{ span.detail }}</div>
              </div>
            </div>

          </div><!-- /waterfall -->

          <!-- 统计汇总 -->
          <div class="stat-row">
            <div class="stat-card">
              <div class="stat-val">{{ dbSpans.length }}</div>
              <div class="stat-lbl">DB Spans</div>
            </div>
            <div class="stat-card">
              <div class="stat-val" style="color:#f56c6c">{{ errSpans.length }}</div>
              <div class="stat-lbl">错误 Spans</div>
            </div>
            <div class="stat-card">
              <div class="stat-val" style="color:#67c23a">{{ fmtMs(dbTotalMs) }}</div>
              <div class="stat-lbl">DB 总耗时</div>
            </div>
            <div class="stat-card">
              <div class="stat-val" style="color:#e6a23c">{{ dbPct }}%</div>
              <div class="stat-lbl">DB 时间占比</div>
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

const SVC_COLORS = {
  'CDP后台':   '#409eff', 'api-gateway': '#409eff',
  '首页大盘':  '#409eff', '行为分析':   '#67c23a',
  '用户宽表':  '#e6a23c', '人群圈选':   '#9b59b6',
  '银行报表':  '#f56c6c', '日志观测':   '#1abc9c',
  '链路追踪':  '#3498db', '指标平台':   '#e67e22',
  '标签分析':  '#c0392b', '经营管理':   '#2ecc71',
  'Doris':     '#27ae60', 'tag-service': '#c0392b',
  'behavior-service': '#67c23a', 'trace-service': '#3498db',
  'user-service': '#e6a23c',
}
const SVC_PALETTE = ['#409eff','#67c23a','#e6a23c','#9b59b6','#f56c6c','#1abc9c','#e67e22','#27ae60','#c0392b','#3498db']
const _colorCache = {}
let   _colorIdx   = 0
const svcColor = s => {
  if (SVC_COLORS[s]) return SVC_COLORS[s]
  if (!_colorCache[s]) _colorCache[s] = SVC_PALETTE[_colorIdx++ % SVC_PALETTE.length]
  return _colorCache[s]
}

const fmtMs = ms => {
  if (ms == null) return '—'
  if (ms >= 1000) return (ms / 1000).toFixed(2) + 's'
  return Math.round(ms) + 'ms'
}
const durStyle = ms => ms > 1000 ? { color:'#f56c6c', fontWeight:'700' }
                     : ms > 300  ? { color:'#e6a23c' }
                     : { color:'#67c23a' }

// ── state ──
const filters      = reactive({ status: '' })
const traces       = ref([])
const loading      = ref(false)
const page         = ref(1)
const total        = ref(0)
const selected     = ref(null)
const detail       = ref({})
const detailLoading= ref(false)
const hovered      = ref(null)

// ── computed ──
const totalMs  = computed(() => detail.value.total_duration_ms || 1)
const hasError = computed(() => detail.value.spans?.some(s => s.status === 'ERROR'))
const dbSpans  = computed(() => detail.value.spans?.filter(s => s.db) || [])
const errSpans = computed(() => detail.value.spans?.filter(s => s.status === 'ERROR') || [])
const dbTotalMs= computed(() => dbSpans.value.reduce((a, s) => a + (s.duration_ms || 0), 0))
const dbPct    = computed(() => totalMs.value > 0
  ? Math.round(dbTotalMs.value / totalMs.value * 100) : 0)

const timeTicks = computed(() => {
  const t = totalMs.value
  // 选一个整洁的步长让刻度不超过5个
  const raw = t / 4
  const mag = Math.pow(10, Math.floor(Math.log10(raw)))
  const step = [1,2,5,10].map(f => f * mag).find(s => t / s <= 5) || mag
  const ticks = []
  for (let v = 0; v <= t; v += step) ticks.push(Math.round(v))
  return ticks
})

const pct = ms => (Math.min(ms / totalMs.value * 100, 100)).toFixed(3) + '%'
const barWidthNum = ms => Math.max(ms / totalMs.value * 100, 0.4)
const barWidth    = ms => barWidthNum(ms).toFixed(3) + '%'
const spanPct     = ms => (ms / totalMs.value * 100).toFixed(1)

// ── API ──
async function loadTraces() {
  loading.value = true
  try {
    const params = { page: page.value, size: 20 }
    if (filters.status) params.status = filters.status
    const result = await traceApi.list(params)
    let list = Array.isArray(result) ? result : (result.traces || [])
    if (filters.status) list = list.filter(t => t.status === filters.status)
    traces.value = list
    total.value  = list.length < 20 ? list.length : page.value * 20 + 1
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
/* ── 布局 ── */
.tv-wrap { display:flex; flex-direction:column; gap:12px }
.toolbar { display:flex; align-items:center; gap:10px; padding:12px 16px }
.tv-body { display:grid; grid-template-columns:300px 1fr; gap:12px; align-items:start }

/* ── Trace 列表 ── */
.trace-panel { max-height:calc(100vh - 160px); overflow-y:auto; padding:0 }
.panel-title  { font-size:13px; font-weight:600; padding:12px 14px 8px; border-bottom:1px solid #f5f5f5 }
.trace-row    { padding:10px 14px; border-bottom:1px solid #f5f5f5; cursor:pointer; transition:background .15s }
.trace-row:hover    { background:#f8f9fa }
.trace-row.selected { background:#ecf5ff; border-left:3px solid #409eff }
.trace-row.error    { border-left:3px solid #f56c6c }
.tr-hd  { display:flex; align-items:center; gap:8px; margin-bottom:3px }
.tr-op  { font-size:12px; font-weight:600; flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap }
.tr-dur { font-size:12px; font-weight:700; flex-shrink:0 }
.tr-meta { display:flex; align-items:center; font-size:10px; color:#909399; margin-bottom:2px }
.mono { font-family:monospace }

/* ── 详情面板 ── */
.detail-panel { min-height:500px; padding:16px; overflow:hidden }
.empty-hint   { text-align:center; padding:80px 0; color:#c0c4cc; font-size:14px }
.detail-hd    { margin-bottom:10px }
.d-tid        { font-family:monospace; font-size:12px; color:#909399; margin-bottom:6px; word-break:break-all }
.summary-row  { display:flex; align-items:center; gap:12px; flex-wrap:wrap }
.s-item       { font-size:13px; color:#606266 }
.svc-legend   { display:flex; flex-wrap:wrap; gap:12px; margin-bottom:12px; padding-bottom:10px; border-bottom:1px solid #f0f0f0 }
.svc-item     { display:flex; align-items:center; gap:5px; font-size:12px }
.dot          { width:9px; height:9px; border-radius:50%; flex-shrink:0; display:inline-block }

/* ── 瀑布图 ── */
.waterfall { overflow-x:auto; font-size:12px }

/* 标尺行 */
.wf-ruler   { display:flex; margin-bottom:2px; height:24px }
.ruler-bar  { position:relative; border-bottom:1px solid #e4e7ed }
.tick-mark  { position:absolute; transform:translateX(-50%); bottom:2px }
.tick-lbl   { font-size:10px; color:#c0c4cc; white-space:nowrap }
.tick-line  { position:absolute; top:0; bottom:0; width:1px; background:#f0f0f0 }

/* span 行 */
.wf-row     { display:flex; align-items:center; margin-bottom:3px; min-height:28px }
.wf-row:hover .bar { opacity:1 }
.wf-row-db  { background:#fafff8 }
.wf-row-err { background:#fff5f5 }

/* 左侧标签列 */
.wf-label-col {
  width: 240px;
  flex-shrink: 0;
  display: flex;
  align-items: center;
  gap: 5px;
  padding-right: 10px;
  overflow: hidden;
}
.idx-num  { font-size:10px; color:#c0c4cc; width:18px; text-align:right; flex-shrink:0 }
.span-info { flex:1; min-width:0 }
.span-svc { font-size:11px; font-weight:600; color:#303133; white-space:nowrap; overflow:hidden; text-overflow:ellipsis }
.span-op  { font-size:10px; color:#909399; white-space:nowrap; overflow:hidden; text-overflow:ellipsis }

/* 右侧甘特列 */
.wf-bar-col {
  flex: 1;
  position: relative;
  display: flex;
  align-items: center;
  min-width: 0;
}
.bg-line  { position:absolute; top:0; bottom:0; width:1px; background:#f5f5f5; pointer-events:none }
.bar-track { flex:1; position:relative; height:20px }
.bar {
  position: absolute;
  height: 100%;
  border-radius: 3px;
  opacity: .82;
  display: flex;
  align-items: center;
  overflow: hidden;
  min-width: 2px;
  transition: opacity .15s;
}
.bar-lbl  { font-size:10px; color:#fff; padding:0 4px; white-space:nowrap; font-weight:600 }
.bar-tip  {
  position: absolute;
  top: -20px;
  background: rgba(0,0,0,.65);
  color: #fff;
  font-size: 10px;
  padding: 2px 5px;
  border-radius: 3px;
  white-space: nowrap;
  pointer-events: none;
  z-index: 10;
}
.bar-pct {
  width: 42px;
  flex-shrink: 0;
  text-align: right;
  font-size: 10px;
  color: #c0c4cc;
  padding-left: 4px;
}

/* SQL 展示行 */
.sql-row  { display:flex; margin-bottom:4px }
.sql-text {
  flex: 1;
  font-size: 10px;
  color: #909399;
  font-family: monospace;
  background: #f8f9fa;
  border-radius: 3px;
  padding: 3px 6px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

/* 统计汇总 */
.stat-row {
  display: flex;
  gap: 12px;
  margin-top: 16px;
  padding-top: 12px;
  border-top: 1px solid #f0f0f0;
}
.stat-card { text-align:center; flex:1; background:#f8f9fa; border-radius:6px; padding:8px 0 }
.stat-val  { font-size:18px; font-weight:700; color:#303133 }
.stat-lbl  { font-size:11px; color:#909399; margin-top:2px }
</style>
