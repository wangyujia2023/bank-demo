<template>
  <div class="tv-wrap">
    <div class="card toolbar">
      <el-select v-model="filters.status" clearable placeholder="状态" style="width:110px" @change="loadTraces">
        <el-option value="OK" label="✅ OK"/><el-option value="ERROR" label="❌ ERROR"/>
      </el-select>
      <el-button type="primary" :loading="loading" @click="loadTraces"><el-icon><Refresh /></el-icon> 刷新</el-button>
      <div style="margin-left:auto;font-size:12px;color:#909399">sys_logs + sys_spans · 实时采集</div>
    </div>

    <div class="tv-body">
      <div class="card trace-panel">
        <div class="panel-title">Trace 列表 <el-tag size="small" style="margin-left:6px">{{traces.length}}</el-tag></div>
        <div v-loading="loading">
          <div v-if="!loading && traces.length===0" style="text-align:center;padding:30px;color:#c0c4cc;font-size:13px">
            暂无链路数据，请先发起请求
          </div>
          <div v-for="t in traces" :key="t.trace_id"
            class="trace-row" :class="{selected:selected?.trace_id===t.trace_id,error:t.status==='ERROR'}"
            @click="selectTrace(t)">
            <div class="tr-hd">
              <el-tag :type="t.status==='ERROR'?'danger':'success'" size="small" effect="dark">{{t.status}}</el-tag>
              <span class="tr-op">{{t.operation}}</span>
              <span class="tr-dur" :style="durStyle(t.duration_ms)">{{t.duration_ms}}ms</span>
            </div>
            <div class="tr-meta">
              <span class="mono" style="color:#909399;font-size:10px">{{t.trace_id?.slice(0,20)}}…</span>
              <span style="margin-left:auto;font-size:10px;color:#909399">{{t.span_count}} spans</span>
            </div>
            <div style="font-size:11px;color:#67c23a;margin-top:2px">{{t.service}}</div>
            <div style="font-size:10px;color:#c0c4cc">{{t.start_time?.toString().slice(0,19)}} · db:{{t.db_time_ms||0}}ms</div>
          </div>
        </div>
        <div style="text-align:center;padding:10px">
          <el-pagination v-model:current-page="page" :page-size="20"
            layout="prev,pager,next" :total="total" @current-change="loadTraces" small/>
        </div>
      </div>

      <div class="card detail-panel">
        <div v-if="!selected" class="empty-hint">← 点击左侧 Trace 查看 HTTP + Doris SQL 链路</div>
        <div v-else v-loading="detailLoading">
          <div class="detail-hd">
            <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
              <span class="d-tid mono">{{detail.trace_id}}</span>
              <el-tag :type="detail.spans?.some(s=>s.status==='ERROR')?'danger':'success'" size="small" effect="dark">
                {{detail.spans?.some(s=>s.status==='ERROR')?'ERROR':'OK'}}
              </el-tag>
            </div>
            <div style="margin-top:6px;font-size:13px;color:#606266">
              总耗时 <b style="color:#409eff">{{detail.total_duration_ms}}ms</b>
              &nbsp;·&nbsp; {{detail.spans?.length}} 个 Span
              &nbsp;·&nbsp; {{(detail.services||[]).join(' → ')}}
            </div>
          </div>

          <div class="svc-legend">
            <div v-for="s in detail.services" :key="s" class="svc-item">
              <span class="svc-dot" :style="{background:svcColor(s)}"/>
              <span style="font-size:12px">{{s}}</span>
            </div>
          </div>

          <div class="gantt-wrap">
            <div class="gantt-hd">
              <div class="glabel">服务 / 操作</div>
              <div class="gbar-col">
                <div class="ruler">
                  <span v-for="t in timeTicks" :key="t" class="tick"
                    :style="{left:(t/detail.total_duration_ms*100)+'%'}">{{t}}ms</span>
                </div>
              </div>
            </div>
            <div v-for="span in detail.spans" :key="span.span_id" class="gantt-row">
              <div class="glabel">
                <span class="svc-dot" :style="{background:svcColor(span.service)}"/>
                <div class="span-label">
                  <div class="span-svc">{{span.service}}</div>
                  <div class="span-op" :title="span.detail">{{span.operation}}</div>
                </div>
                <el-tag v-if="span.status==='ERROR'" type="danger" size="small" effect="dark" style="flex-shrink:0">ERR</el-tag>
                <el-tag v-if="span.db" type="success" size="small" effect="plain" style="flex-shrink:0;font-size:10px">SQL</el-tag>
              </div>
              <div class="gbar-col" style="flex-direction:column;gap:2px">
                <div class="gtrack">
                  <div class="gbar" :style="{
                    left:Math.min(span.offset_ms/detail.total_duration_ms*100,99)+'%',
                    width:Math.max(span.duration_ms/detail.total_duration_ms*100,0.8)+'%',
                    background:spanColor(span)}">
                    <span class="bar-lbl">{{span.duration_ms}}ms</span>
                  </div>
                </div>
                <div v-if="span.db && span.detail" class="sql-prev">{{span.detail}}</div>
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

const SVC_COLORS = {'CDP后台':'#409eff','首页大盘':'#409eff','行为分析':'#67c23a','用户宽表':'#e6a23c',
  '人群圈选':'#9b59b6','银行报表':'#f56c6c','日志观测':'#1abc9c','链路追踪':'#3498db',
  '指标平台':'#e67e22','标签分析':'#c0392b','经营管理':'#2ecc71','Doris':'#27ae60'}
const svcColor = s => SVC_COLORS[s] || '#409eff'
const spanColor = span => span.status==='ERROR'?'#f56c6c':svcColor(span.service)
const durStyle = ms => ms>1000?{color:'#f56c6c',fontWeight:'700'}:ms>300?{color:'#e6a23c'}:{color:'#67c23a'}

const filters = reactive({ status: '' })
const traces  = ref([])
const loading = ref(false)
const page    = ref(1)
const total   = ref(0)
const selected = ref(null)
const detail   = ref({})
const detailLoading = ref(false)

const timeTicks = computed(() => {
  const t = detail.value.total_duration_ms || 1
  const step = Math.ceil(t/4/50)*50||50
  const ticks=[]
  for (let v=0;v<=t;v+=step) ticks.push(v)
  return ticks
})

async function loadTraces() {
  loading.value=true
  try {
    const params={page:page.value,size:20}
    if (filters.status) params.status=filters.status
    let result = await traceApi.list(params)
    let list = Array.isArray(result)?result:(result.traces||[])
    if (filters.status) list=list.filter(t=>t.status===filters.status)
    traces.value=list
    total.value=list.length<20?list.length:(page.value*20+1)
  } finally { loading.value=false }
}

async function selectTrace(t) {
  selected.value=t; detailLoading.value=true
  try { detail.value=await traceApi.detail(t.trace_id) }
  finally { detailLoading.value=false }
}

onMounted(loadTraces)
</script>

<style scoped>
.tv-wrap{display:flex;flex-direction:column;gap:12px}
.toolbar{display:flex;align-items:center;gap:10px;padding:12px 16px;flex-wrap:wrap}
.tv-body{display:grid;grid-template-columns:320px 1fr;gap:12px;align-items:start}
.panel-title{font-size:13px;font-weight:600;color:#303133;padding:12px 14px 8px;border-bottom:1px solid #f5f5f5}
.trace-panel{max-height:calc(100vh - 170px);overflow-y:auto;padding:0}
.trace-row{padding:10px 14px;border-bottom:1px solid #f5f5f5;cursor:pointer;transition:background .15s}
.trace-row:hover{background:#f8f9fa}
.trace-row.selected{background:#ecf5ff;border-left:3px solid #409eff}
.trace-row.error{border-left:3px solid #f56c6c}
.tr-hd{display:flex;align-items:center;gap:8px;margin-bottom:4px}
.tr-op{font-size:12px;font-weight:600;color:#303133;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.tr-dur{font-size:12px;font-weight:700;flex-shrink:0}
.tr-meta{display:flex;align-items:center;margin-bottom:2px}
.mono{font-family:monospace}
.detail-panel{min-height:400px;padding:16px}
.empty-hint{text-align:center;padding:80px 0;color:#c0c4cc;font-size:14px}
.detail-hd{margin-bottom:14px}
.d-tid{font-family:monospace;font-size:13px;color:#409eff;font-weight:600;word-break:break-all}
.svc-legend{display:flex;flex-wrap:wrap;gap:14px;margin-bottom:14px;padding-bottom:12px;border-bottom:1px solid #f0f0f0}
.svc-item{display:flex;align-items:center;gap:5px}
.svc-dot{width:10px;height:10px;border-radius:50%;display:inline-block;flex-shrink:0}
.gantt-wrap{overflow-x:auto}
.gantt-hd{display:flex;margin-bottom:4px}
.glabel{width:220px;flex-shrink:0;font-size:11px;color:#909399;font-weight:600;padding:0 8px;display:flex;align-items:center;gap:6px}
.gbar-col{flex:1;position:relative;min-width:0;display:flex}
.ruler{position:relative;height:18px;border-bottom:1px solid #f0f0f0;flex:1}
.tick{position:absolute;transform:translateX(-50%);font-size:10px;color:#c0c4cc}
.gantt-row{display:flex;align-items:flex-start;margin-bottom:8px}
.span-label{flex:1;min-width:0}
.span-svc{font-size:11px;font-weight:600;color:#303133}
.span-op{font-size:10px;color:#909399;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.gtrack{position:relative;height:22px;background:#f5f7fa;border-radius:4px;overflow:visible;flex:1}
.gbar{position:absolute;height:100%;border-radius:4px;display:flex;align-items:center;overflow:hidden;opacity:.85;min-width:3px;transition:opacity .2s}
.gbar:hover{opacity:1}
.bar-lbl{font-size:10px;color:#fff;padding:0 4px;white-space:nowrap;overflow:hidden;font-weight:600}
.sql-prev{font-size:10px;color:#909399;font-family:monospace;padding:2px 4px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:100%}
</style>
