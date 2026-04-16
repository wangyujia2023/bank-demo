<template>
  <div class="lo-wrap">
    <div class="card toolbar">
      <el-input v-model="filters.path" placeholder="过滤路径..." clearable style="flex:1;max-width:280px" @keyup.enter="load">
        <template #prefix><el-icon><Search /></el-icon></template>
      </el-input>
      <el-select v-model="filters.level" clearable placeholder="级别" style="width:110px" @change="load">
        <el-option value="INFO" label="INFO" /><el-option value="WARN" label="WARN" /><el-option value="ERROR" label="ERROR" />
      </el-select>
      <el-select v-model="filters.service" clearable placeholder="服务" style="width:130px" @change="load">
        <el-option v-for="s in (stats.svc_counts||[])" :key="s.service" :value="s.service" :label="s.service" />
      </el-select>
      <el-button type="primary" :loading="loading" @click="() => { page=1; load() }"><el-icon><Refresh /></el-icon> 刷新</el-button>
      <el-button type="warning" :loading="classifying" @click="runClassify"><el-icon><MagicStick /></el-icon> AI打标签</el-button>
      <el-tag size="small" effect="plain" style="margin-left:auto">{{ dataSource }}</el-tag>
    </div>

    <div class="lo-body">
      <div class="facets">
        <div class="stat-mini" v-for="s in statCards" :key="s.label">
          <div class="sm-val" :style="{color:s.color}">{{ s.value }}</div>
          <div class="sm-lbl">{{ s.label }}</div>
        </div>
        <div class="facet-divider"/>
        <div class="facet-section">
          <div class="facet-title">日志级别</div>
          <div v-for="l in (stats.level_counts||[])" :key="l.level"
            class="facet-item" :class="{active:filters.level===l.level}"
            @click="toggleFilter('level',l.level)">
            <span :class="`lv-badge lv-${l.level?.toLowerCase()}`">{{l.level}}</span>
            <span class="facet-cnt">{{l.cnt?.toLocaleString()}}</span>
          </div>
        </div>
        <div class="facet-divider"/>
        <div class="facet-section">
          <div class="facet-title">服务</div>
          <div v-for="s in (stats.svc_counts||[])" :key="s.service"
            class="facet-item" :class="{active:filters.service===s.service}"
            @click="toggleFilter('service',s.service)">
            <span class="svc-dot" :style="{background:svcColor(s.service)}"/>
            <span class="facet-lbl">{{s.service}}</span>
            <span class="facet-cnt">{{s.cnt?.toLocaleString()}}</span>
          </div>
        </div>
        <div class="facet-divider"/>
        <div class="facet-section">
          <div class="facet-title">Top 路径</div>
          <div v-for="p in (stats.top_paths||[]).slice(0,5)" :key="p.path"
            class="facet-item path-item" @click="filters.path=p.path;load()">
            <div style="font-size:11px;color:#409eff;word-break:break-all">{{p.path}}</div>
            <div style="font-size:10px;color:#909399">{{p.count}}次 · {{p.avg_duration}}ms</div>
          </div>
        </div>
      </div>

      <div class="log-main">
        <div class="card hist-card">
          <div style="display:flex;justify-content:space-between;margin-bottom:6px">
            <span style="font-weight:600;font-size:13px">Top路径请求量</span>
            <el-tag size="small" effect="plain">共 {{ stats.total?.toLocaleString()||0 }} 条</el-tag>
          </div>
          <v-chart :option="chartOpt" style="height:100px" autoresize />
        </div>

        <div class="card log-card" v-loading="loading">
          <div v-if="!loading && logs.length===0" style="text-align:center;padding:40px;color:#c0c4cc">暂无日志，请先操作页面产生请求</div>
          <div v-for="(log,idx) in logs" :key="idx">
            <div class="log-row" :class="rowCls(log)" @click="toggleExpand(idx)">
              <span :class="`lv-badge lv-${log.level?.toLowerCase()}`">{{log.level}}</span>
              <span class="log-ts">{{(log.timestamp||'').toString().slice(0,19)}}</span>
              <span class="log-svc" :style="{color:svcColor(log.service)}">{{log.service}}</span>
              <span class="log-msg">{{log.message}}</span>
              <span class="log-dur" :style="durColor(log.duration_ms)">{{log.duration_ms}}ms</span>
              <span v-if="log.db_time_ms>0" style="font-size:10px;color:#909399;flex-shrink:0">db:{{log.db_time_ms}}ms</span>
              <el-icon size="11" style="margin-left:6px;color:#dcdfe6;flex-shrink:0"><ArrowDown /></el-icon>
            </div>
            <div v-if="expanded.has(idx)" class="log-detail">
              <div class="ld-grid">
                <div class="ld-row"><span class="ld-k">trace_id</span><span class="ld-v mono">{{log.trace_id}}</span></div>
                <div class="ld-row"><span class="ld-k">method</span><span class="ld-v">{{log.method}}</span></div>
                <div class="ld-row"><span class="ld-k">path</span><span class="ld-v">{{log.path}}</span></div>
                <div class="ld-row"><span class="ld-k">status_code</span><span class="ld-v">{{log.status_code}}</span></div>
                <div class="ld-row"><span class="ld-k">duration_ms</span><span class="ld-v">{{log.duration_ms}}</span></div>
                <div class="ld-row"><span class="ld-k">db_time_ms</span><span class="ld-v">{{log.db_time_ms}}</span></div>
                <div class="ld-row"><span class="ld-k">log_tag</span><span class="ld-v">{{log.log_tag}}</span></div>
              </div>
            </div>
          </div>
        </div>

        <div style="text-align:center;padding:12px 0">
          <el-pagination v-model:current-page="page" :page-size="pageSize"
            layout="prev,pager,next,total" :total="total" @current-change="load" />
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, reactive, onMounted } from 'vue'
import { Search, Refresh, ArrowDown, MagicStick } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { BarChart } from 'echarts/charts'
import { GridComponent, TooltipComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import { observeApi } from '@/api'

use([CanvasRenderer, BarChart, GridComponent, TooltipComponent])

const filters  = reactive({ path: '', level: '', service: '' })
const logs     = ref([])
const stats    = ref({ total:0, errors:0, warns:0, slow:0, avg_duration_ms:0, avg_db_ms:0, level_counts:[], svc_counts:[], top_paths:[] })
const loading    = ref(false)
const classifying = ref(false)
const page       = ref(1)
const pageSize   = 20
const total    = ref(0)
const expanded = ref(new Set())
const dataSource = ref('—')

const SVC_COLORS = {'首页大盘':'#409eff','行为分析':'#67c23a','用户宽表':'#e6a23c','人群圈选':'#9b59b6',
  '银行报表':'#f56c6c','日志观测':'#1abc9c','链路追踪':'#3498db','指标平台':'#e67e22',
  '标签分析':'#c0392b','经营管理':'#2ecc71','系统配置':'#95a5a6','CDP后台':'#7f8c8d'}
const svcColor = s => SVC_COLORS[s] || '#909399'
const durColor = ms => ms>1000?{color:'#f56c6c',fontWeight:'700'}:ms>300?{color:'#e6a23c'}:{color:'#67c23a'}
const rowCls = log => log.level==='ERROR'?'row-error':log.level==='WARN'?'row-warn':''

function toggleFilter(key, val) { filters[key] = filters[key]===val?'':val; page.value=1; load() }
function toggleExpand(idx) { const s=new Set(expanded.value); s.has(idx)?s.delete(idx):s.add(idx); expanded.value=s }

const statCards = computed(() => [
  {label:'总请求', value:(stats.value.total||0).toLocaleString(), color:'#303133'},
  {label:'ERROR', value:stats.value.errors||0, color:'#f56c6c'},
  {label:'WARN',  value:stats.value.warns||0,  color:'#e6a23c'},
  {label:'慢请求', value:stats.value.slow||0,   color:'#e6a23c'},
  {label:'均耗时', value:(stats.value.avg_duration_ms||0)+'ms', color:'#409eff'},
  {label:'均DB耗时', value:(stats.value.avg_db_ms||0)+'ms', color:'#67c23a'},
])

const chartOpt = computed(() => {
  const paths = (stats.value.top_paths||[]).slice(0,8)
  return {
    tooltip:{trigger:'axis'},
    grid:{left:8,right:8,top:4,bottom:20},
    xAxis:{type:'category',data:paths.map(p=>p.path.replace('/api/','')),axisLabel:{fontSize:10,rotate:15}},
    yAxis:{type:'value',show:false},
    series:[{type:'bar',barMaxWidth:36,
      data:paths.map((p,i)=>({value:p.count,itemStyle:{color:['#409eff','#67c23a','#e6a23c','#f56c6c','#9b59b6','#1abc9c','#e67e22','#3498db'][i%8]}})),
      label:{show:true,position:'top',fontSize:10}}]
  }
})

async function runClassify() {
  classifying.value = true
  try {
    const res = await observeApi.classify()
    if (res.status === 'success') {
      ElMessage.success(`AI打标签完成，更新 ${res.updated} 条`)
      await load()
    } else {
      ElMessage.warning(res.message || 'AI分类失败')
    }
  } finally { classifying.value = false }
}

async function load() {
  loading.value=true; expanded.value=new Set()
  try {
    const params={page:page.value, size:pageSize}
    if (filters.path)    params.path    = filters.path
    if (filters.level)   params.level   = filters.level
    if (filters.service) params.service = filters.service
    const res = await observeApi.logs(params)
    logs.value  = res.logs  || []
    total.value = res.total || 0
    dataSource.value = res.source || '—'
  } finally { loading.value=false }
}

onMounted(async () => {
  const s = await observeApi.stats()
  stats.value = s
  dataSource.value = s.source || '—'
  await load()
})
</script>

<style scoped>
.lo-wrap{display:flex;flex-direction:column;gap:12px}
.toolbar{display:flex;align-items:center;gap:10px;padding:12px 16px;flex-wrap:wrap}
.lo-body{display:grid;grid-template-columns:200px 1fr;gap:12px;align-items:start}
.facets{background:#fff;border-radius:8px;padding:8px 0 12px;box-shadow:0 1px 4px rgba(0,0,0,.06)}
.stat-mini{text-align:center;padding:6px 12px 2px}
.sm-val{font-size:16px;font-weight:700}
.sm-lbl{font-size:10px;color:#909399;margin-top:1px}
.facet-section{padding:4px 0}
.facet-title{font-size:11px;font-weight:600;color:#909399;text-transform:uppercase;padding:4px 14px 6px}
.facet-divider{height:1px;background:#f0f0f0;margin:6px 14px}
.facet-item{display:flex;align-items:center;gap:7px;padding:5px 14px;cursor:pointer;transition:background .1s}
.facet-item:hover{background:#f5f7fa}
.facet-item.active{background:#ecf5ff}
.path-item{flex-direction:column;align-items:flex-start;gap:2px}
.facet-lbl{flex:1;font-size:12px;color:#303133}
.facet-cnt{font-size:11px;color:#909399;font-weight:600}
.svc-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.lv-badge{font-size:10px;font-weight:700;padding:1px 5px;border-radius:3px;flex-shrink:0;min-width:40px;text-align:center;font-family:monospace}
.lv-info{background:#ecf5ff;color:#409eff}
.lv-warn{background:#fdf6ec;color:#e6a23c}
.lv-error{background:#fef0f0;color:#f56c6c}
.log-main{display:flex;flex-direction:column;gap:12px}
.hist-card{padding:12px 16px}
.log-card{padding:0;overflow:hidden}
.log-row{display:flex;align-items:center;gap:9px;padding:7px 14px;font-family:'JetBrains Mono',Consolas,monospace;font-size:12px;border-bottom:1px solid #f5f5f5;cursor:pointer;transition:background .1s}
.log-row:hover{background:#f8f9fa}
.row-error{background:#fff8f8}
.row-warn{background:#fffbe6}
.log-ts{color:#909399;font-size:11px;flex-shrink:0}
.log-svc{font-size:11px;flex-shrink:0;min-width:80px;font-weight:600}
.log-msg{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#303133}
.log-dur{font-size:11px;font-weight:600;flex-shrink:0}
.log-detail{background:#1a1a2e;padding:10px 16px;border-bottom:1px solid #2d2d4e}
.ld-grid{display:grid;grid-template-columns:1fr 1fr;gap:5px}
.ld-row{display:flex;gap:8px}
.ld-k{font-size:11px;color:#6e7c9a;min-width:80px;flex-shrink:0}
.ld-v{font-size:12px;color:#a8b3cf;word-break:break-all}
.ld-v.mono{font-family:monospace;color:#7ec8e3}
</style>
