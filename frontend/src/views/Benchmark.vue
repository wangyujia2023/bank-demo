<template>
  <div>

    <!-- 说明横幅 -->
    <div class="card bench-banner">
      <div>
        <div class="banner-title">
          <el-tag type="danger" size="small" effect="dark" style="margin-right:8px">HASP</el-tag>
          Doris 高并发点查压测 · 模拟 JMeter 多线程场景
        </div>
        <div class="banner-desc">
          基于 asyncio 并发协程对 <code>user_wide_point_query</code> 表执行点查，展示 Doris 在
          <b>Unique Key + 主键索引</b> 下的极低延迟与高 QPS 能力，并与传统关系型数据库进行对比估算。
        </div>
      </div>
    </div>

    <!-- 配置 + 执行 -->
    <div class="card config-card">
      <div class="config-grid">
        <div class="config-item">
          <div class="config-label">并发线程数（协程）</div>
          <el-slider v-model="config.threads" :min="1" :max="50" :step="1" show-stops :marks="{1:'1',10:'10',20:'20',50:'50'}" />
          <div class="config-val">{{ config.threads }} 线程</div>
        </div>
        <div class="config-item">
          <div class="config-label">每线程执行次数</div>
          <el-slider v-model="config.iterations" :min="5" :max="200" :step="5" :marks="{5:'5',50:'50',100:'100',200:'200'}" />
          <div class="config-val">{{ config.iterations }} 次（总 {{ config.threads * config.iterations }} 次查询）</div>
        </div>
        <div class="config-item">
          <div class="config-label">查询类型</div>
          <el-radio-group v-model="config.query_type" style="margin-top:8px">
            <el-radio-button value="point">点查（主键）</el-radio-button>
            <el-radio-button value="range">范围查询</el-radio-button>
            <el-radio-button value="aggregate">聚合查询</el-radio-button>
          </el-radio-group>
          <div class="query-sql mono">{{ SQL_HINTS[config.query_type] }}</div>
        </div>
      </div>
      <div class="config-footer">
        <el-button type="primary" size="large" :loading="running" @click="runBench">
          <el-icon><VideoPlay /></el-icon>
          {{ running ? `执行中... ${progress}%` : '开始压测' }}
        </el-button>
        <el-button v-if="result" size="large" @click="reset">重置</el-button>
        <div v-if="running" style="flex:1;margin-left:16px">
          <el-progress :percentage="progress" :stroke-width="8" status="active" />
        </div>
      </div>
    </div>

    <!-- 结果 -->
    <div v-if="result">

      <!-- 核心指标卡片 -->
      <div class="metric-grid">
        <div class="metric-card qps">
          <div class="metric-label">QPS</div>
          <div class="metric-val">{{ result.qps.toLocaleString() }}</div>
          <div class="metric-sub">查询/秒</div>
        </div>
        <div class="metric-card avg">
          <div class="metric-label">平均延迟</div>
          <div class="metric-val">{{ result.avg_ms }}<span class="metric-unit">ms</span></div>
          <div class="metric-sub">所有请求均值</div>
        </div>
        <div class="metric-card p50">
          <div class="metric-label">P50</div>
          <div class="metric-val">{{ result.p50_ms }}<span class="metric-unit">ms</span></div>
          <div class="metric-sub">中位数延迟</div>
        </div>
        <div class="metric-card p95">
          <div class="metric-label">P95</div>
          <div class="metric-val">{{ result.p95_ms }}<span class="metric-unit">ms</span></div>
          <div class="metric-sub">95 分位延迟</div>
        </div>
        <div class="metric-card p99">
          <div class="metric-label">P99</div>
          <div class="metric-val">{{ result.p99_ms }}<span class="metric-unit">ms</span></div>
          <div class="metric-sub">99 分位延迟</div>
        </div>
        <div class="metric-card total">
          <div class="metric-label">总查询</div>
          <div class="metric-val">{{ result.total_queries.toLocaleString() }}</div>
          <div class="metric-sub">耗时 {{ result.elapsed_sec }}s &nbsp; 错误 {{ result.errors }}</div>
        </div>
      </div>

      <el-row :gutter="16">
        <!-- 延迟分布直方图 -->
        <el-col :span="14">
          <div class="card">
            <div class="card-title">延迟分布直方图</div>
            <v-chart :option="histOpt" style="height:260px" autoresize />
          </div>
        </el-col>

        <!-- Doris vs 传统DB 对比 -->
        <el-col :span="10">
          <div class="card">
            <div class="card-title">Doris vs 传统关系型数据库（估算对比）</div>
            <div class="compare-table">
              <div class="ct-header">
                <div class="ct-cell"></div>
                <div class="ct-cell doris-col">Apache Doris</div>
                <div class="ct-cell trad-col">传统数据库</div>
                <div class="ct-cell">提升</div>
              </div>
              <div v-for="row in compareRows" :key="row.label" class="ct-row">
                <div class="ct-cell ct-label">{{ row.label }}</div>
                <div class="ct-cell doris-col ct-val-good">{{ row.doris }}</div>
                <div class="ct-cell trad-col ct-val-bad">{{ row.trad }}</div>
                <div class="ct-cell ct-improve">
                  <el-tag type="success" size="small" effect="dark">{{ row.improve }}</el-tag>
                </div>
              </div>
            </div>
            <div class="compare-note">
              * 传统数据库数值基于相同并发负载的行业基准估算<br>
              * Doris HASP 列存 + 主键索引 + Pipeline 引擎加持
            </div>
          </div>
        </el-col>
      </el-row>

      <!-- 线程 QPS 说明 -->
      <div class="card" style="padding:14px 20px">
        <div style="display:flex;gap:32px;align-items:center">
          <div><span style="color:#909399;font-size:13px">线程数</span><br><b style="font-size:20px;color:#409eff">{{ result.config.threads }}</b></div>
          <div><span style="color:#909399;font-size:13px">单线程 QPS</span><br><b style="font-size:20px;color:#67c23a">{{ result.thread_qps }}</b></div>
          <div><span style="color:#909399;font-size:13px">最大延迟</span><br><b style="font-size:20px;color:#e6a23c">{{ result.max_ms }}ms</b></div>
          <div style="flex:1">
            <div style="font-size:12px;color:#909399;margin-bottom:6px">延迟分布（P50 / P95 / P99）</div>
            <el-progress :percentage="100" :stroke-width="12" color="#67c23a" :show-text="false" style="margin-bottom:3px" />
            <div style="display:flex;gap:16px;font-size:12px">
              <span>P50: <b style="color:#67c23a">{{ result.p50_ms }}ms</b></span>
              <span>P95: <b style="color:#e6a23c">{{ result.p95_ms }}ms</b></span>
              <span>P99: <b style="color:#f56c6c">{{ result.p99_ms }}ms</b></span>
            </div>
          </div>
        </div>
      </div>

    </div>

    <!-- ── Doris audit_log 真实点查统计 ── -->
    <div class="card audit-card">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px">
        <span class="card-title" style="margin-bottom:0">Doris audit_log 点查实况</span>
        <el-tag size="small" effect="plain" type="info">__internal_schema.audit_log</el-tag>
        <el-select v-model="auditLimit" style="width:110px" size="small" @change="loadAudit">
          <el-option :value="100" label="近100条" /><el-option :value="200" label="近200条" /><el-option :value="300" label="近300条" />
        </el-select>
        <el-button size="small" :loading="auditLoading" @click="loadAudit"><el-icon><Refresh /></el-icon></el-button>
        <el-tag v-if="audit" size="small" effect="plain" style="margin-left:auto">
          {{ audit.first_time?.slice(0,19) }} → {{ audit.last_time?.slice(0,19) }}
        </el-tag>
      </div>

      <div v-if="!audit || audit.total === 0" style="text-align:center;padding:30px;color:#c0c4cc">
        暂无数据 — 请先执行压测产生点查记录
      </div>
      <template v-else>
        <!-- 指标卡 -->
        <div class="audit-metric-row">
          <div class="am-card" v-for="m in auditMetrics" :key="m.label" :style="{borderColor:m.color}">
            <div class="am-val" :style="{color:m.color}">{{ m.value }}</div>
            <div class="am-lbl">{{ m.label }}</div>
          </div>
        </div>

        <!-- 折线：近期每条记录延迟 -->
        <v-chart :option="auditLineOpt" style="height:160px;margin:12px 0" autoresize />

        <!-- 近20条明细 -->
        <el-table :data="audit.records" size="small" stripe style="font-size:12px">
          <el-table-column prop="time" label="时间" width="160" />
          <el-table-column prop="query_time" label="耗时(ms)" width="100" sortable>
            <template #default="{row}">
              <span :style="{color: row.query_time>100?'#f56c6c':row.query_time>30?'#e6a23c':'#67c23a',fontWeight:'600'}">
                {{ row.query_time }}
              </span>
            </template>
          </el-table-column>
          <el-table-column prop="state" label="状态" width="80">
            <template #default="{row}">
              <el-tag :type="row.state==='EOF'?'success':'danger'" size="small">{{ row.state }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="client_ip" label="来源IP" />
        </el-table>
      </template>
    </div>

  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { VideoPlay, Refresh } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { BarChart, LineChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, MarkLineComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import { benchmarkApi } from '@/api'

use([CanvasRenderer, BarChart, LineChart, GridComponent, TooltipComponent, MarkLineComponent])

const SQL_HINTS = {
  point:     'SELECT user_id, user_name, asset_level, aum_total, log_tags FROM user_wide_point_query WHERE user_id = ?',
  range:     'SELECT user_id, user_name, asset_level FROM user_wide WHERE user_id BETWEEN ? AND ?+9',
  aggregate: 'SELECT asset_level, COUNT(*), AVG(aum_total) FROM user_wide WHERE user_id <= ? GROUP BY asset_level',
}

const config  = ref({ threads: 10, iterations: 20, query_type: 'point' })
const running = ref(false)
const result  = ref(null)
const progress = ref(0)

const audit       = ref(null)
const auditLoading = ref(false)
const auditLimit  = ref(300)

async function loadAudit() {
  auditLoading.value = true
  try { audit.value = await benchmarkApi.auditStats(auditLimit.value) }
  catch { audit.value = null }
  finally { auditLoading.value = false }
}

const auditMetrics = computed(() => {
  if (!audit.value) return []
  const a = audit.value
  return [
    { label: '采样总量',   value: a.total,                      color: '#409eff' },
    { label: 'QPS',       value: a.qps,                        color: '#67c23a' },
    { label: '平均延迟',   value: (a.avg_ms ?? 0) + ' ms',     color: '#1abc9c' },
    { label: 'P50',       value: (a.p50_ms ?? 0) + ' ms',     color: '#3498db' },
    { label: 'P95',       value: (a.p95_ms ?? 0) + ' ms',     color: '#e6a23c' },
    { label: 'P99',       value: (a.p99_ms ?? 0) + ' ms',     color: '#f56c6c' },
    { label: '最大延迟',   value: (a.max_ms ?? 0) + ' ms',     color: '#c0392b' },
    { label: '最小延迟',   value: (a.min_ms ?? 0) + ' ms',     color: '#2ecc71' },
    { label: '错误数',     value: a.error_cnt ?? 0,             color: '#e74c3c' },
  ]
})

const auditLineOpt = computed(() => {
  const recs = (audit.value?.records || []).slice().reverse()
  return {
    tooltip: { trigger: 'axis', formatter: p => `${p[0].name}<br/>耗时: <b>${p[0].value} ms</b>` },
    grid: { left: 48, right: 12, top: 10, bottom: 24 },
    xAxis: { type: 'category', data: recs.map(r => r.time?.slice(11,19) || ''), axisLabel: { fontSize: 10 } },
    yAxis: { type: 'value', name: 'ms', nameTextStyle: { fontSize: 10 } },
    series: [{
      type: 'line', smooth: true, symbol: 'none',
      lineStyle: { color: '#409eff', width: 2 },
      areaStyle: { color: 'rgba(64,158,255,0.1)' },
      data: recs.map(r => r.query_time),
      markLine: {
        silent: true,
        data: [{ type: 'average', name: '均值', lineStyle: { color: '#e6a23c', type: 'dashed' } }],
        label: { formatter: 'avg: {c}ms', fontSize: 10 }
      }
    }]
  }
})

onMounted(loadAudit)

function reset() { result.value = null; progress.value = 0 }

async function runBench() {
  running.value = true
  result.value  = null
  progress.value = 0

  // 模拟进度动画
  const total = config.value.threads * config.value.iterations
  const interval = setInterval(() => {
    if (progress.value < 90) progress.value += Math.floor(Math.random() * 8 + 2)
  }, Math.max(200, (total * 3) / 50))

  try {
    const res = await benchmarkApi.run({ ...config.value })
    clearInterval(interval)
    progress.value = 100
    result.value = res
    ElMessage.success(`压测完成：${res.total_queries} 次查询，QPS ${res.qps}`)
  } catch {
    clearInterval(interval)
  } finally {
    running.value = false
  }
}

const histOpt = computed(() => {
  if (!result.value?.histogram) return {}
  const h = result.value.histogram
  return {
    tooltip: { trigger: 'axis', formatter: p => `${p[0].name}<br/>请求数: <b>${p[0].value}</b>` },
    grid: { left: 50, right: 10, top: 10, bottom: 30 },
    xAxis: { type: 'category', data: h.map(r => r.label), axisLabel: { fontSize: 10, rotate: 30 } },
    yAxis: { type: 'value', name: '请求数' },
    series: [{
      type: 'bar',
      data: h.map((r, i) => ({
        value: r.count,
        itemStyle: { color: i < h.length * 0.5 ? '#67c23a' : i < h.length * 0.85 ? '#e6a23c' : '#f56c6c' }
      })),
      label: { show: true, position: 'top', fontSize: 10 }
    }]
  }
})

const compareRows = computed(() => {
  if (!result.value) return []
  const d = result.value.comparison
  const improve = (dorisV, tradV, higherBetter) => {
    if (!tradV || !dorisV) return '-'
    const ratio = higherBetter ? (dorisV / tradV) : (tradV / dorisV)
    return `${ratio.toFixed(1)}x`
  }
  return [
    { label: 'QPS',       doris: d.doris.qps,    trad: d.traditional.qps,    improve: improve(d.doris.qps, d.traditional.qps, true) },
    { label: '平均延迟(ms)', doris: d.doris.avg_ms, trad: d.traditional.avg_ms, improve: improve(d.doris.avg_ms, d.traditional.avg_ms, false) },
    { label: 'P99延迟(ms)', doris: d.doris.p99_ms, trad: d.traditional.p99_ms, improve: improve(d.doris.p99_ms, d.traditional.p99_ms, false) },
  ]
})
</script>

<style scoped>
.bench-banner {
  display: flex; align-items: center; gap: 16px;
  background: linear-gradient(135deg, #fff0f0 0%, #fff8f0 100%);
  border-left: 4px solid #f56c6c;
}
.banner-title { font-size: 15px; font-weight: 600; color: #1a1a1a; margin-bottom: 6px; }
.banner-desc { font-size: 13px; color: #606266; line-height: 1.6; }
.banner-desc code { background: #f0f0f0; padding: 1px 5px; border-radius: 3px; font-size: 12px; }

.config-card { padding: 20px; }
.config-grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 24px; margin-bottom: 20px; }
.config-label { font-size: 13px; font-weight: 600; color: #303133; margin-bottom: 8px; }
.config-val { font-size: 12px; color: #909399; margin-top: 6px; }
.query-sql { font-size: 11px; color: #67c23a; background: #f0f9eb; padding: 6px 10px; border-radius: 4px; margin-top: 8px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.mono { font-family: 'JetBrains Mono', Consolas, monospace; }
.config-footer { display: flex; align-items: center; gap: 12px; padding-top: 16px; border-top: 1px solid #f0f0f0; }

.metric-grid { display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px; margin-bottom: 16px; }
.metric-card {
  background: #fff; border-radius: 8px; padding: 16px;
  box-shadow: 0 1px 4px rgba(0,0,0,0.06); text-align: center;
  border-top: 3px solid #409eff;
}
.metric-card.qps   { border-color: #409eff; }
.metric-card.avg   { border-color: #67c23a; }
.metric-card.p50   { border-color: #1abc9c; }
.metric-card.p95   { border-color: #e6a23c; }
.metric-card.p99   { border-color: #f56c6c; }
.metric-card.total { border-color: #9b59b6; }
.metric-label { font-size: 12px; color: #909399; margin-bottom: 6px; }
.metric-val { font-size: 26px; font-weight: 700; color: #1a1a1a; }
.metric-unit { font-size: 13px; font-weight: 400; color: #909399; }
.metric-sub { font-size: 11px; color: #c0c4cc; margin-top: 4px; }

.compare-table { margin: 12px 0; }
.ct-header, .ct-row { display: grid; grid-template-columns: 100px 1fr 1fr 80px; gap: 8px; padding: 7px 4px; border-bottom: 1px solid #f0f0f0; align-items: center; }
.ct-header { font-size: 12px; font-weight: 600; color: #909399; background: #fafafa; border-radius: 4px; }
.ct-cell { font-size: 13px; text-align: center; }
.ct-label { text-align: left; font-weight: 600; color: #303133; }
.doris-col { color: #409eff; font-weight: 700; }
.trad-col  { color: #909399; }
.ct-val-good { font-size: 15px; }
.ct-val-bad  { font-size: 13px; }
.ct-improve  { text-align: center; }
.compare-note { font-size: 11px; color: #c0c4cc; line-height: 1.6; padding-top: 8px; border-top: 1px solid #f0f0f0; }

.audit-card { margin-top: 16px; }
.card-title { font-size: 13px; font-weight: 600; color: #303133; }
.audit-metric-row { display: flex; gap: 10px; flex-wrap: wrap; }
.am-card { background: #fafafa; border-radius: 8px; padding: 10px 16px; text-align: center; border-top: 3px solid #409eff; min-width: 90px; flex: 1; }
.am-val { font-size: 20px; font-weight: 700; }
.am-lbl { font-size: 11px; color: #909399; margin-top: 3px; }
</style>
