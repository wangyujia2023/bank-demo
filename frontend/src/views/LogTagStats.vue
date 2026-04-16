<template>
  <div class="ts-wrap">
    <div class="card toolbar">
      <span style="font-weight:600;font-size:14px">日志标签统计分析</span>
      <el-tag v-if="data.total>0" size="small" effect="plain">
        已标签 {{ data.tagged?.toLocaleString() }} / 共 {{ data.total?.toLocaleString() }} 条
      </el-tag>
      <el-button type="primary" :loading="loading" style="margin-left:auto" @click="load">
        <el-icon><Refresh /></el-icon> 刷新
      </el-button>
      <el-button type="warning" :loading="classifying" @click="runClassify">
        <el-icon><MagicStick /></el-icon> AI打标签
      </el-button>
    </div>

    <div v-if="!data.tag_distribution?.length" class="empty-tip">
      暂无标签数据，请先在「日志可观测性」页面点击「AI打标签」
    </div>

    <template v-else>
      <!-- 标签分布 + 耗时热力 -->
      <div class="row2">
        <div class="card chart-card">
          <div class="card-title">标签分布</div>
          <v-chart :option="pieOpt" style="height:280px" autoresize />
        </div>
        <div class="card chart-card">
          <div class="card-title">各标签平均耗时 (ms)</div>
          <v-chart :option="perfOpt" style="height:280px" autoresize />
        </div>
      </div>

      <!-- level × tag 交叉 -->
      <div class="card">
        <div class="card-title">标签 × 日志级别分布</div>
        <v-chart :option="stackOpt" style="height:240px" autoresize />
      </div>

      <!-- 服务 × tag -->
      <div class="card">
        <div class="card-title">标签 × 服务分布</div>
        <v-chart :option="svcOpt" style="height:240px" autoresize />
      </div>

      <!-- 明细表 -->
      <div class="card">
        <div class="card-title">标签明细</div>
        <el-table :data="data.tag_distribution" size="small" stripe>
          <el-table-column prop="log_tag" label="标签" width="160">
            <template #default="{row}">
              <el-tag :type="tagType(row.log_tag)" size="small">{{ row.log_tag }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="cnt" label="数量" width="100" sortable />
          <el-table-column prop="pct" label="占比%" width="100" sortable />
          <el-table-column label="耗时 avg/max">
            <template #default="{row}">
              <span>{{ perfMap[row.log_tag]?.avg_ms ?? '-' }} / {{ perfMap[row.log_tag]?.max_ms ?? '-' }} ms</span>
            </template>
          </el-table-column>
          <el-table-column label="主要级别">
            <template #default="{row}">
              <span v-for="l in levelsByTag[row.log_tag]?.slice(0,3)" :key="l.level" style="margin-right:6px">
                <span :class="`lv-badge lv-${l.level?.toLowerCase()}`">{{l.level}}</span>
                <span style="font-size:11px;color:#909399">×{{l.cnt}}</span>
              </span>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </template>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { Refresh, MagicStick } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { PieChart, BarChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import { observeApi } from '@/api'

use([CanvasRenderer, PieChart, BarChart, GridComponent, TooltipComponent, LegendComponent])

const loading    = ref(false)
const classifying = ref(false)
const data = ref({ tag_distribution: [], level_by_tag: [], svc_by_tag: [], perf_by_tag: [], tagged: 0, total: 0 })

const TAG_COLORS = {
  '慢请求': '#e6a23c', '服务异常': '#f56c6c', '正常请求': '#67c23a',
  '数据库超时': '#c0392b', '认证失败': '#9b59b6', '业务错误': '#e67e22',
  '高频访问': '#3498db', '安全告警': '#e74c3c',
}
const COLORS = ['#409eff','#67c23a','#e6a23c','#f56c6c','#9b59b6','#1abc9c','#e67e22','#3498db','#c0392b','#2ecc71']

function tagType(tag) {
  if (['服务异常','认证失败','安全告警','数据库超时'].includes(tag)) return 'danger'
  if (['慢请求','业务错误','高频访问'].includes(tag)) return 'warning'
  if (tag === '正常请求') return 'success'
  return ''
}

const perfMap = computed(() => Object.fromEntries((data.value.perf_by_tag||[]).map(r => [r.log_tag, r])))
const levelsByTag = computed(() => {
  const m = {}
  for (const r of (data.value.level_by_tag||[])) {
    if (!m[r.log_tag]) m[r.log_tag] = []
    m[r.log_tag].push(r)
  }
  return m
})

const pieOpt = computed(() => {
  const dist = data.value.tag_distribution || []
  return {
    tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
    legend: { orient: 'vertical', right: 10, top: 'middle', textStyle: { fontSize: 11 } },
    series: [{
      type: 'pie', radius: ['38%', '65%'], center: ['38%', '50%'],
      label: { show: false },
      data: dist.map((r, i) => ({
        name: r.log_tag, value: r.cnt,
        itemStyle: { color: TAG_COLORS[r.log_tag] || COLORS[i % COLORS.length] }
      }))
    }]
  }
})

const perfOpt = computed(() => {
  const perf = (data.value.perf_by_tag || []).sort((a, b) => b.avg_ms - a.avg_ms)
  return {
    tooltip: { trigger: 'axis' },
    grid: { left: 16, right: 16, top: 8, bottom: 40 },
    xAxis: { type: 'category', data: perf.map(r => r.log_tag), axisLabel: { fontSize: 10, rotate: 20 } },
    yAxis: { type: 'value', axisLabel: { fontSize: 10 } },
    series: [
      { name: '平均耗时', type: 'bar', barMaxWidth: 36,
        data: perf.map((r, i) => ({ value: r.avg_ms, itemStyle: { color: TAG_COLORS[r.log_tag] || COLORS[i % COLORS.length] } })),
        label: { show: true, position: 'top', fontSize: 10 } },
      { name: '最大耗时', type: 'bar', barMaxWidth: 36,
        data: perf.map(r => ({ value: r.max_ms, itemStyle: { color: '#ddd', opacity: 0.5 } })) },
    ]
  }
})

const stackOpt = computed(() => {
  const tags = [...new Set((data.value.level_by_tag||[]).map(r => r.log_tag))]
  const levels = ['INFO', 'WARN', 'ERROR']
  const byTagLevel = {}
  for (const r of (data.value.level_by_tag || [])) {
    byTagLevel[`${r.log_tag}|${r.level}`] = r.cnt
  }
  return {
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    legend: { data: levels, textStyle: { fontSize: 11 } },
    grid: { left: 16, right: 16, top: 30, bottom: 40 },
    xAxis: { type: 'category', data: tags, axisLabel: { fontSize: 10, rotate: 15 } },
    yAxis: { type: 'value', axisLabel: { fontSize: 10 } },
    series: levels.map(lv => ({
      name: lv, type: 'bar', stack: 'total', barMaxWidth: 40,
      itemStyle: { color: lv==='ERROR'?'#f56c6c':lv==='WARN'?'#e6a23c':'#409eff' },
      data: tags.map(t => byTagLevel[`${t}|${lv}`] || 0)
    }))
  }
})

const svcOpt = computed(() => {
  const tags = [...new Set((data.value.svc_by_tag||[]).map(r => r.log_tag))]
  const svcs = [...new Set((data.value.svc_by_tag||[]).map(r => r.service))]
  const map = {}
  for (const r of (data.value.svc_by_tag || [])) map[`${r.log_tag}|${r.service}`] = r.cnt
  return {
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    legend: { data: svcs, textStyle: { fontSize: 10 }, top: 0, type: 'scroll' },
    grid: { left: 16, right: 16, top: 36, bottom: 40 },
    xAxis: { type: 'category', data: tags, axisLabel: { fontSize: 10, rotate: 15 } },
    yAxis: { type: 'value', axisLabel: { fontSize: 10 } },
    series: svcs.map((svc, i) => ({
      name: svc, type: 'bar', stack: 'svc', barMaxWidth: 40,
      itemStyle: { color: COLORS[i % COLORS.length] },
      data: tags.map(t => map[`${t}|${svc}`] || 0)
    }))
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
  loading.value = true
  try { data.value = await observeApi.tagStats() }
  finally { loading.value = false }
}

onMounted(load)
</script>

<style scoped>
.ts-wrap { display: flex; flex-direction: column; gap: 14px; }
.toolbar { display: flex; align-items: center; gap: 10px; padding: 12px 16px; flex-wrap: wrap; }
.card { background: #fff; border-radius: 8px; padding: 16px; box-shadow: 0 1px 4px rgba(0,0,0,.06); }
.card-title { font-size: 13px; font-weight: 600; color: #303133; margin-bottom: 12px; }
.row2 { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
.chart-card { }
.empty-tip { text-align: center; padding: 60px; color: #c0c4cc; font-size: 14px; background: #fff; border-radius: 8px; }
.lv-badge { font-size: 10px; font-weight: 700; padding: 1px 5px; border-radius: 3px; font-family: monospace; }
.lv-info  { background: #ecf5ff; color: #409eff; }
.lv-warn  { background: #fdf6ec; color: #e6a23c; }
.lv-error { background: #fef0f0; color: #f56c6c; }
</style>
