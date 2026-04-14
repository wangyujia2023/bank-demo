<template>
  <div class="mp-wrap">

    <!-- 左：维度 & 指标目录 -->
    <div class="card catalog">
      <div class="catalog-section">
        <div class="catalog-title">维度 <el-tag size="small" type="info">{{ defs.dimensions.length }}</el-tag></div>
        <div
          v-for="d in defs.dimensions" :key="d.field"
          class="catalog-item"
          :class="{ active: selectedDims.includes(d.field) }"
          @click="toggleDim(d.field)"
        >
          <span class="citem-icon">{{ d.icon }}</span>
          <span class="citem-label">{{ d.label }}</span>
          <el-icon v-if="selectedDims.includes(d.field)" color="#409eff" size="14"><Check /></el-icon>
        </div>
      </div>
      <div class="catalog-divider" />
      <div class="catalog-section">
        <div class="catalog-title">指标 <el-tag size="small" type="success">{{ defs.measures.length }}</el-tag></div>
        <div
          v-for="m in defs.measures" :key="m.alias"
          class="catalog-item"
          :class="{ active: selectedMeasures.includes(m.alias) }"
          @click="toggleMeasure(m.alias)"
        >
          <span class="citem-icon">📊</span>
          <span class="citem-label">{{ m.label }}</span>
          <el-tag v-if="m.fmt==='money'" size="small" effect="plain" type="warning" style="margin-left:auto;font-size:10px">¥</el-tag>
          <el-tag v-else-if="m.fmt==='pct'" size="small" effect="plain" type="info" style="margin-left:auto;font-size:10px">%</el-tag>
          <el-icon v-if="selectedMeasures.includes(m.alias)" color="#67c23a" size="14"><Check /></el-icon>
        </div>
      </div>
    </div>

    <!-- 右：查询区 + 结果 -->
    <div class="right-col">

      <!-- 已选择的维度 & 指标 + 执行按钮 -->
      <div class="card query-bar">
        <div class="qb-row">
          <span class="qb-label">维度</span>
          <div class="chip-area">
            <el-tag
              v-for="f in selectedDims" :key="f"
              closable type="primary" effect="plain" size="small" style="margin:2px"
              @close="toggleDim(f)"
            >{{ dimLabel(f) }}</el-tag>
            <span v-if="!selectedDims.length" class="placeholder">点击左侧添加维度...</span>
          </div>
        </div>
        <div class="qb-row">
          <span class="qb-label">指标</span>
          <div class="chip-area">
            <el-tag
              v-for="a in selectedMeasures" :key="a"
              closable type="success" effect="plain" size="small" style="margin:2px"
              @close="toggleMeasure(a)"
            >{{ measureLabel(a) }}</el-tag>
            <span v-if="!selectedMeasures.length" class="placeholder">点击左侧添加指标...</span>
          </div>
        </div>
        <div class="qb-actions">
          <el-button type="primary" :loading="querying" :disabled="!canQuery" @click="doQuery">
            <el-icon><Search /></el-icon> 执行查询
          </el-button>
          <el-button @click="clearAll">清空</el-button>
          <div class="row-limit">
            返回行数：
            <el-select v-model="limit" style="width:80px" size="small">
              <el-option v-for="n in [20,50,100,200]" :key="n" :value="n" :label="n" />
            </el-select>
          </div>
        </div>
      </div>

      <!-- SQL 预览 -->
      <div v-if="result.sql" class="card sql-card">
        <div class="sql-toggle" @click="sqlExpanded=!sqlExpanded">
          <el-tag type="success" effect="dark" size="small">SQL</el-tag>
          <span style="margin-left:8px;font-size:12px;color:#67c23a">执行的查询语句</span>
          <span style="margin-left:auto;font-size:12px;color:#67c23a">{{ sqlExpanded?'▲ 收起':'▼ 展开' }}</span>
        </div>
        <pre v-if="sqlExpanded" class="sql-code">{{ result.sql }}</pre>
      </div>

      <!-- 结果 -->
      <div v-if="result.rows.length" class="card">
        <div class="card-title" style="display:flex;justify-content:space-between;align-items:center">
          <span>查询结果 <el-tag size="small" style="margin-left:6px">{{ result.rows.length }} 行</el-tag></span>
          <el-radio-group v-model="viewMode" size="small">
            <el-radio-button value="table">表格</el-radio-button>
            <el-radio-button value="chart" :disabled="!canChart">图表</el-radio-button>
          </el-radio-group>
        </div>

        <!-- 表格视图 -->
        <el-table v-if="viewMode==='table'" :data="result.rows" border stripe size="small" max-height="420">
          <el-table-column
            v-for="col in result.columns" :key="col.field"
            :prop="col.field" :label="col.label"
            :align="col.type==='measure'?'right':'left'"
          >
            <template #default="{row}">
              <el-tag v-if="col.type==='dim'" size="small" effect="plain">{{ row[col.field] }}</el-tag>
              <span v-else-if="col.fmt==='pct'" :style="{color: +row[col.field]>30?'#f56c6c':'#409eff', fontWeight:'600'}">{{ row[col.field] }}%</span>
              <span v-else-if="col.fmt==='money'" style="font-weight:600">{{ Number(row[col.field]).toFixed(1) }}</span>
              <span v-else style="font-weight:600">{{ row[col.field] }}</span>
            </template>
          </el-table-column>
        </el-table>

        <!-- 图表视图 -->
        <v-chart v-if="viewMode==='chart'" :option="chartOpt" style="height:380px" autoresize />
      </div>

      <el-empty v-else-if="queried && !querying" description="暂无数据" :image-size="60" />
    </div>

  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { BarChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import { Check, Search } from '@element-plus/icons-vue'
import { metricsApi } from '@/api'

use([CanvasRenderer, BarChart, GridComponent, TooltipComponent, LegendComponent])

const defs = ref({ dimensions: [], measures: [] })
const selectedDims    = ref([])
const selectedMeasures = ref([])
const querying = ref(false)
const queried  = ref(false)
const limit    = ref(50)
const sqlExpanded = ref(false)
const viewMode = ref('table')

const result = ref({ columns: [], rows: [], sql: '' })

const canQuery = computed(() => selectedDims.value.length + selectedMeasures.value.length > 0)
const canChart = computed(() => selectedDims.value.length >= 1 && selectedMeasures.value.length >= 1)

const dimLabel    = f => defs.value.dimensions.find(d => d.field === f)?.label || f
const measureLabel = a => defs.value.measures.find(m => m.alias === a)?.label || a

function toggleDim(f) {
  const i = selectedDims.value.indexOf(f)
  if (i >= 0) selectedDims.value.splice(i, 1)
  else selectedDims.value.push(f)
}
function toggleMeasure(a) {
  const i = selectedMeasures.value.indexOf(a)
  if (i >= 0) selectedMeasures.value.splice(i, 1)
  else selectedMeasures.value.push(a)
}
function clearAll() {
  selectedDims.value = []
  selectedMeasures.value = []
  result.value = { columns: [], rows: [], sql: '' }
  queried.value = false
}

async function doQuery() {
  querying.value = true
  queried.value = true
  try {
    result.value = await metricsApi.query({
      dimensions: selectedDims.value,
      measures: selectedMeasures.value,
      limit: limit.value,
    })
    if (canChart.value) viewMode.value = 'chart'
  } finally { querying.value = false }
}

const COLORS = ['#409eff','#67c23a','#e6a23c','#f56c6c','#9b59b6','#1abc9c','#e74c3c','#2ecc71']

const chartOpt = computed(() => {
  if (!canChart.value || !result.value.rows.length) return {}
  const dimField = selectedDims.value[0]
  const mCols = result.value.columns.filter(c => c.type === 'measure')
  const cats = result.value.rows.map(r => String(r[dimField] ?? ''))
  return {
    tooltip: { trigger: 'axis' },
    legend: { top: 0, data: mCols.map(c => c.label) },
    grid: { left: 70, right: 20, top: 36, bottom: 30 },
    xAxis: { type: 'category', data: cats, axisLabel: { rotate: cats.length > 6 ? 30 : 0 } },
    yAxis: { type: 'value' },
    series: mCols.map((col, i) => ({
      name: col.label,
      type: 'bar',
      data: result.value.rows.map(r => r[col.field]),
      itemStyle: { color: COLORS[i % COLORS.length] },
      label: { show: result.value.rows.length <= 10, position: 'top', fontSize: 11 },
    })),
  }
})

onMounted(async () => { defs.value = await metricsApi.definitions() })
</script>

<style scoped>
.mp-wrap { display: grid; grid-template-columns: 260px 1fr; gap: 16px; align-items: start; }
.catalog { padding: 0; overflow: hidden; max-height: calc(100vh - 140px); overflow-y: auto; }
.catalog-section { padding: 12px 0; }
.catalog-title { font-size: 12px; font-weight: 600; color: #909399; padding: 0 14px 8px; text-transform: uppercase; letter-spacing: 0.5px; }
.catalog-divider { height: 1px; background: #f0f0f0; margin: 0 14px; }
.catalog-item {
  display: flex; align-items: center; gap: 8px; padding: 7px 14px;
  cursor: pointer; transition: all 0.15s; font-size: 13px; color: #303133;
}
.catalog-item:hover { background: #f5f7fa; }
.catalog-item.active { background: #ecf5ff; color: #409eff; }
.citem-icon { font-size: 14px; flex-shrink: 0; }
.citem-label { flex: 1; }

.right-col { display: flex; flex-direction: column; gap: 16px; }
.query-bar { display: flex; flex-direction: column; gap: 10px; }
.qb-row { display: flex; align-items: flex-start; gap: 10px; }
.qb-label { font-size: 13px; font-weight: 600; color: #606266; white-space: nowrap; padding-top: 3px; min-width: 36px; }
.chip-area { flex: 1; display: flex; flex-wrap: wrap; min-height: 28px; padding: 2px 0; }
.placeholder { color: #c0c4cc; font-size: 12px; padding: 4px 0; }
.qb-actions { display: flex; align-items: center; gap: 10px; padding-top: 6px; border-top: 1px solid #f0f0f0; }
.row-limit { margin-left: auto; font-size: 13px; color: #606266; display: flex; align-items: center; gap: 6px; }

.sql-card { padding: 12px 16px; }
.sql-toggle { display: flex; align-items: center; cursor: pointer; }
.sql-toggle:hover { opacity: 0.8; }
.sql-code { background: #1e1e2e; color: #a8b3cf; padding: 12px 16px; border-radius: 6px; margin-top: 10px; font-size: 12px; font-family: monospace; white-space: pre-wrap; }
</style>
