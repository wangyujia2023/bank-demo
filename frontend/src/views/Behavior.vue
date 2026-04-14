<template>
  <div>
    <!-- Tab 切换 -->
    <el-tabs v-model="activeTab" type="card" style="margin-bottom:0">
      <el-tab-pane label="🔽 漏斗分析" name="funnel" />
      <el-tab-pane label="📅 留存分析" name="retention" />
      <el-tab-pane label="📊 交易趋势" name="trend" />
      <el-tab-pane label="💎 RFM 分析" name="rfm" />
    </el-tabs>

    <!-- ═══════════════════ 漏斗分析 ═══════════════════ -->
    <div v-if="activeTab === 'funnel'" class="card" style="border-top-left-radius:0">
      <el-form inline>
        <el-form-item label="渠道">
          <el-select v-model="funnelChannel" clearable placeholder="全部" style="width:100px">
            <el-option v-for="c in channels" :key="c" :label="c" :value="c" />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="funnelLoading" @click="runFunnel">分析</el-button>
        </el-form-item>
      </el-form>

      <div v-if="funnelData" style="margin-top:8px">
        <!-- 漏斗图 -->
        <v-chart :option="funnelOption" style="height:320px" autoresize />

        <!-- 步骤明细表 -->
        <el-table :data="funnelData.steps" border size="small" style="margin-top:12px">
          <el-table-column prop="step"      label="步骤" width="60" />
          <el-table-column prop="step_name" label="事件名称" />
          <el-table-column prop="user_count" label="用户数">
            <template #default="{row}">{{ (row.user_count || 0).toLocaleString() }}</template>
          </el-table-column>
          <el-table-column prop="conversion_rate" label="环节转化率">
            <template #default="{row}">
              <el-progress :percentage="row.conversion_rate || 0" :stroke-width="8" />
            </template>
          </el-table-column>
          <el-table-column prop="overall_rate" label="总转化率">
            <template #default="{row}">
              <el-tag type="primary" size="small">{{ row.overall_rate }}%</el-tag>
            </template>
          </el-table-column>
        </el-table>
        <div style="font-size:12px;color:#909399;margin-top:8px">
          使用 Doris 4.0 内置 <code>window_funnel()</code> 函数 · HASP 加速
        </div>
      </div>
    </div>

    <!-- ═══════════════════ 留存分析 ═══════════════════ -->
    <div v-if="activeTab === 'retention'" class="card" style="border-top-left-radius:0">
      <el-form inline>
        <el-form-item label="起始事件">
          <el-select v-model="retCohortEvent" style="width:120px">
            <el-option v-for="e in eventTypes" :key="e" :label="e" :value="e" />
          </el-select>
        </el-form-item>
        <el-form-item label="回访事件">
          <el-select v-model="retReturnEvent" style="width:120px">
            <el-option v-for="e in eventTypes" :key="e" :label="e" :value="e" />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="retLoading" @click="runRetention">分析</el-button>
        </el-form-item>
      </el-form>

      <div v-if="retentionData" style="margin-top:12px;overflow-x:auto">
        <table class="retention-matrix">
          <thead>
            <tr>
              <th>队列日期</th>
              <th>初始用户</th>
              <th v-for="d in retDays" :key="d">第{{ d }}天</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in retentionData.rows" :key="row.cohort_date">
              <td>{{ row.cohort_date }}</td>
              <td>{{ (row.cohort_size || 0).toLocaleString() }}</td>
              <td v-for="d in retDays" :key="d" :style="heatStyle(row[`d${d}`])">
                {{ row[`d${d}`] != null ? row[`d${d}`] + '%' : '-' }}
              </td>
            </tr>
          </tbody>
        </table>
        <div style="font-size:12px;color:#909399;margin-top:8px">
          颜色深浅代表留存率高低（绿色越深留存率越高）· Doris 4.0 HASP 窗口函数加速
        </div>
      </div>
    </div>

    <!-- ═══════════════════ 交易趋势 ═══════════════════ -->
    <div v-if="activeTab === 'trend'" class="card" style="border-top-left-radius:0">
      <el-form inline>
        <el-form-item label="渠道">
          <el-select v-model="trendChannel" clearable placeholder="全部" style="width:100px">
            <el-option v-for="c in channels" :key="c" :label="c" :value="c" />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="trendLoading" @click="runTrend">查询</el-button>
        </el-form-item>
      </el-form>
      <v-chart v-if="trendData" :option="trendOption" style="height:340px;margin-top:12px" autoresize />
    </div>

    <!-- ═══════════════════ RFM ═══════════════════ -->
    <div v-if="activeTab === 'rfm'" class="card" style="border-top-left-radius:0">
      <el-form inline>
        <el-form-item>
          <el-button type="primary" :loading="rfmLoading" @click="runRfm">分析</el-button>
        </el-form-item>
      </el-form>

      <div v-if="rfmData" style="margin-top:12px">
        <el-row :gutter="16">
          <el-col :span="12">
            <v-chart :option="rfmPieOption" style="height:300px" autoresize />
          </el-col>
          <el-col :span="12">
            <el-table :data="rfmData" border size="small">
              <el-table-column prop="rfm_segment"  label="用户分层" />
              <el-table-column prop="user_count"   label="用户数">
                <template #default="{row}">{{ (row.user_count || 0).toLocaleString() }}</template>
              </el-table-column>
              <el-table-column prop="avg_recency"  label="平均R(天)" />
              <el-table-column prop="avg_frequency" label="平均F(次)" />
              <el-table-column prop="avg_monetary"  label="平均M(元)">
                <template #default="{row}">{{ Number(row.avg_monetary || 0).toFixed(0) }}</template>
              </el-table-column>
            </el-table>
          </el-col>
        </el-row>
        <div style="font-size:12px;color:#909399;margin-top:8px">
          使用 Doris 4.0 <code>NTILE()</code> 窗口函数分位数计算
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { FunnelChart, LineChart, BarChart, PieChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import { behaviorApi } from '@/api'

use([CanvasRenderer, FunnelChart, LineChart, BarChart, PieChart, GridComponent, TooltipComponent, LegendComponent])

const activeTab = ref('funnel')
const channels  = ['APP', 'H5', '网点', '网银', '小程序']
const eventTypes = ['REGISTER', 'LOGIN', 'BROWSE_PRODUCT', 'APPLY', 'TRANSACTION']

// ── 漏斗 ──────────────────────────────────────────────────────
const funnelChannel = ref('')
const funnelLoading = ref(false)
const funnelData    = ref(null)

const funnelOption = computed(() => {
  if (!funnelData.value) return {}
  const steps = funnelData.value.steps || []
  return {
    tooltip: { trigger: 'item', formatter: '{a} <br/>{b}: {c} 人' },
    series: [{
      name: '用户转化', type: 'funnel',
      left: '10%', width: '80%',
      min: 0, max: steps[0]?.user_count || 1,
      sort: 'none',
      data: steps.map(s => ({ name: s.step_name, value: s.user_count })),
      label: { formatter: '{b}\n{c} 人' },
    }]
  }
})

async function runFunnel() {
  funnelLoading.value = true
  try {
    funnelData.value = await behaviorApi.funnel({
      channel: funnelChannel.value || undefined,
    })
  } finally { funnelLoading.value = false }
}

// ── 留存 ──────────────────────────────────────────────────────
const retCohortEvent  = ref('REGISTER')
const retReturnEvent  = ref('LOGIN')
const retLoading      = ref(false)
const retentionData   = ref(null)
const retDays         = [1, 3, 7, 14, 30, 60, 90]

const heatStyle = (v) => {
  if (v == null) return { background: '#f5f7fa', color: '#c0c4cc' }
  const pct = Math.min(v, 100) / 100
  const g = Math.round(70 + pct * 120)
  return { background: `rgba(103,194,58,${0.15 + pct * 0.65})`, color: '#1a1a1a', fontWeight: '500' }
}

async function runRetention() {
  retLoading.value = true
  try {
    retentionData.value = await behaviorApi.retention({
      cohort_event:  retCohortEvent.value,
      return_event:  retReturnEvent.value,
      retention_days: retDays,
    })
  } finally { retLoading.value = false }
}

// ── 趋势 ──────────────────────────────────────────────────────
const trendChannel = ref('')
const trendLoading = ref(false)
const trendData    = ref(null)

const trendOption = computed(() => {
  if (!trendData.value) return {}
  const rows = trendData.value.rows || []
  return {
    tooltip: { trigger: 'axis' },
    legend: { data: ['DAU', '交易笔数', '7日均线'], top: 0 },
    grid: { left: 50, right: 20, top: 36, bottom: 30 },
    xAxis: { type: 'category', data: rows.map(r => r.event_date || r.date) },
    yAxis: [
      { type: 'value', name: '用户数' },
      { type: 'value', name: '交易额', position: 'right' }
    ],
    series: [
      { name: 'DAU', type: 'bar', data: rows.map(r => r.dau), itemStyle: { color: '#409eff' } },
      { name: '交易笔数', type: 'line', yAxisIndex: 1, data: rows.map(r => r.tx_count), smooth: true, lineStyle: { color: '#67c23a' }, itemStyle: { color: '#67c23a' } },
      { name: '7日均线', type: 'line', data: rows.map(r => r.dau_7d_avg ? Number(r.dau_7d_avg).toFixed(0) : null), smooth: true, lineStyle: { color: '#e6a23c', type: 'dashed' }, itemStyle: { color: '#e6a23c' } },
    ]
  }
})

async function runTrend() {
  trendLoading.value = true
  try {
    trendData.value = await behaviorApi.transaction({
      channel: trendChannel.value || undefined,
    })
  } finally { trendLoading.value = false }
}

// ── RFM ──────────────────────────────────────────────────────
const rfmLoading = ref(false)
const rfmData    = ref(null)
const rfmColors  = ['#409eff','#67c23a','#e6a23c','#f56c6c','#909399','#c71585','#20b2aa','#ff7f50']

const rfmPieOption = computed(() => ({
  tooltip: { trigger: 'item' },
  legend: { bottom: 0 },
  series: [{
    type: 'pie', radius: ['35%', '60%'],
    data: (rfmData.value || []).map((r, i) => ({
      name: r.rfm_segment, value: r.user_count,
      itemStyle: { color: rfmColors[i % rfmColors.length] }
    })),
    label: { formatter: '{b}\n{d}%' }
  }]
}))

async function runRfm() {
  rfmLoading.value = true
  try {
    rfmData.value = await behaviorApi.rfm()
  } finally { rfmLoading.value = false }
}

onMounted(() => { runFunnel() })
</script>

<style scoped>
.retention-matrix { border-collapse: collapse; font-size: 12px; width: 100%; }
.retention-matrix th, .retention-matrix td {
  border: 1px solid #ebeef5; padding: 6px 10px; text-align: center; white-space: nowrap;
}
.retention-matrix th { background: #f5f7fa; font-weight: 600; }
</style>
