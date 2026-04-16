<template>
  <div>
    <!-- 初始化提示 -->
    <div v-if="!ready" class="init-banner">
      <span>高频遥测数据未就绪（约14,700条5分钟粒度记录）</span>
      <el-button type="primary" size="small" :loading="initing" @click="doInit" style="margin-left:12px">
        初始化高频数据
      </el-button>
    </div>

    <!-- 指标快照条 -->
    <div v-if="ready" class="metric-bar" v-loading="ovLoading">
      <div v-for="m in overview" :key="m.col" class="metric-chip"
           :class="metricAlertClass(m)">
        <div class="metric-chip-label">{{ m.label }}</div>
        <div class="metric-chip-value">{{ m.mean_v }}<span class="metric-unit">{{ m.unit }}</span></div>
        <div class="metric-chip-sub">σ={{ m.std_v }}  P50={{ m.p50 }}</div>
      </div>
    </div>

    <el-tabs v-model="tab" type="border-card" style="margin-top:12px">

      <!-- ═══ 健康评分 ═══ -->
      <el-tab-pane label="健康评分看板" name="health">
        <el-row :gutter="16" v-loading="healthLoading">
          <el-col :span="10">
            <div class="card">
              <div class="card-title">全星健康排名（过去24h）</div>
              <v-chart :option="healthBarOpt" style="height:420px" autoresize />
            </div>
          </el-col>
          <el-col :span="14">
            <div class="card" style="max-height:460px;overflow-y:auto">
              <div class="card-title">卫星健康详情</div>
              <el-table :data="healthRows" border size="small" :default-sort="{prop:'health_score',order:'descending'}">
                <el-table-column prop="satellite_name" label="卫星" width="100" fixed />
                <el-table-column prop="satellite_type" label="类型" width="70">
                  <template #default="{row}"><el-tag :type="satTypeColor(row.satellite_type)" size="small">{{ row.satellite_type }}</el-tag></template>
                </el-table-column>
                <el-table-column label="健康分" width="100" sortable prop="health_score">
                  <template #default="{row}">
                    <el-progress :percentage="+row.health_score" :stroke-width="10"
                      :color="healthColor(row.health_score)" />
                    <span style="font-size:11px;font-weight:600">{{ row.health_score }}</span>
                  </template>
                </el-table-column>
                <el-table-column label="电量%" width="80" align="right" sortable prop="avg_battery">
                  <template #default="{row}">
                    <span :style="{color:row.avg_battery<45?'#f56c6c':row.avg_battery<65?'#e6a23c':'#67c23a',fontWeight:600}">{{ row.avg_battery }}</span>
                  </template>
                </el-table-column>
                <el-table-column label="CPU℃" width="75" align="right" sortable prop="avg_cpu_temp">
                  <template #default="{row}">
                    <span :style="{color:row.avg_cpu_temp>45?'#f56c6c':row.avg_cpu_temp>38?'#e6a23c':'inherit'}">{{ row.avg_cpu_temp }}</span>
                  </template>
                </el-table-column>
                <el-table-column label="载荷℃" width="75" align="right" prop="avg_payload_temp">
                  <template #default="{row}">
                    <span :style="{color:row.max_payload_temp>50?'#f56c6c':'inherit'}">{{ row.avg_payload_temp }}</span>
                  </template>
                </el-table-column>
                <el-table-column label="信号dB" width="80" align="right" prop="avg_signal">
                  <template #default="{row}">
                    <span :style="{color:row.avg_signal<-90?'#f56c6c':row.avg_signal<-83?'#e6a23c':'#67c23a'}">{{ row.avg_signal }}</span>
                  </template>
                </el-table-column>
                <el-table-column label="姿态°" width="72" align="right" prop="avg_attitude">
                  <template #default="{row}">
                    <span :style="{color:row.avg_attitude>0.6?'#f56c6c':row.avg_attitude>0.35?'#e6a23c':'inherit'}">{{ row.avg_attitude }}</span>
                  </template>
                </el-table-column>
                <el-table-column label="异常次" width="75" align="right" prop="anomaly_cnt">
                  <template #default="{row}">
                    <el-tag :type="row.anomaly_cnt>10?'danger':row.anomaly_cnt>3?'warning':'success'" size="small">{{ row.anomaly_cnt }}</el-tag>
                  </template>
                </el-table-column>
              </el-table>
            </div>
          </el-col>
        </el-row>
      </el-tab-pane>

      <!-- ═══ 指标趋势分析 ═══ -->
      <el-tab-pane label="指标趋势分析" name="trend">
        <div class="filter-bar" style="margin-bottom:12px">
          <span style="font-size:13px;color:#606266;white-space:nowrap">选择卫星：</span>
          <el-select v-model="trendSatId" placeholder="卫星" size="small" style="width:130px" @change="loadTrend">
            <el-option v-for="r in healthRows" :key="r.satellite_id" :value="r.satellite_id"
              :label="r.satellite_name" />
          </el-select>
          <span style="font-size:13px;color:#606266;white-space:nowrap;margin-left:8px">指标：</span>
          <el-select v-model="trendMetric" size="small" style="width:140px" @change="loadTrend">
            <el-option v-for="m in METRICS" :key="m.key" :value="m.key" :label="m.label" />
          </el-select>
          <el-radio-group v-model="trendHours" size="small" @change="loadTrend" style="margin-left:8px">
            <el-radio-button :value="12">12h</el-radio-button>
            <el-radio-button :value="24">24h</el-radio-button>
            <el-radio-button :value="48">48h</el-radio-button>
            <el-radio-button :value="72">72h</el-radio-button>
          </el-radio-group>
          <div style="flex:1" />
          <!-- 时间选择器：仅展示 -->
          <el-date-picker v-model="trendDateRange" type="datetimerange" size="small" style="width:360px"
            start-placeholder="数据起始时间" end-placeholder="数据截止时间" value-format="YYYY-MM-DD HH:mm:ss" />
          <el-tag type="info" size="small" effect="plain" style="margin-left:6px">时间仅展示</el-tag>
        </div>

        <!-- 统计摘要 -->
        <div v-if="trend.stats" class="stat-row" style="grid-template-columns:repeat(6,1fr);margin-bottom:12px">
          <div class="stat-card"><div class="stat-label">最小值</div><div class="stat-value" style="font-size:20px;color:#409eff">{{ trend.stats.min }}</div></div>
          <div class="stat-card"><div class="stat-label">最大值</div><div class="stat-value" style="font-size:20px;color:#e6a23c">{{ trend.stats.max }}</div></div>
          <div class="stat-card"><div class="stat-label">均值</div><div class="stat-value" style="font-size:20px">{{ trend.stats.avg }}</div></div>
          <div class="stat-card"><div class="stat-label">标准差σ</div><div class="stat-value" style="font-size:20px;color:#909399">{{ trend.stats.std }}</div></div>
          <div class="stat-card"><div class="stat-label">数据点数</div><div class="stat-value" style="font-size:20px">{{ trend.stats.total }}</div></div>
          <div class="stat-card"><div class="stat-label">异常点数</div><div class="stat-value" style="font-size:20px;color:#f56c6c">{{ trend.stats.anomaly_cnt }}</div></div>
        </div>

        <div class="card" v-loading="trendLoading">
          <div class="card-title">
            {{ satName(trendSatId) }} — {{ metricLabel(trendMetric) }} 时序
            <span style="font-size:12px;color:#909399;margin-left:8px">蓝线=原始值 / 橙线=1h滚动均值 / 橙色阴影=±2σ带 / 红点=异常</span>
          </div>
          <v-chart :option="trendChartOpt" style="height:320px" autoresize />
        </div>
      </el-tab-pane>

      <!-- ═══ 异常检测报告 ═══ -->
      <el-tab-pane label="异常检测" name="anomaly">
        <div class="filter-bar" style="margin-bottom:12px">
          <span style="font-size:13px;color:#606266">时间窗口：</span>
          <el-radio-group v-model="anomalyHours" size="small" @change="loadAnomaly">
            <el-radio-button :value="12">12h</el-radio-button>
            <el-radio-button :value="24">24h</el-radio-button>
            <el-radio-button :value="48">48h</el-radio-button>
            <el-radio-button :value="72">72h</el-radio-button>
          </el-radio-group>
          <div style="flex:1" />
          <!-- 时间仅展示 -->
          <el-date-picker v-model="anomalyDateRange" type="datetimerange" size="small" style="width:360px"
            start-placeholder="分析起始时间" end-placeholder="分析截止时间" value-format="YYYY-MM-DD HH:mm:ss" />
          <el-tag type="info" size="small" effect="plain" style="margin-left:6px">时间仅展示</el-tag>
        </div>

        <el-row :gutter="16" v-loading="anomalyLoading">
          <el-col :span="10">
            <div class="card">
              <div class="card-title">按指标异常计数</div>
              <v-chart :option="anomalyMetricBarOpt" style="height:220px" autoresize />
            </div>
          </el-col>
          <el-col :span="14">
            <div class="card">
              <div class="card-title">按卫星异常计数（flag）</div>
              <v-chart :option="anomalySatBarOpt" style="height:220px" autoresize />
            </div>
          </el-col>
        </el-row>

        <el-row :gutter="16">
          <el-col :span="24">
            <div class="card">
              <div class="card-title">异常分布热力（每小时slot）</div>
              <v-chart :option="anomalyHourlyOpt" style="height:120px" autoresize />
            </div>
          </el-col>
        </el-row>

        <div class="card" v-loading="anomalyLoading">
          <div class="card-title">Z-score 异常点明细（>2.5σ）</div>
          <el-table :data="anomaly.detail" border stripe size="small" max-height="360">
            <el-table-column prop="satellite"  label="卫星"   width="100" />
            <el-table-column prop="sat_type"   label="类型"   width="70">
              <template #default="{row}"><el-tag :type="satTypeColor(row.sat_type)" size="small">{{ row.sat_type }}</el-tag></template>
            </el-table-column>
            <el-table-column prop="metric"     label="指标"   width="90" />
            <el-table-column prop="time"       label="时间"   width="155" />
            <el-table-column label="实测值" width="90" align="right">
              <template #default="{row}"><b style="color:#f56c6c">{{ row.value }}</b></template>
            </el-table-column>
            <el-table-column label="全局均值" width="90" align="right">
              <template #default="{row}">{{ row.mean }}</template>
            </el-table-column>
            <el-table-column label="Z-score" width="90" align="right" sortable prop="z_score">
              <template #default="{row}">
                <b :style="{color:row.z_score>4?'#f56c6c':row.z_score>3?'#e6a23c':'#909399'}">{{ row.z_score }}</b>
              </template>
            </el-table-column>
            <el-table-column label="等级" width="75" align="center">
              <template #default="{row}">
                <el-tag :type="row.severity==='严重'?'danger':row.severity==='警告'?'warning':'info'" size="small">{{ row.severity }}</el-tag>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </el-tab-pane>

      <!-- ═══ 多星指标对比 ═══ -->
      <el-tab-pane label="多星指标对比" name="compare">
        <div class="filter-bar" style="margin-bottom:12px">
          <span style="font-size:13px;color:#606266">时间窗口：</span>
          <el-radio-group v-model="compareHours" size="small" @change="loadCompare">
            <el-radio-button :value="12">12h</el-radio-button>
            <el-radio-button :value="24">24h</el-radio-button>
            <el-radio-button :value="48">48h</el-radio-button>
          </el-radio-group>
          <el-select v-model="compareMetric" size="small" style="width:140px;margin-left:12px" @change="loadCompare">
            <el-option v-for="m in METRICS" :key="m.key" :value="m.key" :label="m.label" />
          </el-select>
          <div style="flex:1" />
          <!-- 时间仅展示 -->
          <el-date-picker v-model="compareDateRange" type="daterange" size="small" style="width:230px"
            start-placeholder="起始日期" end-placeholder="截止日期" value-format="YYYY-MM-DD" />
          <el-tag type="info" size="small" effect="plain" style="margin-left:6px">时间仅展示</el-tag>
        </div>
        <el-row :gutter="16" v-loading="compareLoading">
          <el-col :span="12">
            <div class="card">
              <div class="card-title">各卫星 — {{ metricLabel(compareMetric) }} 均值对比</div>
              <v-chart :option="compareBarOpt" style="height:340px" autoresize />
            </div>
          </el-col>
          <el-col :span="12">
            <div class="card">
              <div class="card-title">多星雷达对比（归一化）</div>
              <v-chart :option="radarOpt" style="height:340px" autoresize />
            </div>
          </el-col>
        </el-row>
        <div class="card">
          <div class="card-title">全星座指标数据表</div>
          <el-table :data="compare.rows" border stripe size="small">
            <el-table-column prop="satellite_name" label="卫星" width="100" fixed />
            <el-table-column prop="satellite_type" label="类型" width="70">
              <template #default="{row}"><el-tag :type="satTypeColor(row.satellite_type)" size="small">{{ row.satellite_type }}</el-tag></template>
            </el-table-column>
            <el-table-column prop="avg_battery"      label="电量%"   width="80"  align="right" sortable />
            <el-table-column prop="std_battery"      label="电量σ"   width="75"  align="right" />
            <el-table-column prop="avg_cpu_temp"     label="CPU℃"   width="75"  align="right" sortable />
            <el-table-column prop="avg_payload_temp" label="载荷℃"  width="75"  align="right" sortable />
            <el-table-column prop="avg_signal"       label="信号dB"  width="80"  align="right" sortable />
            <el-table-column prop="std_signal"       label="信号σ"   width="75"  align="right" />
            <el-table-column prop="avg_snr"          label="SNR dB" width="80"  align="right" sortable />
            <el-table-column prop="avg_attitude"     label="姿态°"  width="75"  align="right" sortable />
            <el-table-column prop="avg_buffer"       label="存储%"  width="75"  align="right" sortable />
            <el-table-column label="异常率%" width="90" align="right" sortable prop="anomaly_cnt">
              <template #default="{row}">
                <span :style="{color: anomalyRate(row)>5?'#f56c6c':anomalyRate(row)>2?'#e6a23c':'#67c23a',fontWeight:600}">
                  {{ anomalyRate(row) }}%
                </span>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </el-tab-pane>

    </el-tabs>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted } from 'vue'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { BarChart, LineChart, RadarChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent, MarkAreaComponent, MarkPointComponent, RadarComponent, DataZoomComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import { ElMessage } from 'element-plus'
import { satelliteHfApi } from '@/api'

use([CanvasRenderer, BarChart, LineChart, RadarChart,
     GridComponent, TooltipComponent, LegendComponent,
     MarkAreaComponent, MarkPointComponent, RadarComponent, DataZoomComponent])

const METRICS = [
  { key: 'battery_pct',        label: '电池电量 %'  },
  { key: 'solar_power_w',      label: '太阳能功率 W' },
  { key: 'cpu_temp_c',         label: 'CPU温度 ℃'  },
  { key: 'thermal_payload_c',  label: '载荷温度 ℃'  },
  { key: 'signal_strength_db', label: '信号强度 dB' },
  { key: 'link_snr_db',        label: '信噪比 dB'   },
  { key: 'orbit_altitude_km',  label: '轨道高度 km' },
  { key: 'attitude_error_deg', label: '姿态误差 °'  },
  { key: 'data_buffer_pct',    label: '存储占用 %'  },
]

const tab         = ref('health')
const ready       = ref(true)
const initing     = ref(false)
const ovLoading   = ref(false)
const healthLoading  = ref(false)
const trendLoading   = ref(false)
const anomalyLoading = ref(false)
const compareLoading = ref(false)

const overview   = ref([])
const healthRows = ref([])
const trend      = ref({ times: [], values: [], rolling: [], upper: [], lower: [], anomalies: [], stats: null })
const anomaly    = ref({ summary_by_metric: [], detail: [], by_sat: [], hourly: [] })
const compare    = ref({ rows: [], hours: 24 })

// 筛选条件（时间仅展示）
const trendSatId     = ref(null)
const trendMetric    = ref('battery_pct')
const trendHours     = ref(24)
const trendDateRange = ref(null)

const anomalyHours     = ref(48)
const anomalyDateRange = ref(null)

const compareHours     = ref(24)
const compareMetric    = ref('battery_pct')
const compareDateRange = ref(null)

const satTypeColor = t => ({'遥感':'','导航':'success','通信':'warning','气象':'info','预警':'danger','侦察':'danger','中继':'','科学':'success'}[t]||'info')
const healthColor  = s => s >= 80 ? '#67c23a' : s >= 60 ? '#e6a23c' : '#f56c6c'
const metricLabel  = k => METRICS.find(m => m.key === k)?.label || k
const satName      = id => healthRows.value.find(r => r.satellite_id === id)?.satellite_name || id
const anomalyRate  = row => row.total_cnt ? Math.round(row.anomaly_cnt / row.total_cnt * 100 * 10) / 10 : 0

function metricAlertClass(m) {
  if (m.col === 'cpu_temp_c'    && m.mean_v > 38)  return 'chip-warn'
  if (m.col === 'battery_pct'   && m.mean_v < 60)  return 'chip-warn'
  if (m.col === 'signal_strength_db' && m.mean_v < -85) return 'chip-alert'
  return ''
}

// ── 初始化 ─────────────────────────────────────────────────────────
async function doInit() {
  initing.value = true
  try {
    const r = await satelliteHfApi.init()
    ElMessage.success(`高频数据初始化完成：${r.total || 0} 条记录`)
    ready.value = true
    loadAll()
  } catch { /* interceptor shows error */ }
  finally { initing.value = false }
}

async function checkReady() {
  try {
    const r = await satelliteHfApi.health()
    if (r && r.length > 0) {
      ready.value = true
      healthRows.value = r
      healthLoading.value = false
    } else {
      ready.value = false
      healthLoading.value = false
    }
  } catch { ready.value = false; healthLoading.value = false }
}

// ── 加载 ──────────────────────────────────────────────────────────
async function loadOverview() {
  ovLoading.value = true
  overview.value = await satelliteHfApi.overview().finally(() => ovLoading.value = false)
}

async function loadHealth() {
  healthLoading.value = true
  healthRows.value = await satelliteHfApi.health().finally(() => healthLoading.value = false)
  if (healthRows.value.length && !trendSatId.value) {
    trendSatId.value = healthRows.value[0].satellite_id
  }
}

async function loadTrend() {
  if (!trendSatId.value) return
  trendLoading.value = true
  trend.value = await satelliteHfApi.trend(trendSatId.value, trendMetric.value, trendHours.value)
    .finally(() => trendLoading.value = false)
}

async function loadAnomaly() {
  anomalyLoading.value = true
  anomaly.value = await satelliteHfApi.anomaly(anomalyHours.value).finally(() => anomalyLoading.value = false)
}

async function loadCompare() {
  compareLoading.value = true
  compare.value = await satelliteHfApi.compare(compareHours.value).finally(() => compareLoading.value = false)
}

let loaded = {}
watch(tab, v => {
  if (v === 'trend'   && !loaded.trend)   { loaded.trend = true;   loadTrend() }
  if (v === 'anomaly' && !loaded.anomaly) { loaded.anomaly = true;  loadAnomaly() }
  if (v === 'compare' && !loaded.compare) { loaded.compare = true;  loadCompare() }
})

watch(trendSatId, () => { if (tab.value === 'trend') loadTrend() })

async function loadAll() {
  loaded = {}
  await Promise.all([loadOverview(), loadHealth()])
}

onMounted(async () => {
  healthLoading.value = true
  ovLoading.value = true
  await Promise.all([checkReady(), loadOverview()])
})

// ── ECharts options ───────────────────────────────────────────────
const COLORS = ['#409eff','#67c23a','#e6a23c','#f56c6c','#9b59b6','#1abc9c','#e74c3c','#3498db',
                '#2ecc71','#f39c12','#d35400','#c0392b','#8e44ad','#2980b9','#27ae60','#e67e22','#16a085']

const healthBarOpt = computed(() => ({
  tooltip: { trigger: 'axis', formatter: p => `${p[0].name}<br/>健康分：<b>${p[0].value}</b>` },
  grid: { left: 90, right: 60, top: 10, bottom: 10 },
  xAxis: { type: 'value', max: 100, axisLabel: { fontSize: 10 } },
  yAxis: { type: 'category', data: [...healthRows.value].reverse().map(r => r.satellite_name), axisLabel: { fontSize: 11 } },
  series: [{
    type: 'bar',
    data: [...healthRows.value].reverse().map(r => ({
      value: r.health_score,
      itemStyle: { color: r.health_score >= 80 ? '#67c23a' : r.health_score >= 60 ? '#e6a23c' : '#f56c6c' }
    })),
    label: { show: true, position: 'right', fontSize: 11, formatter: p => p.value }
  }]
}))

const trendChartOpt = computed(() => {
  const t = trend.value
  if (!t.times?.length) return {}
  const anomalyItems = t.anomalies.map(a => ({
    coord: [a.t, a.v],
    value: a.v,
    symbol: 'circle',
    symbolSize: 7,
    itemStyle: { color: '#f56c6c' },
    label: { show: false }
  }))
  return {
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    legend: { data: ['原始值', '1h滚动均值'], bottom: 0, textStyle: { fontSize: 11 } },
    grid: { left: 60, right: 20, top: 20, bottom: 40 },
    dataZoom: [{ type: 'inside' }, { type: 'slider', height: 18, bottom: 22 }],
    xAxis: { type: 'category', data: t.times.map(s => String(s).slice(5, 16)), axisLabel: { fontSize: 9, interval: Math.floor(t.times.length / 20) } },
    yAxis: { type: 'value', scale: true },
    series: [
      // ±2σ 阴影带
      { name: 'upper', type: 'line', data: t.upper, lineStyle: { opacity: 0 }, symbol: 'none',
        areaStyle: { color: 'rgba(230,162,60,0.12)' }, stack: 'band', z: 0 },
      { name: 'lower', type: 'line', data: t.lower.map((v, i) => t.upper[i] - v),
        lineStyle: { opacity: 0 }, symbol: 'none', areaStyle: { color: 'rgba(230,162,60,0)' }, stack: 'band', z: 0 },
      // 原始值
      { name: '原始值', type: 'line', data: t.values, symbol: 'none',
        lineStyle: { color: '#409eff', width: 1, opacity: 0.7 },
        itemStyle: { color: '#409eff' },
        markPoint: { data: anomalyItems, symbol: 'circle', symbolSize: 7 }
      },
      // 滚动均值
      { name: '1h滚动均值', type: 'line', data: t.rolling, symbol: 'none',
        lineStyle: { color: '#e6a23c', width: 2.5 }, itemStyle: { color: '#e6a23c' }
      },
    ]
  }
})

const anomalyMetricBarOpt = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: 80, right: 20, top: 10, bottom: 20 },
  xAxis: { type: 'value' },
  yAxis: { type: 'category', data: anomaly.value.summary_by_metric.map(r => r.metric), axisLabel: { fontSize: 11 } },
  series: [{
    type: 'bar',
    data: anomaly.value.summary_by_metric.map((r, i) => ({ value: r.count, itemStyle: { color: COLORS[i % COLORS.length] } })),
    label: { show: true, position: 'right', fontSize: 11 }
  }]
}))

const anomalySatBarOpt = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: 90, right: 40, top: 10, bottom: 20 },
  xAxis: { type: 'value' },
  yAxis: { type: 'category', data: anomaly.value.by_sat.map(r => r.satellite_name), axisLabel: { fontSize: 11 } },
  series: [{
    type: 'bar',
    data: anomaly.value.by_sat.map(r => ({
      value: r.cnt,
      itemStyle: { color: r.cnt > 50 ? '#f56c6c' : r.cnt > 20 ? '#e6a23c' : '#67c23a' }
    })),
    label: { show: true, position: 'right', fontSize: 11 }
  }]
}))

const anomalyHourlyOpt = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: 50, right: 20, top: 8, bottom: 24 },
  xAxis: { type: 'category', data: anomaly.value.hourly.map(r => r.slot), axisLabel: { fontSize: 9, interval: Math.floor((anomaly.value.hourly.length || 1) / 12) } },
  yAxis: { type: 'value', axisLabel: { fontSize: 10 } },
  series: [{
    type: 'bar',
    data: anomaly.value.hourly.map(r => ({
      value: r.cnt,
      itemStyle: { color: r.cnt > 8 ? '#f56c6c' : r.cnt > 3 ? '#e6a23c' : '#409eff' }
    })),
  }]
}))

const compareBarOpt = computed(() => {
  const rows = compare.value.rows || []
  const metKey = compareMetric.value
  const vals = rows.map(r => parseFloat(r[`avg_${metKey.replace('_pct','').replace('_w','').replace('_c','').replace('_db','').replace('_deg','').replace('km','').replace('_strength','')}`] || r[`avg_${metKey.split('_').slice(0,-1).join('_')}`] || r[`avg_${metKey}`] || 0))
  // Simpler: just read the right key
  const key = 'avg_' + {
    battery_pct: 'battery', solar_power_w: 'solar', cpu_temp_c: 'cpu_temp',
    thermal_payload_c: 'payload_temp', signal_strength_db: 'signal',
    link_snr_db: 'snr', orbit_altitude_km: 'altitude', attitude_error_deg: 'attitude',
    data_buffer_pct: 'buffer',
  }[metKey] || metKey
  return {
    tooltip: { trigger: 'axis' },
    grid: { left: 90, right: 40, top: 10, bottom: 10 },
    xAxis: { type: 'value', scale: true },
    yAxis: { type: 'category', data: rows.map(r => r.satellite_name), axisLabel: { fontSize: 11 } },
    series: [{
      type: 'bar',
      data: rows.map((r, i) => ({
        value: r[key] ?? r[`avg_${metKey}`] ?? 0,
        itemStyle: { color: COLORS[i % COLORS.length] }
      })),
      label: { show: true, position: 'right', fontSize: 11 }
    }]
  }
})

const radarOpt = computed(() => {
  const rows = compare.value.rows || []
  if (!rows.length) return {}
  // 5个归一化指标
  const indicators = [
    { name: '电量',   max: 100 },
    { name: '信号',   max: 100 },  // 归一化到0-100
    { name: '温度',   max: 100 },  // 反向：越低越好，归一化
    { name: 'SNR',    max: 100 },
    { name: '姿态稳', max: 100 },  // 反向
  ]
  const seriesData = rows.slice(0, 8).map((r, i) => ({
    name: r.satellite_name,
    value: [
      r.avg_battery ?? 0,
      Math.max(0, 100 + (r.avg_signal ?? -100)),   // signal: -100~-60 → 0~40 → ×2.5
      Math.max(0, 100 - (r.avg_cpu_temp ?? 50) * 1.5),
      Math.min(100, (r.avg_snr ?? 0) * 3.3),
      Math.max(0, 100 - (r.avg_attitude ?? 1) * 100),
    ],
    lineStyle: { color: COLORS[i] },
    itemStyle: { color: COLORS[i] },
    areaStyle: { opacity: 0.05 },
  }))
  return {
    tooltip: { trigger: 'item' },
    legend: { bottom: 0, textStyle: { fontSize: 10 }, type: 'scroll' },
    radar: { indicator: indicators, radius: '62%', center: ['50%','48%'], splitArea: { areaStyle: { color: ['#f9f9fa','#fff'] } } },
    series: [{ type: 'radar', data: seriesData }]
  }
})
</script>

<style scoped>
.init-banner {
  background: #fff3cd; border: 1px solid #ffc107; border-radius: 6px;
  padding: 10px 16px; margin-bottom: 12px; display: flex; align-items: center;
  font-size: 13px; color: #856404;
}
.metric-bar {
  display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 4px;
}
.metric-chip {
  background: #fff; border-radius: 6px; padding: 8px 14px;
  box-shadow: 0 1px 3px rgba(0,0,0,0.07); border-left: 3px solid #409eff;
  min-width: 110px;
}
.metric-chip.chip-warn  { border-left-color: #e6a23c; background: #fdf6ec; }
.metric-chip.chip-alert { border-left-color: #f56c6c; background: #fef0f0; }
.metric-chip-label { font-size: 11px; color: #909399; }
.metric-chip-value { font-size: 18px; font-weight: 700; color: #1a1a1a; }
.metric-unit       { font-size: 11px; color: #909399; margin-left: 2px; }
.metric-chip-sub   { font-size: 10px; color: #c0c4cc; }
</style>
