<template>
  <div>
    <!-- 初始化条幅 -->
    <div v-if="!initialized" class="init-banner">
      <span>数据表未初始化，点击按钮生成模拟数据</span>
      <el-button type="primary" size="small" :loading="initing" @click="doInit" style="margin-left:12px">
        初始化数据
      </el-button>
    </div>

    <el-tabs v-model="tab" type="border-card">

      <!-- ═══ 态势总览 ═══ -->
      <el-tab-pane label="态势总览" name="overview">
        <div class="stat-row" style="grid-template-columns:repeat(8,1fr)" v-loading="ovLoading">
          <div class="stat-card"><div class="stat-label">卫星总数</div><div class="stat-value" style="color:#409eff">{{ ov.stats.total }}</div><div class="stat-sub">{{ ov.stats.type_count }} 个类型</div></div>
          <div class="stat-card"><div class="stat-label">在轨运行</div><div class="stat-value" style="color:#67c23a">{{ ov.stats.active }}</div><div class="stat-sub">正常工作</div></div>
          <div class="stat-card"><div class="stat-label">卫星故障</div><div class="stat-value" style="color:#f56c6c">{{ ov.stats.fault }}</div><div class="stat-sub">需处置</div></div>
          <div class="stat-card"><div class="stat-label">今日任务</div><div class="stat-value" style="color:#e6a23c">{{ ov.stats.today_tasks }}</div><div class="stat-sub">异常 {{ ov.stats.today_errors }}</div></div>
          <div class="stat-card"><div class="stat-label">本周任务</div><div class="stat-value" style="color:#409eff">{{ ov.stats.week_tasks }}</div><div class="stat-sub">{{ ov.stats.week_data_gb }} GB</div></div>
          <div class="stat-card"><div class="stat-label">遥测异常</div><div class="stat-value" style="color:#f56c6c">{{ ov.stats.anomaly_sats }}</div><div class="stat-sub">过去24小时</div></div>
          <div class="stat-card"><div class="stat-label">地面站</div><div class="stat-value" style="color:#67c23a">{{ ov.stats.station_ok }}</div><div class="stat-sub">共 {{ ov.stats.station_total }} 座</div></div>
          <div class="stat-card"><div class="stat-label">数据在线率</div><div class="stat-value" style="color:#67c23a">{{ ov.stats.active && ov.stats.total ? Math.round(ov.stats.active/ov.stats.total*100) : 0 }}%</div><div class="stat-sub">在轨占比</div></div>
        </div>

        <el-row :gutter="16">
          <el-col :span="8">
            <div class="card">
              <div class="card-title">卫星类型分布</div>
              <v-chart :option="typePieOpt" style="height:260px" autoresize />
            </div>
          </el-col>
          <el-col :span="8">
            <div class="card">
              <div class="card-title">轨道类型分布</div>
              <v-chart :option="orbitPieOpt" style="height:260px" autoresize />
            </div>
          </el-col>
          <el-col :span="8">
            <div class="card">
              <div class="card-title">卫星状态</div>
              <v-chart :option="statusPieOpt" style="height:260px" autoresize />
            </div>
          </el-col>
        </el-row>
      </el-tab-pane>

      <!-- ═══ 卫星星座 ═══ -->
      <el-tab-pane label="卫星星座" name="sats">
        <div class="filter-bar">
          <el-select v-model="sf.satellite_type" placeholder="卫星类型" clearable size="small" style="width:110px" @change="loadSats">
            <el-option v-for="v in SAT_TYPES" :key="v" :value="v" :label="v" />
          </el-select>
          <el-select v-model="sf.orbit_type" placeholder="轨道类型" clearable size="small" style="width:100px" @change="loadSats">
            <el-option v-for="v in ['LEO','MEO','GEO','HEO']" :key="v" :value="v" :label="v" />
          </el-select>
          <el-select v-model="sf.status" placeholder="状态" clearable size="small" style="width:90px" @change="loadSats">
            <el-option v-for="v in ['在轨','故障','退役']" :key="v" :value="v" :label="v" />
          </el-select>
          <el-button size="small" @click="sf.satellite_type=sf.orbit_type=sf.status=''; loadSats()">重置</el-button>
          <div style="flex:1" />
          <el-date-picker v-model="dateRange" type="daterange" size="small" style="width:230px"
            start-placeholder="入轨开始" end-placeholder="入轨截止"
            :disabled-date="() => false"
            value-format="YYYY-MM-DD"
          />
          <el-tag type="info" size="small" effect="plain" style="margin-left:6px">时间仅展示</el-tag>
          <span style="font-size:13px;color:#606266;margin-left:12px">共 {{ sats.total }} 颗</span>
        </div>

        <el-table :data="sats.rows" v-loading="satLoading" border stripe size="small" style="margin-top:12px" @row-click="openTele">
          <el-table-column prop="satellite_id"   label="编号" width="60" align="center" />
          <el-table-column prop="satellite_name" label="卫星名称" width="110">
            <template #default="{row}">
              <b>{{ row.satellite_name }}</b>
            </template>
          </el-table-column>
          <el-table-column prop="satellite_type" label="类型" width="75">
            <template #default="{row}">
              <el-tag :type="satTypeColor(row.satellite_type)" size="small">{{ row.satellite_type }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="orbit_type" label="轨道" width="65" align="center">
            <template #default="{row}">
              <el-tag type="info" size="small" effect="plain">{{ row.orbit_type }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="altitude_km" label="轨道高度(km)" width="120" align="right">
            <template #default="{row}">{{ Number(row.altitude_km).toLocaleString() }}</template>
          </el-table-column>
          <el-table-column prop="period_min" label="周期(min)" width="90" align="right" />
          <el-table-column prop="inclination_deg" label="倾角(°)" width="80" align="right" />
          <el-table-column prop="mass_kg" label="质量(kg)" width="85" align="right">
            <template #default="{row}">{{ Number(row.mass_kg).toLocaleString() }}</template>
          </el-table-column>
          <el-table-column prop="launch_date" label="发射日期" width="100" />
          <el-table-column prop="payload_type" label="载荷" width="110" />
          <el-table-column prop="status" label="状态" width="70" align="center">
            <template #default="{row}">
              <el-tag :type="row.status==='在轨'?'success':row.status==='故障'?'danger':'info'" size="small">{{ row.status }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="电量" width="90" align="center">
            <template #default="{row}">
              <el-progress v-if="row.latest_battery!=null"
                :percentage="+row.latest_battery"
                :color="row.latest_battery<30?'#f56c6c':row.latest_battery<60?'#e6a23c':'#67c23a'"
                :stroke-width="6" :show-text="false" />
              <span style="font-size:11px">{{ row.latest_battery!=null ? (+row.latest_battery).toFixed(0)+'%' : '-' }}</span>
            </template>
          </el-table-column>
          <el-table-column prop="task_cnt" label="总任务数" width="85" align="right" />
          <el-table-column prop="operator" label="运营方" />
        </el-table>

        <!-- 遥测详情抽屉 -->
        <el-drawer v-model="teleDrawer" :title="`${teleTarget?.satellite_name} — 遥测时序`" size="60%">
          <div style="padding:0 8px">
            <el-radio-group v-model="teleHours" size="small" @change="loadTeleSeries" style="margin-bottom:12px">
              <el-radio-button :value="24">24h</el-radio-button>
              <el-radio-button :value="48">48h</el-radio-button>
              <el-radio-button :value="72">72h</el-radio-button>
            </el-radio-group>
            <v-chart :option="teleSeriesOpt" style="height:260px" autoresize />
            <v-chart :option="teleTempOpt"   style="height:180px;margin-top:8px" autoresize />
          </div>
        </el-drawer>
      </el-tab-pane>

      <!-- ═══ 任务分析 ═══ -->
      <el-tab-pane label="任务分析" name="tasks">
        <div class="filter-bar" style="margin-bottom:12px">
          <el-select v-model="tf.task_type" placeholder="任务类型" clearable size="small" style="width:110px">
            <el-option v-for="v in TASK_TYPES" :key="v" :value="v" :label="v" />
          </el-select>
          <el-select v-model="tf.priority" placeholder="优先级" clearable size="small" style="width:90px">
            <el-option v-for="v in [1,2,3,4,5]" :key="v" :value="v" :label="`P${v}`" />
          </el-select>
          <el-select v-model="tf.status" placeholder="任务状态" clearable size="small" style="width:100px">
            <el-option v-for="v in ['已完成','执行中','异常']" :key="v" :value="v" :label="v" />
          </el-select>
          <div style="flex:1" />
          <el-date-picker v-model="taskDateRange" type="daterange" size="small" style="width:230px"
            start-placeholder="任务开始" end-placeholder="任务截止" value-format="YYYY-MM-DD"
          />
          <el-tag type="info" size="small" effect="plain" style="margin-left:6px">时间仅展示</el-tag>
        </div>
        <el-row :gutter="16" v-loading="taskLoading">
          <el-col :span="12">
            <div class="card">
              <div class="card-title">近30天任务量趋势</div>
              <v-chart :option="dailyTrendOpt" style="height:240px" autoresize />
            </div>
          </el-col>
          <el-col :span="12">
            <div class="card">
              <div class="card-title">任务类型分布</div>
              <v-chart :option="taskTypePieOpt" style="height:240px" autoresize />
            </div>
          </el-col>
        </el-row>
        <el-row :gutter="16">
          <el-col :span="14">
            <div class="card">
              <div class="card-title">目标区域热度 TOP12</div>
              <v-chart :option="areaBarOpt" style="height:260px" autoresize />
            </div>
          </el-col>
          <el-col :span="10">
            <div class="card">
              <div class="card-title">任务优先级分布</div>
              <v-chart :option="priorityBarOpt" style="height:260px" autoresize />
            </div>
          </el-col>
        </el-row>
        <div class="card">
          <div class="card-title">各卫星任务执行统计</div>
          <el-table :data="filteredBySat" border stripe size="small">
            <el-table-column prop="satellite_name" label="卫星" width="110" />
            <el-table-column prop="satellite_type" label="类型" width="75">
              <template #default="{row}"><el-tag :type="satTypeColor(row.satellite_type)" size="small">{{ row.satellite_type }}</el-tag></template>
            </el-table-column>
            <el-table-column prop="task_cnt" label="任务数" width="80" align="right" />
            <el-table-column label="异常率" width="130">
              <template #default="{row}">
                <el-progress :percentage="row.task_cnt?Math.round(row.err_cnt/row.task_cnt*100):0"
                  :color="row.err_cnt/row.task_cnt>0.1?'#f56c6c':'#67c23a'" :stroke-width="8" />
              </template>
            </el-table-column>
            <el-table-column label="数据量(GB)" align="right">
              <template #default="{row}">
                <b style="color:#409eff">{{ row.total_gb }}</b>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </el-tab-pane>

      <!-- ═══ 遥测监控 ═══ -->
      <el-tab-pane label="遥测监控" name="tele">
        <div class="filter-bar" style="margin-bottom:12px">
          <el-select v-model="tlf.satellite_type" placeholder="卫星类型" clearable size="small" style="width:110px">
            <el-option v-for="v in SAT_TYPES" :key="v" :value="v" :label="v" />
          </el-select>
          <el-select v-model="tlf.anomaly" placeholder="遥测状态" clearable size="small" style="width:100px">
            <el-option :value="0" label="正常" /><el-option :value="1" label="异常" />
          </el-select>
          <div style="flex:1" />
          <el-date-picker v-model="teleDateRange" type="datetimerange" size="small" style="width:350px"
            start-placeholder="采集开始时间" end-placeholder="采集截止时间" value-format="YYYY-MM-DD HH:mm:ss"
          />
          <el-tag type="info" size="small" effect="plain" style="margin-left:6px">时间仅展示</el-tag>
        </div>
        <el-row :gutter="16" v-loading="teleLoading">
          <el-col :span="24">
            <div class="card">
              <div class="card-title">过去48h异常遥测趋势</div>
              <v-chart :option="anomalyTrendOpt" style="height:180px" autoresize />
            </div>
          </el-col>
        </el-row>
        <el-table :data="filteredTele" border stripe size="small">
          <el-table-column prop="satellite_name" label="卫星" width="110" fixed />
          <el-table-column prop="satellite_type" label="类型" width="75">
            <template #default="{row}"><el-tag :type="satTypeColor(row.satellite_type)" size="small">{{ row.satellite_type }}</el-tag></template>
          </el-table-column>
          <el-table-column label="电量" width="140">
            <template #default="{row}">
              <el-progress :percentage="+row.battery_pct"
                :color="row.battery_pct<30?'#f56c6c':row.battery_pct<60?'#e6a23c':'#67c23a'"
                :stroke-width="8" />
              <span style="font-size:11px">{{ (+row.battery_pct).toFixed(1) }}%</span>
            </template>
          </el-table-column>
          <el-table-column label="太阳能(W)" width="90" align="right">
            <template #default="{row}">{{ (+row.solar_power_w).toFixed(0) }}</template>
          </el-table-column>
          <el-table-column label="CPU温度(℃)" width="105" align="right">
            <template #default="{row}">
              <span :style="{color:row.cpu_temp_c>45?'#f56c6c':row.cpu_temp_c>35?'#e6a23c':'inherit',fontWeight:600}">
                {{ (+row.cpu_temp_c).toFixed(1) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="信号强度(dB)" width="110" align="right">
            <template #default="{row}">
              <span :style="{color:row.signal_strength_db<-90?'#f56c6c':row.signal_strength_db<-80?'#e6a23c':'#67c23a'}">
                {{ (+row.signal_strength_db).toFixed(1) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="轨道高度(km)" width="115" align="right">
            <template #default="{row}">{{ Number(row.orbit_altitude_km).toLocaleString() }}</template>
          </el-table-column>
          <el-table-column label="状态" width="70" align="center" fixed="right">
            <template #default="{row}">
              <el-tag :type="row.anomaly_flag?'danger':'success'" size="small">
                {{ row.anomaly_flag ? '异常' : '正常' }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="record_time" label="采集时间" width="160" />
        </el-table>
      </el-tab-pane>

      <!-- ═══ 地面站 ═══ -->
      <el-tab-pane label="地面站" name="stations">
        <el-row :gutter="16" v-loading="stationLoading">
          <el-col v-for="st in stations.stations" :key="st.station_id" :span="6">
            <div class="station-card" :class="st.status==='正常'?'ok':st.status==='维护'?'warn':'err'">
              <div class="station-name">{{ st.station_name }}</div>
              <div class="station-loc">{{ st.location }}
                <el-tag :type="st.status==='正常'?'success':st.status==='维护'?'warning':'danger'" size="small" style="margin-left:6px">{{ st.status }}</el-tag>
              </div>
              <div class="station-meta">
                <span>类型：{{ st.station_type }}</span>
                <span>天线：{{ st.antenna_count }} 面</span>
              </div>
              <div class="station-meta">
                <span>覆盖半径：{{ st.coverage_radius_km }} km</span>
                <span>日接触：{{ st.daily_contacts }} 次</span>
              </div>
              <el-progress
                :percentage="+st.uptime_pct"
                :color="st.uptime_pct>=99?'#67c23a':st.uptime_pct>=95?'#e6a23c':'#f56c6c'"
                :stroke-width="6"
              />
              <div style="font-size:11px;color:#909399;margin-top:4px">在线率 {{ st.uptime_pct }}%</div>
            </div>
          </el-col>
        </el-row>
      </el-tab-pane>

    </el-tabs>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted } from 'vue'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { BarChart, PieChart, LineChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent, DataZoomComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import { ElMessage } from 'element-plus'
import { satelliteApi } from '@/api'

use([CanvasRenderer, BarChart, PieChart, LineChart, GridComponent, TooltipComponent, LegendComponent, DataZoomComponent])

const tab = ref('overview')
const SAT_TYPES  = ['遥感','导航','通信','气象','预警','侦察','中继','科学']
const TASK_TYPES = ['成像侦察','通信中继','导航定位','气象探测','海洋监测','预警探测','科学实验','轨道维护']
const COLORS = ['#409eff','#67c23a','#e6a23c','#f56c6c','#9b59b6','#1abc9c','#e74c3c','#3498db']

const initialized = ref(true)
const initing = ref(false)

const ovLoading = ref(false), satLoading = ref(false)
const taskLoading = ref(false), teleLoading = ref(false), stationLoading = ref(false)

const ov = ref({ stats: {}, type_dist: [], orbit_dist: [], status_dist: [] })
const sats = ref({ rows: [], total: 0 })
const ta = ref({ by_type: [], by_priority: [], daily_trend: [], by_area: [], by_sat: [] })
const tele = ref({ latest: [], anomaly_trend: [] })
const stations = ref({ stations: [], contacts: [] })

const sf  = ref({ satellite_type: '', orbit_type: '', status: '' })
const tf  = ref({ task_type: '', priority: null, status: '' })
const tlf = ref({ satellite_type: '', anomaly: null })

// 时间选择器（仅展示，不传参）
const dateRange     = ref(null)
const taskDateRange = ref(null)
const teleDateRange = ref(null)

// 遥测抽屉
const teleDrawer = ref(false)
const teleTarget = ref(null)
const teleHours = ref(48)
const teleSeries = ref({ series: [] })

const satTypeColor = t => ({'遥感':'','导航':'success','通信':'warning','气象':'info','预警':'danger','侦察':'danger','中继':'','科学':'success'}[t]||'info')

const filteredBySat = computed(() => {
  let rows = ta.value.by_sat || []
  if (tf.value.task_type) rows = rows.filter(r => r.satellite_type && ta.value.by_type.find(t => t.task_type === tf.value.task_type))
  return rows
})

const filteredTele = computed(() => {
  let rows = tele.value.latest || []
  if (tlf.value.satellite_type) rows = rows.filter(r => r.satellite_type === tlf.value.satellite_type)
  if (tlf.value.anomaly !== null && tlf.value.anomaly !== '') rows = rows.filter(r => r.anomaly_flag == tlf.value.anomaly)
  return rows
})

// ── 初始化 ─────────────────────────────────────────────────────────
async function doInit() {
  initing.value = true
  try {
    const r = await satelliteApi.init()
    ElMessage.success(`初始化完成：${r.tasks || 0} 条任务，${r.telemetry || 0} 条遥测`)
    initialized.value = true
    loadAll()
  } catch { /* error shown by interceptor */ }
  finally { initing.value = false }
}

async function checkInit() {
  try {
    const r = await satelliteApi.overview()
    initialized.value = (r?.stats?.total || 0) > 0
    if (initialized.value) { ov.value = r; ovLoading.value = false }
    else ovLoading.value = false
  } catch { initialized.value = false; ovLoading.value = false }
}

// ── 数据加载 ────────────────────────────────────────────────────────
async function loadOverview() {
  ovLoading.value = true
  ov.value = await satelliteApi.overview().finally(() => ovLoading.value = false)
}

async function loadSats() {
  satLoading.value = true
  const params = {}
  if (sf.value.satellite_type) params.satellite_type = sf.value.satellite_type
  if (sf.value.orbit_type)     params.orbit_type     = sf.value.orbit_type
  if (sf.value.status)         params.status         = sf.value.status
  sats.value = await satelliteApi.list(params).finally(() => satLoading.value = false)
}

async function loadTasks() {
  taskLoading.value = true
  ta.value = await satelliteApi.taskAnalysis().finally(() => taskLoading.value = false)
}

async function loadTele() {
  teleLoading.value = true
  tele.value = await satelliteApi.telemetry().finally(() => teleLoading.value = false)
}

async function loadStations() {
  stationLoading.value = true
  stations.value = await satelliteApi.stations().finally(() => stationLoading.value = false)
}

async function loadTeleSeries() {
  if (!teleTarget.value) return
  teleSeries.value = await satelliteApi.telemetrySeries(teleTarget.value.satellite_id, teleHours.value)
}

function openTele(row) {
  teleTarget.value = row
  teleDrawer.value = true
  loadTeleSeries()
}

let loaded = {}
watch(tab, v => {
  if (v === 'sats'     && !loaded.sats)     { loaded.sats = true;     loadSats() }
  if (v === 'tasks'    && !loaded.tasks)    { loaded.tasks = true;    loadTasks() }
  if (v === 'tele'     && !loaded.tele)     { loaded.tele = true;     loadTele() }
  if (v === 'stations' && !loaded.stations) { loaded.stations = true; loadStations() }
})

function loadAll() {
  loaded = {}
  loadOverview()
}

onMounted(async () => {
  ovLoading.value = true
  await checkInit()
})

// ── ECharts ────────────────────────────────────────────────────────
const typePieOpt = computed(() => ({
  tooltip: { trigger: 'item', formatter: '{b}: {c} 颗 ({d}%)' },
  legend: { bottom: 0, textStyle: { fontSize: 11 } },
  color: COLORS,
  series: [{ type: 'pie', radius: ['38%','62%'],
    data: ov.value.type_dist.map(r => ({ name: r.satellite_type, value: r.cnt })),
    label: { formatter: '{b}\n{d}%', fontSize: 11 }
  }]
}))

const orbitPieOpt = computed(() => ({
  tooltip: { trigger: 'item', formatter: '{b}: {c} 颗 ({d}%)' },
  legend: { bottom: 0, textStyle: { fontSize: 11 } },
  color: ['#409eff','#67c23a','#e6a23c','#f56c6c'],
  series: [{ type: 'pie', radius: ['38%','62%'],
    data: ov.value.orbit_dist.map(r => ({ name: r.orbit_type, value: r.cnt })),
    label: { formatter: '{b}\n{d}%', fontSize: 11 }
  }]
}))

const statusPieOpt = computed(() => ({
  tooltip: { trigger: 'item' },
  legend: { bottom: 0, textStyle: { fontSize: 11 } },
  color: ['#67c23a','#f56c6c','#909399'],
  series: [{ type: 'pie', radius: '60%',
    data: ov.value.status_dist.map(r => ({ name: r.status, value: r.cnt })),
    label: { formatter: '{b}: {c}', fontSize: 12 }
  }]
}))

const dailyTrendOpt = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: 40, right: 60, top: 10, bottom: 30 },
  xAxis: { type: 'category', data: ta.value.daily_trend.map(r => r.dt?.slice(5)), axisLabel: { fontSize: 10 } },
  yAxis: [{ type: 'value', name: '任务数' }, { type: 'value', name: '数据量(GB)', position: 'right' }],
  series: [
    { name: '任务数', type: 'bar', data: ta.value.daily_trend.map(r => r.cnt), itemStyle: { color: '#409eff' } },
    { name: '异常数', type: 'bar', stack: 'err', data: ta.value.daily_trend.map(r => r.err_cnt), itemStyle: { color: '#f56c6c' } },
    { name: '数据量', type: 'line', yAxisIndex: 1, smooth: true, data: ta.value.daily_trend.map(r => r.total_gb), itemStyle: { color: '#67c23a' }, lineStyle: { width: 2 } },
  ]
}))

const taskTypePieOpt = computed(() => ({
  tooltip: { trigger: 'item', formatter: '{b}: {c} 次 ({d}%)' },
  legend: { bottom: 0, orient: 'vertical', right: 0, textStyle: { fontSize: 11 } },
  color: COLORS,
  series: [{ type: 'pie', radius: ['35%','58%'], center: ['40%','48%'],
    data: ta.value.by_type.map(r => ({ name: r.task_type, value: r.cnt })),
    label: { fontSize: 11 }
  }]
}))

const areaBarOpt = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: 90, right: 60, top: 10, bottom: 10 },
  xAxis: { type: 'value' },
  yAxis: { type: 'category', data: ta.value.by_area.map(r => r.target_area), axisLabel: { fontSize: 11 } },
  series: [
    { name: '普通', type: 'bar', stack: 'a', data: ta.value.by_area.map(r => r.cnt - r.high_priority), itemStyle: { color: '#409eff' } },
    { name: '高优先级', type: 'bar', stack: 'a', data: ta.value.by_area.map(r => r.high_priority), itemStyle: { color: '#f56c6c' }, label: { show: true, position: 'right', fontSize: 11 } },
  ]
}))

const priorityBarOpt = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: 40, right: 20, top: 10, bottom: 30 },
  xAxis: { type: 'category', data: ta.value.by_priority.map(r => `P${r.priority}`) },
  yAxis: { type: 'value' },
  series: [
    { name: '任务数', type: 'bar', data: ta.value.by_priority.map((r, i) => ({ value: r.cnt, itemStyle: { color: COLORS[i % COLORS.length] } })), label: { show: true, position: 'top', fontSize: 11 } },
  ]
}))

const anomalyTrendOpt = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: 50, right: 20, top: 10, bottom: 30 },
  xAxis: { type: 'category', data: tele.value.anomaly_trend.map(r => r.hour_slot?.slice(8, 16)), axisLabel: { fontSize: 10 } },
  yAxis: { type: 'value' },
  series: [
    { name: '遥测总数', type: 'bar', data: tele.value.anomaly_trend.map(r => r.total), itemStyle: { color: '#409eff55' } },
    { name: '异常数',   type: 'line', data: tele.value.anomaly_trend.map(r => r.anomaly), smooth: true, itemStyle: { color: '#f56c6c' }, lineStyle: { width: 2 } },
  ]
}))

const teleSeriesOpt = computed(() => {
  const s = teleSeries.value.series || []
  return {
    tooltip: { trigger: 'axis' },
    legend: { data: ['电量(%)', '太阳能(W/10)'], bottom: 0, textStyle: { fontSize: 11 } },
    grid: { left: 50, right: 20, top: 10, bottom: 40 },
    xAxis: { type: 'category', data: s.map(r => String(r.record_time).slice(8, 16)), axisLabel: { fontSize: 9 } },
    yAxis: { type: 'value', max: 120 },
    series: [
      { name: '电量(%)', type: 'line', smooth: true, data: s.map(r => r.battery_pct), itemStyle: { color: '#67c23a' }, lineStyle: { width: 1.5 }, symbol: 'none' },
      { name: '太阳能(W/10)', type: 'line', smooth: true, data: s.map(r => +(r.solar_power_w / 10).toFixed(1)), itemStyle: { color: '#e6a23c' }, lineStyle: { width: 1.5 }, symbol: 'none' },
    ]
  }
})

const teleTempOpt = computed(() => {
  const s = teleSeries.value.series || []
  return {
    tooltip: { trigger: 'axis' },
    grid: { left: 50, right: 20, top: 10, bottom: 10 },
    xAxis: { type: 'category', data: s.map(r => String(r.record_time).slice(8, 16)), axisLabel: { fontSize: 9 } },
    yAxis: { type: 'value', name: '℃' },
    series: [
      { name: 'CPU温度', type: 'line', smooth: true, data: s.map(r => r.cpu_temp_c), areaStyle: { opacity: 0.15 },
        itemStyle: { color: '#f56c6c' }, lineStyle: { width: 1.5 }, symbol: 'none',
        markLine: { data: [{ yAxis: 40, lineStyle: { color: '#f56c6c', type: 'dashed' } }], label: { formatter: '警戒40℃' } }
      },
    ]
  }
})
</script>

<style scoped>
.init-banner {
  background: #fff3cd; border: 1px solid #ffc107; border-radius: 6px;
  padding: 10px 16px; margin-bottom: 12px; display: flex; align-items: center;
  font-size: 13px; color: #856404;
}
.station-card {
  background: #fff; border-radius: 8px; padding: 16px; margin-bottom: 16px;
  box-shadow: 0 1px 4px rgba(0,0,0,0.08); border-left: 4px solid #67c23a;
}
.station-card.warn { border-left-color: #e6a23c; }
.station-card.err  { border-left-color: #f56c6c; }
.station-name { font-size: 15px; font-weight: 600; color: #1a1a1a; margin-bottom: 4px; }
.station-loc  { font-size: 12px; color: #606266; margin-bottom: 8px; }
.station-meta { display: flex; justify-content: space-between; font-size: 12px; color: #909399; margin-bottom: 6px; }
</style>
