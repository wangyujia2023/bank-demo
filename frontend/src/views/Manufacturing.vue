<template>
  <div class="mfg-wrap">
    <CollapseCard v-model:open="archOpen">
      <template #header-left>
        <span>🏭</span>
        <span class="ch-title">工厂数字孪生 · 全链路数据架构</span>
        <span class="badge blue">Apache Doris 4.0 HASP</span>
        <span class="badge green">Stream Load 实时写入</span>
        <span class="badge orange">50+ 维度指标</span>
      </template>
      <div>
        <div class="arch-flow">
          <div class="arch-node sensor"><div class="ni">🏭</div><div class="nl">产线传感器</div><div class="ns">温度/震动/转速/电流</div></div>
          <div class="flow-pipe"><div class="flow-beam"></div></div>
          <div class="arch-node"><div class="ni">📡</div><div class="nl">边缘采集</div><div class="ns">IoT Gateway</div></div>
          <div class="flow-pipe"><div class="flow-beam"></div></div>
          <div class="arch-node"><div class="ni">🌊</div><div class="nl">Stream Load</div><div class="ns">HTTP PUT 批量写入</div></div>
          <div class="flow-pipe"><div class="flow-beam"></div></div>
          <div class="arch-node doris"><div class="ni">⚡</div><div class="nl">Apache Doris 4.0</div><div class="ns">DUPLICATE KEY · 4桶</div></div>
          <div class="flow-pipe"><div class="flow-beam"></div></div>
          <div class="arch-node"><div class="ni">🔍</div><div class="nl">实时分析引擎</div><div class="ns">毫秒级聚合查询</div></div>
          <div class="flow-pipe"><div class="flow-beam"></div></div>
          <div class="arch-node viz"><div class="ni">🖥️</div><div class="nl">数字孪生看板</div><div class="ns">5维度分析平台</div></div>
        </div>
        <div class="schema-row">
          <div class="schema-box">
            <div class="schema-lbl">📋 mfg_metrics_v2 — 50+ 指标全字段模型</div>
            <pre class="schema-code">-- 物理状态(15): temperature, bearing_temp, coolant_temp, vibration, noise_level,
--               spindle_speed, feed_rate, cutting_force, torque, motor_current,
--               power_kw, hydraulic_bar, air_pressure, coolant_flow, tool_wear_pct
-- 生产效能(12): oee, availability, performance_rate, quality_rate,
--               output_count, planned_output, defect_count, scrap_count,
--               rework_count, cycle_time_s, plan_cycle_s, run_hours
-- 质量工艺(10): yield_rate, first_pass_yield, scrap_rate, rework_rate,
--               cpk_value, ppm_value, surface_roughness, dimensional_error,
--               hardness_hrc, tensile_strength
-- 能耗环境(8):  energy_kwh, energy_per_unit, co2_kg, env_temp, env_humidity,
--               coolant_consumed_l, compressed_air_m3, water_l
-- 维保告警(5):  tool_change_cnt, alarm_count, unplanned_down_min,
--               planned_down_min, mtbf_h
DUPLICATE KEY(ts, machine_id)  DISTRIBUTED BY HASH(machine_id) BUCKETS 4</pre>
          </div>
          <div class="schema-stats">
            <div class="ss"><div class="sv">{{ ov?.current_step||0 }}</div><div class="sk">已模拟步数</div></div>
            <div class="ss"><div class="sv">{{ (ov?.current_step||0)*15 }}min</div><div class="sk">仿真时长</div></div>
            <div class="ss"><div class="sv">50+</div><div class="sk">采集指标</div></div>
            <div class="ss"><div class="sv">&lt;1ms</div><div class="sk">查询延迟</div></div>
            <div class="ss-btn">
              <el-button type="primary" size="small" :loading="generating" @click="doGen(20)">一键初始化演示数据(20步)</el-button>
            </div>
          </div>
        </div>
      </div>
    </CollapseCard>

    <CollapseCard v-model:open="ctrlOpen">
      <template #header-left>
        <span class="sdot" :class="scriptColor"></span>
        <span class="ch-title">{{ statusText }}</span>
        <span v-if="ov&&!ov.empty" class="ch-sim">仿真时间 <b>{{ ov.last_ts }}</b> · 第 <b>{{ ov.current_step }}</b> 步</span>
      </template>
      <div class="ctrl-body">
        <div class="ctrl-row">
          <el-button-group>
            <el-button type="primary" size="large" :loading="generating" @click="doGen(1)" style="min-width:90px">{{ generating?'生成中…':'⚡ +1步' }}</el-button>
            <el-button type="primary" size="large" :loading="generating" @click="doGen(5)">+5步</el-button>
            <el-button type="primary" size="large" :loading="generating" @click="doGen(10)">+10步</el-button>
          </el-button-group>
          <el-button :type="autoOn?'warning':'default'" size="large" @click="toggleAuto">{{ autoOn?'⏸ 停止':'▶ 自动演练' }}</el-button>
          <el-button size="large" @click="resetData" :disabled="generating">🔄 重置</el-button>
        </div>
        <div class="ctrl-hint">每步推进15分钟 · 自动演练每3秒生成1步 · 连续操作可完整观察黄金时段→疲劳→熔断→恢复全生命周期</div>
      </div>
    </CollapseCard>

    <!-- ③ KPI 指标条 -->
    <div class="kpi-row" v-if="ov&&!ov.empty">
      <div class="kpi" v-for="k in kpis" :key="k.label">
        <div class="kv" :style="{color:k.color}">{{ k.value }}</div>
        <div class="kl">{{ k.label }}</div>
      </div>
    </div>

    <!-- ④ 主分析 Tabs -->
    <div class="card tabs-card" v-if="ov&&!ov.empty">
      <el-tabs v-model="activeTab" @tab-click="onTabChange">

        <!-- Tab1: 设备状态 -->
        <el-tab-pane label="⚙️ 设备状态" name="device">
          <!-- 机器选择行 -->
          <div class="machine-chips">
            <div v-for="m in machines" :key="m.machine_id"
              :class="['mchip', m.script_color, selMachine===m.machine_id?'sel':'']"
              @click="selectMachine(m)">
              <span class="mchip-dot" :class="m.script_color"></span>
              <span class="mchip-id">{{ m.machine_id }}</span>
              <span class="mchip-oee">{{ pct(m.oee) }}%</span>
            </div>
          </div>

          <!-- 详情面板 -->
          <div v-if="selM" class="mdetail">
            <div class="mdetail-hd">
              <span class="mdetail-id">{{ selM.machine_id }}</span>
              <el-tag :type="selM.script_color==='green'?'success':selM.script_color==='red'?'danger':'warning'" size="small">{{ selM.script_name }}</el-tag>
              <span class="mdetail-type">{{ selM.machine_type }} · {{ selM.line_id }}</span>
              <span style="margin-left:auto;font-size:12px;color:#909399">{{ selM.ts }}</span>
            </div>

            <!-- 6大效能进度 -->
            <div class="perf-grid">
              <div class="perf-card" v-for="p in perfCards" :key="p.label">
                <div class="pc-label">{{ p.label }}</div>
                <div class="pc-val" :class="p.cls">{{ p.val }}</div>
                <el-progress :percentage="p.pct" :color="p.color" :stroke-width="6" :show-text="false" />
              </div>
            </div>

            <!-- 50指标四分区 -->
            <div class="metrics-sections">
              <div class="ms-section" v-for="sec in metricSections" :key="sec.title">
                <div class="ms-title">{{ sec.title }}</div>
                <div class="ms-grid">
	                  <div class="ms-item" v-for="it in sec.items" :key="it.label">
	                    <div class="mi-label">{{ it.label }}</div>
	                    <div class="mi-val" :class="it.cls||''">{{ it.val }}</div>
	                    <div class="mi-unit">{{ it.unit }}</div>
	                  </div>
	                </div>
	              </div>
	            </div>

            <!-- 设备趋势图 -->
            <div class="trend-title">近期运行趋势</div>
            <div ref="mTrendChart" class="chart-h200"></div>
          </div>
        </el-tab-pane>

        <!-- Tab2: 工艺分析 -->
        <el-tab-pane label="🔧 工艺分析" name="process">
          <div class="two-col">
            <div>
              <div class="ct">节拍时间 vs 计划</div>
              <div ref="cycleChart" class="chart-h260"></div>
            </div>
            <div>
              <div class="ct">产量完成率</div>
              <div ref="outputChart" class="chart-h260"></div>
            </div>
          </div>
          <div class="two-col" style="margin-top:14px">
            <div>
              <div class="ct">主轴转速 & 切削力趋势</div>
              <div ref="spindleChart" class="chart-h240"></div>
            </div>
            <div>
              <div class="ct">过程能力 Cpk（各设备）</div>
              <div ref="cpkChart" class="chart-h240"></div>
            </div>
          </div>
        </el-tab-pane>

        <!-- Tab3: 质量管理 -->
        <el-tab-pane label="✅ 质量管理" name="quality">
          <div class="two-col">
            <div>
              <div class="ct">首次通过率 FPY & PPM（各设备）</div>
              <div ref="fpyChart" class="chart-h260"></div>
            </div>
            <div>
              <div class="ct">废品 & 返工分布</div>
              <div ref="scrapChart" class="chart-h260"></div>
            </div>
          </div>
          <div class="two-col" style="margin-top:14px">
            <div>
              <div class="ct">表面粗糙度 Ra & 尺寸偏差（各设备）</div>
              <div ref="roughChart" class="chart-h240"></div>
            </div>
            <div>
              <div class="ct">硬度 & 抗拉强度（各设备）</div>
              <div ref="hardChart" class="chart-h240"></div>
            </div>
          </div>
        </el-tab-pane>

        <!-- Tab4: 能耗环境 -->
        <el-tab-pane label="⚡ 能耗环境" name="energy">
          <div class="two-col">
            <div>
              <div class="ct">各设备总能耗 & 碳排放</div>
              <div ref="energyChart" class="chart-h260"></div>
            </div>
            <div>
              <div class="ct">单件能耗 & 平均功耗（各设备）</div>
              <div ref="epuChart" class="chart-h260"></div>
            </div>
          </div>
          <div class="two-col" style="margin-top:14px">
            <div>
              <div class="ct">辅料消耗（冷却液 / 压缩空气 / 用水）</div>
              <div ref="consumeChart" class="chart-h240"></div>
            </div>
            <div>
              <div class="ct">环境温湿度（各设备）</div>
              <div ref="envChart" class="chart-h240"></div>
            </div>
          </div>
        </el-tab-pane>

        <!-- Tab5: 预测维保 -->
        <el-tab-pane label="🔔 预测维保" name="maint">
          <div class="two-col">
            <div>
              <div class="ct">刀具磨损度 & 换刀次数</div>
              <div ref="wearChart" class="chart-h260"></div>
            </div>
            <div>
              <div class="ct">告警次数 & 非计划停机（各设备）</div>
              <div ref="alarmChart" class="chart-h260"></div>
            </div>
          </div>
          <!-- 维保详情表 -->
          <div class="ct" style="margin-top:14px">维保状态总览</div>
          <el-table :data="maintStats" border stripe size="small" style="margin-top:8px">
            <el-table-column prop="machine_id"   label="设备"     width="100" />
            <el-table-column prop="machine_type" label="类型"     width="110" />
            <el-table-column prop="line_id"      label="产线"     width="72" />
            <el-table-column label="刀具磨损" width="90" align="right">
              <template #default="{row}">
                <span :class="wearCls(row.avg_tool_wear)">{{ row.avg_tool_wear }}%</span>
              </template>
            </el-table-column>
            <el-table-column label="最大磨损" width="86" align="right">
              <template #default="{row}">
                <span :class="wearCls(row.max_tool_wear)">{{ row.max_tool_wear }}%</span>
              </template>
            </el-table-column>
            <el-table-column prop="tool_changes"  label="换刀次数"  width="80" align="right"/>
            <el-table-column prop="total_alarms"  label="总告警"    width="76" align="right"/>
            <el-table-column label="非计划停机" width="96" align="right">
              <template #default="{row}">
                <span :class="row.total_down>30?'rt':''">{{ row.total_down }}min</span>
              </template>
            </el-table-column>
            <el-table-column prop="avg_mtbf"      label="MTBF(h)"  width="88" align="right"/>
            <el-table-column label="维保建议" min-width="120">
              <template #default="{row}">
                <el-tag :type="maintAdvice(row).type" size="small">{{ maintAdvice(row).text }}</el-tag>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

      </el-tabs>
    </div>

    <!-- 空状态 -->
    <div class="empty-state" v-if="!ov||ov.empty">
      <div style="font-size:56px;margin-bottom:12px">🏭</div>
      <div style="font-size:16px;font-weight:600;margin-bottom:6px">数字孪生沙盘就绪</div>
      <div style="font-size:13px;color:#909399;margin-bottom:16px">点击「⚡ +1步」或展开架构图使用一键初始化</div>
      <el-button type="primary" size="large" :loading="generating" @click="doGen(1)">开始演练</el-button>
    </div>

  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, nextTick, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { LineChart, BarChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent, MarkLineComponent } from 'echarts/components'
import * as echarts from 'echarts/core'
import { mfgApi } from '@/api'
import CollapseCard from '@/components/common/CollapseCard.vue'

use([CanvasRenderer, LineChart, BarChart, GridComponent, TooltipComponent, LegendComponent, MarkLineComponent])

// ── 折叠 ──────────────────────────────────────────────
const archOpen = ref(false)
const ctrlOpen = ref(false)

// ── 状态 ──────────────────────────────────────────────
const generating = ref(false)
const autoOn     = ref(false)
let autoTimer    = null

const ov         = ref(null)
const machines   = ref([])
const selMachine = ref('')
const selM       = ref(null)
const maintStats = ref([])
const activeTab  = ref('device')

// ── 图表 refs ─────────────────────────────────────────
const mTrendChart   = ref(null)
const cycleChart    = ref(null)
const outputChart   = ref(null)
const spindleChart  = ref(null)
const cpkChart      = ref(null)
const fpyChart      = ref(null)
const scrapChart    = ref(null)
const roughChart    = ref(null)
const hardChart     = ref(null)
const energyChart   = ref(null)
const epuChart      = ref(null)
const consumeChart  = ref(null)
const envChart      = ref(null)
const wearChart     = ref(null)
const alarmChart    = ref(null)

const CHARTS = {}
function initChart(ref, key) {
  if (!ref.value) return null
  if (!CHARTS[key]) CHARTS[key] = echarts.init(ref.value)
  return CHARTS[key]
}

// ── 计算属性 ──────────────────────────────────────────
const scriptColor = computed(() => ov.value?.script_color || 'grey')
const statusText  = computed(() => {
  if (!ov.value || ov.value.empty) return '等待演练启动…'
  const c = { green:'产线正常运行', yellow:'设备进入疲劳状态', red:'⚠️ 高温熔断告警！' }
  return `当前状态：${ov.value.script_name} — ${c[ov.value.script_color]||''}`
})

const kpis = computed(() => {
  if (!ov.value || ov.value.empty) return []
  const o = ov.value
  return [
    { label:'综合OEE',   value: pct(o.avg_oee)+'%',  color: oeeColor(o.avg_oee) },
    { label:'可用率',    value: pct(o.avg_avail)+'%', color: oeeColor(o.avg_avail) },
    { label:'性能率',    value: pct(o.avg_perf)+'%',  color: '#409eff' },
    { label:'质量率',    value: pct(o.avg_qual)+'%',  color: '#409eff' },
    { label:'累计产量',  value: (o.total_output||0).toLocaleString(), color:'#303133' },
    { label:'综合良品率',value: pct4(o.yield_rate)+'%', color: o.yield_rate>0.95?'#67c23a':'#e6a23c' },
    { label:'平均Cpk',   value: flt(o.avg_cpk),       color: o.avg_cpk>1.33?'#67c23a':o.avg_cpk>1?'#e6a23c':'#f56c6c' },
    { label:'总能耗',    value: flt(o.total_energy)+'kWh', color:'#9b59b6' },
    { label:'碳排放',    value: flt(o.total_co2)+'kg',    color:'#1abc9c' },
    { label:'告警次数',  value: (o.total_alarms||0)+'次',  color: o.total_alarms>20?'#f56c6c':'#303133' },
    { label:'平均刀具磨损',value: flt(o.avg_tool_wear)+'%', color: o.avg_tool_wear>70?'#f56c6c':o.avg_tool_wear>40?'#e6a23c':'#67c23a' },
    { label:'首次通过率', value: flt(o.avg_fpy)+'%',  color:'#409eff' },
  ]
})

const perfCards = computed(() => {
  if (!selM.value) return []
  const m = selM.value
  return [
    { label:'综合OEE',  val: pct(m.oee)+'%',          pct: pct(m.oee),            color: oeeColor(m.oee),         cls: oeeClass(m.oee) },
    { label:'可用率',   val: pct(m.availability)+'%',  pct: pct(m.availability),   color: oeeColor(m.availability),cls: oeeClass(m.availability) },
    { label:'性能率',   val: pct(m.performance_rate)+'%',pct:pct(m.performance_rate),color:'#409eff',cls:'' },
    { label:'质量率',   val: pct(m.quality_rate)+'%',  pct: pct(m.quality_rate),   color:'#67c23a',cls:'' },
    { label:'刀具磨损', val: flt(m.tool_wear_pct)+'%', pct: Math.min(m.tool_wear_pct,100), color: wearColor(m.tool_wear_pct), cls: wearClass(m.tool_wear_pct) },
    { label:'功耗效率', val: flt(100-Math.min(m.power_kw/50*100,100))+'%',
      pct: Math.max(0,100-m.power_kw/50*100), color:'#9b59b6', cls:'' },
  ]
})

const metricSections = computed(() => {
  if (!selM.value) return []
  const m = selM.value
  return [
    { title:'🌡️ 物理状态', items:[
      { label:'主轴温度',   val: m.temperature,     unit:'°C',   cls: m.temperature>85?'red':m.temperature>70?'yel':'' },
      { label:'轴承温度',   val: m.bearing_temp,    unit:'°C',   cls: m.bearing_temp>70?'red':m.bearing_temp>58?'yel':'' },
      { label:'冷却液温',  val: m.coolant_temp,    unit:'°C',   cls: m.coolant_temp>40?'red':'' },
      { label:'机械震动',   val: m.vibration,       unit:'mm/s', cls: m.vibration>5?'red':m.vibration>3?'yel':'' },
      { label:'噪音',       val: m.noise_level,     unit:'dB',   cls: m.noise_level>88?'red':m.noise_level>76?'yel':'' },
      { label:'主轴转速',   val: m.spindle_speed?.toLocaleString(), unit:'rpm', cls:'' },
      { label:'进给速率',   val: m.feed_rate,       unit:'mm/min',cls:'' },
      { label:'切削力',     val: m.cutting_force,   unit:'N',    cls: m.cutting_force>600?'red':'' },
      { label:'扭矩',       val: m.torque,          unit:'N·m',  cls: m.torque>100?'red':'' },
      { label:'电机电流',   val: m.motor_current,   unit:'A',    cls: m.motor_current>40?'red':'' },
      { label:'实时功耗',   val: m.power_kw,        unit:'kW',   cls: m.power_kw>35?'red':'' },
      { label:'液压',       val: m.hydraulic_bar,   unit:'bar',  cls: m.hydraulic_bar<65?'red':'' },
      { label:'气压',       val: m.air_pressure,    unit:'MPa',  cls: m.air_pressure<0.4?'red':'' },
      { label:'冷却液流量', val: m.coolant_flow,    unit:'L/min',cls: m.coolant_flow<3?'red':'' },
      { label:'刀具磨损',   val: m.tool_wear_pct,   unit:'%',    cls: wearClass(m.tool_wear_pct) },
    ]},
    { title:'📦 生产效能', items:[
      { label:'实际产出',   val: m.output_count,   unit:'件',   cls:'' },
      { label:'计划产出',   val: m.planned_output, unit:'件',   cls:'' },
      { label:'缺陷件',     val: m.defect_count,   unit:'件',   cls: m.defect_count>20?'red':'' },
      { label:'报废件',     val: m.scrap_count,    unit:'件',   cls: m.scrap_count>10?'red':'' },
      { label:'返工件',     val: m.rework_count,   unit:'件',   cls:'' },
      { label:'实际节拍',   val: m.cycle_time_s,   unit:'s',    cls: m.cycle_time_s>80?'red':m.cycle_time_s>65?'yel':'' },
      { label:'计划节拍',   val: m.plan_cycle_s,   unit:'s',    cls:'' },
      { label:'节拍效率',   val: m.plan_cycle_s?flt(m.plan_cycle_s/m.cycle_time_s*100):'--', unit:'%', cls:'' },
      { label:'运行时长',   val: m.run_hours,      unit:'h',    cls:'' },
    ]},
    { title:'✅ 质量工艺', items:[
      { label:'综合良品率', val: pct4(m.yield_rate)+'%',      unit:'',    cls: m.yield_rate<0.9?'red':'' },
      { label:'首次通过率', val: pct4(m.first_pass_yield)+'%',unit:'',    cls: m.first_pass_yield<0.9?'red':'' },
      { label:'废品率',     val: m.scrap_rate,                 unit:'%',   cls: m.scrap_rate>5?'red':m.scrap_rate>2?'yel':'' },
      { label:'返工率',     val: m.rework_rate,                unit:'%',   cls:'' },
      { label:'过程能力Cpk',val: m.cpk_value,                  unit:'',    cls: m.cpk_value<1?'red':m.cpk_value<1.33?'yel':'grn' },
      { label:'百万缺陷PPM',val: (m.ppm_value||0).toLocaleString(), unit:'ppm', cls: m.ppm_value>5000?'red':'' },
      { label:'表面粗糙度', val: m.surface_roughness,          unit:'Ra μm',cls: m.surface_roughness>3?'red':m.surface_roughness>2?'yel':'' },
      { label:'尺寸偏差',   val: m.dimensional_error,          unit:'μm',  cls: m.dimensional_error>25?'red':'' },
      { label:'硬度',       val: m.hardness_hrc,               unit:'HRC', cls:'' },
      { label:'抗拉强度',   val: m.tensile_strength,           unit:'MPa', cls:'' },
    ]},
    { title:'⚡ 能耗环境', items:[
      { label:'单步能耗',   val: m.energy_kwh,           unit:'kWh',  cls:'' },
      { label:'单件能耗',   val: m.energy_per_unit,       unit:'kWh/件',cls:'' },
      { label:'碳排放',     val: m.co2_kg,               unit:'kg',   cls:'' },
      { label:'环境温度',   val: m.env_temp,             unit:'°C',   cls:'' },
      { label:'环境湿度',   val: m.env_humidity,         unit:'%',    cls:'' },
      { label:'冷却液消耗', val: m.coolant_consumed_l,   unit:'L',    cls:'' },
      { label:'压缩空气',   val: m.compressed_air_m3,    unit:'m³',   cls:'' },
      { label:'用水量',     val: m.water_l,              unit:'L',    cls:'' },
    ]},
    { title:'🔔 维保告警', items:[
      { label:'换刀次数',   val: m.tool_change_cnt,      unit:'次',  cls:'' },
      { label:'告警次数',   val: m.alarm_count,          unit:'次',  cls: m.alarm_count>8?'red':m.alarm_count>3?'yel':'' },
      { label:'非计划停机', val: m.unplanned_down_min,   unit:'min', cls: m.unplanned_down_min>30?'red':'' },
      { label:'计划停机',   val: m.planned_down_min,     unit:'min', cls:'' },
      { label:'MTBF',       val: m.mtbf_h,               unit:'h',   cls: m.mtbf_h<40?'red':m.mtbf_h<80?'yel':'grn' },
    ]},
  ]
})

// ── 工具函数 ──────────────────────────────────────────
const pct  = v => v ? (v*100).toFixed(1) : '0.0'
const pct4 = v => v ? (v*100).toFixed(2) : '0.00'
const flt  = v => v !== undefined && v !== null ? Number(v).toFixed(2) : '--'
const COLORS = ['#409eff','#67c23a','#e6a23c','#f56c6c','#9b59b6','#1abc9c']
const oeeColor = v => v>=0.85?'#67c23a':v>=0.70?'#e6a23c':'#f56c6c'
const oeeClass = v => v>=0.85?'grn':v>=0.70?'yel':'red'
const wearColor= v => v>=80?'#f56c6c':v>=50?'#e6a23c':'#67c23a'
const wearClass= v => v>=80?'red':v>=50?'yel':'grn'
const wearCls  = v => +v>=80?'rt fw':+v>=50?'yw fw':''

function maintAdvice(row) {
  if (+row.max_tool_wear >= 90) return { type:'danger',  text:'⚠️ 立即换刀' }
  if (+row.max_tool_wear >= 70) return { type:'warning', text:'🔶 计划换刀' }
  if (+row.total_alarms  >  10) return { type:'warning', text:'⚠️ 告警频发' }
  if (+row.total_down    >  30) return { type:'warning', text:'检查停机原因' }
  return { type:'success', text:'✅ 运行正常' }
}

// ── 数据加载 ──────────────────────────────────────────
async function loadOverview() {
  const [o, ms] = await Promise.all([mfgApi.overview(), mfgApi.machineStatus()])
  ov.value = o
  machines.value = ms
  if (ms.length && !selMachine.value) selectMachine(ms[0])
  else if (selMachine.value) {
    const cur = ms.find(m => m.machine_id === selMachine.value)
    if (cur) selM.value = cur
  }
}

async function selectMachine(m) {
  selMachine.value = m.machine_id
  selM.value = m
  await nextTick()
  loadMachineTrend(m.machine_id)
}

async function loadMachineTrend(mid) {
  const data = await mfgApi.machineTrend(mid)
  await nextTick()
  renderMTrend(data)
}

async function doGen(steps=1) {
  if (generating.value) return
  generating.value = true
  try {
    const res = steps===1 ? await mfgApi.generate() : await mfgApi.batch(steps)
    ElMessage.success(res.msg || '数据生成成功')
    await loadOverview()
    if (activeTab.value !== 'device') loadTabData(activeTab.value)
  } catch { ElMessage.error('数据生成失败，请检查 Doris 连接') }
  finally  { generating.value = false }
}

function toggleAuto() {
  autoOn.value = !autoOn.value
  if (autoOn.value) {
    autoTimer = setInterval(() => doGen(1), 3000)
    ElMessage.info('自动演练已启动')
  } else {
    clearInterval(autoTimer); autoTimer = null
    ElMessage.info('自动演练已暂停')
  }
}

async function resetData() {
  await ElMessageBox.confirm('确认清空所有仿真数据？', '重置', { type:'warning' })
  await mfgApi.reset()
  ov.value = null; machines.value = []; selM.value = null; selMachine.value = ''
  if (autoOn.value) { autoOn.value=false; clearInterval(autoTimer); autoTimer=null }
  ElMessage.success('已重置')
}

// ── Tab 切换 & 数据加载 ───────────────────────────────
function onTabChange({ paneName }) { loadTabData(paneName) }

async function loadTabData(tab) {
  await nextTick()
  if (tab === 'process') {
    const [trend, qual] = await Promise.all([mfgApi.processTrend(), mfgApi.qualityStats()])
    renderCycle(trend); renderOutput(trend); renderSpindle(trend); renderCpk(qual)
  } else if (tab === 'quality') {
    const q = await mfgApi.qualityStats()
    renderFpy(q); renderScrap(q); renderRough(q); renderHard(q)
  } else if (tab === 'energy') {
    const e = await mfgApi.energyStats()
    renderEnergy(e); renderEpu(e); renderConsume(e); renderEnv(e)
  } else if (tab === 'maint') {
    const m = await mfgApi.maintenanceStats()
    maintStats.value = m
    renderWear(m); renderAlarm(m)
  }
}

// ── 图表渲染 ──────────────────────────────────────────
const baseGrid = { top:28, bottom:32, left:56, right:16 }

function renderMTrend(data) {
  const c = initChart(mTrendChart, 'mtrend'); if (!c) return
  const ts = data.map(d => d.ts?.slice(11,16))
  c.setOption({
    tooltip: { trigger:'axis' },
    legend: { top:0, data:['OEE%','温度°C','震动','功耗kW'], textStyle:{fontSize:10} },
    grid: { top:28, bottom:28, left:52, right:52 },
    xAxis: { type:'category', data:ts, axisLabel:{fontSize:9,rotate:ts.length>10?30:0} },
    yAxis: [
      { type:'value', name:'%/°C', min:0, axisLabel:{fontSize:9} },
      { type:'value', name:'mm/s·kW', axisLabel:{fontSize:9} },
    ],
    series: [
      { name:'OEE%',  type:'line', smooth:true, data:data.map(d=>d.oee),          lineStyle:{color:'#409eff',width:2}, symbol:'none' },
      { name:'温度°C',type:'line', smooth:true, data:data.map(d=>d.temperature),  lineStyle:{color:'#f56c6c',width:1.5,type:'dashed'}, symbol:'none' },
      { name:'震动',  type:'line', smooth:true, yAxisIndex:1, data:data.map(d=>d.vibration), lineStyle:{color:'#e6a23c',width:1.5}, symbol:'none' },
      { name:'功耗kW',type:'line', smooth:true, yAxisIndex:1, data:data.map(d=>d.power_kw),  lineStyle:{color:'#9b59b6',width:1.5}, symbol:'none' },
    ],
  })
}

function renderCycle(data) {
  const c = initChart(cycleChart,'cycle'); if(!c) return
  const ts = data.map(d=>d.ts?.slice(11,16))
  c.setOption({ tooltip:{trigger:'axis'}, legend:{top:0,textStyle:{fontSize:10}},
    grid:{...baseGrid,bottom:36},
    xAxis:{type:'category',data:ts,axisLabel:{fontSize:9,rotate:30}},
    yAxis:{type:'value',name:'s',axisLabel:{fontSize:9}},
    series:[
      {name:'实际节拍',type:'line',smooth:true,data:data.map(d=>d.cycle_time),lineStyle:{color:'#f56c6c',width:2},symbol:'none'},
      {name:'计划节拍',type:'line',data:data.map(d=>d.plan_cycle),lineStyle:{color:'#67c23a',type:'dashed',width:1.5},symbol:'none'},
    ]})
}

function renderOutput(data) {
  const c = initChart(outputChart,'output'); if(!c) return
  const ts = data.map(d=>d.ts?.slice(11,16))
  c.setOption({ tooltip:{trigger:'axis'}, legend:{top:0,textStyle:{fontSize:10}},
    grid:{...baseGrid,bottom:36},
    xAxis:{type:'category',data:ts,axisLabel:{fontSize:9,rotate:30}},
    yAxis:{type:'value',name:'件',axisLabel:{fontSize:9}},
    series:[
      {name:'实际产量',type:'bar',data:data.map(d=>d.output),itemStyle:{color:'#409eff'}},
      {name:'计划产量',type:'line',data:data.map(d=>d.planned),lineStyle:{color:'#e6a23c',type:'dashed',width:1.5},symbol:'circle'},
    ]})
}

function renderSpindle(data) {
  const c = initChart(spindleChart,'spindle'); if(!c) return
  const ts = data.map(d=>d.ts?.slice(11,16))
  c.setOption({ tooltip:{trigger:'axis'}, legend:{top:0,textStyle:{fontSize:10}},
    grid:{...baseGrid,bottom:36},
    xAxis:{type:'category',data:ts,axisLabel:{fontSize:9,rotate:30}},
    yAxis:[{type:'value',name:'rpm',axisLabel:{fontSize:9}},{type:'value',name:'N',axisLabel:{fontSize:9}}],
    series:[
      {name:'主轴转速',type:'line',smooth:true,data:data.map(d=>d.spindle_speed),lineStyle:{color:'#409eff',width:2},symbol:'none'},
      {name:'切削力',  type:'line',smooth:true,yAxisIndex:1,data:data.map(d=>d.cutting_force),lineStyle:{color:'#f56c6c',width:1.5},symbol:'none'},
    ]})
}

function renderCpk(q) {
  const c = initChart(cpkChart,'cpk'); if(!c) return
  const ids = q.map(d=>d.machine_id)
  c.setOption({ tooltip:{trigger:'axis'},
    grid:{...baseGrid},
    xAxis:{type:'category',data:ids,axisLabel:{fontSize:10}},
    yAxis:{type:'value',name:'Cpk',axisLabel:{fontSize:9}},
    series:[{
      type:'bar', data:q.map(d=>({value:d.cpk, itemStyle:{color:+d.cpk>=1.33?'#67c23a':+d.cpk>=1?'#e6a23c':'#f56c6c'}})),
      label:{show:true,position:'top',fontSize:10},
      markLine:{silent:true,data:[{yAxis:1.33,name:'1.33',lineStyle:{color:'#67c23a',type:'dashed'}},{yAxis:1,name:'1.0',lineStyle:{color:'#f56c6c',type:'dashed'}}]},
    }]})
}

function renderFpy(q) {
  const c = initChart(fpyChart,'fpy'); if(!c) return
  const ids = q.map(d=>d.machine_id)
  c.setOption({ tooltip:{trigger:'axis'}, legend:{top:0,textStyle:{fontSize:10}},
    grid:{...baseGrid},
    xAxis:{type:'category',data:ids,axisLabel:{fontSize:10}},
    yAxis:[{type:'value',name:'%',axisLabel:{fontSize:9}},{type:'value',name:'PPM',axisLabel:{fontSize:9}}],
    series:[
      {name:'FPY%',type:'bar',data:q.map(d=>+d.fpy),itemStyle:{color:'#67c23a'}},
      {name:'PPM', type:'line',yAxisIndex:1,data:q.map(d=>+d.ppm),lineStyle:{color:'#f56c6c',width:2},symbol:'circle'},
    ]})
}

function renderScrap(q) {
  const c = initChart(scrapChart,'scrap'); if(!c) return
  const ids = q.map(d=>d.machine_id)
  c.setOption({ tooltip:{trigger:'axis'}, legend:{top:0,textStyle:{fontSize:10}},
    grid:{...baseGrid},
    xAxis:{type:'category',data:ids,axisLabel:{fontSize:10}},
    yAxis:{type:'value',name:'件',axisLabel:{fontSize:9}},
    series:[
      {name:'报废',type:'bar',stack:'s',data:q.map(d=>+d.total_scrap), itemStyle:{color:'#f56c6c'}},
      {name:'返工',type:'bar',stack:'s',data:q.map(d=>+d.total_rework),itemStyle:{color:'#e6a23c'}},
    ]})
}

function renderRough(q) {
  const c = initChart(roughChart,'rough'); if(!c) return
  const ids = q.map(d=>d.machine_id)
  c.setOption({ tooltip:{trigger:'axis'}, legend:{top:0,textStyle:{fontSize:10}},
    grid:{...baseGrid},
    xAxis:{type:'category',data:ids,axisLabel:{fontSize:10}},
    yAxis:[{type:'value',name:'Ra μm',axisLabel:{fontSize:9}},{type:'value',name:'μm',axisLabel:{fontSize:9}}],
    series:[
      {name:'粗糙度Ra',type:'bar',data:q.map(d=>+d.roughness),itemStyle:{color:'#409eff'}},
      {name:'尺寸偏差',type:'line',yAxisIndex:1,data:q.map(d=>+d.dim_err),lineStyle:{color:'#e6a23c',width:2},symbol:'circle'},
    ]})
}

function renderHard(q) {
  const c = initChart(hardChart,'hard'); if(!c) return
  const ids = q.map(d=>d.machine_id)
  c.setOption({ tooltip:{trigger:'axis'}, legend:{top:0,textStyle:{fontSize:10}},
    grid:{...baseGrid},
    xAxis:{type:'category',data:ids,axisLabel:{fontSize:10}},
    yAxis:[{type:'value',name:'HRC',axisLabel:{fontSize:9}},{type:'value',name:'MPa',axisLabel:{fontSize:9}}],
    series:[
      {name:'硬度HRC',    type:'bar',data:q.map(d=>+d.hardness),itemStyle:{color:'#9b59b6'}},
      {name:'抗拉强度MPa',type:'line',yAxisIndex:1,data:q.map(d=>+d.tensile),lineStyle:{color:'#1abc9c',width:2},symbol:'circle'},
    ]})
}

function renderEnergy(e) {
  const c = initChart(energyChart,'energy'); if(!c) return
  const ids = e.map(d=>d.machine_id)
  c.setOption({ tooltip:{trigger:'axis'}, legend:{top:0,textStyle:{fontSize:10}},
    grid:{...baseGrid},
    xAxis:{type:'category',data:ids,axisLabel:{fontSize:10}},
    yAxis:[{type:'value',name:'kWh',axisLabel:{fontSize:9}},{type:'value',name:'kg',axisLabel:{fontSize:9}}],
    series:[
      {name:'总能耗kWh',type:'bar',data:e.map(d=>+d.total_energy),itemStyle:{color:'#9b59b6'}},
      {name:'碳排放kg',  type:'line',yAxisIndex:1,data:e.map(d=>+d.total_co2),lineStyle:{color:'#1abc9c',width:2},symbol:'circle'},
    ]})
}

function renderEpu(e) {
  const c = initChart(epuChart,'epu'); if(!c) return
  const ids = e.map(d=>d.machine_id)
  c.setOption({ tooltip:{trigger:'axis'}, legend:{top:0,textStyle:{fontSize:10}},
    grid:{...baseGrid},
    xAxis:{type:'category',data:ids,axisLabel:{fontSize:10}},
    yAxis:[{type:'value',name:'kWh/件',axisLabel:{fontSize:9}},{type:'value',name:'kW',axisLabel:{fontSize:9}}],
    series:[
      {name:'单件能耗',type:'bar',data:e.map(d=>+d.avg_epu),itemStyle:{color:'#e6a23c'}},
      {name:'平均功耗',type:'line',yAxisIndex:1,data:e.map(d=>+d.avg_power),lineStyle:{color:'#f56c6c',width:2},symbol:'circle'},
    ]})
}

function renderConsume(e) {
  const c = initChart(consumeChart,'consume'); if(!c) return
  const ids = e.map(d=>d.machine_id)
  c.setOption({ tooltip:{trigger:'axis'}, legend:{top:0,textStyle:{fontSize:10}},
    grid:{...baseGrid},
    xAxis:{type:'category',data:ids,axisLabel:{fontSize:10}},
    yAxis:{type:'value',axisLabel:{fontSize:9}},
    series:[
      {name:'冷却液L',   type:'bar',stack:'s',data:e.map(d=>+d.coolant_l),    itemStyle:{color:'#409eff'}},
      {name:'压缩空气m³',type:'bar',stack:'s',data:e.map(d=>+d.air_m3),      itemStyle:{color:'#67c23a'}},
      {name:'用水L',     type:'bar',stack:'s',data:e.map(d=>+d.water_l),     itemStyle:{color:'#1abc9c'}},
    ]})
}

function renderEnv(e) {
  const c = initChart(envChart,'env'); if(!c) return
  const ids = e.map(d=>d.machine_id)
  c.setOption({ tooltip:{trigger:'axis'}, legend:{top:0,textStyle:{fontSize:10}},
    grid:{...baseGrid},
    xAxis:{type:'category',data:ids,axisLabel:{fontSize:10}},
    yAxis:[{type:'value',name:'°C',axisLabel:{fontSize:9}},{type:'value',name:'%',axisLabel:{fontSize:9}}],
    series:[
      {name:'环境温度',type:'bar',data:e.map(d=>+d.env_temp),    itemStyle:{color:'#f56c6c'}},
      {name:'环境湿度',type:'line',yAxisIndex:1,data:e.map(d=>+d.humidity),lineStyle:{color:'#409eff',width:2},symbol:'circle'},
    ]})
}

function renderWear(m) {
  const c = initChart(wearChart,'wear'); if(!c) return
  const ids = m.map(d=>d.machine_id)
  c.setOption({ tooltip:{trigger:'axis'}, legend:{top:0,textStyle:{fontSize:10}},
    grid:{...baseGrid},
    xAxis:{type:'category',data:ids,axisLabel:{fontSize:10}},
    yAxis:[{type:'value',name:'%',min:0,max:100,axisLabel:{fontSize:9}},{type:'value',name:'次',axisLabel:{fontSize:9}}],
    series:[
      {name:'平均磨损',type:'bar',data:m.map(d=>({value:+d.avg_tool_wear,itemStyle:{color:wearColor(+d.avg_tool_wear)}})),
        markLine:{silent:true,data:[{yAxis:80,name:'危险',lineStyle:{color:'#f56c6c',type:'dashed'}},{yAxis:50,name:'警告',lineStyle:{color:'#e6a23c',type:'dashed'}}]}},
      {name:'最大磨损',type:'line',data:m.map(d=>+d.max_tool_wear),lineStyle:{color:'#f56c6c',type:'dashed',width:2},symbol:'circle'},
      {name:'换刀次数',type:'line',yAxisIndex:1,data:m.map(d=>+d.tool_changes),lineStyle:{color:'#9b59b6',width:1.5},symbol:'circle'},
    ]})
}

function renderAlarm(m) {
  const c = initChart(alarmChart,'alarm'); if(!c) return
  const ids = m.map(d=>d.machine_id)
  c.setOption({ tooltip:{trigger:'axis'}, legend:{top:0,textStyle:{fontSize:10}},
    grid:{...baseGrid},
    xAxis:{type:'category',data:ids,axisLabel:{fontSize:10}},
    yAxis:[{type:'value',name:'次',axisLabel:{fontSize:9}},{type:'value',name:'min',axisLabel:{fontSize:9}}],
    series:[
      {name:'告警次数',   type:'bar',data:m.map(d=>+d.total_alarms), itemStyle:{color:'#f56c6c'}},
      {name:'非计划停机',type:'line',yAxisIndex:1,data:m.map(d=>+d.total_down),lineStyle:{color:'#e6a23c',width:2},symbol:'circle'},
    ]})
}

onMounted(async () => {
  await mfgApi.init()
  await loadOverview()
})

onUnmounted(() => {
  if (autoTimer) clearInterval(autoTimer)
  Object.values(CHARTS).forEach(c => c?.dispose())
})
</script>

<style scoped>
.mfg-wrap { display:flex; flex-direction:column; gap:14px; }

/* 折叠卡片 */
.collapse-card { padding:0; overflow:hidden; }
.collapse-hd {
  display:flex; align-items:center; gap:8px;
  padding:12px 18px; cursor:pointer; user-select:none;
}
.collapse-hd:hover { background:#fafafa; }
.ch-title { font-size:14px; font-weight:600; color:#1a1a1a; }
.ch-sim   { font-size:12px; color:#909399; margin-left:6px; }
.ch-tog   { margin-left:auto; font-size:12px; color:#c0c4cc; flex-shrink:0; }
.badge { padding:1px 8px; border-radius:10px; font-size:11px; font-weight:500; flex-shrink:0; }
.badge.blue   { background:#ecf5ff; color:#409eff; }
.badge.green  { background:#f0f9eb; color:#67c23a; }
.badge.orange { background:#fdf6ec; color:#e6a23c; }
.sdot { display:inline-block; width:10px; height:10px; border-radius:50%; flex-shrink:0; }
.sdot.green  { background:#67c23a; box-shadow:0 0 6px #67c23a; }
.sdot.yellow { background:#e6a23c; box-shadow:0 0 6px #e6a23c; }
.sdot.red    { background:#f56c6c; box-shadow:0 0 6px #f56c6c; animation:pulse 1s infinite; }
.sdot.grey   { background:#c0c4cc; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.35} }

/* 架构图 */
.arch-body { padding:0 16px 16px; border-top:1px solid #f0f0f0; }
.arch-flow { display:flex; align-items:center; justify-content:center; padding:16px 0 12px; flex-wrap:wrap; }
.arch-node { display:flex; flex-direction:column; align-items:center; background:#f5f7fa; border-radius:8px; padding:8px 12px; min-width:94px; border:1.5px solid #e4e7ed; }
.arch-node.doris { background:#ecf5ff; border-color:#409eff; }
.arch-node.viz   { background:#f0f9eb; border-color:#67c23a; }
.ni { font-size:18px; margin-bottom:3px; }
.nl { font-size:11px; font-weight:600; color:#303133; text-align:center; }
.ns { font-size:9px; color:#909399; margin-top:1px; text-align:center; }
.flow-pipe { display:flex; align-items:center; padding:0 3px; }
.flow-beam { width:30px; height:2px; background:linear-gradient(to right,#c0c4cc,#409eff,#c0c4cc); background-size:200% 100%; animation:beam 1.4s linear infinite; position:relative; }
.flow-beam::after { content:'▶'; position:absolute; right:-8px; top:-7px; font-size:9px; color:#409eff; }
@keyframes beam { 0%{background-position:200% 0} 100%{background-position:-200% 0} }
.schema-row { display:flex; gap:14px; align-items:flex-start; }
.schema-box { flex:1; background:#1e1e2e; border-radius:8px; padding:10px 14px; overflow-x:auto; }
.schema-lbl { font-size:11px; color:#67c23a; font-weight:600; margin-bottom:6px; }
.schema-code { color:#a8b3cf; font-size:10px; font-family:monospace; white-space:pre; margin:0; line-height:1.7; }
.schema-stats { display:flex; flex-direction:column; gap:8px; min-width:130px; }
.ss { background:#f5f7fa; border-radius:6px; padding:8px 12px; text-align:center; }
.sv { font-size:18px; font-weight:700; color:#409eff; }
.sk { font-size:10px; color:#909399; margin-top:1px; }
.ss-btn { text-align:center; }

/* 控制台 */
.ctrl-body { padding:12px 16px 14px; border-top:1px solid #f0f0f0; display:flex; flex-direction:column; align-items:center; gap:10px; }
.ctrl-row { display:flex; gap:8px; flex-wrap:wrap; justify-content:center; }
.ctrl-hint { font-size:12px; color:#909399; text-align:center; }

/* KPI 条 */
.kpi-row { display:grid; grid-template-columns:repeat(6,1fr); gap:10px; }
.kpi { background:#fff; border-radius:8px; padding:12px 14px; box-shadow:0 1px 4px rgba(0,0,0,.06); }
.kv { font-size:20px; font-weight:700; }
.kl { font-size:11px; color:#909399; margin-top:3px; }

/* Tab */
.tabs-card { padding:16px; }

/* 机器选择 */
.machine-chips { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:14px; }
.mchip { display:flex; align-items:center; gap:6px; padding:8px 14px; border-radius:8px; cursor:pointer; border:2px solid #ebeef5; background:#fafafa; transition:all .15s; user-select:none; }
.mchip:hover { border-color:#409eff; }
.mchip.sel   { border-color:#409eff; background:#ecf5ff; }
.mchip.green { border-left:3px solid #67c23a; }
.mchip.yellow{ border-left:3px solid #e6a23c; }
.mchip.red   { border-left:3px solid #f56c6c; }
.mchip-dot   { width:8px; height:8px; border-radius:50%; flex-shrink:0; }
.mchip-dot.green  { background:#67c23a; }
.mchip-dot.yellow { background:#e6a23c; }
.mchip-dot.red    { background:#f56c6c; }
.mchip-id  { font-size:13px; font-weight:600; color:#303133; }
.mchip-oee { font-size:12px; color:#909399; }

/* 设备详情 */
.mdetail    { border:1px solid #ebeef5; border-radius:8px; padding:14px; }
.mdetail-hd { display:flex; align-items:center; gap:8px; margin-bottom:12px; }
.mdetail-id { font-size:16px; font-weight:700; color:#1a1a1a; }
.mdetail-type { font-size:12px; color:#909399; }

/* 6效能卡 */
.perf-grid { display:grid; grid-template-columns:repeat(6,1fr); gap:10px; margin-bottom:14px; }
.perf-card { background:#f5f7fa; border-radius:6px; padding:10px 12px; }
.pc-label  { font-size:11px; color:#909399; margin-bottom:4px; }
.pc-val    { font-size:18px; font-weight:700; color:#303133; margin-bottom:6px; }

/* 50指标分区 */
.metrics-sections { display:grid; grid-template-columns:repeat(5,1fr); gap:12px; margin-bottom:14px; }
.ms-section { background:#f9fafc; border-radius:6px; padding:10px 12px; }
.ms-title   { font-size:12px; font-weight:600; color:#606266; margin-bottom:8px; }
.ms-grid    { display:flex; flex-direction:column; gap:5px; }
.ms-item    { display:flex; align-items:baseline; gap:4px; }
.mi-label   { font-size:11px; color:#909399; flex:1; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.mi-val     { font-size:12px; font-weight:600; color:#303133; white-space:nowrap; }
.mi-unit    { font-size:9px; color:#c0c4cc; }

/* 趋势 */
.trend-title { font-size:12px; font-weight:600; color:#606266; margin-bottom:6px; }
.chart-h200  { height:200px; }
.chart-h240  { height:240px; }
.chart-h260  { height:260px; }

/* 双列布局 */
.two-col { display:grid; grid-template-columns:1fr 1fr; gap:14px; }
.ct { font-size:13px; font-weight:600; color:#303133; margin-bottom:8px; }

/* 颜色 */
.grn { color:#67c23a !important; }
.yel { color:#e6a23c !important; }
.red { color:#f56c6c !important; }
.fw  { font-weight:600; }
.rt  { color:#f56c6c; }
.yw  { color:#e6a23c; }

/* 空状态 */
.empty-state { text-align:center; padding:70px 0; }
</style>
