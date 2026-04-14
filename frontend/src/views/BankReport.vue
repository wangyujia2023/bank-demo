<template>
  <div>
    <el-tabs v-model="tab" type="border-card">

      <!-- ═══ 业务概况 ═══ -->
      <el-tab-pane label="业务概况" name="overview">
        <div class="stat-row" v-loading="loading">
          <div class="stat-card"><div class="stat-label">用户总数</div><div class="stat-value" style="color:#409eff">{{ fmt(ov.stats.total_users) }}</div><div class="stat-sub">全量用户</div></div>
          <div class="stat-card"><div class="stat-label">总 AUM（万）</div><div class="stat-value" style="color:#67c23a">{{ fmtMoney(ov.stats.total_aum) }}</div><div class="stat-sub">平均 {{ fmtMoney(ov.stats.avg_aum) }} 万</div></div>
          <div class="stat-card"><div class="stat-label">异常用户</div><div class="stat-value" style="color:#f56c6c">{{ fmt(ov.stats.anomaly_users) }}</div><div class="stat-sub">平均流失率 {{ ov.stats.avg_churn_pct }}%</div></div>
          <div class="stat-card"><div class="stat-label">覆盖城市</div><div class="stat-value" style="color:#e6a23c">{{ fmt(ov.stats.city_count) }}</div><div class="stat-sub">新客 {{ fmt(ov.stats.new_users) }} 人</div></div>
        </div>
        <el-row :gutter="16">
          <el-col :span="14">
            <div class="card">
              <div class="card-title">资产等级分布</div>
              <v-chart :option="assetBarOpt" style="height:280px" autoresize />
            </div>
          </el-col>
          <el-col :span="10">
            <div class="card">
              <div class="card-title">生命周期占比</div>
              <v-chart :option="lifecyclePieOpt" style="height:280px" autoresize />
            </div>
          </el-col>
        </el-row>
        <el-row :gutter="16">
          <el-col :span="12">
            <div class="card">
              <div class="card-title">活跃度分布</div>
              <v-chart :option="activeBarOpt" style="height:200px" autoresize />
            </div>
          </el-col>
          <el-col :span="12">
            <div class="card">
              <div class="card-title">城市 TOP10 AUM</div>
              <el-table :data="ov.city_dist" size="small" stripe>
                <el-table-column prop="city" label="城市" width="80" />
                <el-table-column prop="user_cnt" label="用户数" width="75" align="right" />
                <el-table-column label="AUM(万)">
                  <template #default="{row}">
                    <el-progress :percentage="Math.round(row.total_aum/maxCityAum*100)" :stroke-width="8" :show-text="false" />
                    <span style="font-size:11px;color:#606266">{{ fmtMoney(row.total_aum) }}</span>
                  </template>
                </el-table-column>
                <el-table-column prop="anomaly" label="异常" width="60" align="center">
                  <template #default="{row}">
                    <el-tag v-if="row.anomaly>0" type="danger" size="small">{{ row.anomaly }}</el-tag>
                    <span v-else style="color:#67c23a">0</span>
                  </template>
                </el-table-column>
              </el-table>
            </div>
          </el-col>
        </el-row>
      </el-tab-pane>

      <!-- ═══ 交易报表 ═══ -->
      <el-tab-pane label="交易报表" name="tx">
        <el-row :gutter="16" v-loading="txLoading">
          <el-col :span="14">
            <div class="card">
              <div class="card-title">渠道交易金额分布</div>
              <v-chart :option="channelBarOpt" style="height:280px" autoresize />
            </div>
          </el-col>
          <el-col :span="10">
            <div class="card">
              <div class="card-title">渠道明细</div>
              <el-table :data="tx.channel_stats" size="small" stripe border>
                <el-table-column prop="channel" label="渠道" width="90" />
                <el-table-column prop="tx_count" label="笔数" align="right" width="70" />
                <el-table-column label="总金额(万)" align="right">
                  <template #default="{row}">{{ fmtMoney(row.total_amount) }}</template>
                </el-table-column>
                <el-table-column label="均单(万)" align="right">
                  <template #default="{row}">{{ fmtMoney(row.avg_amount) }}</template>
                </el-table-column>
              </el-table>
            </div>
          </el-col>
        </el-row>
        <el-row :gutter="16">
          <el-col :span="12">
            <div class="card">
              <div class="card-title">事件类型分布</div>
              <v-chart :option="eventBarOpt" style="height:220px" autoresize />
            </div>
          </el-col>
          <el-col :span="12">
            <div class="card">
              <div class="card-title">按小时交易量趋势</div>
              <v-chart :option="hourlyLineOpt" style="height:220px" autoresize />
            </div>
          </el-col>
        </el-row>
      </el-tab-pane>

      <!-- ═══ 风险报表 ═══ -->
      <el-tab-pane label="风险报表" name="risk">
        <div class="stat-row" v-loading="riskLoading">
          <div class="stat-card"><div class="stat-label">异常用户数</div><div class="stat-value" style="color:#f56c6c">{{ fmt(rk.risk_stats.anomaly_count) }}</div><div class="stat-sub">占比 {{ riskRate }}%</div></div>
          <div class="stat-card"><div class="stat-label">异常用户AUM(万)</div><div class="stat-value" style="color:#e6a23c">{{ fmtMoney(rk.risk_stats.anomaly_aum) }}</div><div class="stat-sub">需重点管控</div></div>
          <div class="stat-card"><div class="stat-label">平均流失率</div><div class="stat-value" style="color:#f56c6c">{{ rk.risk_stats.avg_churn_pct }}%</div><div class="stat-sub">全量用户</div></div>
          <div class="stat-card"><div class="stat-label">总用户数</div><div class="stat-value" style="color:#409eff">{{ fmt(rk.risk_stats.total) }}</div><div class="stat-sub">分析基数</div></div>
        </div>
        <el-row :gutter="16">
          <el-col :span="14">
            <div class="card">
              <div class="card-title">各资产等级异常率 & 流失率</div>
              <el-table :data="rk.risk_by_asset" border stripe size="small">
                <el-table-column prop="asset_level" label="资产等级" width="100">
                  <template #default="{row}"><el-tag :type="assetType(row.asset_level)" size="small">{{ row.asset_level }}</el-tag></template>
                </el-table-column>
                <el-table-column prop="total" label="用户数" width="75" align="right" />
                <el-table-column prop="anomaly" label="异常数" width="75" align="right">
                  <template #default="{row}"><span style="color:#f56c6c;font-weight:600">{{ row.anomaly }}</span></template>
                </el-table-column>
                <el-table-column label="异常率" width="130">
                  <template #default="{row}">
                    <el-progress :percentage="+row.anomaly_rate" :stroke-width="8" :color="row.anomaly_rate>30?'#f56c6c':'#e6a23c'" />
                  </template>
                </el-table-column>
                <el-table-column prop="avg_churn" label="流失率%" width="80" align="right">
                  <template #default="{row}"><span :style="{color:row.avg_churn>30?'#f56c6c':'#e6a23c',fontWeight:'600'}">{{ row.avg_churn }}%</span></template>
                </el-table-column>
              </el-table>
            </div>
          </el-col>
          <el-col :span="10">
            <div class="card">
              <div class="card-title">信用等级分布</div>
              <v-chart :option="creditPieOpt" style="height:260px" autoresize />
            </div>
          </el-col>
        </el-row>
        <div class="card">
          <div class="card-title">高风险城市 TOP10</div>
          <v-chart :option="riskCityOpt" style="height:200px" autoresize />
        </div>
      </el-tab-pane>

    </el-tabs>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { BarChart, PieChart, LineChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import { reportApi } from '@/api'

use([CanvasRenderer, BarChart, PieChart, LineChart, GridComponent, TooltipComponent, LegendComponent])

const tab = ref('overview')
const loading = ref(false), txLoading = ref(false), riskLoading = ref(false)

const ov = ref({ stats: {}, asset_dist: [], lifecycle_dist: [], city_dist: [], active_dist: [] })
const tx = ref({ channel_stats: [], event_dist: [], hourly_trend: [] })
const rk = ref({ risk_stats: {}, risk_by_asset: [], risk_by_city: [], credit_dist: [] })

const fmt = v => v == null ? '-' : Number(v).toLocaleString()
const fmtMoney = v => v == null ? '-' : Number(v).toFixed(1)
const assetType = l => ({'VIP私行':'danger','VIP钻石':'warning','VIP铂金':'','VIP黄金':'success'}[l]||'info')
const maxCityAum = computed(() => Math.max(...ov.value.city_dist.map(r => r.total_aum), 1))
const riskRate = computed(() => {
  const { anomaly_count, total } = rk.value.risk_stats
  return total ? ((anomaly_count / total) * 100).toFixed(1) : 0
})

// ECharts options
const COLORS = ['#409eff','#67c23a','#e6a23c','#f56c6c','#9b59b6','#1abc9c']

const assetBarOpt = computed(() => ({
  tooltip: { trigger: 'axis' },
  color: COLORS,
  grid: { left: 80, right: 60, top: 10, bottom: 30 },
  xAxis: { type: 'category', data: ov.value.asset_dist.map(r => r.asset_level) },
  yAxis: [{ type: 'value', name: '用户数' }, { type: 'value', name: 'AUM(万)', position: 'right' }],
  series: [
    { name: '用户数', type: 'bar', data: ov.value.asset_dist.map(r => r.user_cnt), itemStyle: { color: '#409eff' } },
    { name: '总AUM', type: 'bar', yAxisIndex: 1, data: ov.value.asset_dist.map(r => r.total_aum), itemStyle: { color: '#67c23a' } },
  ]
}))

const lifecyclePieOpt = computed(() => ({
  tooltip: { trigger: 'item', formatter: '{b}: {c}人 ({d}%)' },
  legend: { bottom: 0, textStyle: { fontSize: 11 } },
  series: [{ type: 'pie', radius: ['40%','65%'], data: ov.value.lifecycle_dist.map(r => ({ name: r.lifecycle_stage, value: r.cnt })), label: { formatter: '{b}\n{d}%', fontSize: 11 } }]
}))

const activeBarOpt = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: 60, right: 10, top: 10, bottom: 20 },
  xAxis: { type: 'category', data: ov.value.active_dist.map(r => r.active_level) },
  yAxis: { type: 'value' },
  series: [{ type: 'bar', data: ov.value.active_dist.map((r, i) => ({ value: r.cnt, itemStyle: { color: COLORS[i % COLORS.length] } })), label: { show: true, position: 'top', fontSize: 11 } }]
}))

const channelBarOpt = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: 80, right: 20, top: 10, bottom: 30 },
  xAxis: { type: 'category', data: tx.value.channel_stats.map(r => r.channel) },
  yAxis: [{ type: 'value', name: '总金额(万)' }, { type: 'value', name: '笔数', position: 'right' }],
  series: [
    { name: '总金额', type: 'bar', data: tx.value.channel_stats.map(r => r.total_amount), itemStyle: { color: '#409eff' } },
    { name: '笔数', type: 'bar', yAxisIndex: 1, data: tx.value.channel_stats.map(r => r.tx_count), itemStyle: { color: '#e6a23c' } },
  ]
}))

const eventBarOpt = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: 80, right: 10, top: 10, bottom: 10 },
  xAxis: { type: 'value' },
  yAxis: { type: 'category', data: tx.value.event_dist.map(r => r.event_type) },
  series: [{ type: 'bar', data: tx.value.event_dist.map((r, i) => ({ value: r.cnt, itemStyle: { color: COLORS[i%COLORS.length] } })), label: { show: true, position: 'right', fontSize: 11 } }]
}))

const hourlyLineOpt = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: 40, right: 10, top: 10, bottom: 20 },
  xAxis: { type: 'category', data: tx.value.hourly_trend.map(r => `${r.hour_val}:00`) },
  yAxis: { type: 'value' },
  series: [{ type: 'line', smooth: true, data: tx.value.hourly_trend.map(r => r.cnt), areaStyle: { opacity: 0.2 }, itemStyle: { color: '#409eff' } }]
}))

const creditPieOpt = computed(() => ({
  tooltip: { trigger: 'item', formatter: '{b}: {c}人 (均分 {d})' },
  legend: { bottom: 0, textStyle: { fontSize: 11 } },
  series: [{ type: 'pie', radius: '60%', data: rk.value.credit_dist.map(r => ({ name: r.credit_grade, value: r.cnt })), label: { formatter: '{b}\n{d}%', fontSize: 11 } }]
}))

const riskCityOpt = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: 70, right: 60, top: 10, bottom: 10 },
  xAxis: { type: 'value' },
  yAxis: { type: 'category', data: rk.value.risk_by_city.map(r => r.city) },
  series: [
    { name: '异常数', type: 'bar', data: rk.value.risk_by_city.map(r => r.anomaly_count), itemStyle: { color: '#f56c6c' }, label: { show: true, position: 'right', fontSize: 11 } },
  ]
}))

onMounted(async () => {
  loading.value = true
  ov.value = await reportApi.overview().finally(() => loading.value = false)

  txLoading.value = true
  tx.value = await reportApi.transaction().finally(() => txLoading.value = false)

  riskLoading.value = true
  rk.value = await reportApi.risk().finally(() => riskLoading.value = false)
})
</script>
