<template>
  <div class="sec-wrap">
    <div class="card collapse-card">
      <div class="collapse-hd" @click="archOpen = !archOpen">
        <span>📈</span>
        <span class="ch-title">券商经营与风控 · Doris 实时数仓链路</span>
        <span class="badge green">UNIQUE KEY 严谨模型</span>
        <span class="badge orange">经纪 + 财富 + 两融</span>
        <span class="ch-tog">{{ archOpen ? '▲' : '▼' }}</span>
      </div>
      <div v-show="archOpen" class="arch-body">
        <div class="arch-flow">
          <div class="arch-node"><div class="ni">🧾</div><div class="nl">委托成交</div><div class="ns">APP / 投顾终端 / 柜台</div></div>
          <div class="flow-pipe"><div class="flow-beam"></div></div>
          <div class="arch-node"><div class="ni">📊</div><div class="nl">行情分钟流</div><div class="ns">个股 / 板块 / 净流入</div></div>
          <div class="flow-pipe"><div class="flow-beam"></div></div>
          <div class="arch-node doris"><div class="ni">⚡</div><div class="nl">Doris</div><div class="ns">6张 UNIQUE KEY 表</div></div>
          <div class="flow-pipe"><div class="flow-beam"></div></div>
          <div class="arch-node"><div class="ni">🔍</div><div class="nl">实时分析</div><div class="ns">营业部 / 客户 / 风险</div></div>
          <div class="flow-pipe"><div class="flow-beam"></div></div>
          <div class="arch-node viz"><div class="ni">🖥️</div><div class="nl">演示看板</div><div class="ns">简洁直观可共鸣</div></div>
        </div>
        <div class="schema-row">
          <div class="schema-box">
            <div class="schema-lbl">核心表模型</div>
            <pre class="schema-code">sec_account_snapshot   UNIQUE KEY(account_id)        -- 客户最新资产/两融快照
sec_position_snapshot  UNIQUE KEY(account_id,symbol) -- 客户最新持仓快照
sec_risk_snapshot      UNIQUE KEY(alert_id)          -- 当前风险告警
sec_market_minute      UNIQUE KEY(ts,symbol)         -- 分钟行情与净流入
sec_trade_detail       UNIQUE KEY(trade_id)          -- 成交明细
sec_branch_metrics     UNIQUE KEY(ts,branch_id)      -- 营业部分钟经营指标
            </pre>
          </div>
          <div class="schema-stats">
            <div class="ss"><div class="sv">{{ overview.account_cnt || 24 }}</div><div class="sk">客户账户</div></div>
            <div class="ss"><div class="sv">6表</div><div class="sk">严谨模型</div></div>
            <div class="ss"><div class="sv">4家</div><div class="sk">营业部</div></div>
            <div class="ss"><div class="sv">12只</div><div class="sk">核心股票</div></div>
            <div class="ss-btn">
              <el-button type="primary" size="small" :loading="loading" @click="doInit">一键初始化演示</el-button>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div v-if="overview.empty" class="card empty-card">
      <div class="empty-icon">🏦</div>
      <div class="empty-title">证券实时数仓尚未初始化</div>
      <div class="empty-sub">点击上方「一键初始化演示」后即可看到真实业务风格的账户、成交、持仓与两融预警。</div>
    </div>

    <template v-else>
      <div class="kpi-row">
        <div class="kpi" v-for="k in kpis" :key="k.label">
          <div class="kv" :style="{ color: k.color }">{{ k.value }}</div>
          <div class="kl">{{ k.label }}</div>
          <div class="ks">{{ k.sub }}</div>
        </div>
      </div>

      <div class="card filter-card">
        <div class="filter-left">
          <el-select v-model="branchFilter" size="small" clearable placeholder="营业部" style="width:180px">
            <el-option v-for="b in branchOptions" :key="b" :label="b" :value="b" />
          </el-select>
          <el-date-picker
            v-model="localTimeRange"
            type="datetimerange"
            size="small"
            unlink-panels
            start-placeholder="开始时间"
            end-placeholder="结束时间"
            style="width:360px"
          />
        </div>
      </div>

      <div class="card tabs-card">
        <el-tabs v-model="activeTab">
          <el-tab-pane label="经营总览" name="overview" lazy>
            <div class="two-col">
              <div>
                <div class="ct">分钟成交趋势</div>
                <v-chart :option="trendOption" class="chart-h260" autoresize />
              </div>
              <div>
                <div class="ct">客户层级分布</div>
                <v-chart :option="tierOption" class="chart-h260" autoresize />
              </div>
            </div>
            <div class="ct mt14">营业部实时经营</div>
            <el-table :data="filteredBranches" border stripe size="small" style="margin-top:8px">
              <el-table-column prop="branch_name" label="营业部" min-width="140" />
              <el-table-column label="成交额(亿)" width="100" align="right">
                <template #default="{ row }">{{ fmtYi(row.turnover_amt) }}</template>
              </el-table-column>
              <el-table-column label="净流入(万)" width="110" align="right">
                <template #default="{ row }">
                  <span :class="numCls(row.net_inflow_amt)">{{ fmtWan(row.net_inflow_amt) }}</span>
                </template>
              </el-table-column>
              <el-table-column prop="active_clients" label="活跃客户" width="90" align="right" />
              <el-table-column prop="margin_clients" label="两融客户" width="90" align="right" />
              <el-table-column prop="avg_maintenance" label="平均维保比" width="100" align="right" />
              <el-table-column prop="phase_name" label="阶段" width="90" />
            </el-table>
          </el-tab-pane>

          <el-tab-pane label="实时成交" name="trades" lazy>
            <el-table :data="filteredTrades" border stripe size="small">
              <el-table-column prop="ts" label="成交时间" width="160" />
              <el-table-column prop="branch_name" label="营业部" width="140" />
              <el-table-column prop="client_name" label="客户" width="80" />
              <el-table-column prop="security_name" label="证券" width="110" />
              <el-table-column prop="sector_name" label="板块" width="100" />
              <el-table-column prop="side" label="方向" width="70">
                <template #default="{ row }">
                  <span :class="row.side === '买入' ? 'up' : 'down'">{{ row.side }}</span>
                </template>
              </el-table-column>
              <el-table-column prop="price" label="价格" width="80" align="right" />
              <el-table-column prop="qty" label="数量" width="90" align="right" />
              <el-table-column label="成交额(万)" width="100" align="right">
                <template #default="{ row }">{{ fmtWan(row.amount) }}</template>
              </el-table-column>
              <el-table-column prop="rm_name" label="客户经理" width="90" />
              <el-table-column prop="channel" label="渠道" width="90" />
            </el-table>
          </el-tab-pane>

          <el-tab-pane label="客户资产" name="accounts" lazy>
            <el-table :data="filteredAccounts" border stripe size="small">
              <el-table-column prop="account_id" label="账户" width="88" />
              <el-table-column prop="client_name" label="客户" width="80" />
              <el-table-column prop="branch_name" label="营业部" width="140" />
              <el-table-column prop="client_tier" label="层级" width="80" />
              <el-table-column prop="rm_name" label="客户经理" width="90" />
              <el-table-column label="总资产(万)" width="100" align="right">
                <template #default="{ row }">{{ row.total_asset_wan }}</template>
              </el-table-column>
              <el-table-column label="持仓市值(万)" width="110" align="right">
                <template #default="{ row }">{{ row.market_value_wan }}</template>
              </el-table-column>
              <el-table-column label="浮盈亏(万)" width="100" align="right">
                <template #default="{ row }">
                  <span :class="numCls(row.unrealized_pnl_wan)">{{ row.unrealized_pnl_wan }}</span>
                </template>
              </el-table-column>
              <el-table-column prop="maintenance_ratio" label="维保比" width="90" align="right" />
              <el-table-column prop="concentration_pct" label="集中度%" width="90" align="right" />
              <el-table-column prop="trade_count_today" label="成交笔数" width="90" align="right" />
            </el-table>
          </el-tab-pane>

          <el-tab-pane label="持仓热度" name="positions" lazy>
            <div class="two-col">
              <div>
                <div class="ct">板块热度</div>
                <v-chart :option="sectorOption" class="chart-h260" autoresize />
              </div>
              <div>
                <div class="ct">板块持仓市值</div>
                <el-table :data="sectorHeat" border stripe size="small" style="margin-top:8px">
                  <el-table-column prop="sector_name" label="板块" min-width="100" />
                  <el-table-column prop="avg_change_pct" label="平均涨跌%" width="100" align="right" />
                  <el-table-column prop="turnover_bn" label="成交额(亿)" width="100" align="right" />
                  <el-table-column prop="net_inflow_bn" label="净流入(亿)" width="100" align="right">
                    <template #default="{ row }">
                      <span :class="numCls(row.net_inflow_bn)">{{ row.net_inflow_bn }}</span>
                    </template>
                  </el-table-column>
                </el-table>
              </div>
            </div>
            <div class="ct mt14">重点持仓</div>
            <el-table :data="filteredTopPositions" border stripe size="small" style="margin-top:8px">
              <el-table-column prop="account_id" label="账户" width="88" />
              <el-table-column prop="branch_name" label="营业部" width="140" />
              <el-table-column prop="security_name" label="证券" width="110" />
              <el-table-column prop="sector_name" label="板块" width="100" />
              <el-table-column prop="qty" label="持仓" width="90" align="right" />
              <el-table-column label="市值(万)" width="90" align="right">
                <template #default="{ row }">{{ row.market_value_wan }}</template>
              </el-table-column>
              <el-table-column label="浮盈亏(万)" width="100" align="right">
                <template #default="{ row }">
                  <span :class="numCls(row.pnl_wan)">{{ row.pnl_wan }}</span>
                </template>
              </el-table-column>
              <el-table-column prop="weight_pct" label="权重%" width="90" align="right" />
            </el-table>
          </el-tab-pane>

          <el-tab-pane label="风险预警" name="risk" lazy>
            <el-table :data="filteredRisks" border stripe size="small">
              <el-table-column prop="branch_name" label="营业部" width="140" />
              <el-table-column prop="client_name" label="客户" width="80" />
              <el-table-column prop="alert_type" label="预警类型" width="120" />
              <el-table-column prop="risk_level" label="等级" width="70">
                <template #default="{ row }">
                  <el-tag :type="riskTag(row.risk_level)" size="small">{{ row.risk_level }}</el-tag>
                </template>
              </el-table-column>
              <el-table-column label="指标值" width="90" align="right">
                <template #default="{ row }">{{ row.metric_value }}</template>
              </el-table-column>
              <el-table-column label="阈值" width="90" align="right">
                <template #default="{ row }">{{ row.threshold_value }}</template>
              </el-table-column>
              <el-table-column label="重点证券" width="120">
                <template #default="{ row }">{{ row.position_name || '-' }}</template>
              </el-table-column>
              <el-table-column prop="suggestion" label="建议动作" min-width="220" />
              <el-table-column prop="update_ts" label="刷新时间" width="160" />
            </el-table>
          </el-tab-pane>
        </el-tabs>
      </div>
    </template>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import dayjs from 'dayjs'
import { ElMessage } from 'element-plus'
import VChart from 'vue-echarts'
import { securitiesApi } from '@/api'

const archOpen = ref(false)
const ctrlOpen = ref(true)
const loading = ref(false)
const activeTab = ref('overview')
const overview = ref({ empty: true, tier_dist: [] })
const trend = ref([])
const trades = ref([])
const accounts = ref([])
const positions = ref({ sector_stats: [], top_positions: [] })
const sectorHeat = ref([])
const risks = ref([])
const branches = ref([])
const branchFilter = ref('')
const localTimeRange = ref([new Date('2025-03-03 09:25:00'), new Date('2025-03-03 15:00:00')])
const loadedTabs = ref(new Set(['overview']))

const branchOptions = computed(() => [...new Set(branches.value.map(i => i.branch_name))])
const phaseColor = computed(() => {
  const p = overview.value.phase_name || ''
  if (p.includes('拉升')) return 'green'
  if (p.includes('风控')) return 'red'
  if (p.includes('震荡')) return 'yellow'
  return 'blue'
})

const inLocalRange = ts => {
  if (!localTimeRange.value?.length || !ts) return true
  const t = new Date(ts).getTime()
  const [s, e] = localTimeRange.value
  return t >= new Date(s).getTime() && t <= new Date(e).getTime()
}

const filteredTrades = computed(() =>
  trades.value.filter(i => (!branchFilter.value || i.branch_name === branchFilter.value) && inLocalRange(i.ts))
)
const filteredBranches = computed(() =>
  branches.value.filter(i => !branchFilter.value || i.branch_name === branchFilter.value)
)
const filteredAccounts = computed(() =>
  accounts.value.filter(i => !branchFilter.value || i.branch_name === branchFilter.value)
)
const filteredTopPositions = computed(() =>
  positions.value.top_positions.filter(i => (!branchFilter.value || i.branch_name === branchFilter.value))
)
const filteredRisks = computed(() =>
  risks.value.filter(i => (!branchFilter.value || i.branch_name === branchFilter.value))
)

const kpis = computed(() => [
  {
    label: '客户总AUM',
    value: `${fmtYi(overview.value.total_aum)}亿`,
    sub: `${overview.value.account_cnt || 0} 个客户账户`,
    color: '#409eff',
  },
  {
    label: '当日成交额',
    value: `${fmtYi(overview.value.turnover_today)}亿`,
    sub: `${overview.value.trade_count || 0} 笔成交`,
    color: '#67c23a',
  },
  {
    label: '净佣金收入',
    value: `${fmtWan(overview.value.commission_income)}万`,
    sub: `${overview.value.top_branch || '-'} 领先`,
    color: '#e6a23c',
  },
  {
    label: '实时风险预警',
    value: `${overview.value.risk_alert_cnt || 0}`,
    sub: `${overview.value.margin_account_cnt || 0} 个两融账户`,
    color: '#f56c6c',
  },
  {
    label: '热点板块',
    value: overview.value.hot_sector || '-',
    sub: `净流入 ${fmtYi(overview.value.hot_sector_inflow)} 亿`,
    color: '#7a5af8',
  },
  {
    label: '平均维保比',
    value: overview.value.avg_maintenance || '-',
    sub: `${overview.value.concentration_alert_cnt || 0} 个集中度预警`,
    color: '#303133',
  },
])

const trendOption = computed(() => ({
  tooltip: { trigger: 'axis' },
  legend: { top: 0, data: ['成交额(亿)', '活跃客户'] },
  grid: { left: 45, right: 20, top: 36, bottom: 30 },
  xAxis: { type: 'category', data: trend.value.filter(i => inLocalRange(i.ts)).map(i => dayjs(i.ts).format('HH:mm')) },
  yAxis: [{ type: 'value' }, { type: 'value' }],
  series: [
    {
      name: '成交额(亿)',
      type: 'line',
      smooth: true,
      data: trend.value.filter(i => inLocalRange(i.ts)).map(i => i.turnover_bn),
      lineStyle: { color: '#409eff' },
      itemStyle: { color: '#409eff' },
      areaStyle: { opacity: 0.14 },
    },
    {
      name: '活跃客户',
      type: 'bar',
      yAxisIndex: 1,
      data: trend.value.filter(i => inLocalRange(i.ts)).map(i => i.active_clients),
      itemStyle: { color: '#67c23a' },
    },
  ],
}))

const tierOption = computed(() => ({
  tooltip: { trigger: 'item' },
  legend: { bottom: 0, left: 'center' },
  series: [{
    type: 'pie',
    radius: ['42%', '68%'],
    center: ['50%', '42%'],
    data: (overview.value.tier_dist || []).map(i => ({ name: i.client_tier, value: i.cnt })),
    label: { formatter: '{b}: {d}%' },
  }],
}))

const sectorOption = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: 45, right: 18, top: 20, bottom: 40 },
  xAxis: { type: 'category', axisLabel: { rotate: 25 }, data: sectorHeat.value.map(i => i.sector_name) },
  yAxis: { type: 'value' },
  series: [{
    type: 'bar',
    data: sectorHeat.value.map(i => ({
      value: i.net_inflow_bn,
      itemStyle: { color: i.net_inflow_bn >= 0 ? '#f56c6c' : '#67c23a' },
    })),
    label: { show: true, position: 'top' },
  }],
}))

const fmtYi = v => (Number(v || 0) / 100000000).toFixed(2)
const fmtWan = v => (Number(v || 0) / 10000).toFixed(2)
const numCls = v => Number(v) >= 0 ? 'up' : 'down'
const riskTag = v => v === '高' ? 'danger' : v === '中' ? 'warning' : 'info'

async function loadAll() {
  const [ov, tr, br] = await Promise.all([
    securitiesApi.overview(),
    securitiesApi.trend(),
    securitiesApi.branches(),
  ])
  overview.value = ov || { empty: true, tier_dist: [] }
  trend.value = tr || []
  branches.value = br || []
  if (trend.value.length) {
    localTimeRange.value = [new Date(trend.value[0].ts), new Date(trend.value[trend.value.length - 1].ts)]
  }
}

async function loadTabData(tab) {
  if (tab === 'trades' && !loadedTabs.value.has('trades')) {
    trades.value = await securitiesApi.trades(50)
    loadedTabs.value.add('trades')
  }
  if (tab === 'accounts' && !loadedTabs.value.has('accounts')) {
    accounts.value = await securitiesApi.accounts()
    loadedTabs.value.add('accounts')
  }
  if (tab === 'positions' && !loadedTabs.value.has('positions')) {
    const [po, sh] = await Promise.all([securitiesApi.positions(), securitiesApi.sectorHeat()])
    positions.value = po || { sector_stats: [], top_positions: [] }
    sectorHeat.value = sh || []
    loadedTabs.value.add('positions')
  }
  if (tab === 'risk' && !loadedTabs.value.has('risk')) {
    risks.value = await securitiesApi.riskAlerts()
    loadedTabs.value.add('risk')
  }
}

async function refreshLoadedTabs() {
  const tasks = []
  if (loadedTabs.value.has('trades')) tasks.push(securitiesApi.trades(50).then(res => { trades.value = res || [] }))
  if (loadedTabs.value.has('accounts')) tasks.push(securitiesApi.accounts().then(res => { accounts.value = res || [] }))
  if (loadedTabs.value.has('positions')) {
    tasks.push(securitiesApi.positions().then(res => { positions.value = res || { sector_stats: [], top_positions: [] } }))
    tasks.push(securitiesApi.sectorHeat().then(res => { sectorHeat.value = res || [] }))
  }
  if (loadedTabs.value.has('risk')) tasks.push(securitiesApi.riskAlerts().then(res => { risks.value = res || [] }))
  await Promise.all(tasks)
}

async function withLoading(fn, successText) {
  loading.value = true
  try {
    await fn()
    await loadAll()
    await refreshLoadedTabs()
    if (successText) ElMessage.success(successText)
  } finally {
    loading.value = false
  }
}

function doInit() {
  return withLoading(async () => {
    await securitiesApi.init()
    await securitiesApi.batch(18)
  }, '证券演示数据已初始化')
}

function doStep() {
  return withLoading(async () => {
    await securitiesApi.generate()
  })
}

function doBatch(steps) {
  return withLoading(async () => {
    await securitiesApi.batch(steps)
  })
}

function doReset() {
  return withLoading(async () => {
    await securitiesApi.reset()
    await securitiesApi.batch(12)
  }, '证券沙盘已重置')
}

watch(activeTab, tab => {
  loadTabData(tab)
}, { immediate: true })

onMounted(loadAll)
</script>

<style scoped>
.sec-wrap { display: flex; flex-direction: column; gap: 16px; }
.collapse-card, .tabs-card, .filter-card { padding: 0; overflow: hidden; }
.collapse-hd {
  display: flex; align-items: center; gap: 8px; padding: 14px 16px; cursor: pointer;
  border-bottom: 1px solid #f0f0f0; font-size: 13px; color: #303133;
}
.ch-title { font-size: 15px; font-weight: 700; color: #1f2937; }
.ch-tog { margin-left: auto; color: #909399; }
.badge {
  font-size: 11px; padding: 3px 8px; border-radius: 999px; color: #fff;
}
.badge.blue { background: #409eff; }
.badge.green { background: #67c23a; }
.badge.orange { background: #e6a23c; }
.arch-body, .ctrl-body { padding: 16px; }
.arch-flow {
  display: grid; grid-template-columns: 1fr 40px 1fr 40px 1fr 40px 1fr 40px 1fr;
  align-items: center; gap: 0; margin-bottom: 16px;
}
.arch-node {
  min-height: 90px; border-radius: 12px; background: linear-gradient(135deg, #f8fbff, #eef5ff);
  border: 1px solid #dbeafe; display: flex; flex-direction: column; justify-content: center; align-items: center;
  text-align: center; padding: 8px;
}
.arch-node.doris { background: linear-gradient(135deg, #fff8eb, #fff1cf); border-color: #fde2a7; }
.arch-node.viz { background: linear-gradient(135deg, #f4fbf7, #e8f7ef); border-color: #cdeed8; }
.ni { font-size: 24px; margin-bottom: 6px; }
.nl { font-size: 13px; font-weight: 700; color: #1f2937; }
.ns { font-size: 11px; color: #6b7280; margin-top: 4px; }
.flow-pipe { position: relative; height: 2px; background: linear-gradient(90deg, #bcd7ff, #ffd07f); }
.flow-beam { position: absolute; inset: -3px 0; background: transparent; }
.schema-row { display: grid; grid-template-columns: 1fr 240px; gap: 16px; }
.schema-box {
  background: #0f172a; color: #d7e3ff; border-radius: 12px; padding: 14px 16px;
}
.schema-lbl { font-size: 13px; color: #f8fafc; margin-bottom: 8px; font-weight: 700; }
.schema-code { white-space: pre-wrap; font-size: 12px; line-height: 1.6; font-family: Menlo, monospace; }
.schema-stats {
  display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; align-content: start;
}
.ss, .ss-btn {
  background: #f8fafc; border: 1px solid #edf2f7; border-radius: 12px; padding: 14px; text-align: center;
}
.sv { font-size: 22px; font-weight: 700; color: #111827; }
.sk { font-size: 12px; color: #6b7280; margin-top: 4px; }
.ss-btn { grid-column: 1 / -1; }
.sdot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.sdot.blue { background: #409eff; }
.sdot.green { background: #67c23a; box-shadow: 0 0 6px #67c23a; }
.sdot.yellow { background: #e6a23c; box-shadow: 0 0 6px #e6a23c; }
.sdot.red { background: #f56c6c; box-shadow: 0 0 6px #f56c6c; }
.ch-sim { font-size: 12px; color: #6b7280; }
.ctrl-row { display: flex; gap: 12px; align-items: center; margin-bottom: 8px; }
.ctrl-hint { font-size: 12px; color: #6b7280; }
.empty-card { text-align: center; padding: 40px 20px; }
.empty-icon { font-size: 44px; margin-bottom: 10px; }
.empty-title { font-size: 16px; font-weight: 700; color: #1f2937; margin-bottom: 8px; }
.empty-sub { font-size: 13px; color: #6b7280; line-height: 1.7; }
.kpi-row {
  display: grid; grid-template-columns: repeat(6, 1fr); gap: 14px;
}
.kpi {
  background: #fff; border-radius: 12px; padding: 16px; box-shadow: 0 1px 3px rgba(0,0,0,.06);
}
.kv { font-size: 22px; font-weight: 700; }
.kl { font-size: 12px; color: #6b7280; margin-top: 6px; }
.ks { font-size: 11px; color: #9ca3af; margin-top: 4px; }
.filter-card {
  display: flex; justify-content: space-between; align-items: center; padding: 14px 16px;
}
.filter-left { display: flex; gap: 10px; align-items: center; }
.filter-tip { font-size: 12px; color: #909399; }
.two-col { display: grid; grid-template-columns: 1.2fr 1fr; gap: 16px; }
.ct { font-size: 14px; font-weight: 700; color: #1f2937; margin-bottom: 8px; }
.chart-h260 { height: 260px; }
.mt14 { margin-top: 14px; }
.up { color: #f56c6c; font-weight: 600; }
.down { color: #67c23a; font-weight: 600; }
:deep(.el-tabs__header) { padding: 0 16px 0 20px; margin-bottom: 0; }
:deep(.el-tabs__content) { padding: 16px 16px 16px 20px; }
@media (max-width: 1200px) {
  .kpi-row { grid-template-columns: repeat(3, 1fr); }
  .schema-row, .two-col { grid-template-columns: 1fr; }
  .arch-flow { grid-template-columns: 1fr; gap: 10px; }
  .flow-pipe { display: none; }
  .filter-card { flex-direction: column; align-items: flex-start; gap: 8px; }
}
@media (max-width: 768px) {
  .kpi-row { grid-template-columns: repeat(2, 1fr); }
  .filter-left { flex-direction: column; align-items: stretch; width: 100%; }
}
</style>
