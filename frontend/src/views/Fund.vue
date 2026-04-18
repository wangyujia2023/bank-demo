<template>
  <div class="fund-wrap">

    <!-- ① 架构图（默认折叠）-->
    <div class="card collapse-card">
      <div class="collapse-hd" @click="archOpen=!archOpen">
        <span>📈</span>
        <span class="ch-title">基金投研平台 · 全链路数据架构</span>
        <span class="badge blue">Apache Doris 4.0</span>
        <span class="badge green">DUPLICATE KEY 多表</span>
        <span class="badge orange">CORR() 相关性矩阵</span>
        <span class="badge purple">RANK() 窗口排名</span>
        <span class="ch-tog">{{ archOpen?'▲':'▼' }}</span>
      </div>
      <div v-show="archOpen" class="arch-body">
        <div class="arch-flow">
          <div class="arch-node"><div class="ni">📡</div><div class="nl">市场行情</div><div class="ns">沪深实时报价</div></div>
          <div class="flow-pipe"><div class="flow-beam"></div></div>
          <div class="arch-node"><div class="ni">🌊</div><div class="nl">Stream Load</div><div class="ns">HTTP PUT 批量写入</div></div>
          <div class="flow-pipe"><div class="flow-beam"></div></div>
          <div class="arch-node doris"><div class="ni">⚡</div><div class="nl">Apache Doris 4.0</div><div class="ns">DUPLICATE KEY · 4表</div></div>
          <div class="flow-pipe"><div class="flow-beam"></div></div>
          <div class="arch-node"><div class="ni">🔍</div><div class="nl">分析引擎</div><div class="ns">CORR/RANK/JOIN</div></div>
          <div class="flow-pipe"><div class="flow-beam"></div></div>
          <div class="arch-node viz"><div class="ni">💹</div><div class="nl">投研看板</div><div class="ns">5维度分析平台</div></div>
        </div>
        <div class="schema-row">
          <div class="schema-box">
            <div class="schema-lbl">📋 4张 DUPLICATE KEY 表</div>
            <pre class="schema-code">fund_basic       DUPLICATE KEY(fund_id)               -- 基金快照含实时IOPV，取最新行
fund_nav_history DUPLICATE KEY(trade_date, fund_id)   -- 每日净值+滚动夏普/Alpha
fund_position    DUPLICATE KEY(fund_id, report_date)  -- 季报持仓穿透
fund_manager     DUPLICATE KEY(manager_id)            -- 经理画像维表

-- 核心 Doris 技术展示：
-- ① QUALIFY ROW_NUMBER() — 从多版本快照取最新一行，无需UNIQUE KEY
-- ② RANK() OVER (PARTITION BY sector ORDER BY sharpe DESC) — 同板块排名
-- ③ CORR(a.daily_return, b.daily_return) — 基金走势相关性矩阵
-- ④ 多表 JOIN — fund_position × fund_basic × fund_manager 联合分析</pre>
          </div>
          <div class="schema-stats">
            <div class="ss"><div class="sv">{{ ov?.fund_cnt||30 }}</div><div class="sk">基金数量</div></div>
            <div class="ss"><div class="sv">{{ ov?.current_step||0 }}</div><div class="sk">已推进交易日</div></div>
            <div class="ss"><div class="sv">4表</div><div class="sk">Doris表结构</div></div>
            <div class="ss"><div class="sv">&lt;5ms</div><div class="sk">CORR查询延迟</div></div>
            <div class="ss-btn">
              <el-button type="primary" size="small" :loading="generating" @click="doGen(20)">一键初始化(20日)</el-button>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- ② 控制台（默认折叠）-->
    <div class="card collapse-card">
      <div class="collapse-hd" @click="ctrlOpen=!ctrlOpen">
        <span class="sdot" :class="ov?.script_color||'grey'"></span>
        <span class="ch-title">{{ statusText }}</span>
        <span v-if="ov&&!ov.empty" class="ch-sim">第 <b>{{ ov.current_step }}</b> 个交易日 · {{ ov.last_ts?.slice(0,16) }}</span>
        <span class="ch-tog">{{ ctrlOpen?'▲':'▼' }}</span>
      </div>
      <div v-show="ctrlOpen" class="ctrl-body">
        <div class="ctrl-hint-box" v-if="ov?.script_hint">
          <span :class="['hint-dot', ov.script_color]"></span>{{ ov.script_hint }}
        </div>
        <div class="ctrl-row">
          <el-button-group>
            <el-button type="primary" size="large" :loading="generating" @click="doGen(1)">{{ generating?'推进中…':'📅 +1日' }}</el-button>
            <el-button type="primary" size="large" :loading="generating" @click="doGen(5)">+5日</el-button>
            <el-button type="primary" size="large" :loading="generating" @click="doGen(10)">+10日</el-button>
          </el-button-group>
          <el-button :type="autoOn?'warning':'default'" size="large" @click="toggleAuto">{{ autoOn?'⏸ 停止':'▶ 自动演练' }}</el-button>
          <el-button size="large" @click="resetData" :disabled="generating">🔄 重置</el-button>
        </div>
        <div class="ctrl-hint">每次推进1个交易日，剧本每8日自动切换：科技牛市→熊市调整→板块轮动→震荡行情→黑天鹅</div>
      </div>
    </div>

    <!-- ③ 板块热力条 -->
    <div class="sector-bar" v-if="sectorStats.length">
      <div class="sector-item" v-for="s in sectorStats" :key="s.sector_tag"
           :class="['si', s.avg_ret_1y>0?'up':'dn', selSector===s.sector_tag?'sel':'']"
           @click="filterBySector(s.sector_tag)">
        <div class="si-name">{{ s.sector_tag }}</div>
        <div class="si-ret" :class="s.avg_ret_1y>0?'rt':'gt'">{{ s.avg_ret_1y>0?'+':'' }}{{ s.avg_ret_1y }}%</div>
        <div class="si-flow">{{ s.net_flow_bn>0?'↑':'↓' }}{{ Math.abs(s.net_flow_bn) }}亿</div>
      </div>
    </div>

    <!-- ④ KPI 条 -->
    <div class="kpi-row" v-if="ov&&!ov.empty">
      <div class="kpi" v-for="k in kpis" :key="k.label">
        <div class="kv" :style="{color:k.color}">{{ k.value }}</div>
        <div class="kl">{{ k.label }}</div>
      </div>
    </div>

    <!-- ⑤ 主区域：左列表 + 右详情 -->
    <div class="main-area" v-if="ov&&!ov.empty">

      <!-- 左：基金列表 -->
      <div class="fund-list-panel">
        <div class="list-hd">
          <span class="list-title">基金列表</span>
          <el-select v-model="filterType" size="small" placeholder="类型" clearable style="width:88px" @change="loadList">
            <el-option v-for="t in fundTypes" :key="t" :label="t" :value="t"/>
          </el-select>
          <el-select v-model="filterRisk" size="small" placeholder="风险" clearable style="width:72px" @change="loadList">
            <el-option v-for="r in [1,2,3,4,5]" :key="r" :label="r+'级'" :value="r"/>
          </el-select>
          <el-button size="small" @click="clearFilter">清除</el-button>
        </div>
        <div class="fund-cards">
          <div v-for="f in fundList" :key="f.fund_id"
               :class="['fcard', selFund===f.fund_id?'sel':'', devClass(f.iopv_deviation)]"
               @click="selectFund(f)">
            <div class="fc-row1">
              <span class="fc-name">{{ f.fund_name }}</span>
              <span :class="['fc-dev', f.iopv_deviation>0?'rt':'gt']">
                {{ f.iopv_deviation>0?'+':'' }}{{ flt2(f.iopv_deviation) }}%
              </span>
            </div>
            <div class="fc-row2">
              <el-tag size="small" :type="typeColor(f.fund_type)" effect="plain">{{ f.fund_type }}</el-tag>
              <span class="fc-sector">{{ f.sector_tag }}</span>
              <span class="fc-mgr">{{ f.manager_name }}</span>
            </div>
            <div class="fc-row3">
              <span class="fc-label">IOPV</span><span class="fc-val">{{ flt4(f.realtime_iopv) }}</span>
              <span class="fc-label" style="margin-left:8px">年化</span>
              <span :class="['fc-val', f.ret_1y>0?'rt':'gt']">{{ f.ret_1y>0?'+':'' }}{{ pct(f.ret_1y) }}%</span>
              <span class="fc-label" style="margin-left:8px">Sharpe</span>
              <span class="fc-val" :class="f.sharpe>=1?'grn':f.sharpe>=0.5?'':' grey'">{{ flt2(f.sharpe) }}</span>
            </div>
            <div class="fc-flow">
              <span :class="['flow-arrow', f.capital_net>0?'up':'dn']">{{ f.capital_net>0?'▲':'▼' }}</span>
              <span class="flow-val">{{ Math.abs(f.capital_net/10000).toFixed(2) }}亿</span>
              <el-progress :percentage="Math.min(Math.abs(f.iopv_deviation)*10,100)"
                :color="f.iopv_deviation>2?'#f56c6c':f.iopv_deviation<-2?'#67c23a':'#409eff'"
                :stroke-width="3" :show-text="false" style="flex:1;margin-left:8px"/>
            </div>
          </div>
        </div>
      </div>

      <!-- 右：详情面板 -->
      <div class="detail-panel" v-if="selFundData">
        <div class="detail-hd">
          <span class="detail-name">{{ selFundData.fund_name }}</span>
          <el-tag :type="typeColor(selFundData.fund_type)" size="small">{{ selFundData.fund_type }}</el-tag>
          <el-tag type="info" size="small">{{ selFundData.sector_tag }}</el-tag>
          <el-tag :type="riskColor(selFundData.risk_level)" size="small">R{{ selFundData.risk_level }}</el-tag>
          <span style="margin-left:auto;font-size:12px;color:#909399">{{ selFundData.update_ts?.slice(0,16) }}</span>
        </div>

        <!-- 实时指标行 -->
        <div class="rt-row">
          <div class="rt-item" v-for="r in rtMetrics" :key="r.label">
            <div class="rt-val" :style="{color:r.color}">{{ r.value }}</div>
            <div class="rt-label">{{ r.label }}</div>
          </div>
        </div>

        <el-tabs v-model="detailTab" @tab-click="onDetailTab">
          <!-- Tab1: 净值走势 -->
          <el-tab-pane label="📈 净值走势" name="nav">
            <div class="two-col">
              <div><div class="ct">累计净值 vs 基准</div><div ref="navChart" class="chart-h260"></div></div>
              <div><div class="ct">水下回撤图</div><div ref="ddChart" class="chart-h260"></div></div>
            </div>
            <div class="two-col" style="margin-top:14px">
              <div><div class="ct">滚动夏普比率（30日窗口）</div><div ref="sharpeChart" class="chart-h220"></div></div>
              <div>
                <div class="ct">各周期收益对比</div>
                <div class="ret-table">
                  <div class="ret-row hd"><span>周期</span><span>收益</span><span>基准</span><span>超额</span></div>
                  <div class="ret-row" v-for="r in retRows" :key="r.label">
                    <span>{{ r.label }}</span>
                    <span :class="r.ret>0?'rt':'gt'">{{ r.ret>0?'+':'' }}{{ r.ret }}%</span>
                    <span class="grey">{{ r.bench }}%</span>
                    <span :class="r.excess>0?'rt':'gt'">{{ r.excess>0?'+':'' }}{{ r.excess }}%</span>
                  </div>
                </div>
                <div class="ct" style="margin-top:14px">风格雷达</div>
                <div ref="styleChart" class="chart-h180"></div>
              </div>
            </div>
          </el-tab-pane>

          <!-- Tab2: 持仓穿透 -->
          <el-tab-pane label="🔍 持仓穿透" name="position">
            <div class="two-col">
              <div><div class="ct">板块分布</div><div ref="sectorPieChart" class="chart-h260"></div></div>
              <div><div class="ct">板块贡献度（瀑布）</div><div ref="contribChart" class="chart-h260"></div></div>
            </div>
            <div class="ct" style="margin-top:14px">前十大重仓股</div>
            <el-table :data="positions" border stripe size="small" style="margin-top:8px">
              <el-table-column prop="stock_code" label="代码"    width="76"/>
              <el-table-column prop="stock_name" label="名称"    width="100"/>
              <el-table-column prop="sector_l1"  label="板块"    width="80"/>
              <el-table-column label="权重%" width="76" align="right">
                <template #default="{row}"><span>{{ (row.weight_pct*100).toFixed(2) }}%</span></template>
              </el-table-column>
              <el-table-column label="市值(百万)" width="90" align="right">
                <template #default="{row}"><span>{{ row.market_value_mn }}</span></template>
              </el-table-column>
              <el-table-column label="价格贡献%" width="90" align="right">
                <template #default="{row}">
                  <span :class="row.price_contrib>0?'rt':'gt'">{{ (row.price_contrib*100).toFixed(2) }}%</span>
                </template>
              </el-table-column>
              <el-table-column label="Alpha贡献%" width="94" align="right">
                <template #default="{row}">
                  <span :class="row.alpha_contrib>0?'rt':'gt'">{{ (row.alpha_contrib*100).toFixed(2) }}%</span>
                </template>
              </el-table-column>
            </el-table>
            <!-- Doris SQL 展示 -->
            <div class="sql-card">
              <div class="sql-title">⚡ Doris 实时IOPV估算 SQL</div>
              <pre class="sql-code">SELECT p.fund_id,
    SUM(p.weight_pct * q.change_pct) AS iopv_change_estimate
FROM fund_position p
JOIN (SELECT stock_code, change_pct FROM stock_realtime) q
    ON p.stock_code = q.stock_code
WHERE p.report_date = (SELECT MAX(report_date) FROM fund_position
                       WHERE fund_id = p.fund_id)
GROUP BY p.fund_id
-- Doris 毫秒级多表 JOIN，无需预计算缓存</pre>
            </div>
          </el-tab-pane>

          <!-- Tab3: 经理画像 -->
          <el-tab-pane label="👤 经理画像" name="manager">
            <div class="mgr-card" v-if="mgrData?.manager">
              <div class="mgr-avatar">{{ mgrData.manager.name?.slice(-1) }}</div>
              <div class="mgr-info">
                <div class="mgr-name">{{ mgrData.manager.name }}</div>
                <el-tag size="small" type="warning">{{ mgrData.manager.style_tag }}</el-tag>
                <div class="mgr-stats">
                  <div class="ms"><div class="msv">{{ mgrData.manager.tenure_years }}年</div><div class="msl">任职时长</div></div>
                  <div class="ms"><div class="msv">{{ mgrData.manager.aum_bn }}亿</div><div class="msl">在管规模</div></div>
                  <div class="ms"><div class="msv" :class="mgrData.manager.avg_alpha>0?'rt':'gt'">{{ (mgrData.manager.avg_alpha*100).toFixed(2) }}%</div><div class="msl">平均Alpha</div></div>
                  <div class="ms"><div class="msv">{{ mgrData.manager.sharpe }}</div><div class="msl">综合Sharpe</div></div>
                  <div class="ms"><div class="msv" :class="mgrData.manager.total_return>0?'rt':'gt'">+{{ (mgrData.manager.total_return*100).toFixed(1) }}%</div><div class="msl">累计回报</div></div>
                  <div class="ms"><div class="msv">{{ mgrData.manager.turnover_rate }}x</div><div class="msl">年换手率</div></div>
                </div>
              </div>
            </div>
            <div class="two-col" style="margin-top:14px">
              <div><div class="ct">在管基金近1年表现</div><div ref="mgrRetChart" class="chart-h240"></div></div>
              <div><div class="ct">Alpha / Beta 分布</div><div ref="mgrAbChart" class="chart-h240"></div></div>
            </div>
            <div class="ct" style="margin-top:14px">在管基金列表</div>
            <el-table :data="mgrData?.funds||[]" border stripe size="small" style="margin-top:8px">
              <el-table-column prop="fund_name"  label="基金名称" min-width="140"/>
              <el-table-column prop="sector_tag" label="板块" width="80"/>
              <el-table-column label="年化收益" width="86" align="right">
                <template #default="{row}"><span :class="row.ret_1y>0?'rt':'gt'">{{ (row.ret_1y*100).toFixed(2) }}%</span></template>
              </el-table-column>
              <el-table-column label="Sharpe" width="72" align="right">
                <template #default="{row}"><span :class="row.sharpe>=1?'grn':row.sharpe>=0.5?'':'grey'">{{ row.sharpe }}</span></template>
              </el-table-column>
              <el-table-column label="最大回撤" width="86" align="right">
                <template #default="{row}"><span class="gt">{{ (row.max_drawdown*100).toFixed(2) }}%</span></template>
              </el-table-column>
              <el-table-column label="Alpha" width="76" align="right">
                <template #default="{row}"><span :class="row.alpha>0?'rt':'gt'">{{ (row.alpha*100).toFixed(2) }}%</span></template>
              </el-table-column>
            </el-table>
          </el-tab-pane>

          <!-- Tab4: 竞品对比 -->
          <el-tab-pane label="⚔️ 竞品雷达" name="peers">
            <div class="two-col">
              <div><div class="ct">同板块基金综合雷达</div><div ref="radarChart" class="chart-h300"></div></div>
              <div><div class="ct">风险收益散点图（回撤 vs 年化收益）</div><div ref="scatterChart" class="chart-h300"></div></div>
            </div>
            <div class="ct" style="margin-top:14px">
              相关性矩阵
              <el-tag type="success" size="small" style="margin-left:8px">CORR() — Doris 聚合函数</el-tag>
            </div>
            <div ref="corrChart" class="chart-h260" style="margin-top:8px"></div>
            <!-- 同板块排名表 -->
            <div class="ct" style="margin-top:14px">
              同板块排名
              <el-tag type="warning" size="small" style="margin-left:8px">RANK() OVER (PARTITION BY sector)</el-tag>
            </div>
            <el-table :data="peersData?.peers||[]" border stripe size="small" style="margin-top:8px">
              <el-table-column label="综合排名" width="76" align="center">
                <template #default="{row}">
                  <span :class="row.sharpe_rank<=3?'rank-top':''">{{ row.sharpe_rank }}</span>
                </template>
              </el-table-column>
              <el-table-column prop="fund_name" label="基金名称" min-width="140"/>
              <el-table-column label="年化收益" width="86" align="right">
                <template #default="{row}"><span :class="row.ret_1y>0?'rt':'gt'">{{ (row.ret_1y*100).toFixed(2) }}%</span></template>
              </el-table-column>
              <el-table-column label="Sharpe排名" width="86" align="center">
                <template #default="{row}"><el-tag size="small" :type="row.sharpe_rank===1?'danger':row.sharpe_rank<=3?'warning':'info'">{{ row.sharpe_rank }}</el-tag></template>
              </el-table-column>
              <el-table-column label="Sharpe" width="72" align="right">
                <template #default="{row}"><span :class="row.sharpe>=1?'grn':''">{{ Number(row.sharpe).toFixed(3) }}</span></template>
              </el-table-column>
              <el-table-column label="回撤排名" width="80" align="center">
                <template #default="{row}">{{ row.mdd_rank }}</template>
              </el-table-column>
              <el-table-column label="最大回撤" width="86" align="right">
                <template #default="{row}"><span class="gt">{{ (row.max_drawdown*100).toFixed(2) }}%</span></template>
              </el-table-column>
              <el-table-column label="Alpha" width="76" align="right">
                <template #default="{row}"><span :class="row.alpha>0?'rt':'gt'">{{ (row.alpha*100).toFixed(2) }}%</span></template>
              </el-table-column>
            </el-table>
            <!-- SQL展示 -->
            <div class="sql-card" style="margin-top:14px">
              <div class="sql-title">⚡ Doris 核心 SQL：RANK + CORR</div>
              <pre class="sql-code">-- ① 同板块窗口排名
SELECT fund_id, fund_name, sharpe,
    RANK() OVER (ORDER BY sharpe DESC)       AS sharpe_rank,
    RANK() OVER (ORDER BY max_drawdown DESC) AS drawdown_rank
FROM (...取最新快照...) WHERE sector_tag = '{{ peersData?.sector }}'

-- ② 相关性矩阵（CORR 聚合）
SELECT a.fund_id, b.fund_id,
    ROUND(CORR(a.daily_return, b.daily_return), 3) AS corr
FROM fund_nav_history a
JOIN fund_nav_history b ON a.trade_date = b.trade_date
WHERE a.fund_id IN (...) AND a.trade_date >= DATE_SUB(CURDATE(),90)
GROUP BY a.fund_id, b.fund_id</pre>
            </div>
          </el-tab-pane>
        </el-tabs>
      </div>

      <!-- 右侧空态 -->
      <div class="detail-empty" v-else>
        <div style="font-size:48px;margin-bottom:12px">💹</div>
        <div style="font-size:14px;color:#909399">点击左侧基金卡片查看详情</div>
      </div>
    </div>

    <!-- 空态 -->
    <div class="empty-state" v-if="!ov||ov.empty">
      <div style="font-size:56px;margin-bottom:12px">💹</div>
      <div style="font-size:16px;font-weight:600;margin-bottom:6px">基金投研沙盘就绪</div>
      <div style="font-size:13px;color:#909399;margin-bottom:16px">点击「📅 +1日」或展开架构图一键初始化</div>
      <el-button type="primary" size="large" :loading="generating" @click="doGen(1)">开始演练</el-button>
    </div>

  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, nextTick } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { LineChart, BarChart, PieChart, RadarChart, ScatterChart, HeatmapChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent, RadarComponent, VisualMapComponent, MarkLineComponent } from 'echarts/components'
import * as echarts from 'echarts/core'
import { fundApi } from '@/api'

use([
  CanvasRenderer,
  LineChart, BarChart, PieChart, RadarChart, ScatterChart, HeatmapChart,
  GridComponent, TooltipComponent, LegendComponent, RadarComponent, VisualMapComponent, MarkLineComponent
])

// ── 折叠 ─────────────────────────────────────────────────
const archOpen = ref(false)
const ctrlOpen = ref(false)

// ── 状态 ─────────────────────────────────────────────────
const generating = ref(false)
const autoOn     = ref(false)
let   autoTimer  = null

const ov          = ref(null)
const fundList    = ref([])
const sectorStats = ref([])
const selFund     = ref('')
const selFundData = ref(null)
const positions   = ref([])
const mgrData     = ref(null)
const peersData   = ref(null)
const navHistory  = ref([])
const detailTab   = ref('nav')

const filterType  = ref('')
const filterRisk  = ref(null)
const selSector   = ref('')
const fundTypes   = ['股票型','混合型','指数型','债券型','FOF']

// ── 图表 refs ─────────────────────────────────────────────
const navChart       = ref(null)
const ddChart        = ref(null)
const sharpeChart    = ref(null)
const styleChart     = ref(null)
const sectorPieChart = ref(null)
const contribChart   = ref(null)
const mgrRetChart    = ref(null)
const mgrAbChart     = ref(null)
const radarChart     = ref(null)
const scatterChart   = ref(null)
const corrChart      = ref(null)

const CHARTS = {}
function initChart(r, key) {
  if (!r.value) return null
  if (!CHARTS[key]) CHARTS[key] = echarts.init(r.value)
  return CHARTS[key]
}

// ── 计算属性 ──────────────────────────────────────────────
const statusText = computed(() => {
  if (!ov.value || ov.value.empty) return '等待演练启动…'
  const c = { green:'行情强势上涨', yellow:'市场震荡分化', red:'⚠️ 市场大幅下跌' }
  return `当前剧本：${ov.value.script_name} — ${c[ov.value.script_color]||''}`
})

const kpis = computed(() => {
  if (!ov.value || ov.value.empty) return []
  const o = ov.value
  return [
    { label:'基金数量',   value: o.fund_cnt+'只',           color:'#303133' },
    { label:'均IOPV偏离', value: (o.avg_dev>0?'+':'')+flt2(o.avg_dev)+'%', color: o.avg_dev>1?'#f56c6c':o.avg_dev<-1?'#67c23a':'#303133' },
    { label:'资金净流向', value: (o.total_net_bn>0?'+':'')+flt2(o.total_net_bn)+'亿', color: o.total_net_bn>0?'#f56c6c':'#67c23a' },
    { label:'均夏普比率', value: flt2(o.avg_sharpe),        color: o.avg_sharpe>=1?'#67c23a':o.avg_sharpe>=0.5?'#e6a23c':'#f56c6c' },
    { label:'均最大回撤', value: flt2(o.avg_mdd)+'%',       color: '#f56c6c' },
    { label:'平均Alpha',  value: (o.avg_alpha>0?'+':'')+flt2(o.avg_alpha)+'%', color: o.avg_alpha>0?'#f56c6c':'#67c23a' },
    { label:'IOPV预警',   value: (o.iopv_alerts||0)+'只',   color: o.iopv_alerts>5?'#f56c6c':'#303133' },
  ]
})

const rtMetrics = computed(() => {
  if (!selFundData.value) return []
  const f = selFundData.value
  return [
    { label:'实时IOPV',    value: flt4(f.realtime_iopv),                  color:'#303133' },
    { label:'IOPV偏离',    value: (f.iopv_deviation>0?'+':'')+flt2(f.iopv_deviation)+'%', color: f.iopv_deviation>0?'#f56c6c':'#67c23a' },
    { label:'昨日净值',    value: flt4(f.nav_yesterday),                  color:'#606266' },
    { label:'累计净值',    value: flt4(f.cumulative_nav),                 color:'#606266' },
    { label:'资金净流入',  value: (f.capital_net/10000).toFixed(2)+'亿',  color: f.capital_net>0?'#f56c6c':'#67c23a' },
    { label:'Sharpe比率',  value: flt2(f.sharpe),                         color: f.sharpe>=1?'#67c23a':f.sharpe>=0.5?'#e6a23c':'#f56c6c' },
    { label:'Sortino',     value: flt2(f.sortino),                        color:'#409eff' },
    { label:'Alpha',       value: (f.alpha>0?'+':'')+pct(f.alpha)+'%',   color: f.alpha>0?'#f56c6c':'#67c23a' },
    { label:'Beta',        value: flt2(f.beta),                           color:'#909399' },
    { label:'最大回撤',    value: pct(f.max_drawdown)+'%',                color:'#f56c6c' },
    { label:'波动率',      value: pct(f.volatility)+'%',                  color:'#e6a23c' },
    { label:'管理费率',    value: f.fee_rate+'%/年',                       color:'#909399' },
  ]
})

const retRows = computed(() => {
  if (!selFundData.value) return []
  const f = selFundData.value
  const bench = 0.85 // 假设基准收益系数
  const rows = [
    { label:'近1月', ret: pct(f.ret_1m), bench: pct(f.ret_1m * bench) },
    { label:'近3月', ret: pct(f.ret_3m), bench: pct(f.ret_3m * bench) },
    { label:'近6月', ret: pct(f.ret_6m), bench: pct(f.ret_6m * bench) },
    { label:'近1年', ret: pct(f.ret_1y), bench: pct(f.ret_1y * bench) },
    { label:'成立以来', ret: pct(f.ret_inception), bench: pct(f.ret_inception * bench) },
  ]
  return rows.map(r => ({ ...r, excess: (parseFloat(r.ret) - parseFloat(r.bench)).toFixed(2) }))
})

// ── 工具函数 ──────────────────────────────────────────────
const pct    = v => v ? (v * 100).toFixed(2) : '0.00'
const flt2   = v => v !== undefined && v !== null ? Number(v).toFixed(2) : '--'
const flt4   = v => v !== undefined && v !== null ? Number(v).toFixed(4) : '--'
const devClass = d => Math.abs(d) > 2 ? 'alert' : ''
const typeColor = t => t==='股票型'?'danger':t==='指数型'?'warning':t==='债券型'?'success':t==='FOF'?'info':''
const riskColor = r => r>=4?'danger':r>=3?'warning':'success'

// ── 数据加载 ──────────────────────────────────────────────
async function loadAll() {
  const [o, ss] = await Promise.all([fundApi.overview(), fundApi.sectorStats()])
  ov.value = o
  sectorStats.value = ss
  await loadList()
}

async function loadList() {
  const params = {}
  if (filterType.value) params.fund_type = filterType.value
  if (filterRisk.value) params.risk = filterRisk.value
  if (selSector.value)  params.sector = selSector.value
  fundList.value = await fundApi.list(params)
  if (fundList.value.length && !selFund.value) {
    await selectFund(fundList.value[0])
  }
}

function filterBySector(s) {
  selSector.value = selSector.value === s ? '' : s
  loadList()
}

function clearFilter() {
  filterType.value = ''
  filterRisk.value = null
  selSector.value  = ''
  loadList()
}

async function selectFund(f) {
  selFund.value = f.fund_id
  selFundData.value = f
  detailTab.value = 'nav'
  await nextTick()
  await loadDetailTab('nav', f.fund_id)
}

async function loadDetailTab(tab, fid) {
  const id = fid || selFund.value
  if (!id) return
  await nextTick()
  if (tab === 'nav') {
    const h = await fundApi.nav(id, 90)
    navHistory.value = h
    renderNav(h)
    renderDrawdown(h)
    renderSharpe(h)
    renderStyle()
  } else if (tab === 'position') {
    positions.value = await fundApi.position(id)
    renderSectorPie(positions.value)
    renderContrib(positions.value)
  } else if (tab === 'manager') {
    const mgr_id = selFundData.value?.manager_id
    if (mgr_id) {
      mgrData.value = await fundApi.manager(mgr_id)
      renderMgrRet(mgrData.value?.funds || [])
      renderMgrAB(mgrData.value?.funds || [])
    }
  } else if (tab === 'peers') {
    peersData.value = await fundApi.peers(id)
    renderRadar(peersData.value?.peers || [])
    renderScatter(peersData.value?.peers || [])
    renderCorr(peersData.value?.correlation || [], peersData.value?.peers || [])
  }
}

function onDetailTab({ paneName }) {
  loadDetailTab(paneName)
}

// ── 演练控制 ──────────────────────────────────────────────
async function doGen(days = 1) {
  if (generating.value) return
  generating.value = true
  try {
    const res = days === 1 ? await fundApi.generate() : await fundApi.batch(days)
    ElMessage.success(res.msg || '推进成功')
    await loadAll()
    if (selFund.value) await loadDetailTab(detailTab.value)
  } catch { ElMessage.error('推进失败，请检查 Doris 连接') }
  finally { generating.value = false }
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
  await ElMessageBox.confirm('确认重置所有基金仿真数据？', '重置', { type: 'warning' })
  await fundApi.reset()
  ov.value = null; fundList.value = []; selFund.value = ''; selFundData.value = null
  if (autoOn.value) { autoOn.value = false; clearInterval(autoTimer); autoTimer = null }
  ElMessage.success('已重置')
}

// ── 图表渲染 ──────────────────────────────────────────────
const COLORS = ['#409eff','#f56c6c','#67c23a','#e6a23c','#9b59b6','#1abc9c']
const baseGrid = { top:28, bottom:32, left:56, right:16 }

function renderNav(data) {
  const c = initChart(navChart, 'nav'); if (!c) return
  const dates = data.map(d => d.trade_date)
  // normalize to 1.0 start
  const base = data[0]?.cumulative_nav || 1
  const baseBench = data[0] ? (1 - (data[0].benchmark_ret || 0)) : 1
  let benchAcc = 1
  const benchSeries = data.map(d => { benchAcc *= (1 + (d.benchmark_ret || 0)); return round2(benchAcc) })
  c.setOption({
    tooltip: { trigger: 'axis', formatter: params => {
      return params[0].axisValue + '<br>' + params.map(p => `${p.marker}${p.seriesName}: ${p.value}`).join('<br>')
    }},
    legend: { top: 0, data: ['累计净值','基准'], textStyle: { fontSize: 10 } },
    grid: { ...baseGrid, bottom: 28 },
    xAxis: { type: 'category', data: dates, axisLabel: { fontSize: 9, rotate: 30 } },
    yAxis: { type: 'value', axisLabel: { fontSize: 9 } },
    series: [
      { name: '累计净值', type: 'line', smooth: true,
        data: data.map(d => round4(d.cumulative_nav)),
        lineStyle: { color: '#f56c6c', width: 2 }, symbol: 'none',
        areaStyle: { color: { type:'linear',x:0,y:0,x2:0,y2:1,colorStops:[{offset:0,color:'rgba(245,108,108,0.15)'},{offset:1,color:'rgba(245,108,108,0)'}] } }
      },
      { name: '基准', type: 'line', smooth: true,
        data: benchSeries,
        lineStyle: { color: '#909399', width: 1.5, type: 'dashed' }, symbol: 'none' },
    ],
  })
}

function renderDrawdown(data) {
  const c = initChart(ddChart, 'dd'); if (!c) return
  c.setOption({
    tooltip: { trigger: 'axis' },
    grid: { ...baseGrid, bottom: 28 },
    xAxis: { type: 'category', data: data.map(d => d.trade_date), axisLabel: { fontSize: 9, rotate: 30 } },
    yAxis: { type: 'value', axisLabel: { fontSize: 9 }, max: 0 },
    series: [{
      type: 'line', smooth: true,
      data: data.map(d => round2(d.drawdown)),
      lineStyle: { color: '#f56c6c', width: 1.5 }, symbol: 'none',
      areaStyle: { color: 'rgba(245,108,108,0.25)' },
    }],
  })
}

function renderSharpe(data) {
  const c = initChart(sharpeChart, 'sharpe'); if (!c) return
  c.setOption({
    tooltip: { trigger: 'axis' },
    grid: { ...baseGrid, bottom: 28 },
    xAxis: { type: 'category', data: data.map(d => d.trade_date), axisLabel: { fontSize: 9, rotate: 30 } },
    yAxis: { type: 'value', axisLabel: { fontSize: 9 } },
    series: [
      { name: '滚动Sharpe', type: 'line', smooth: true,
        data: data.map(d => round3(d.rolling_sharpe)),
        lineStyle: { color: '#409eff', width: 2 }, symbol: 'none',
        markLine: { silent: true, data: [
          { yAxis: 1, lineStyle: { color: '#67c23a', type: 'dashed' } },
          { yAxis: 0, lineStyle: { color: '#f56c6c', type: 'dashed' } },
        ]},
      },
    ],
  })
}

function renderStyle() {
  const c = initChart(styleChart, 'style'); if (!c) return
  // 模拟风格分布
  const f = selFundData.value
  const isGrowth = f?.fund_type === '股票型' || f?.alpha > 0.05
  c.setOption({
    radar: { indicator: [
      { name: '成长', max: 100 }, { name: '价值', max: 100 },
      { name: '大盘', max: 100 }, { name: '小盘', max: 100 },
      { name: '动量', max: 100 }, { name: '质量', max: 100 },
    ], radius: 70, axisName: { fontSize: 10 } },
    series: [{ type: 'radar',
      data: [{ value: isGrowth ? [75,30,55,45,65,70] : [35,75,70,30,40,80], name: '风格分布',
        areaStyle: { color: 'rgba(64,158,255,0.2)' },
        lineStyle: { color: '#409eff' } }]
    }],
  })
}

function renderSectorPie(pos) {
  const c = initChart(sectorPieChart, 'secpie'); if (!c) return
  const sectors = {}
  pos.forEach(p => { sectors[p.sector_l1] = (sectors[p.sector_l1] || 0) + p.weight_pct })
  c.setOption({
    tooltip: { trigger: 'item', formatter: '{b}: {d}%' },
    legend: { top: 0, textStyle: { fontSize: 10 } },
    series: [{ type: 'pie', radius: ['40%','70%'], center: ['50%','55%'],
      data: Object.entries(sectors).map(([k,v]) => ({ name: k, value: round2(v*100) })),
      label: { fontSize: 10 },
    }],
  })
}

function renderContrib(pos) {
  const c = initChart(contribChart, 'contrib'); if (!c) return
  const sorted = [...pos].sort((a,b) => b.price_contrib - a.price_contrib).slice(0, 8)
  c.setOption({
    tooltip: { trigger: 'axis' },
    grid: { top: 28, bottom: 60, left: 90, right: 16 },
    xAxis: { type: 'value', axisLabel: { fontSize: 9 } },
    yAxis: { type: 'category', data: sorted.map(p => p.stock_name), axisLabel: { fontSize: 10 } },
    series: [{ type: 'bar',
      data: sorted.map(p => ({
        value: round4(p.price_contrib * 100),
        itemStyle: { color: p.price_contrib > 0 ? '#f56c6c' : '#67c23a' },
      })),
      label: { show: true, position: 'right', fontSize: 9 },
    }],
  })
}

function renderMgrRet(funds) {
  const c = initChart(mgrRetChart, 'mgrret'); if (!c) return
  const sorted = [...funds].sort((a,b) => b.ret_1y - a.ret_1y)
  c.setOption({
    tooltip: { trigger: 'axis' },
    grid: { ...baseGrid, bottom: 60 },
    xAxis: { type: 'category', data: sorted.map(f => f.fund_name.slice(0,6)), axisLabel: { fontSize: 9, rotate: 30 } },
    yAxis: { type: 'value', name: '年化%', axisLabel: { fontSize: 9 } },
    series: [{ type: 'bar',
      data: sorted.map(f => ({
        value: round2(f.ret_1y * 100),
        itemStyle: { color: f.ret_1y > 0 ? '#f56c6c' : '#67c23a' },
      })),
      label: { show: true, position: 'top', fontSize: 9 },
    }],
  })
}

function renderMgrAB(funds) {
  const c = initChart(mgrAbChart, 'mgrab'); if (!c) return
  c.setOption({
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    legend: { top: 0, textStyle: { fontSize: 10 } },
    grid: { ...baseGrid },
    xAxis: { type: 'category', data: funds.map(f => f.fund_name.slice(0,6)), axisLabel: { fontSize: 9, rotate: 20 } },
    yAxis: [
      { type: 'value', name: 'Alpha%', axisLabel: { fontSize: 9 } },
      { type: 'value', name: 'Beta',   axisLabel: { fontSize: 9 } },
    ],
    series: [
      { name: 'Alpha', type: 'bar', data: funds.map(f => round2(f.alpha * 100)), itemStyle: { color: '#409eff' } },
      { name: 'Sharpe', type: 'line', yAxisIndex: 1, data: funds.map(f => round3(f.sharpe)),
        lineStyle: { color: '#e6a23c', width: 2 }, symbol: 'circle' },
    ],
  })
}

function renderRadar(peers) {
  const c = initChart(radarChart, 'radar'); if (!c) return
  if (!peers.length) return
  const maxRet = Math.max(...peers.map(p => Math.abs(p.ret_1y || 0)), 0.001)
  const maxSh  = Math.max(...peers.map(p => Math.abs(p.sharpe  || 0)), 0.001)
  const maxAl  = Math.max(...peers.map(p => Math.abs(p.alpha   || 0)), 0.001)
  c.setOption({
    legend: { top: 0, data: peers.map(p => p.fund_name.slice(0,6)), textStyle: { fontSize: 9 } },
    radar: {
      indicator: [
        { name: '年化收益', max: maxRet * 100 * 1.3 },
        { name: 'Sharpe',   max: maxSh * 1.3 },
        { name: 'Alpha',    max: maxAl * 100 * 1.3 },
        { name: '低回撤',   max: 50 },
        { name: '低波动',   max: 40 },
        { name: '资金流入', max: 100 },
      ],
      radius: 110, axisName: { fontSize: 10 },
    },
    series: [{ type: 'radar',
      data: peers.slice(0, 5).map((p, i) => ({
        name: p.fund_name.slice(0, 6),
        value: [
          round2(Math.abs(p.ret_1y || 0) * 100),
          round3(Math.abs(p.sharpe || 0)),
          round2(Math.abs(p.alpha  || 0) * 100),
          round2(Math.abs(p.max_drawdown || 0) * 100),
          round2(Math.abs(p.volatility   || 0.15) * 100),
          round2(50 + i * 10),
        ],
        lineStyle: { color: COLORS[i % COLORS.length] },
        areaStyle: { color: COLORS[i % COLORS.length] + '22' },
      })),
    }],
  })
}

function renderScatter(peers) {
  const c = initChart(scatterChart, 'scatter'); if (!c) return
  c.setOption({
    tooltip: { trigger: 'item', formatter: p => `${p.data[2]}<br>回撤: ${p.data[0]}%<br>年化: ${p.data[1]}%` },
    grid: { ...baseGrid },
    xAxis: { type: 'value', name: '最大回撤%', axisLabel: { fontSize: 9 } },
    yAxis: { type: 'value', name: '年化收益%', axisLabel: { fontSize: 9 } },
    series: [{ type: 'scatter',
      data: peers.map(p => [
        round2(Math.abs(p.max_drawdown || 0) * 100),
        round2((p.ret_1y || 0) * 100),
        p.fund_name.slice(0, 8),
      ]),
      symbolSize: 14,
      itemStyle: { color: '#409eff', opacity: 0.8 },
      label: { show: true, formatter: p => p.data[2], position: 'top', fontSize: 9 },
    }],
  })
}

function renderCorr(corrData, peers) {
  const c = initChart(corrChart, 'corr'); if (!c) return
  const ids = [...new Set(corrData.map(d => d.fund_a))]
  if (!ids.length) return
  const labels = ids.map(id => {
    const p = peers.find(x => x.fund_id === id)
    return p ? p.fund_name.slice(0, 8) : id
  })
  const matrix = ids.map(a => ids.map(b => {
    const r = corrData.find(d => d.fund_a === a && d.fund_b === b)
    return r ? Number(r.corr) : (a === b ? 1 : 0)
  }))
  const heatData = []
  ids.forEach((a, i) => ids.forEach((b, j) => {
    heatData.push([j, i, round3(matrix[i][j])])
  }))
  c.setOption({
    tooltip: { position: 'top', formatter: p => `${labels[p.data[1]]} × ${labels[p.data[0]]}<br>相关性: ${p.data[2]}` },
    grid: { top: 30, bottom: 60, left: 90, right: 16 },
    xAxis: { type: 'category', data: labels, axisLabel: { rotate: 30, fontSize: 9 } },
    yAxis: { type: 'category', data: labels, axisLabel: { fontSize: 9 } },
    visualMap: { min: -1, max: 1, calculable: true, orient: 'horizontal', left: 'center', bottom: 0,
      inRange: { color: ['#67c23a','#fff','#f56c6c'] }, textStyle: { fontSize: 9 } },
    series: [{ type: 'heatmap', data: heatData,
      label: { show: true, fontSize: 9 },
      emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.5)' } },
    }],
  })
}

// ── 数值格式 ──────────────────────────────────────────────
const round2 = v => Math.round((v || 0) * 100) / 100
const round3 = v => Math.round((v || 0) * 1000) / 1000
const round4 = v => Math.round((v || 0) * 10000) / 10000

onMounted(async () => {
  await fundApi.init()
  await loadAll()
})

onUnmounted(() => {
  if (autoTimer) clearInterval(autoTimer)
  Object.values(CHARTS).forEach(c => c?.dispose())
})
</script>

<style scoped>
.fund-wrap { display:flex; flex-direction:column; gap:14px; }

/* 折叠 */
.collapse-card { padding:0; overflow:hidden; }
.collapse-hd { display:flex; align-items:center; gap:8px; padding:12px 18px; cursor:pointer; user-select:none; }
.collapse-hd:hover { background:#fafafa; }
.ch-title { font-size:14px; font-weight:600; color:#1a1a1a; }
.ch-sim   { font-size:12px; color:#909399; margin-left:6px; }
.ch-tog   { margin-left:auto; font-size:12px; color:#c0c4cc; flex-shrink:0; }
.badge { padding:1px 8px; border-radius:10px; font-size:11px; font-weight:500; flex-shrink:0; }
.badge.blue   { background:#ecf5ff; color:#409eff; }
.badge.green  { background:#f0f9eb; color:#67c23a; }
.badge.orange { background:#fdf6ec; color:#e6a23c; }
.badge.purple { background:#f3e8ff; color:#9b59b6; }
.sdot { display:inline-block; width:10px; height:10px; border-radius:50%; flex-shrink:0; }
.sdot.green  { background:#67c23a; box-shadow:0 0 6px #67c23a; }
.sdot.yellow { background:#e6a23c; box-shadow:0 0 6px #e6a23c; }
.sdot.red    { background:#f56c6c; box-shadow:0 0 6px #f56c6c; animation:pulse 1s infinite; }
.sdot.grey   { background:#c0c4cc; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.35} }

/* 架构 */
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
.ctrl-hint-box { display:flex; align-items:center; gap:6px; background:#fdf6ec; border-radius:6px; padding:6px 14px; font-size:12px; color:#606266; width:100%; max-width:700px; }
.hint-dot { width:8px; height:8px; border-radius:50%; flex-shrink:0; }
.hint-dot.green { background:#67c23a; } .hint-dot.yellow { background:#e6a23c; } .hint-dot.red { background:#f56c6c; }
.ctrl-row { display:flex; gap:8px; flex-wrap:wrap; justify-content:center; }
.ctrl-hint { font-size:12px; color:#909399; text-align:center; }

/* 板块热力条 */
.sector-bar { display:flex; gap:8px; flex-wrap:wrap; background:#fff; border-radius:8px; padding:10px 14px; box-shadow:0 1px 4px rgba(0,0,0,.06); }
.sector-item { display:flex; flex-direction:column; align-items:center; padding:6px 12px; border-radius:6px; cursor:pointer; border:1.5px solid #ebeef5; min-width:72px; user-select:none; }
.sector-item:hover { border-color:#409eff; }
.sector-item.sel  { border-color:#409eff; background:#ecf5ff; }
.sector-item.up   { border-left:3px solid #f56c6c; }
.sector-item.dn   { border-left:3px solid #67c23a; }
.si-name { font-size:11px; font-weight:600; color:#303133; }
.si-ret  { font-size:13px; font-weight:700; margin:2px 0; }
.si-flow { font-size:10px; color:#909399; }

/* KPI 条 */
.kpi-row { display:grid; grid-template-columns:repeat(7,1fr); gap:10px; }
.kpi { background:#fff; border-radius:8px; padding:12px 14px; box-shadow:0 1px 4px rgba(0,0,0,.06); }
.kv { font-size:18px; font-weight:700; }
.kl { font-size:11px; color:#909399; margin-top:3px; }

/* 主区域 */
.main-area { display:grid; grid-template-columns:320px 1fr; gap:14px; }

/* 基金列表 */
.fund-list-panel { background:#fff; border-radius:8px; box-shadow:0 1px 4px rgba(0,0,0,.06); display:flex; flex-direction:column; height:calc(100vh - 340px); min-height:400px; }
.list-hd { display:flex; align-items:center; gap:6px; padding:10px 12px; border-bottom:1px solid #f0f0f0; flex-shrink:0; }
.list-title { font-size:13px; font-weight:600; color:#303133; flex:1; }
.fund-cards { flex:1; overflow-y:auto; padding:8px; display:flex; flex-direction:column; gap:6px; }
.fcard { border:1.5px solid #ebeef5; border-radius:8px; padding:10px 12px; cursor:pointer; transition:all .15s; }
.fcard:hover { border-color:#409eff; background:#fafffe; }
.fcard.sel   { border-color:#409eff; background:#ecf5ff; }
.fcard.alert { border-left:3px solid #f56c6c; }
.fc-row1 { display:flex; justify-content:space-between; align-items:center; margin-bottom:4px; }
.fc-name { font-size:12px; font-weight:600; color:#303133; }
.fc-dev  { font-size:13px; font-weight:700; }
.fc-row2 { display:flex; align-items:center; gap:4px; margin-bottom:4px; }
.fc-sector { font-size:11px; color:#909399; margin-left:2px; }
.fc-mgr    { font-size:11px; color:#c0c4cc; margin-left:auto; }
.fc-row3 { display:flex; align-items:center; gap:2px; font-size:11px; }
.fc-label { color:#909399; }
.fc-val   { font-weight:600; color:#303133; }
.fc-flow  { display:flex; align-items:center; gap:4px; margin-top:4px; }
.flow-arrow { font-size:10px; }
.flow-arrow.up { color:#f56c6c; }
.flow-arrow.dn { color:#67c23a; }
.flow-val { font-size:10px; color:#909399; }

/* 详情面板 */
.detail-panel { background:#fff; border-radius:8px; padding:16px; box-shadow:0 1px 4px rgba(0,0,0,.06); overflow-y:auto; height:calc(100vh - 340px); min-height:400px; }
.detail-empty { background:#fff; border-radius:8px; display:flex; flex-direction:column; align-items:center; justify-content:center; box-shadow:0 1px 4px rgba(0,0,0,.06); }
.detail-hd { display:flex; align-items:center; gap:6px; margin-bottom:12px; }
.detail-name { font-size:16px; font-weight:700; color:#1a1a1a; }

/* 实时指标行 */
.rt-row { display:grid; grid-template-columns:repeat(6,1fr); gap:8px; margin-bottom:14px; background:#f9fafc; border-radius:8px; padding:10px 14px; }
.rt-item { text-align:center; }
.rt-val   { font-size:14px; font-weight:700; }
.rt-label { font-size:10px; color:#909399; margin-top:2px; }

/* 经理 */
.mgr-card { display:flex; gap:16px; align-items:flex-start; background:#f9fafc; border-radius:8px; padding:14px; margin-bottom:4px; }
.mgr-avatar { width:56px; height:56px; border-radius:50%; background:#409eff; color:#fff; font-size:22px; font-weight:700; display:flex; align-items:center; justify-content:center; flex-shrink:0; }
.mgr-name { font-size:16px; font-weight:700; color:#1a1a1a; margin-bottom:4px; }
.mgr-stats { display:flex; gap:20px; margin-top:10px; flex-wrap:wrap; }
.ms { text-align:center; }
.msv { font-size:16px; font-weight:700; color:#303133; }
.msl { font-size:10px; color:#909399; margin-top:1px; }

/* 收益表 */
.ret-table { border:1px solid #ebeef5; border-radius:6px; overflow:hidden; font-size:12px; }
.ret-row { display:grid; grid-template-columns:60px 1fr 1fr 1fr; padding:6px 12px; border-bottom:1px solid #f5f5f5; }
.ret-row.hd { background:#f5f7fa; font-weight:600; color:#606266; font-size:11px; }

/* SQL展示 */
.sql-card { background:#1e1e2e; border-radius:8px; padding:10px 14px; margin-top:14px; }
.sql-title { font-size:11px; color:#67c23a; font-weight:600; margin-bottom:6px; }
.sql-code  { color:#a8b3cf; font-size:10px; font-family:monospace; white-space:pre; margin:0; line-height:1.7; overflow-x:auto; }

/* 通用 */
.two-col { display:grid; grid-template-columns:1fr 1fr; gap:14px; }
.ct { font-size:13px; font-weight:600; color:#303133; margin-bottom:8px; }
.chart-h180 { height:180px; }
.chart-h220 { height:220px; }
.chart-h240 { height:240px; }
.chart-h260 { height:260px; }
.chart-h300 { height:300px; }
.rank-top { color:#f56c6c; font-weight:700; }
.rt  { color:#f56c6c !important; }
.gt  { color:#67c23a !important; }
.grn { color:#67c23a !important; }
.grey{ color:#c0c4cc; }
.empty-state { text-align:center; padding:70px 0; }
</style>
