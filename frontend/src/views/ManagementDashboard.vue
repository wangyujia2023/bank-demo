<template>
  <div style="padding: 16px; background: #f5f7fa; min-height: 100vh">
    <!-- KPI 头部 -->
    <el-row :gutter="16" style="margin-bottom: 20px">
      <el-col :span="6">
        <div class="kpi" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%)">
          <div class="kpi-label">💰 今日营收</div>
          <div class="kpi-value">{{ fmt(biz.revenue) }}</div>
          <div class="kpi-unit">元</div>
        </div>
      </el-col>
      <el-col :span="6">
        <div class="kpi" style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%)">
          <div class="kpi-label">📊 今日利润</div>
          <div class="kpi-value">{{ fmt(biz.profit) }}</div>
          <div class="kpi-unit">元</div>
        </div>
      </el-col>
      <el-col :span="6">
        <div class="kpi" style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%)">
          <div class="kpi-label">💎 资产规模</div>
          <div class="kpi-value">{{ fmt(totalAUM / 100000000) }}B</div>
          <div class="kpi-unit">元</div>
        </div>
      </el-col>
      <el-col :span="6">
        <div class="kpi" style="background: linear-gradient(135deg, #fa709a 0%, #fee140 100%)">
          <div class="kpi-label">⚠️ 风险敞口</div>
          <div class="kpi-value">{{ fmt(totalRiskExposure / 100000000) }}B</div>
          <div class="kpi-unit">元</div>
        </div>
      </el-col>
    </el-row>

    <!-- 经营趋势 + AUM分布 -->
    <el-row :gutter="16" style="margin-bottom: 20px">
      <el-col :span="14">
        <div class="card">
          <div class="card-title">📈 近7日经营趋势</div>
          <v-chart :option="trendOption" style="height: 320px" autoresize />
        </div>
      </el-col>
      <el-col :span="10">
        <div class="card">
          <div class="card-title">🏦 AUM产品分布</div>
          <v-chart :option="aumOption" style="height: 320px" autoresize />
        </div>
      </el-col>
    </el-row>

    <!-- 头寸分布 + 风险分级 -->
    <el-row :gutter="16" style="margin-bottom: 20px">
      <el-col :span="12">
        <div class="card">
          <div class="card-title">📍 头寸配置</div>
          <v-chart :option="positionOption" style="height: 280px" autoresize />
        </div>
      </el-col>
      <el-col :span="12">
        <div class="card">
          <div class="card-title">⚠️ 风险等级分布</div>
          <el-table :data="risk" stripe size="small" style="margin-bottom: 16px">
            <el-table-column prop="risk_level" label="风险等级" width="100" align="center">
              <template #default="{ row }">
                <el-tag :type="riskType(row.risk_level)" size="small">{{ row.risk_level }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="exposure_amount" label="敞口金额" align="right">
              <template #default="{ row }">{{ fmt(row.exposure_amount / 100000000) }}B</template>
            </el-table-column>
            <el-table-column prop="default_count" label="违约数" width="80" align="center" />
          </el-table>
        </div>
      </el-col>
    </el-row>

    <!-- AUM详情 + 产品销售 -->
    <el-row :gutter="16">
      <el-col :span="12">
        <div class="card">
          <div class="card-title">📦 各类产品AUM</div>
          <el-table :data="aum" stripe size="small">
            <el-table-column prop="product_type" label="产品类型" width="120" />
            <el-table-column prop="aum_amount" label="AUM规模" align="right">
              <template #default="{ row }">{{ fmt(row.aum_amount / 100000000) }}B</template>
            </el-table-column>
            <el-table-column prop="client_count" label="客户数" width="100" align="center" />
            <el-table-column prop="yoy_growth" label="同比" width="80" align="right">
              <template #default="{ row }">{{ row.yoy_growth }}%</template>
            </el-table-column>
          </el-table>
        </div>
      </el-col>
      <el-col :span="12">
        <div class="card">
          <div class="card-title">🎯 产品销售排行</div>
          <el-table :data="products" stripe size="small">
            <el-table-column prop="product_name" label="产品名称" width="130" />
            <el-table-column prop="sales_amount" label="销售金额" align="right" width="120">
              <template #default="{ row }">{{ fmt(row.sales_amount / 1000000) }}M</template>
            </el-table-column>
            <el-table-column prop="success_rate" label="成功率" width="80" align="right">
              <template #default="{ row }">{{ row.success_rate }}%</template>
            </el-table-column>
            <el-table-column prop="rating" label="评分" width="70" align="center">
              <template #default="{ row }">
                <span style="color: #f5a623">⭐ {{ row.rating }}</span>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </el-col>
    </el-row>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import VChart from 'vue-echarts'
import { managementApi } from '@/api'

const biz = ref({})
const aum = ref([])
const risk = ref([])
const position = ref([])
const products = ref([])
const trend = ref([])

const fmt = (v) => v == null ? '-' : Number(v).toLocaleString('zh-CN', { maximumFractionDigits: 0 })

// 计算总AUM和总风险敞口
const totalAUM = computed(() => aum.value.reduce((sum, item) => sum + (item.aum_amount || 0), 0))
const totalRiskExposure = computed(() => risk.value.reduce((sum, item) => sum + (item.exposure_amount || 0), 0))

// 经营趋势图
const trendOption = computed(() => ({
  tooltip: { trigger: 'axis' },
  legend: { data: ['营收', '利润'], top: 0 },
  grid: { left: 50, right: 20, top: 36, bottom: 30 },
  xAxis: { type: 'category', data: trend.value.map(r => r.metric_date) },
  yAxis: { type: 'value' },
  series: [
    {
      name: '营收',
      type: 'line',
      smooth: true,
      data: trend.value.map(r => r.revenue / 100000000),
      lineStyle: { color: '#667eea', width: 2 },
      areaStyle: { color: '#667eea', opacity: 0.1 }
    },
    {
      name: '利润',
      type: 'line',
      smooth: true,
      data: trend.value.map(r => r.profit / 100000000),
      lineStyle: { color: '#f5576c', width: 2 },
      areaStyle: { color: '#f5576c', opacity: 0.1 }
    }
  ]
}))

// AUM分布圆饼图
const aumOption = computed(() => ({
  tooltip: { trigger: 'item' },
  legend: { bottom: 0, left: 'center' },
  series: [{
    type: 'pie',
    radius: ['35%', '65%'],
    center: ['50%', '45%'],
    data: aum.value.map(r => ({ name: r.product_type, value: r.aum_amount })),
    label: { formatter: '{b}: {d}%' }
  }]
}))

// 头寸分布条形图
const positionOption = computed(() => ({
  tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
  grid: { left: 80, right: 20, top: 20, bottom: 30 },
  xAxis: { type: 'category', data: position.value.map(r => r.asset_class) },
  yAxis: { type: 'value' },
  series: [{
    type: 'bar',
    data: position.value.map(r => r.position_amount / 100000000),
    itemStyle: { color: '#4facfe' },
    label: { show: true, position: 'top', formatter: '{c}B' }
  }]
}))

// 风险等级颜色
const riskType = (level) => ({
  '低': 'success',
  '中': 'warning',
  '高': 'danger',
  '极高': 'danger'
}[level] || 'info')

// 加载数据
onMounted(async () => {
  const data = await managementApi.overview()
  biz.value = data.biz
  aum.value = data.aum
  risk.value = data.risk
  position.value = data.position
  products.value = data.products
  trend.value = data.trend
})
</script>

<style scoped>
.kpi {
  padding: 20px;
  border-radius: 8px;
  color: white;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
}

.kpi-label {
  font-size: 14px;
  opacity: 0.9;
  margin-bottom: 8px;
}

.kpi-value {
  font-size: 28px;
  font-weight: 700;
  margin-bottom: 4px;
}

.kpi-unit {
  font-size: 12px;
  opacity: 0.8;
}

.card {
  background: white;
  padding: 16px;
  border-radius: 8px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
}

.card-title {
  font-size: 14px;
  font-weight: 600;
  margin-bottom: 12px;
  color: #333;
  border-bottom: 2px solid #f5f5f5;
  padding-bottom: 8px;
}
</style>
