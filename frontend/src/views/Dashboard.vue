<template>
  <div>
    <!-- 核心指标 -->
    <div class="stat-row">
      <div class="stat-card">
        <div class="stat-label">👤 存量用户总数</div>
        <div class="stat-value" style="color:#409eff">{{ fmt(data.user_stat?.total_users) }}</div>
        <div class="stat-sub">活跃用户 {{ fmt(data.user_stat?.active_users) }} 人</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">📋 今日日志量</div>
        <div class="stat-value" style="color:#67c23a">{{ fmt(data.log_stat?.total_logs) }}</div>
        <div class="stat-sub">AI已打标签 {{ fmt(data.log_stat?.log_users) }} 人次</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">⚠️ 异常日志数</div>
        <div class="stat-value" style="color:#f56c6c">{{ fmt(data.log_stat?.high_risk_logs) }}</div>
        <div class="stat-sub">异常操作 {{ fmt(data.log_stat?.anomaly_logs) }} 条</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">👥 活跃人群包</div>
        <div class="stat-value" style="color:#e6a23c">{{ fmt(data.segment_stat?.total_segments) }}</div>
        <div class="stat-sub">覆盖用户 {{ fmt(data.segment_stat?.total_crowd) }} 人</div>
      </div>
    </div>

    <el-row :gutter="16">
      <!-- 日志趋势图 -->
      <el-col :span="16">
        <div class="card">
          <div class="card-title">📈 日志趋势（近14天）</div>
          <v-chart :option="trendOption" style="height:280px" autoresize />
        </div>
      </el-col>

      <!-- 资产等级分布 -->
      <el-col :span="8">
        <div class="card">
          <div class="card-title">💰 用户资产等级分布</div>
          <v-chart :option="assetOption" style="height:280px" autoresize />
        </div>
      </el-col>
    </el-row>

    <!-- 快捷入口 -->
    <div class="card">
      <div class="card-title">🚀 快速入口</div>
      <el-row :gutter="12">
        <el-col :span="6" v-for="item in shortcuts" :key="item.path">
          <el-card
            shadow="hover"
            style="cursor:pointer;text-align:center;padding:16px 0"
            @click="$router.push(item.path)"
          >
            <el-icon :size="32" :color="item.color"><component :is="item.icon" /></el-icon>
            <div style="margin-top:8px;font-size:14px;font-weight:500">{{ item.label }}</div>
            <div style="font-size:12px;color:#909399;margin-top:4px">{{ item.desc }}</div>
          </el-card>
        </el-col>
      </el-row>
    </div>

    <!-- 异常用户预警 -->
    <div class="card">
      <div class="card-title">🔴 异常用户预警（今日）</div>
      <el-table :data="anomalyUsers" stripe size="small">
        <el-table-column prop="user_id"   label="用户ID" width="100" />
        <el-table-column prop="user_name" label="姓名"   width="90" />
        <el-table-column prop="asset_level" label="资产等级" width="100">
          <template #default="{row}">
            <el-tag :type="assetTagType(row.asset_level)" size="small">{{ row.asset_level }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="city"      label="城市"   width="80" />
        <el-table-column label="异常类型">
          <template #default>
            <el-tag type="danger" size="small">异地登录 / 大额转账</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="风险等级" width="90">
          <template #default><el-tag type="danger" size="small">高风险</el-tag></template>
        </el-table-column>
        <el-table-column label="操作" width="120">
          <template #default="{row}">
            <el-button type="primary" size="small" link @click="$router.push(`/user?user_id=${row.user_id}`)">
              查看详情
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import VChart from 'vue-echarts'
import { dashboardApi, userApi } from '@/api'
import { cachedRequest } from '@/utils/cache'
// ✅ ECharts 已在 plugins/echarts.js 中全局导入，无需重复

const data = ref({ user_stat: {}, log_stat: {}, segment_stat: {}, asset_level_dist: [], log_trend: [] })
const anomalyUsers = ref([])

const fmt = (v) => v == null ? '-' : Number(v).toLocaleString()

const shortcuts = [
  { path: '/user',         icon: 'User',       color: '#409eff', label: '用户宽表查询', desc: '多条件筛选用户画像' },
  { path: '/segment',      icon: 'Grid',       color: '#67c23a', label: '人群圈选',     desc: 'Bitmap 精准圈人' },
  { path: '/behavior',     icon: 'TrendCharts',color: '#e6a23c', label: '行为分析',     desc: '漏斗/留存/RFM' },
  { path: '/ai-tag',       icon: 'MagicStick', color: '#f56c6c', label: 'AI 打标签',   desc: 'Doris AI Function' },
]

const assetTagType = (level) => ({
  'VIP私行': 'danger', 'VIP钻石': 'warning', 'VIP铂金': '', 'VIP黄金': 'success'
}[level] || 'info')

const trendOption = computed(() => {
  const trend = data.value.log_trend || []
  return {
    tooltip: { trigger: 'axis' },
    legend: { data: ['日志总量', '高风险日志'], top: 0 },
    grid: { left: 40, right: 20, top: 36, bottom: 30 },
    xAxis: { type: 'category', data: trend.map(r => r.date || r.log_date) },
    yAxis: { type: 'value' },
    series: [
      {
        name: '日志总量', type: 'line', smooth: true,
        data: trend.map(r => r.log_count),
        areaStyle: { opacity: 0.15 },
        lineStyle: { color: '#409eff' }, itemStyle: { color: '#409eff' }
      },
      {
        name: '高风险日志', type: 'bar',
        data: trend.map(r => r.risk_count),
        itemStyle: { color: '#f56c6c' }
      }
    ]
  }
})

const assetOption = computed(() => ({
  tooltip: { trigger: 'item' },
  legend: { bottom: 0, left: 'center' },
  series: [{
    type: 'pie', radius: ['40%', '65%'], center: ['50%', '44%'],
    data: (data.value.asset_level_dist || []).map(r => ({ name: r.label, value: r.value })),
    label: { formatter: '{b}: {d}%' }
  }]
}))

onMounted(async () => {
  // 并行加载两个API，使用缓存减少重复请求
  const [dashData, anomalyRes] = await Promise.all([
    cachedRequest('/dashboard', {}, () => dashboardApi.overview(), { ttl: 5 * 60 * 1000 }),
    cachedRequest('/user/wide', { anomaly_flag: 1, page_size: 5 },
      () => userApi.queryWide({ anomaly_flag: 1, page_size: 5 }),
      { ttl: 3 * 60 * 1000 }
    )
  ])
  data.value = dashData
  anomalyUsers.value = anomalyRes.rows || []
})
</script>
