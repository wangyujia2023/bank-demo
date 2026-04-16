<template>
  <div>

    <!-- ═══ AI_CLASSIFY 核心展示区 ═══ -->
    <div class="card showcase-card">
      <div class="showcase-header">
        <div>
          <div class="showcase-title">Doris AI_FUNCTION 打标签能力演示</div>
          <div class="showcase-sub">通过 AI_CLASSIFY() 内置函数，对用户行为日志进行语义理解并自动打标签，无需离线建模</div>
        </div>
        <div class="showcase-actions">
          <el-button
            type="primary"
            size="large"
            :loading="classifying"
            :icon="MagicStick"
            @click="runClassify"
          >执行 AI 打标签</el-button>
          <el-button size="large" @click="reloadAll">刷新分析</el-button>
        </div>
      </div>

      <!-- SQL 展示（默认折叠） -->
      <div class="sql-showcase">
        <div class="sql-label" @click="sqlExpanded = !sqlExpanded" style="cursor:pointer;user-select:none">
          <el-tag type="success" effect="dark" size="small">核心 SQL</el-tag>
          <span style="margin-left:8px;color:#67c23a;font-size:12px">在 Doris 中直接执行，无需外部调用</span>
          <span style="margin-left:auto;color:#67c23a;font-size:12px">{{ sqlExpanded ? '▲ 收起' : '▼ 展开查看' }}</span>
        </div>
        <div v-if="!sqlExpanded" class="sql-collapsed" @click="sqlExpanded = true">
          <span class="sql-collapsed-hint">UPDATE user_wide SET log_tags = AI_CLASSIFY('qwen_llm_resource', CONCAT(...), ARRAY[...])  </span>
          <el-link type="success" :underline="false" style="font-size:12px">展开完整 SQL</el-link>
        </div>
        <pre v-else class="sql-code">{{ showcaseSQL }}</pre>
      </div>

      <!-- 执行结果 -->
      <div v-if="classifyResult" class="classify-result">
        <el-alert
          :title="`打标签完成：共处理 ${classifyResult.total} 名用户，成功标记 ${classifyResult.tagged} 名`"
          type="success"
          :closable="false"
          show-icon
        >
          <template #default>
            <div style="margin-top:8px">
              <span style="color:#606266;font-size:13px">标记样例：</span>
              <el-tag
                v-for="s in classifyResult.samples" :key="s.user_id"
                style="margin:2px 4px"
                effect="plain"
              >用户 {{ s.user_id }}：{{ s.tags.join('、') }}</el-tag>
            </div>
          </template>
        </el-alert>
      </div>

      <!-- 状态指示 -->
      <div class="flow-row">
        <div class="flow-step">
          <div class="flow-icon" style="background:#e8f4ff;color:#409eff">
            <el-icon size="22"><DataBoard /></el-icon>
          </div>
          <div class="flow-text">
            <div class="flow-title">用户行为日志</div>
            <div class="flow-desc">asset_level / aum / active_level</div>
          </div>
        </div>
        <div class="flow-arrow">→</div>
        <div class="flow-step">
          <div class="flow-icon" style="background:#f0f9eb;color:#67c23a">
            <el-icon size="22"><Cpu /></el-icon>
          </div>
          <div class="flow-text">
            <div class="flow-title">AI_CLASSIFY()</div>
            <div class="flow-desc">Doris 内置 LLM 推理函数</div>
          </div>
        </div>
        <div class="flow-arrow">→</div>
        <div class="flow-step">
          <div class="flow-icon" style="background:#fff8e6;color:#e6a23c">
            <el-icon size="22"><Finished /></el-icon>
          </div>
          <div class="flow-text">
            <div class="flow-title">标签写回</div>
            <div class="flow-desc">log_tags 字段实时更新</div>
          </div>
        </div>
        <div class="flow-arrow">→</div>
        <div class="flow-step">
          <div class="flow-icon" style="background:#fef0f0;color:#f56c6c">
            <el-icon size="22"><TrendCharts /></el-icon>
          </div>
          <div class="flow-text">
            <div class="flow-title">标签分析</div>
            <div class="flow-desc">用户洞察 / 风险识别</div>
          </div>
        </div>
      </div>
    </div>

    <!-- 顶部统计 -->
    <div class="stat-row">
      <div class="stat-card">
        <div class="stat-label">用户总数</div>
        <div class="stat-value" style="color:#409eff">{{ fmt(summary.total_users) }}</div>
        <div class="stat-sub">全量用户</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">已打标签用户</div>
        <div class="stat-value" style="color:#67c23a">{{ fmt(summary.tagged_users) }}</div>
        <div class="stat-sub">标签覆盖率 {{ coverPct }}%</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">异常标记用户</div>
        <div class="stat-value" style="color:#f56c6c">{{ fmt(summary.risk_users) }}</div>
        <div class="stat-sub">含风险类标签</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">标签种类</div>
        <div class="stat-value" style="color:#e6a23c">{{ tagDist.length }}</div>
        <div class="stat-sub">覆盖 {{ tagCategories.length }} 个分类</div>
      </div>
    </div>

    <!-- ═══ 分析 Tab ═══ -->
    <el-tabs v-model="activeTab" type="card" style="margin-bottom:0">
      <el-tab-pane label="标签分布" name="dist" />
      <el-tab-pane label="风险标签分析" name="risk" />
      <el-tab-pane label="标签 × 资产交叉" name="cross" />
      <el-tab-pane label="标签共现" name="cooc" />
      <el-tab-pane label="用户明细" name="users" />
    </el-tabs>

    <!-- ─── 标签分布 ─── -->
    <div v-if="activeTab === 'dist'" class="card" style="border-top-left-radius:0">
      <el-row :gutter="16">
        <el-col :span="10">
          <div class="card-title">标签覆盖用户数</div>
          <v-chart :option="distBarOption" style="height:360px" autoresize />
        </el-col>
        <el-col :span="7">
          <div class="card-title">按分类占比</div>
          <v-chart :option="catPieOption" style="height:360px" autoresize />
        </el-col>
        <el-col :span="7">
          <div class="card-title">标签详情</div>
          <div class="tag-list">
            <div
              v-for="t in tagDist" :key="t.tag_name"
              class="tag-item"
              :class="{ risk: t.is_risk }"
              @click="jumpToUsers(t.tag_name)"
            >
              <div class="tag-dot" :style="{ background: t.color }"></div>
              <div class="tag-info">
                <span class="tag-name">{{ t.tag_name }}</span>
                <span class="tag-cat">{{ t.category }}</span>
              </div>
              <div class="tag-count">{{ t.user_count }}<small>人</small></div>
              <el-tag v-if="t.is_risk" type="danger" size="small" effect="plain" style="margin-left:4px">风险</el-tag>
            </div>
          </div>
        </el-col>
      </el-row>
    </div>

    <!-- ─── 风险标签分析 ─── -->
    <div v-if="activeTab === 'risk'" class="card" style="border-top-left-radius:0">
      <div class="card-title">风险类标签关联用户的异常情况</div>
      <el-row :gutter="16">
        <el-col :span="14">
          <el-table :data="riskData" border stripe size="small">
            <el-table-column prop="tag_name" label="风险标签" width="100">
              <template #default="{row}">
                <el-tag type="danger" size="small" effect="dark">{{ row.tag_name }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="total_users" label="用户数" width="80" align="center" />
            <el-table-column prop="anomaly_users" label="异常用户" width="90" align="center">
              <template #default="{row}">
                <span style="color:#f56c6c;font-weight:600">{{ row.anomaly_users }}</span>
              </template>
            </el-table-column>
            <el-table-column label="异常率" width="140">
              <template #default="{row}">
                <el-progress
                  :percentage="row.total_users ? Math.round(row.anomaly_users * 100 / row.total_users) : 0"
                  :stroke-width="8"
                  :color="row.anomaly_users / row.total_users > 0.4 ? '#f56c6c' : '#e6a23c'"
                />
              </template>
            </el-table-column>
            <el-table-column prop="avg_aum" label="平均AUM(万)" width="110" align="right">
              <template #default="{row}">{{ row.avg_aum.toFixed(1) }}</template>
            </el-table-column>
            <el-table-column prop="avg_churn_pct" label="平均流失率%" width="110" align="right">
              <template #default="{row}">
                <span :style="{color: row.avg_churn_pct > 30 ? '#f56c6c' : '#e6a23c', fontWeight:'600'}">
                  {{ row.avg_churn_pct }}%
                </span>
              </template>
            </el-table-column>
          </el-table>
        </el-col>
        <el-col :span="10">
          <v-chart :option="riskBarOption" style="height:300px" autoresize />
        </el-col>
      </el-row>
    </div>

    <!-- ─── 标签 × 资产交叉 ─── -->
    <div v-if="activeTab === 'cross'" class="card" style="border-top-left-radius:0">
      <div class="card-title">各标签下用户资产等级分布</div>
      <div class="cross-grid">
        <div v-for="t in crossData" :key="t.tag_name" class="cross-item">
          <div class="cross-tag" :style="{ borderColor: t.color, color: t.color }">
            {{ t.tag_name }}
            <small>{{ t.category }}</small>
          </div>
          <div class="cross-bars">
            <div v-for="d in t.asset_dist" :key="d.level" class="cross-bar-row">
              <span class="cross-label">
                <el-tag :type="assetTagType(d.level)" size="small" style="font-size:10px">{{ d.level }}</el-tag>
              </span>
              <el-progress
                :percentage="Math.min(d.cnt * 100, 100)"
                :stroke-width="10"
                :show-text="false"
                :color="t.color"
                style="flex:1"
              />
              <span class="cross-cnt">{{ d.cnt }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- ─── 标签共现 ─── -->
    <div v-if="activeTab === 'cooc'" class="card" style="border-top-left-radius:0">
      <div class="card-title">标签共现（同时出现在同一用户的标签对）</div>
      <el-row :gutter="16">
        <el-col :span="12">
          <el-table :data="coocData" border size="small" stripe>
            <el-table-column type="index" width="40" />
            <el-table-column prop="tag_a" label="标签 A" width="110">
              <template #default="{row}">
                <el-tag size="small" effect="plain" :style="{borderColor: tagColor(row.tag_a), color: tagColor(row.tag_a)}">
                  {{ row.tag_a }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="+" width="30" align="center">
              <template #default><b style="color:#c0c4cc">+</b></template>
            </el-table-column>
            <el-table-column prop="tag_b" label="标签 B" width="110">
              <template #default="{row}">
                <el-tag size="small" effect="plain" :style="{borderColor: tagColor(row.tag_b), color: tagColor(row.tag_b)}">
                  {{ row.tag_b }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="count" label="共现用户数" align="center">
              <template #default="{row}">
                <el-tag type="primary" effect="dark" size="small">{{ row.count }}</el-tag>
              </template>
            </el-table-column>
          </el-table>
        </el-col>
        <el-col :span="12">
          <v-chart :option="coocChordOption" style="height:360px" autoresize />
        </el-col>
      </el-row>
    </div>

    <!-- ─── 用户明细 ─── -->
    <div v-if="activeTab === 'users'" class="card" style="border-top-left-radius:0">
      <el-form inline style="margin-bottom:12px">
        <el-form-item label="按标签筛选">
          <el-select v-model="userFilter.tag_name" clearable placeholder="全部标签" style="width:130px" @change="loadUsers">
            <el-option v-for="t in tagDist" :key="t.tag_name" :label="t.tag_name" :value="t.tag_name">
              <span :style="{color: t.color}">● </span>{{ t.tag_name }}
              <span style="color:#c0c4cc;margin-left:4px">({{ t.user_count }}人)</span>
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="仅看异常">
          <el-switch v-model="userFilter.onlyRisk" @change="loadUsers" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="loadUsers">查询</el-button>
        </el-form-item>
      </el-form>

      <el-table :data="userRows" v-loading="userLoading" border size="small" stripe>
        <el-table-column prop="user_id"        label="用户ID"  width="80" />
        <el-table-column prop="user_name"      label="姓名"    width="72" />
        <el-table-column prop="age_group"      label="年龄段"  width="72" />
        <el-table-column prop="city"           label="城市"    width="65" />
        <el-table-column prop="asset_level"    label="资产等级" width="90">
          <template #default="{row}">
            <el-tag :type="assetTagType(row.asset_level)" size="small">{{ row.asset_level }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="aum_total"      label="AUM(万)" width="85" align="right">
          <template #default="{row}">{{ row.aum_total.toFixed(1) }}</template>
        </el-table-column>
        <el-table-column prop="active_level"   label="活跃"   width="65" />
        <el-table-column prop="lifecycle_stage" label="周期"  width="80" />
        <el-table-column prop="log_tags"       label="AI标签" min-width="180">
          <template #default="{row}">
            <template v-if="parseTags(row.log_tags).length">
              <el-tag
                v-for="tag in parseTags(row.log_tags)" :key="tag"
                size="small" effect="plain" round
                :style="{ marginRight:'3px', borderColor: tagColor(tag), color: tagColor(tag) }"
              >{{ tag }}</el-tag>
            </template>
            <span v-else style="color:#c0c4cc;font-size:12px">— 未打标签</span>
          </template>
        </el-table-column>
        <el-table-column prop="anomaly_flag"   label="异常"   width="60" align="center">
          <template #default="{row}">
            <el-tag v-if="+row.anomaly_flag" type="danger" size="small">是</el-tag>
            <span v-else style="color:#67c23a;font-size:12px">否</span>
          </template>
        </el-table-column>
      </el-table>
    </div>

  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { MagicStick, DataBoard, Cpu, Finished, TrendCharts } from '@element-plus/icons-vue'
import { BarChart, PieChart } from 'echarts/charts'
import VChart from 'vue-echarts'
import { tagAnalysisApi } from '@/api'


const showcaseSQL = `-- Doris AI_CLASSIFY 打标签（一键执行，无需离线建模）
UPDATE user_wide
SET log_tags = AI_CLASSIFY(
    'qwen_llm_resource',                          -- Doris Resource：已注册的 LLM 端点
    CONCAT(
        '资产等级:', asset_level,
        ' AUM:', aum_total, '万',
        ' 活跃度:', active_level,
        ' 周期:', lifecycle_stage
    ),                                            -- 输入：用户画像文本
    ARRAY[
        '高净值','基金偏好','理财偏好','贷款需求',
        '异地登录','大额转账','频繁操作','保险偏好',
        '稳健型','贵宾','新客','高频交易'
    ]                                             -- 候选标签集合
)
WHERE log_tags IS NULL OR log_tags = '[]';`

const activeTab   = ref('dist')
const classifying = ref(false)
const classifyResult = ref(null)
const sqlExpanded = ref(false)

const summary   = ref({ total_users: 0, tagged_users: 0, risk_users: 0 })
const tagDist   = ref([])
const riskData  = ref([])
const crossData = ref([])
const coocData  = ref([])
const userRows  = ref([])
const userLoading = ref(false)
const userFilter  = ref({ tag_name: '', onlyRisk: false })

const coverPct = computed(() =>
  summary.value.total_users
    ? Math.round(summary.value.tagged_users * 100 / summary.value.total_users)
    : 0
)
const tagCategories = computed(() => [...new Set(tagDist.value.map(t => t.category))])
const fmt = v => v == null ? '-' : Number(v).toLocaleString()
const assetTagType = l => ({ 'VIP私行': 'danger', 'VIP钻石': 'warning', 'VIP铂金': '', 'VIP黄金': 'success' }[l] || 'info')
const tagColorMap = computed(() => Object.fromEntries(tagDist.value.map(t => [t.tag_name, t.color])))
function tagColor(name) { return tagColorMap.value[name] || '#909399' }
function parseTags(raw) {
  if (!raw || raw === '[]') return []
  try { return JSON.parse(raw) } catch { return [] }
}
function jumpToUsers(tag) {
  userFilter.value.tag_name = tag
  activeTab.value = 'users'
  loadUsers()
}

async function runClassify() {
  classifying.value = true
  classifyResult.value = null
  try {
    const res = await tagAnalysisApi.runClassify()
    classifyResult.value = res
    ElMessage.success(`AI 打标签完成，已标记 ${res.tagged} 名用户`)
    await reloadAll()
  } finally {
    classifying.value = false
  }
}

async function reloadAll() {
  await loadOverview()
  Promise.all([loadRisk(), loadCross(), loadCooc(), loadUsers()])
}

async function loadOverview() {
  const res = await tagAnalysisApi.overview()
  tagDist.value  = res.tag_distribution || []
  summary.value  = res.summary || {}
}
async function loadRisk()  { riskData.value  = await tagAnalysisApi.risk() }
async function loadCross() { crossData.value = await tagAnalysisApi.cross() }
async function loadCooc()  { coocData.value  = await tagAnalysisApi.cooccurrence() }
async function loadUsers() {
  userLoading.value = true
  try {
    const params = {}
    if (userFilter.value.tag_name) params.tag_name = userFilter.value.tag_name
    if (userFilter.value.onlyRisk) params.is_risk = 1
    userRows.value = await tagAnalysisApi.users(params)
  } finally { userLoading.value = false }
}

// ── ECharts ──────────────────────────────────────────────────────
const distBarOption = computed(() => ({
  tooltip: { trigger: 'axis' },
  grid: { left: 70, right: 20, top: 10, bottom: 10 },
  xAxis: { type: 'value' },
  yAxis: { type: 'category', data: tagDist.value.map(t => t.tag_name) },
  series: [{
    type: 'bar',
    data: tagDist.value.map(t => ({ value: t.user_count, itemStyle: { color: t.color } })),
    label: { show: true, position: 'right', fontSize: 11 }
  }]
}))

const catPieOption = computed(() => {
  const catMap = {}
  tagDist.value.forEach(t => { catMap[t.category] = (catMap[t.category] || 0) + t.user_count })
  return {
    tooltip: { trigger: 'item', formatter: '{b}: {c}人 ({d}%)' },
    legend: { bottom: 0, textStyle: { fontSize: 11 } },
    series: [{
      type: 'pie', radius: ['35%', '60%'],
      data: Object.entries(catMap).map(([name, value]) => ({ name, value })),
      label: { formatter: '{b}\n{d}%', fontSize: 11 }
    }]
  }
})

const riskBarOption = computed(() => ({
  tooltip: { trigger: 'axis' },
  legend: { data: ['总用户', '异常用户'], top: 0 },
  grid: { left: 70, right: 20, top: 36, bottom: 10 },
  xAxis: { type: 'value' },
  yAxis: { type: 'category', data: riskData.value.map(r => r.tag_name) },
  series: [
    { name: '总用户',  type: 'bar', data: riskData.value.map(r => r.total_users),  itemStyle: { color: '#c0d9f0' } },
    { name: '异常用户', type: 'bar', data: riskData.value.map(r => r.anomaly_users), itemStyle: { color: '#f56c6c' } },
  ]
}))

const coocChordOption = computed(() => {
  if (!coocData.value.length) return {}
  const allTags = [...new Set(coocData.value.flatMap(r => [r.tag_a, r.tag_b]))]
  return {
    tooltip: {},
    grid: { left: 20, right: 20, top: 20, bottom: 20 },
    xAxis: { type: 'category', data: allTags, axisLabel: { fontSize: 10, rotate: 30 } },
    yAxis: { type: 'category', data: allTags, axisLabel: { fontSize: 10 } },
    series: [{
      type: 'scatter',
      data: coocData.value.flatMap(r => [
        [allTags.indexOf(r.tag_a), allTags.indexOf(r.tag_b), r.count],
        [allTags.indexOf(r.tag_b), allTags.indexOf(r.tag_a), r.count],
      ]),
      symbolSize: v => Math.max(v[2] * 14, 8),
      itemStyle: { color: '#409eff', opacity: 0.75 },
      tooltip: {
        formatter: p => {
          const [xi, yi, cnt] = p.value
          return `${allTags[xi]} + ${allTags[yi]}<br/>共现 <b>${cnt}</b> 人`
        }
      }
    }]
  }
})

onMounted(async () => {
  // 并行加载所有数据
  await Promise.all([
    loadOverview(),
    loadRisk(),
    loadCross(),
    loadCooc(),
    loadUsers()
  ])
})
</script>

<style scoped>
.showcase-card { border: 1.5px solid #409eff22; background: linear-gradient(135deg, #f0f7ff 0%, #fff 60%); }
.showcase-header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 16px; }
.showcase-title { font-size: 17px; font-weight: 700; color: #1a1a1a; margin-bottom: 4px; }
.showcase-sub { font-size: 13px; color: #606266; }
.showcase-actions { display: flex; gap: 8px; flex-shrink: 0; }

.sql-showcase { background: #1e1e2e; border-radius: 8px; padding: 16px 20px; margin-bottom: 16px; }
.sql-label { margin-bottom: 10px; display: flex; align-items: center; border-radius: 4px; padding: 2px 4px; transition: background 0.15s; }
.sql-label:hover { background: rgba(255,255,255,0.06); }
.sql-collapsed {
  display: flex; align-items: center; gap: 8px; padding: 8px 4px;
  cursor: pointer;
}
.sql-collapsed-hint {
  font-family: 'JetBrains Mono', Consolas, monospace;
  font-size: 12px; color: #6e7c9a; white-space: nowrap;
  overflow: hidden; text-overflow: ellipsis; max-width: 480px;
}
.sql-code {
  font-family: 'JetBrains Mono', 'Fira Code', Consolas, monospace;
  font-size: 13px; line-height: 1.7; color: #a8b3cf;
  white-space: pre; overflow-x: auto; margin: 0;
}

.classify-result { margin-bottom: 16px; }

.flow-row { display: flex; align-items: center; gap: 0; padding-top: 4px; }
.flow-step { display: flex; align-items: center; gap: 10px; flex: 1; }
.flow-icon { width: 44px; height: 44px; border-radius: 10px; display: flex; align-items: center; justify-content: center; flex-shrink: 0; }
.flow-text { flex: 1; }
.flow-title { font-size: 13px; font-weight: 600; color: #1a1a1a; }
.flow-desc { font-size: 11px; color: #909399; margin-top: 2px; }
.flow-arrow { font-size: 22px; color: #c0c4cc; padding: 0 8px; flex-shrink: 0; }

.tag-list { display: flex; flex-direction: column; gap: 6px; max-height: 360px; overflow-y: auto; }
.tag-item {
  display: flex; align-items: center; gap: 8px;
  padding: 7px 10px; border-radius: 6px; background: #fafafa;
  border: 1px solid #f0f0f0; cursor: pointer; transition: all 0.15s;
}
.tag-item:hover { border-color: #c6e2ff; background: #f0f7ff; }
.tag-item.risk { border-color: #fbc4c4; background: #fff5f5; }
.tag-dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
.tag-info { flex: 1; display: flex; flex-direction: column; }
.tag-name { font-size: 13px; font-weight: 600; color: #1a1a1a; }
.tag-cat  { font-size: 11px; color: #909399; }
.tag-count { font-size: 16px; font-weight: 700; color: #409eff; }
.tag-count small { font-size: 11px; font-weight: 400; color: #909399; margin-left: 2px; }

.cross-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }
.cross-item { background: #fafafa; border-radius: 8px; padding: 12px; border: 1px solid #f0f0f0; }
.cross-tag {
  font-size: 13px; font-weight: 600; padding: 4px 10px;
  border-radius: 14px; border: 1.5px solid; display: inline-flex;
  flex-direction: column; margin-bottom: 10px;
}
.cross-tag small { font-size: 10px; opacity: 0.7; font-weight: 400; }
.cross-bars { display: flex; flex-direction: column; gap: 6px; }
.cross-bar-row { display: flex; align-items: center; gap: 6px; }
.cross-label { width: 72px; flex-shrink: 0; }
.cross-cnt { font-size: 11px; color: #909399; width: 20px; text-align: right; flex-shrink: 0; }
</style>
