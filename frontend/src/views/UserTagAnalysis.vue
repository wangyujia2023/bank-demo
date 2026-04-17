<template>
  <div>
    <el-tabs v-model="activeTab" type="card" style="margin-bottom:0">
      <el-tab-pane label="📋 宽表查询" name="wide" />
      <el-tab-pane label="🔄 宽表→高表 ETL" name="etl" />
      <el-tab-pane label="🎯 Bitmap 人群交并差" name="bitmap" />
      <el-tab-pane label="📊 行为分析" name="behavior" />
    </el-tabs>

    <!-- ═══════════════════ 宽表查询 ═══════════════════ -->
    <div v-if="activeTab === 'wide'" class="card" style="border-top-left-radius:0">
      <el-row :gutter="16">
        <!-- 标签选择器 -->
        <el-col :span="16">
          <div style="font-size:13px;font-weight:600;margin-bottom:10px">
            选择标签（多选为 AND 交集，即同时具备所有标签的用户）
          </div>
          <div v-for="grp in tagMeta" :key="grp.category" style="margin-bottom:12px">
            <div style="font-size:12px;color:#909399;margin-bottom:6px">{{ grp.category }}</div>
            <el-checkbox-group v-model="selectedTagIds" style="display:flex;flex-wrap:wrap;gap:6px">
              <el-checkbox
                v-for="t in grp.tags" :key="t.tag_id"
                :value="t.tag_id" border size="small"
              >{{ t.label }}</el-checkbox>
            </el-checkbox-group>
          </div>
        </el-col>

        <!-- 查询区 -->
        <el-col :span="8">
          <div style="padding:12px;background:#f5f7fa;border-radius:6px">
            <div style="font-size:13px;font-weight:600;margin-bottom:10px">已选标签</div>
            <el-tag
              v-for="tid in selectedTagIds" :key="tid" closable
              @close="selectedTagIds = selectedTagIds.filter(x => x !== tid)"
              style="margin:3px"
            >{{ tagLabel(tid) }}</el-tag>
            <div v-if="!selectedTagIds.length" style="color:#c0c4cc;font-size:12px">未选择任何标签（查全部）</div>
            <el-divider />
            <el-button type="primary" style="width:100%" :loading="wideLoading" @click="queryWide">
              查询
            </el-button>
          </div>

          <!-- 分布统计 -->
          <div style="margin-top:16px">
            <div style="font-size:13px;font-weight:600;margin-bottom:8px">标签命中用户数（TOP 15）</div>
            <div v-if="distribution.length">
              <div
                v-for="item in distribution.slice(0,15)" :key="item.col"
                style="display:flex;align-items:center;gap:8px;margin-bottom:4px;font-size:12px"
              >
                <span style="width:90px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{{ item.label }}</span>
                <el-progress
                  :percentage="distPercent(item.count)"
                  :stroke-width="10"
                  style="flex:1"
                  :format="() => item.count.toLocaleString()"
                />
              </div>
            </div>
            <el-button size="small" plain @click="loadDistribution" style="margin-top:6px">刷新分布</el-button>
          </div>
        </el-col>
      </el-row>

      <!-- 结果表 -->
      <div v-if="wideResult" style="margin-top:16px">
        <div style="margin-bottom:8px;font-size:13px;color:#606266">
          共 <b>{{ wideResult.total.toLocaleString() }}</b> 条，
          第 {{ wideResult.page }} 页
        </div>
        <el-table :data="wideResult.rows" border size="small" style="width:100%">
          <el-table-column prop="customer_id" label="用户ID" width="100" />
          <el-table-column label="标签" min-width="300">
            <template #default="{row}">
              <el-tag
                v-for="tag in row.active_tags" :key="tag" size="small" type="success"
                style="margin:2px"
              >{{ tag }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="update_time" label="更新时间" width="160" />
        </el-table>
        <el-pagination
          v-if="wideResult.total > wideResult.page_size"
          style="margin-top:10px"
          layout="prev, pager, next"
          :total="wideResult.total"
          :page-size="wideResult.page_size"
          :current-page="widePage"
          @current-change="onWidePage"
        />
        <div style="font-size:12px;color:#909399;margin-top:8px">
          Doris <code>DUPLICATE KEY</code> 宽表 · 多标签列 TINYINT 0/1 · 向量化扫描
        </div>
      </div>
    </div>

    <!-- ═══════════════════ ETL ═══════════════════ -->
    <div v-if="activeTab === 'etl'" class="card" style="border-top-left-radius:0">
      <el-row :gutter="20">
        <el-col :span="10">
          <div class="card-title">宽表 → 高表 ETL</div>
          <p style="font-size:13px;color:#606266;line-height:1.8">
            将 <code>bank.user_tag_wide</code> 各 TINYINT 标签列<br/>
            通过 <b>BITMAP_UNION(TO_BITMAP(customer_id))</b><br/>
            写入 <code>bank.t_customer_tags</code>（高表）<br/><br/>
            SQL 核心：每个标签列做一次 GROUP BY + BITMAP_UNION，<br/>
            UNION ALL 合并所有标签，一次 INSERT 完成
          </p>
          <el-button type="primary" :loading="etlLoading" @click="runEtl" style="margin-top:8px">
            执行 ETL 同步
          </el-button>
          <div v-if="etlResult" style="margin-top:12px">
            <el-alert
              :title="etlResult.success ? `同步完成：高表共 ${etlResult.tag_rows} 行` : `ETL 失败：${etlResult.message}`"
              :type="etlResult.success ? 'success' : 'error'"
              show-icon :closable="false"
            />
          </div>
        </el-col>
        <el-col :span="14">
          <div class="card-title">高表标签 Bitmap 用户数</div>
          <el-button size="small" plain @click="loadEtlOverview" style="margin-bottom:10px">刷新</el-button>
          <el-table :data="etlOverview" border size="small" max-height="420">
            <el-table-column prop="tag_id"    label="tag_id"  width="75" />
            <el-table-column prop="tag_name"  label="列名"    width="170" />
            <el-table-column prop="label"     label="标签名"  width="110" />
            <el-table-column prop="category"  label="分类"    width="80" />
            <el-table-column prop="user_count" label="命中用户数">
              <template #default="{row}">
                {{ (row.user_count || 0).toLocaleString() }}
              </template>
            </el-table-column>
          </el-table>
          <div style="font-size:12px;color:#909399;margin-top:8px">
            Doris <code>AGGREGATE KEY</code> 高表 · <code>BITMAP_UNION</code> 聚合 · <code>BITMAP_COUNT</code> 统计
          </div>
        </el-col>
      </el-row>
    </div>

    <!-- ═══════════════════ Bitmap 人群 ═══════════════════ -->
    <div v-if="activeTab === 'bitmap'" class="card" style="border-top-left-radius:0">
      <el-row :gutter="20">
        <!-- 单人群圈选 -->
        <el-col :span="12">
          <div class="card-title">单人群圈选（交集 / 排除）</div>
          <div style="margin-bottom:10px">
            <div style="font-size:13px;margin-bottom:6px">包含标签（AND 交集）</div>
            <el-select
              v-model="bitmapInclude" multiple collapse-tags collapse-tags-tooltip
              placeholder="选择包含标签" style="width:100%"
              @change="bitmapResult = null"
            >
              <el-option-group v-for="grp in tagMeta" :key="grp.category" :label="grp.category">
                <el-option v-for="t in grp.tags" :key="t.tag_id" :label="t.label" :value="t.tag_id" />
              </el-option-group>
            </el-select>
          </div>
          <div style="margin-bottom:12px">
            <div style="font-size:13px;margin-bottom:6px">排除标签（NOT 差集）</div>
            <el-select
              v-model="bitmapExclude" multiple collapse-tags collapse-tags-tooltip
              placeholder="选择排除标签" style="width:100%"
              @change="bitmapResult = null"
            >
              <el-option-group v-for="grp in tagMeta" :key="grp.category" :label="grp.category">
                <el-option v-for="t in grp.tags" :key="t.tag_id" :label="t.label" :value="t.tag_id" />
              </el-option-group>
            </el-select>
          </div>
          <el-button type="primary" :loading="bitmapLoading" @click="computeBitmap" style="width:100%">
            Bitmap 计算人群规模
          </el-button>
          <div v-if="bitmapResult" style="margin-top:16px;text-align:center">
            <el-statistic title="人群规模" :value="bitmapResult.crowd_size">
              <template #suffix>人</template>
            </el-statistic>
            <div style="font-size:12px;color:#909399;margin-top:6px">
              Doris <code>BITMAP_AND</code> + <code>BITMAP_AND_NOT</code> · <code>BITMAP_COUNT</code>
            </div>
          </div>
        </el-col>

        <!-- 两人群集合运算 -->
        <el-col :span="12">
          <div class="card-title">两人群集合运算（交 / 并 / 差）</div>
          <div style="margin-bottom:10px">
            <div style="font-size:13px;margin-bottom:6px">人群 A</div>
            <el-select v-model="setA" multiple collapse-tags placeholder="选择标签（AND组合）" style="width:100%">
              <el-option-group v-for="grp in tagMeta" :key="grp.category" :label="grp.category">
                <el-option v-for="t in grp.tags" :key="t.tag_id" :label="t.label" :value="t.tag_id" />
              </el-option-group>
            </el-select>
          </div>
          <div style="margin-bottom:10px">
            <div style="font-size:13px;margin-bottom:6px">人群 B</div>
            <el-select v-model="setB" multiple collapse-tags placeholder="选择标签（AND组合）" style="width:100%">
              <el-option-group v-for="grp in tagMeta" :key="grp.category" :label="grp.category">
                <el-option v-for="t in grp.tags" :key="t.tag_id" :label="t.label" :value="t.tag_id" />
              </el-option-group>
            </el-select>
          </div>
          <el-radio-group v-model="setOp" style="margin-bottom:12px">
            <el-radio-button value="AND">交集 ∩</el-radio-button>
            <el-radio-button value="OR">并集 ∪</el-radio-button>
            <el-radio-button value="NOT">差集 A-B</el-radio-button>
          </el-radio-group>
          <el-button type="primary" :loading="setOpsLoading" @click="computeSetOps" style="width:100%">
            计算集合运算
          </el-button>
          <div v-if="setOpsResult" style="margin-top:16px">
            <el-row :gutter="12">
              <el-col :span="8">
                <el-statistic title="人群 A" :value="setOpsResult.size_a" />
              </el-col>
              <el-col :span="8">
                <el-statistic title="人群 B" :value="setOpsResult.size_b" />
              </el-col>
              <el-col :span="8">
                <el-statistic
                  :title="{ AND:'交集', OR:'并集', NOT:'差集' }[setOpsResult.operation]"
                  :value="setOpsResult.size_result"
                />
              </el-col>
            </el-row>
            <div style="font-size:12px;color:#909399;margin-top:8px;text-align:center">
              Doris <code>BITMAP_AND</code> / <code>BITMAP_OR</code> / <code>BITMAP_AND_NOT</code>
            </div>
          </div>
        </el-col>
      </el-row>
    </div>

    <!-- ═══════════════════ 行为分析 ═══════════════════ -->
    <div v-if="activeTab === 'behavior'" class="card" style="border-top-left-radius:0">
      <el-tabs v-model="behaviorTab" type="border-card">
        <!-- 漏斗 -->
        <el-tab-pane label="漏斗分析 window_funnel()" name="funnel">
          <el-form inline style="margin-top:8px">
            <el-form-item label="窗口(秒)">
              <el-input-number v-model="funnelWindow" :min="3600" :max="2592000" :step="86400" style="width:130px" />
            </el-form-item>
            <el-form-item label="人群过滤">
              <el-select v-model="funnelFilterTags" multiple collapse-tags placeholder="（可选）高表标签" style="width:220px">
                <el-option-group v-for="grp in tagMeta" :key="grp.category" :label="grp.category">
                  <el-option v-for="t in grp.tags" :key="t.tag_id" :label="t.label" :value="t.tag_id" />
                </el-option-group>
              </el-select>
            </el-form-item>
            <el-form-item>
              <el-button type="primary" :loading="funnelLoading" @click="runFunnel">分析</el-button>
            </el-form-item>
          </el-form>

          <!-- 步骤配置 -->
          <div style="margin-bottom:12px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
            <span style="font-size:13px">漏斗步骤：</span>
            <el-tag
              v-for="(s, i) in funnelSteps" :key="i" closable type="primary"
              @close="funnelSteps.splice(i, 1)" size="small"
            >{{ i+1 }}. {{ s }}</el-tag>
            <el-select v-model="newStep" placeholder="添加步骤" size="small" style="width:160px" @change="addStep">
              <el-option v-for="e in eventTypes" :key="e" :label="e" :value="e" />
            </el-select>
          </div>

          <div v-if="funnelData">
            <v-chart :option="funnelOption" style="height:300px" autoresize />
            <el-table :data="funnelData.steps" border size="small" style="margin-top:10px">
              <el-table-column prop="step"      label="步骤" width="60" />
              <el-table-column prop="step_name" label="事件" />
              <el-table-column prop="user_count" label="用户数">
                <template #default="{row}">{{ (row.user_count||0).toLocaleString() }}</template>
              </el-table-column>
              <el-table-column prop="conversion_rate" label="环节转化率">
                <template #default="{row}">
                  <el-progress :percentage="row.conversion_rate||0" :stroke-width="8" />
                </template>
              </el-table-column>
              <el-table-column prop="overall_rate" label="总转化率">
                <template #default="{row}"><el-tag size="small">{{ row.overall_rate }}%</el-tag></template>
              </el-table-column>
            </el-table>
            <div style="font-size:12px;color:#909399;margin-top:6px">
              Doris 内置 <code>window_funnel(window, mode, timestamp, cond1, cond2, ...)</code>
            </div>
          </div>
        </el-tab-pane>

        <!-- 留存 -->
        <el-tab-pane label="留存分析 retention()" name="retention">
          <el-form inline style="margin-top:8px">
            <el-form-item label="起始事件">
              <el-select v-model="retCohort" style="width:130px">
                <el-option v-for="e in eventTypes" :key="e" :label="e" :value="e" />
              </el-select>
            </el-form-item>
            <el-form-item label="回访事件">
              <el-select v-model="retReturn" style="width:130px">
                <el-option v-for="e in eventTypes" :key="e" :label="e" :value="e" />
              </el-select>
            </el-form-item>
            <el-form-item>
              <el-button type="primary" :loading="retLoading" @click="runRetention">分析</el-button>
            </el-form-item>
          </el-form>
          <div v-if="retentionData" style="overflow-x:auto;margin-top:12px">
            <table class="retention-matrix">
              <thead>
                <tr>
                  <th>队列日期</th>
                  <th>初始用户</th>
                  <th v-for="d in retentionData.retention_days" :key="d">第{{ d }}天</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in retentionData.rows" :key="row.cohort_date">
                  <td>{{ row.cohort_date }}</td>
                  <td>{{ (row.cohort_size||0).toLocaleString() }}</td>
                  <td
                    v-for="d in retentionData.retention_days" :key="d"
                    :style="{ background: heatColor(row[`d${d}_rate`]) }"
                  >
                    {{ row[`d${d}_rate`] }}%
                  </td>
                </tr>
              </tbody>
            </table>
            <div style="font-size:12px;color:#909399;margin-top:6px">
              Doris 留存分析 · <code>DATEDIFF</code> + <code>COUNT DISTINCT</code>
            </div>
          </div>
        </el-tab-pane>

        <!-- 路径 -->
        <el-tab-pane label="行为路径分析" name="path">
          <el-button type="primary" :loading="pathLoading" @click="runPath" style="margin-top:8px">
            分析 Top 10 路径
          </el-button>
          <div v-if="pathData" style="margin-top:16px">
            <v-chart :option="pathOption" style="height:360px" autoresize />
            <div style="font-size:12px;color:#909399;margin-top:6px">
              两步路径 JOIN · 1小时窗口 · <code>CONCAT(event_a, ' -> ', event_b)</code>
            </div>
          </div>
        </el-tab-pane>
      </el-tabs>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { use } from 'echarts/core'
import { FunnelChart, BarChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'
import axios from 'axios'

use([FunnelChart, BarChart, GridComponent, TooltipComponent, LegendComponent, CanvasRenderer])

const BASE = '/api'

// ── tab ──
const activeTab   = ref('wide')
const behaviorTab = ref('funnel')

// ── 标签元数据 ──
const tagMeta = ref([])
async function loadTagMeta() {
  const { data } = await axios.get(`${BASE}/cdp/wide/tag-meta`)
  tagMeta.value = data
}
function tagLabel(tid) {
  for (const grp of tagMeta.value) {
    const t = grp.tags.find(x => x.tag_id === tid)
    if (t) return t.label
  }
  return tid
}

// ══ 宽表查询 ══
const selectedTagIds = ref([])
const wideLoading    = ref(false)
const wideResult     = ref(null)
const widePage       = ref(1)
const distribution   = ref([])
const distMax        = computed(() => Math.max(...distribution.value.map(d => d.count), 1))
const distPercent    = cnt => Math.round(cnt * 100 / distMax.value)

async function queryWide(page = 1) {
  wideLoading.value = true
  widePage.value = page
  try {
    const { data } = await axios.post(`${BASE}/cdp/wide/query`, {
      tag_ids: selectedTagIds.value,
      page,
      page_size: 20,
    })
    wideResult.value = data
  } finally {
    wideLoading.value = false
  }
}

async function loadDistribution() {
  const { data } = await axios.get(`${BASE}/cdp/wide/distribution`)
  distribution.value = data
}

function onWidePage(p) { queryWide(p) }

// ══ ETL ══
const etlLoading  = ref(false)
const etlResult   = ref(null)
const etlOverview = ref([])

async function runEtl() {
  etlLoading.value = true
  try {
    const { data } = await axios.post(`${BASE}/cdp/etl/sync`)
    etlResult.value = data
    await loadEtlOverview()
  } finally {
    etlLoading.value = false
  }
}

async function loadEtlOverview() {
  const { data } = await axios.get(`${BASE}/cdp/etl/overview`)
  etlOverview.value = data
}

// ══ Bitmap ══
const bitmapInclude = ref([])
const bitmapExclude = ref([])
const bitmapLoading = ref(false)
const bitmapResult  = ref(null)

async function computeBitmap() {
  bitmapLoading.value = true
  try {
    const { data } = await axios.post(`${BASE}/cdp/bitmap/compute`, {
      include_tag_ids: bitmapInclude.value,
      exclude_tag_ids: bitmapExclude.value,
    })
    bitmapResult.value = data
  } finally {
    bitmapLoading.value = false
  }
}

const setA         = ref([])
const setB         = ref([])
const setOp        = ref('AND')
const setOpsLoading= ref(false)
const setOpsResult = ref(null)

async function computeSetOps() {
  setOpsLoading.value = true
  try {
    const { data } = await axios.post(`${BASE}/cdp/bitmap/two-set`, {
      tag_ids_a: setA.value,
      tag_ids_b: setB.value,
      operation: setOp.value,
    })
    setOpsResult.value = data
  } finally {
    setOpsLoading.value = false
  }
}

// ══ 行为分析 ══
const eventTypes    = ['REGISTER', 'LOGIN', 'BROWSE_PRODUCT', 'APPLY', 'TRANSACTION', 'TRANSFER', 'REPAY']
const funnelSteps   = ref(['REGISTER', 'LOGIN', 'BROWSE_PRODUCT', 'APPLY', 'TRANSACTION'])
const funnelWindow  = ref(86400)
const funnelFilterTags = ref([])
const newStep       = ref('')
const funnelLoading = ref(false)
const funnelData    = ref(null)

function addStep(v) {
  if (v && !funnelSteps.value.includes(v)) funnelSteps.value.push(v)
  newStep.value = ''
}

async function runFunnel() {
  funnelLoading.value = true
  try {
    const { data } = await axios.post(`${BASE}/cdp/behavior/funnel`, {
      steps:           funnelSteps.value,
      window_seconds:  funnelWindow.value,
      filter_tag_ids:  funnelFilterTags.value.length ? funnelFilterTags.value : null,
    })
    funnelData.value = data
  } finally {
    funnelLoading.value = false
  }
}

const funnelOption = computed(() => {
  if (!funnelData.value) return {}
  return {
    tooltip: { trigger: 'item' },
    series: [{
      type: 'funnel', sort: 'none',
      label: { position: 'inside', formatter: '{b}: {c}人' },
      data: funnelData.value.steps.map(s => ({ name: s.step_name, value: s.user_count })),
    }],
  }
})

// 留存
const retCohort   = ref('REGISTER')
const retReturn   = ref('LOGIN')
const retLoading  = ref(false)
const retentionData = ref(null)

async function runRetention() {
  retLoading.value = true
  try {
    const { data } = await axios.post(`${BASE}/cdp/behavior/retention`, {
      cohort_event: retCohort.value,
      return_event: retReturn.value,
    })
    retentionData.value = data
  } finally {
    retLoading.value = false
  }
}

function heatColor(rate) {
  if (!rate) return '#f5f7fa'
  const r = Math.min(rate, 100) / 100
  const g = Math.round(200 - r * 100)
  const b = Math.round(240 - r * 80)
  return `rgba(64, ${g}, ${b}, ${0.3 + r * 0.5})`
}

// 路径
const pathLoading = ref(false)
const pathData    = ref(null)

async function runPath() {
  pathLoading.value = true
  try {
    const { data } = await axios.get(`${BASE}/cdp/behavior/path?top_n=10`)
    pathData.value = data
  } finally {
    pathLoading.value = false
  }
}

const pathOption = computed(() => {
  if (!pathData.value) return {}
  const rows = [...pathData.value].sort((a, b) => a.freq - b.freq)
  return {
    tooltip: { trigger: 'axis' },
    grid: { left: '30%', right: '8%' },
    xAxis: { type: 'value' },
    yAxis: { type: 'category', data: rows.map(r => r.path) },
    series: [{
      type: 'bar', data: rows.map(r => r.freq),
      label: { show: true, position: 'right' },
    }],
  }
})

onMounted(async () => {
  await loadTagMeta()
  await loadDistribution()
  await loadEtlOverview()
})
</script>

<style scoped>
.retention-matrix {
  border-collapse: collapse;
  font-size: 12px;
}
.retention-matrix th,
.retention-matrix td {
  border: 1px solid #ebeef5;
  padding: 6px 10px;
  text-align: center;
  white-space: nowrap;
}
.retention-matrix th {
  background: #f5f7fa;
  font-weight: 600;
}
</style>
