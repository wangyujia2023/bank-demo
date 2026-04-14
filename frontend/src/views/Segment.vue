<template>
  <div>
    <el-row :gutter="16">

      <!-- 左：圈选配置 -->
      <el-col :span="14">
        <div class="card">
          <div class="card-title">🎯 人群圈选（Doris Bitmap + HASP）</div>

          <!-- 规则组 -->
          <div
            v-for="(rule, idx) in rules"
            :key="idx"
            style="display:flex;align-items:center;gap:8px;margin-bottom:10px;
                   padding:12px;border:1px solid #e4e7ed;border-radius:6px;background:#fafafa"
          >
            <span style="font-size:12px;color:#909399;width:20px;flex-shrink:0">{{ idx + 1 }}</span>

            <el-select v-model="rule.tag_name" placeholder="选择标签" style="width:160px" @change="clearValues(rule)">
              <el-option-group v-for="grp in tagGroups" :key="grp.label" :label="grp.label">
                <el-option v-for="t in grp.tags" :key="t.value" :label="t.label" :value="t.value" />
              </el-option-group>
            </el-select>

            <el-select v-model="rule.op" style="width:95px;flex-shrink:0">
              <el-option label="包含(OR)" value="OR" />
              <el-option label="全含(AND)" value="AND" />
            </el-select>

            <el-select
              v-model="rule.tag_values"
              multiple collapse-tags collapse-tags-tooltip
              placeholder="选择标签值"
              style="flex:1"
            >
              <el-option
                v-for="opt in getOptions(rule.tag_name)"
                :key="opt" :label="opt" :value="opt"
              />
            </el-select>

            <el-tooltip content="排除此条件（NOT）">
              <el-checkbox v-model="rule.exclude" style="flex-shrink:0">排除</el-checkbox>
            </el-tooltip>

            <el-button type="danger" link :icon="Delete" @click="removeRule(idx)" style="flex-shrink:0" />
          </div>

          <el-button type="primary" plain :icon="Plus" @click="addRule" style="width:100%;margin-bottom:16px">
            添加规则（规则间 AND 逻辑）
          </el-button>

          <!-- 操作区 -->
          <el-row :gutter="10">
            <el-col :span="8">
              <el-button type="primary" style="width:100%" :loading="counting" :disabled="!hasValidRules" @click="handleCount">
                📊 实时估算人数
              </el-button>
            </el-col>
            <el-col :span="16">
              <el-input v-model="segName" placeholder="人群包名称（保存时必填）">
                <template #append>
                  <el-button :loading="saving" :disabled="!hasValidRules || !segName.trim()" @click="handleSave">
                    💾 保存人群包
                  </el-button>
                </template>
              </el-input>
            </el-col>
          </el-row>

          <!-- 估算结果 -->
          <transition name="fade">
            <div v-if="countResult !== null" class="count-result">
              <div>
                <div style="font-size:12px;opacity:.8;margin-bottom:4px">预估人群规模</div>
                <div style="font-size:40px;font-weight:800">{{ countResult.toLocaleString() }}</div>
                <div style="font-size:12px;opacity:.7;margin-top:4px">已激活规则 {{ activeRuleCount }} 条</div>
              </div>
              <div style="text-align:right">
                <div style="font-size:12px;opacity:.8">Doris Bitmap 计算</div>
                <div style="font-size:12px;opacity:.8">HASP 向量化加速</div>
                <div style="font-size:20px;font-weight:700;margin-top:4px">{{ countCost }}ms</div>
              </div>
            </div>
          </transition>

          <!-- 标签说明 -->
          <el-collapse style="margin-top:16px">
            <el-collapse-item title="📌 可用标签说明" name="1">
              <div style="display:flex;flex-wrap:wrap;gap:6px">
                <el-tag v-for="t in allTagOptions" :key="t.value" size="small" effect="plain">
                  {{ t.label }}（{{ getOptions(t.value).join(' / ') }}）
                </el-tag>
              </div>
            </el-collapse-item>
          </el-collapse>
        </div>
      </el-col>

      <!-- 右：人群包列表 -->
      <el-col :span="10">
        <div class="card">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
            <span class="card-title" style="margin-bottom:0">📦 已保存人群包</span>
            <el-button size="small" :icon="Refresh" @click="loadSegments" :loading="loadingList">刷新</el-button>
          </div>

          <div v-if="loadingList" style="text-align:center;padding:30px;color:#c0c4cc">加载中...</div>
          <el-empty v-else-if="!segments.length" description="暂无人群包，请先配置规则并保存" />

          <div v-for="seg in segments" :key="seg.segment_id" class="seg-card">
            <div class="seg-header">
              <div class="seg-title">{{ seg.segment_name }}</div>
              <el-tag type="primary" size="small" effect="dark">
                {{ (seg.user_count || 0).toLocaleString() }} 人
              </el-tag>
            </div>

            <div v-if="seg.segment_desc" class="seg-desc">{{ seg.segment_desc }}</div>

            <!-- 规则预览 -->
            <div v-if="Array.isArray(seg.rule_config) && seg.rule_config.length" class="seg-rules">
              <span
                v-for="(r, i) in seg.rule_config"
                :key="i"
                class="rule-chip"
                :class="{ exclude: r.exclude }"
              >
                <span v-if="r.exclude" style="color:#f56c6c">NOT </span>
                {{ getTagLabel(r.tag_name) }}
                <b>{{ r.op }}</b>
                {{ r.tag_values.join('、') }}
              </span>
            </div>

            <div class="seg-meta">
              <span>{{ seg.created_by }}</span>
              <span>{{ seg.snap_date }}</span>
            </div>

            <div class="seg-actions">
              <el-button size="small" type="primary" link @click="reuseRules(seg)">
                <el-icon><CopyDocument /></el-icon> 复用规则
              </el-button>
              <el-button size="small" type="success" link @click="viewStats(seg)">
                <el-icon><DataAnalysis /></el-icon> 分布分析
              </el-button>
              <el-button size="small" type="info" link @click="viewUsers(seg)">
                <el-icon><User /></el-icon> 查看用户
              </el-button>
              <el-button size="small" type="danger" link @click="deleteSegment(seg.segment_id)">
                <el-icon><Delete /></el-icon>
              </el-button>
            </div>
          </div>
        </div>
      </el-col>
    </el-row>

    <!-- 用户列表对话框 -->
    <el-dialog v-model="userDialogVisible" :title="`用户列表 — ${selectedSeg?.segment_name}`" width="900px">
      <el-table :data="segUsers" size="small" stripe border max-height="480">
        <el-table-column prop="user_id"        label="用户ID"  width="80" />
        <el-table-column prop="user_name"      label="姓名"    width="72" />
        <el-table-column prop="phone"          label="手机号"  width="125" />
        <el-table-column prop="age"            label="年龄"    width="55" />
        <el-table-column prop="city"           label="城市"    width="70" />
        <el-table-column prop="asset_level"    label="资产等级" width="90">
          <template #default="{row}">
            <el-tag :type="assetTagType(row.asset_level)" size="small">{{ row.asset_level }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="aum_total"      label="AUM(万)"  width="90">
          <template #default="{row}">{{ Number(row.aum_total || 0).toFixed(1) }}</template>
        </el-table-column>
        <el-table-column prop="active_level"   label="活跃"    width="68" />
        <el-table-column prop="lifecycle_stage" label="生命周期" width="85" />
        <el-table-column prop="credit_grade"   label="信用"    width="60" />
        <el-table-column prop="churn_prob"     label="流失率">
          <template #default="{row}">
            <el-progress :percentage="+(Number(row.churn_prob||0)*100).toFixed(0)"
              :stroke-width="5" :show-text="false"
              :color="Number(row.churn_prob||0) > 0.4 ? '#f56c6c' : '#67c23a'" />
          </template>
        </el-table-column>
      </el-table>
    </el-dialog>

    <!-- 分布分析对话框 -->
    <el-dialog v-model="statsDialogVisible" :title="`分布分析 — ${selectedSeg?.segment_name}（${selectedSeg?.user_count}人）`" width="760px">
      <div v-if="loadingStats" style="text-align:center;padding:40px;color:#909399">分析中...</div>
      <div v-else-if="segStats">
        <el-row :gutter="16">
          <el-col :span="12">
            <div class="stat-block">
              <div class="stat-block-title">资产等级分布</div>
              <div v-for="d in segStats.asset_dist" :key="d.asset_level" class="dist-row">
                <span class="dist-label">
                  <el-tag :type="assetTagType(d.asset_level)" size="small">{{ d.asset_level }}</el-tag>
                </span>
                <el-progress :percentage="pct(d.cnt, segStats.asset_dist)" :stroke-width="14"
                  :format="() => d.cnt + '人'" style="flex:1" />
              </div>
            </div>
          </el-col>
          <el-col :span="12">
            <div class="stat-block">
              <div class="stat-block-title">活跃等级分布</div>
              <div v-for="d in segStats.active_dist" :key="d.active_level" class="dist-row">
                <span class="dist-label">{{ d.active_level }}</span>
                <el-progress :percentage="pct(d.cnt, segStats.active_dist)" :stroke-width="14"
                  :format="() => d.cnt + '人'" style="flex:1"
                  :color="{ '高活': '#67c23a', '中活': '#409eff', '低活': '#e6a23c', '沉睡': '#909399' }[d.active_level]" />
              </div>
            </div>
          </el-col>
          <el-col :span="12" style="margin-top:16px">
            <div class="stat-block">
              <div class="stat-block-title">生命周期分布</div>
              <div v-for="d in segStats.lifecycle_dist" :key="d.lifecycle_stage" class="dist-row">
                <span class="dist-label">{{ d.lifecycle_stage }}</span>
                <el-progress :percentage="pct(d.cnt, segStats.lifecycle_dist)" :stroke-width="14"
                  :format="() => d.cnt + '人'" style="flex:1" />
              </div>
            </div>
          </el-col>
          <el-col :span="12" style="margin-top:16px">
            <div class="stat-block">
              <div class="stat-block-title">偏好渠道分布</div>
              <div v-for="d in segStats.channel_dist" :key="d.preferred_channel" class="dist-row">
                <span class="dist-label">{{ d.preferred_channel }}</span>
                <el-progress :percentage="pct(d.cnt, segStats.channel_dist)" :stroke-width="14"
                  :format="() => d.cnt + '人'" style="flex:1" color="#e6a23c" />
              </div>
            </div>
          </el-col>
        </el-row>

        <!-- AUM 统计 -->
        <div class="stat-block" style="margin-top:16px">
          <div class="stat-block-title">AUM 资产统计（万元）</div>
          <el-row :gutter="12">
            <el-col :span="8">
              <div class="aum-card">
                <div style="font-size:11px;color:#909399">平均 AUM</div>
                <div style="font-size:22px;font-weight:700;color:#409eff">
                  {{ segStats.aum_stat.avg_aum?.toFixed(1) || 0 }}
                </div>
              </div>
            </el-col>
            <el-col :span="8">
              <div class="aum-card">
                <div style="font-size:11px;color:#909399">最高 AUM</div>
                <div style="font-size:22px;font-weight:700;color:#67c23a">
                  {{ segStats.aum_stat.max_aum?.toFixed(1) || 0 }}
                </div>
              </div>
            </el-col>
            <el-col :span="8">
              <div class="aum-card">
                <div style="font-size:11px;color:#909399">最低 AUM</div>
                <div style="font-size:22px;font-weight:700;color:#e6a23c">
                  {{ segStats.aum_stat.min_aum?.toFixed(1) || 0 }}
                </div>
              </div>
            </el-col>
          </el-row>
        </div>
      </div>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { Plus, Delete, Refresh, CopyDocument, DataAnalysis, User } from '@element-plus/icons-vue'
import { segmentApi } from '@/api'
import { ElMessage, ElMessageBox } from 'element-plus'

const tagGroups = [
  {
    label: '资产信息',
    tags: [
      { label: '资产等级', value: 'asset_level' },
      { label: 'AUM分层',  value: 'aum_tier' },
    ]
  },
  {
    label: '行为特征',
    tags: [
      { label: '活跃等级',   value: 'active_level' },
      { label: '偏好渠道',   value: 'preferred_channel' },
      { label: 'APP登录频次', value: 'app_login_tier' },
    ]
  },
  {
    label: '生命周期',
    tags: [
      { label: '生命周期阶段', value: 'lifecycle_stage' },
    ]
  },
  {
    label: '用户属性',
    tags: [
      { label: '年龄段',   value: 'age_group' },
      { label: '信用等级', value: 'credit_grade' },
      { label: '风险等级', value: 'risk_level' },
    ]
  },
  {
    label: '风控',
    tags: [
      { label: '异常标记', value: 'anomaly_flag' },
    ]
  },
]

const tagOptions = {
  asset_level:       ['VIP私行', 'VIP钻石', 'VIP铂金', 'VIP黄金', '普通'],
  aum_tier:          ['<10万', '10-50万', '50-200万', '200-500万', '>500万'],
  active_level:      ['高活', '中活', '低活', '沉睡'],
  preferred_channel: ['APP', '网点', '小程序', '网银'],
  app_login_tier:    ['高频(>30次)', '中频(10-30次)', '低频(<10次)'],
  lifecycle_stage:   ['新客', '成长', '成熟', '沉睡', '流失预警'],
  age_group:         ['18-25', '26-35', '36-45', '46-55', '55+'],
  credit_grade:      ['AAA', 'AA', 'A', 'B', 'C'],
  risk_level:        ['1-保守', '2-稳健', '3-平衡', '4-进取', '5-激进'],
  anomaly_flag:      ['1', '0'],
}

const allTagOptions = tagGroups.flatMap(g => g.tags)
const getOptions = (tagName) => tagOptions[tagName] || []
const clearValues = (rule) => { rule.tag_values = [] }
const getTagLabel = (val) => allTagOptions.find(t => t.value === val)?.label || val
const assetTagType = l => ({ 'VIP私行': 'danger', 'VIP钻石': 'warning', 'VIP铂金': '', 'VIP黄金': 'success' }[l] || 'info')

const rules        = ref([{ tag_name: 'asset_level', op: 'OR', tag_values: [], exclude: false }])
const segments     = ref([])
const segUsers     = ref([])
const segStats     = ref(null)
const segName      = ref('')
const counting     = ref(false)
const saving       = ref(false)
const loadingList  = ref(false)
const loadingStats = ref(false)
const countResult  = ref(null)
const countCost    = ref(0)
const userDialogVisible  = ref(false)
const statsDialogVisible = ref(false)
const selectedSeg  = ref(null)

const hasValidRules = computed(() => rules.value.some(r => r.tag_name && r.tag_values.length))
const activeRuleCount = computed(() => rules.value.filter(r => r.tag_name && r.tag_values.length).length)

function addRule() {
  rules.value.push({ tag_name: '', op: 'OR', tag_values: [], exclude: false })
}
function removeRule(idx) { rules.value.splice(idx, 1) }

function reuseRules(seg) {
  if (!Array.isArray(seg.rule_config) || !seg.rule_config.length) {
    ElMessage.warning('该人群包无规则配置')
    return
  }
  rules.value = seg.rule_config.map(r => ({ ...r }))
  segName.value = seg.segment_name + '_复制'
  countResult.value = null
  ElMessage.success('规则已复用，可修改后重新估算或保存')
}

function pct(cnt, arr) {
  const total = arr.reduce((s, d) => s + Number(d.cnt || 0), 0)
  return total ? Math.round(Number(cnt) * 100 / total) : 0
}

async function handleCount() {
  const validRules = rules.value.filter(r => r.tag_name && r.tag_values.length)
  counting.value = true
  const t0 = Date.now()
  try {
    const res = await segmentApi.count({ rules: validRules })
    countResult.value = res.crowd_size || 0
    countCost.value = Date.now() - t0
  } finally { counting.value = false }
}

async function handleSave() {
  const validRules = rules.value.filter(r => r.tag_name && r.tag_values.length)
  saving.value = true
  try {
    const res = await segmentApi.create({
      segment_name: segName.value,
      description: '',
      rules: validRules,
      created_by: '数据分析师',
    })
    ElMessage.success(`保存成功，共 ${(res.user_count || 0).toLocaleString()} 人`)
    segName.value = ''
    countResult.value = null
    loadSegments()
  } finally { saving.value = false }
}

async function loadSegments() {
  loadingList.value = true
  try {
    segments.value = await segmentApi.list()
  } finally { loadingList.value = false }
}

async function viewUsers(seg) {
  selectedSeg.value = seg
  const res = await segmentApi.users(seg.segment_id, 1, 50)
  segUsers.value = res.rows || []
  userDialogVisible.value = true
}

async function viewStats(seg) {
  selectedSeg.value = seg
  statsDialogVisible.value = true
  loadingStats.value = true
  segStats.value = null
  try {
    segStats.value = await segmentApi.stats(seg.segment_id)
  } finally { loadingStats.value = false }
}

async function deleteSegment(id) {
  await ElMessageBox.confirm('确认删除该人群包？', '确认', { type: 'warning' })
  await segmentApi.delete(id)
  ElMessage.success('已删除')
  loadSegments()
}

onMounted(loadSegments)
</script>

<style scoped>
.seg-card {
  padding: 12px;
  border: 1px solid #e4e7ed;
  border-radius: 8px;
  margin-bottom: 10px;
  transition: box-shadow 0.2s;
}
.seg-card:hover { box-shadow: 0 2px 10px rgba(0,0,0,0.08); }
.seg-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px; }
.seg-title { font-weight: 600; font-size: 14px; color: #1a1a1a; }
.seg-desc { font-size: 12px; color: #909399; margin-bottom: 6px; }
.seg-rules { display: flex; flex-wrap: wrap; gap: 4px; margin: 6px 0; }
.rule-chip {
  font-size: 11px; padding: 2px 8px; border-radius: 10px;
  background: #f0f7ff; color: #409eff; border: 1px solid #c6e2ff;
}
.rule-chip.exclude { background: #fff0f0; color: #f56c6c; border-color: #fbc4c4; }
.seg-meta { font-size: 11px; color: #c0c4cc; margin: 6px 0 4px; display: flex; gap: 12px; }
.seg-actions { display: flex; gap: 2px; flex-wrap: wrap; }

.count-result {
  margin-top: 14px;
  padding: 16px 20px;
  background: linear-gradient(135deg, #1a56db, #0ea5e9);
  border-radius: 10px;
  color: #fff;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.stat-block { background: #fafafa; border-radius: 8px; padding: 14px; }
.stat-block-title { font-size: 13px; font-weight: 600; color: #1a1a1a; margin-bottom: 10px; }
.dist-row { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }
.dist-label { font-size: 12px; color: #606266; width: 72px; flex-shrink: 0; }
.aum-card { background: #fff; border: 1px solid #e4e7ed; border-radius: 6px; padding: 12px; text-align: center; }

.fade-enter-active, .fade-leave-active { transition: opacity 0.3s, transform 0.3s; }
.fade-enter-from, .fade-leave-to { opacity: 0; transform: translateY(-8px); }
</style>
