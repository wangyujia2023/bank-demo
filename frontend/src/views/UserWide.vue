<template>
  <div>
    <!-- 查询条件 -->
    <div class="card">
      <div class="card-title">🔍 多条件查询（Apache Doris 4.0 HASP 加速）</div>
      <el-form :model="query" inline label-width="80px">
        <el-form-item label="用户姓名">
          <el-input v-model="query.user_name" placeholder="模糊搜索" clearable style="width:140px" />
        </el-form-item>
        <el-form-item label="身份证号">
          <el-input v-model="query.id_card" placeholder="精确匹配" clearable style="width:180px" />
        </el-form-item>
        <el-form-item label="手机号">
          <el-input v-model="query.phone" placeholder="精确匹配" clearable style="width:140px" />
        </el-form-item>
        <el-form-item label="资产等级">
          <el-select v-model="query.asset_level" clearable placeholder="全部" style="width:130px">
            <el-option v-for="o in assetLevels" :key="o" :label="o" :value="o" />
          </el-select>
        </el-form-item>
        <el-form-item label="活跃等级">
          <el-select v-model="query.active_level" clearable placeholder="全部" style="width:110px">
            <el-option v-for="o in activeLevels" :key="o" :label="o" :value="o" />
          </el-select>
        </el-form-item>
        <el-form-item label="生命周期">
          <el-select v-model="query.lifecycle_stage" clearable placeholder="全部" style="width:120px">
            <el-option v-for="o in lifecycles" :key="o" :label="o" :value="o" />
          </el-select>
        </el-form-item>
        <el-form-item label="偏好渠道">
          <el-select v-model="query.preferred_channel" clearable placeholder="全部" style="width:110px">
            <el-option v-for="o in channels" :key="o" :label="o" :value="o" />
          </el-select>
        </el-form-item>
        <el-form-item label="年龄范围">
          <el-input-number v-model="query.age_min" :min="18" :max="99" style="width:90px" controls-position="right" />
          <span style="margin:0 6px">-</span>
          <el-input-number v-model="query.age_max" :min="18" :max="99" style="width:90px" controls-position="right" />
        </el-form-item>
        <el-form-item label="AUM(万)">
          <el-input-number v-model="query.aum_min" :min="0" style="width:90px" controls-position="right" />
          <span style="margin:0 6px">-</span>
          <el-input-number v-model="query.aum_max" :min="0" style="width:90px" controls-position="right" />
        </el-form-item>
        <el-form-item label="异常用户">
          <el-select v-model="query.anomaly_flag" clearable placeholder="全部" style="width:90px">
            <el-option label="是" :value="1" />
            <el-option label="否" :value="0" />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="loading" @click="handleSearch">查询</el-button>
          <el-button @click="handleReset">重置</el-button>
          <el-button type="success" :loading="exporting" @click="handleExport">导出 Excel</el-button>
        </el-form-item>
      </el-form>
      <div style="font-size:12px;color:#909399;margin-top:4px">
        ✅ 已启用 Doris 4.0 HASP 列存加速 · 分区裁剪优化 · 查询时间通常 &lt;1s
      </div>
    </div>

    <!-- 结果统计 -->
    <div class="card" style="padding:12px 20px">
      <el-row align="middle">
        <el-col :span="12">
          <span style="font-size:13px;color:#606266">
            共查询到 <strong style="color:#409eff;font-size:16px">{{ total.toLocaleString() }}</strong> 名用户
          </span>
        </el-col>
        <el-col :span="12" style="text-align:right">
          <el-tag size="small" type="info">{{ queryTime }}ms</el-tag>
        </el-col>
      </el-row>
    </div>

    <!-- 数据表格 -->
    <div class="card" style="padding:0">
      <el-table
        :data="rows"
        v-loading="loading"
        stripe
        border
        size="small"
        style="width:100%"
        :row-class-name="rowClass"
        @row-click="showDetail"
      >
        <el-table-column prop="user_id"   label="用户ID"   width="90" fixed />
        <el-table-column prop="user_name" label="姓名"     width="80" fixed />
        <el-table-column prop="phone"     label="手机号"   width="130" />
        <el-table-column prop="id_card"   label="身份证"   width="150" />
        <el-table-column prop="age"       label="年龄"     width="60" />
        <el-table-column prop="city"      label="城市"     width="80" />
        <el-table-column prop="asset_level" label="资产等级" width="100">
          <template #default="{row}">
            <el-tag :type="assetTagType(row.asset_level)" size="small">{{ row.asset_level }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="aum_total" label="AUM(万)" width="100" sortable>
          <template #default="{row}">{{ fmtNum(row.aum_total) }}</template>
        </el-table-column>
        <el-table-column prop="deposit_amount" label="存款(万)" width="90">
          <template #default="{row}">{{ fmtNum(row.deposit_amount) }}</template>
        </el-table-column>
        <el-table-column prop="fund_amount"    label="基金(万)" width="90">
          <template #default="{row}">{{ fmtNum(row.fund_amount) }}</template>
        </el-table-column>
        <el-table-column prop="loan_amount"    label="贷款(万)" width="90">
          <template #default="{row}">{{ fmtNum(row.loan_amount) }}</template>
        </el-table-column>
        <el-table-column prop="preferred_channel" label="偏好渠道" width="90" />
        <el-table-column prop="active_level"   label="活跃等级" width="90">
          <template #default="{row}">
            <el-tag :type="activeTagType(row.active_level)" size="small">{{ row.active_level }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="lifecycle_stage" label="生命周期" width="100" />
        <el-table-column prop="credit_score"   label="信用分" width="80" />
        <el-table-column prop="churn_prob"     label="流失概率" width="90">
          <template #default="{row}">
            <el-progress
              :percentage="Math.round((row.churn_prob || 0) * 100)"
              :color="row.churn_prob > 0.5 ? '#f56c6c' : '#67c23a'"
              :stroke-width="8"
              style="width:70px"
            />
          </template>
        </el-table-column>
        <el-table-column prop="anomaly_flag"   label="异常" width="70">
          <template #default="{row}">
            <el-tag v-if="row.anomaly_flag" type="danger" size="small">⚠ 异常</el-tag>
            <el-tag v-else type="success" size="small">正常</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="100" fixed="right">
          <template #default="{row}">
            <el-button type="primary" size="small" link @click.stop="showDetail(row)">360°视图</el-button>
          </template>
        </el-table-column>
      </el-table>

      <el-pagination
        v-model:current-page="page"
        v-model:page-size="pageSize"
        :page-sizes="[20, 50, 100, 200]"
        :total="total"
        layout="total, sizes, prev, pager, next, jumper"
        style="padding:12px 20px;justify-content:flex-end;display:flex"
        @change="fetchData"
      />
    </div>

    <!-- 用户360°视图抽屉 -->
    <el-drawer v-model="drawerVisible" title="用户 360° 视图" size="500px">
      <div v-if="selectedUser">
        <el-descriptions :column="2" border size="small">
          <el-descriptions-item label="用户ID">{{ selectedUser.user_id }}</el-descriptions-item>
          <el-descriptions-item label="姓名">{{ selectedUser.user_name }}</el-descriptions-item>
          <el-descriptions-item label="手机号">{{ selectedUser.phone }}</el-descriptions-item>
          <el-descriptions-item label="身份证">{{ selectedUser.id_card }}</el-descriptions-item>
          <el-descriptions-item label="年龄">{{ selectedUser.age }}</el-descriptions-item>
          <el-descriptions-item label="城市">{{ selectedUser.city }}</el-descriptions-item>
          <el-descriptions-item label="资产等级">
            <el-tag :type="assetTagType(selectedUser.asset_level)" size="small">{{ selectedUser.asset_level }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="AUM总资产">{{ fmtNum(selectedUser.aum_total) }} 万</el-descriptions-item>
          <el-descriptions-item label="存款余额">{{ fmtNum(selectedUser.deposit_amount) }} 万</el-descriptions-item>
          <el-descriptions-item label="基金持有">{{ fmtNum(selectedUser.fund_amount) }} 万</el-descriptions-item>
          <el-descriptions-item label="理财余额">{{ fmtNum(selectedUser.wm_amount) }} 万</el-descriptions-item>
          <el-descriptions-item label="贷款余额">{{ fmtNum(selectedUser.loan_amount) }} 万</el-descriptions-item>
          <el-descriptions-item label="信用评分">{{ selectedUser.credit_score }}</el-descriptions-item>
          <el-descriptions-item label="信用等级">{{ selectedUser.credit_grade }}</el-descriptions-item>
          <el-descriptions-item label="风险偏好">{{ ['','保守','稳健','平衡','进取','激进'][selectedUser.risk_level] }}</el-descriptions-item>
          <el-descriptions-item label="偏好渠道">{{ selectedUser.preferred_channel }}</el-descriptions-item>
          <el-descriptions-item label="APP登录(月)">{{ selectedUser.app_login_30d }} 次</el-descriptions-item>
          <el-descriptions-item label="活跃等级">{{ selectedUser.active_level }}</el-descriptions-item>
          <el-descriptions-item label="生命周期">{{ selectedUser.lifecycle_stage }}</el-descriptions-item>
          <el-descriptions-item label="流失概率">{{ ((selectedUser.churn_prob || 0) * 100).toFixed(1) }}%</el-descriptions-item>
          <el-descriptions-item label="异常标记">
            <el-tag :type="selectedUser.anomaly_flag ? 'danger' : 'success'" size="small">
              {{ selectedUser.anomaly_flag ? '⚠ 异常' : '正常' }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="注册日期" :span="2">{{ selectedUser.register_date }}</el-descriptions-item>
        </el-descriptions>

        <div v-if="selectedUser.log_tags" style="margin-top:16px">
          <div style="font-size:13px;font-weight:600;margin-bottom:8px">🏷️ AI 日志标签</div>
          <el-tag
            v-for="tag in parseTags(selectedUser.log_tags)"
            :key="tag" style="margin:3px" size="small" type="warning"
          >{{ tag }}</el-tag>
        </div>
      </div>
    </el-drawer>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { userApi } from '@/api'
import { ElMessage } from 'element-plus'

const assetLevels = ['VIP私行', 'VIP钻石', 'VIP铂金', 'VIP黄金', '普通']
const activeLevels = ['高活', '中活', '低活', '沉睡']
const lifecycles   = ['新客', '成长', '成熟', '沉睡', '流失预警']
const channels     = ['APP', '网点', '小程序', '网银']

const query = reactive({
  user_name: '', id_card: '', phone: '',
  asset_level: '', active_level: '', lifecycle_stage: '', preferred_channel: '',
  age_min: undefined, age_max: undefined,
  aum_min: undefined, aum_max: undefined,
  anomaly_flag: undefined,
})
const rows     = ref([])
const total    = ref(0)
const page     = ref(1)
const pageSize = ref(20)
const loading  = ref(false)
const exporting = ref(false)
const queryTime = ref(0)
const drawerVisible = ref(false)
const selectedUser  = ref(null)

const fmtNum = v => v == null ? '-' : Number(v).toFixed(2)
const parseTags = v => { try { return JSON.parse(v) } catch { return [] } }
const assetTagType = l => ({ 'VIP私行': 'danger', 'VIP钻石': 'warning', 'VIP铂金': '', 'VIP黄金': 'success' }[l] || 'info')
const activeTagType = l => ({ '高活': 'success', '中活': '', '低活': 'warning', '沉睡': 'info' }[l] || '')
const rowClass = ({ row }) => row.anomaly_flag ? 'anomaly-row' : ''

async function fetchData() {
  loading.value = true
  const t0 = Date.now()
  try {
    const params = { page: page.value, page_size: pageSize.value }
    Object.entries(query).forEach(([k, v]) => { if (v !== '' && v !== undefined && v !== null) params[k] = v })
    const res = await userApi.queryWide(params)
    rows.value  = res.rows || []
    total.value = res.total || 0
    queryTime.value = Date.now() - t0
  } finally { loading.value = false }
}

function handleSearch() { page.value = 1; fetchData() }
function handleReset() {
  Object.keys(query).forEach(k => query[k] = undefined)
  query.user_name = query.id_card = query.phone = query.asset_level =
    query.active_level = query.lifecycle_stage = query.preferred_channel = ''
  handleSearch()
}

function showDetail(row) { selectedUser.value = row; drawerVisible.value = true }

async function handleExport() {
  exporting.value = true
  try {
    ElMessage.info('导出功能：将当前查询结果导出为 Excel（实际部署时接入后端 /api/user/export 接口）')
  } finally { exporting.value = false }
}

onMounted(fetchData)
</script>

<style>
.anomaly-row td { background: #fff5f5 !important; }
</style>
