<template>
  <div class="vs-wrap">

    <!-- 顶部横幅 -->
    <div class="card hasp-banner">
      <div class="banner-left">
        <div class="banner-title">
          <el-tag type="danger" size="small" effect="dark" style="margin-right:8px">HASP</el-tag>
          Doris 混合检索能力演示 · 向量 + 标量协同
        </div>
        <div class="banner-desc">
          上传照片提取 <b>8维特征向量</b>，结合标签标量过滤与文本描述，展示
          <code>cosine_distance()</code> + <code>WHERE</code> 在 HASP 向量化引擎下的混合检索能力。
        </div>
      </div>
      <el-button type="primary" :loading="initing" @click="initData" plain>
        <el-icon><RefreshRight /></el-icon> 初始化示例数据
      </el-button>
    </div>

    <div class="main-grid">

      <!-- 左：用户库 -->
      <div class="card users-panel">
        <div class="panel-title">
          用户库
          <el-tag size="small" style="margin-left:6px">{{ users.length }}</el-tag>
          <el-button size="small" type="primary" plain style="margin-left:auto" @click="openUploadDialog">
            <el-icon><Plus /></el-icon>
          </el-button>
        </div>
        <div v-if="!users.length" class="empty-tip"><el-empty :image-size="50" description="请先初始化" /></div>
        <div v-else class="user-grid">
          <div
            v-for="u in users" :key="u.user_id"
            class="user-mini"
            :class="{ selected: queryPhoto.userId === u.user_id }"
            @click="useUserAsQuery(u)"
          >
            <img :src="avatarUrl(u.avatar_style)" class="mini-avatar" />
            <div class="mini-name">{{ u.user_name }}</div>
            <div class="mini-labels">
              <el-tag v-for="lb in u.labels" :key="lb" size="small" effect="plain" style="font-size:10px;margin:1px">{{ lb }}</el-tag>
            </div>
          </div>
        </div>

        <!-- 标签库 -->
        <div class="panel-title" style="margin-top:12px">标签库</div>
        <div class="label-chips">
          <div
            v-for="lb in labels" :key="lb.label_id"
            class="lchip"
            :style="{ borderColor: lb.color, color: lb.color }"
          >
            <span class="lchip-name">{{ lb.label_name }}</span>
            <span class="lchip-cnt">{{ lb.user_count }}</span>
          </div>
        </div>
      </div>

      <!-- 右：检索面板 -->
      <div class="right-col">

        <!-- 搜索输入区 -->
        <div class="card search-card">
          <!-- 模式 Tab -->
          <div class="mode-tabs">
            <div
              v-for="m in MODES" :key="m.key"
              class="mode-tab"
              :class="{ active: searchMode === m.key }"
              @click="searchMode = m.key"
            >
              <span class="mode-icon">{{ m.icon }}</span>
              <div>
                <div class="mode-title">{{ m.title }}</div>
                <div class="mode-desc">{{ m.desc }}</div>
              </div>
            </div>
          </div>

          <div class="input-area">
            <!-- 照片上传（向量 / 混合模式） -->
            <div v-if="searchMode !== 'scalar'" class="input-block">
              <div class="input-label">
                <el-icon><Picture /></el-icon> 查询照片
                <span class="input-hint">上传后自动提取 8 维特征向量</span>
              </div>
              <div class="photo-row">
                <div
                  class="photo-drop"
                  :class="{ loaded: queryPhoto.previewUrl }"
                  @click="triggerFile"
                  @dragover.prevent
                  @drop.prevent="onDrop"
                >
                  <img v-if="queryPhoto.previewUrl" :src="queryPhoto.previewUrl" class="photo-preview" />
                  <div v-else class="photo-hint">
                    <el-icon size="28" color="#c0c4cc"><Upload /></el-icon>
                    <div style="font-size:12px;color:#c0c4cc;margin-top:6px">点击或拖拽</div>
                  </div>
                </div>
                <div v-if="queryPhoto.embedding.length" class="embed-preview">
                  <div class="embed-title">提取向量（8维）</div>
                  <VecBar :vec="queryPhoto.embedding" :dim-labels="dimLabels" />
                  <div class="embed-from">
                    <el-tag size="small" :type="queryPhoto.userId ? 'primary' : 'success'" effect="plain">
                      {{ queryPhoto.userId ? '来自用户库' : '来自上传照片' }}
                    </el-tag>
                  </div>
                </div>
                <div v-else-if="!queryPhoto.previewUrl" class="embed-preview empty-embed">
                  <div style="text-align:center;color:#c0c4cc">
                    <div style="font-size:28px;margin-bottom:8px">📷</div>
                    上传照片后自动生成向量
                  </div>
                </div>
              </div>
              <input ref="fileInputRef" type="file" accept="image/*" style="display:none" @change="onFileChange" />
            </div>

            <!-- 标签标量过滤（标量 / 混合模式） -->
            <div v-if="searchMode !== 'vector'" class="input-block">
              <div class="input-label">
                <el-icon><PriceTag /></el-icon> 标签过滤
                <span class="input-hint">AND 逻辑，选择的标签必须全部命中</span>
              </div>
              <div class="tag-selector">
                <div
                  v-for="lb in labels" :key="lb.label_id"
                  class="tag-opt"
                  :class="{ selected: labelFilters.includes(lb.label_name) }"
                  :style="{ borderColor: labelFilters.includes(lb.label_name) ? lb.color : '#e4e7ed', color: labelFilters.includes(lb.label_name) ? lb.color : '#606266' }"
                  @click="toggleLabel(lb.label_name)"
                >{{ lb.label_name }}</div>
              </div>
            </div>

            <!-- 描述文本（混合模式） -->
            <div v-if="searchMode === 'hybrid'" class="input-block">
              <div class="input-label">
                <el-icon><ChatLineRound /></el-icon> 描述文本
                <span class="input-hint">关键词自动映射为向量维度权重</span>
              </div>
              <el-input
                v-model="description"
                type="textarea"
                :rows="2"
                placeholder="如：活跃热情 冒险进取 科技创新 社交外向…"
              />
              <div v-if="description" class="desc-keywords">
                <span v-for="kw in matchedKeywords" :key="kw" class="kw-tag">{{ kw }}</span>
              </div>
            </div>

            <!-- 执行区 -->
            <div class="exec-row">
              <span style="font-size:13px;color:#606266">Top-K：</span>
              <el-slider v-model="topK" :min="1" :max="10" :step="1" show-stops style="width:140px;margin:0 10px" />
              <b style="margin-right:16px">{{ topK }}</b>
              <el-button
                type="primary"
                size="large"
                :loading="searching"
                :disabled="!canSearch"
                @click="doSearch"
              >
                <el-icon><Search /></el-icon>
                {{ MODE_BTN[searchMode] }}
              </el-button>
              <el-button v-if="result" @click="clearResult">清除</el-button>
            </div>
          </div>
        </div>

        <!-- SQL 展示 -->
        <div v-if="result" class="card sql-card">
          <div class="sql-header" @click="sqlOpen = !sqlOpen">
            <div style="display:flex;align-items:center;gap:8px">
              <el-tag :type="MODE_COLOR[result.mode]" size="small" effect="dark">{{ MODE_LABEL[result.mode] }}</el-tag>
              <span style="font-size:13px;font-weight:600;color:#1a1a1a">执行的 HASP SQL</span>
              <el-tag size="small" effect="plain" type="success">cosine_distance</el-tag>
              <el-tag v-if="result.label_filters.length" size="small" effect="plain" type="warning">标量过滤</el-tag>
            </div>
            <span style="font-size:12px;color:#67c23a;cursor:pointer">{{ sqlOpen ? '▲ 收起' : '▼ 展开' }}</span>
          </div>
          <pre v-if="sqlOpen" class="sql-code">{{ result.sql }}</pre>
        </div>

        <!-- 结果 -->
        <div v-if="result" class="card">
          <div class="card-title">
            检索结果
            <el-tag size="small" style="margin-left:8px">{{ result.results.length }} 条</el-tag>
            <span v-if="result.mode !== 'scalar'" style="font-size:12px;color:#909399;margin-left:12px">按相似度排序</span>
          </div>
          <div v-if="!result.results.length" class="empty-tip" style="padding:20px 0">
            <el-empty description="无匹配结果" :image-size="60" />
          </div>
          <div v-else class="result-list">
            <div v-for="(r, idx) in result.results" :key="r.user_id" class="result-item">
              <div class="rank-badge" :class="`rank-${idx+1}`">{{ idx + 1 }}</div>
              <img :src="avatarUrl(r.avatar_style)" class="res-avatar" />
              <div class="res-info">
                <div class="res-name">{{ r.user_name }}</div>
                <div class="res-desc">{{ r.description }}</div>
                <div style="margin-top:4px">
                  <el-tag
                    v-for="lb in r.labels" :key="lb"
                    size="small" effect="plain" round
                    :type="labelFilters.includes(lb) ? 'warning' : ''"
                    style="margin:2px 2px 0 0;font-size:11px"
                  >{{ lb }}</el-tag>
                </div>
              </div>
              <div v-if="result.mode !== 'scalar'" class="res-score">
                <div class="score-val" :style="{color: scoreColor(r.similarity)}">
                  {{ (r.similarity * 100).toFixed(1) }}%
                </div>
                <div class="score-label">相似度</div>
                <el-progress
                  :percentage="+(r.similarity * 100).toFixed(1)"
                  :stroke-width="6"
                  :color="scoreColor(r.similarity)"
                  style="width:80px;margin-top:4px"
                  :show-text="false"
                />
                <div class="dist-val">dist={{ r.distance?.toFixed(4) }}</div>
              </div>
            </div>
          </div>
        </div>

      </div>
    </div>

    <!-- 新增用户 Dialog（保留原有功能） -->
    <el-dialog v-model="uploadDialog" title="新增用户画像" width="560px" :close-on-click-modal="false">
      <div class="upload-body">
        <div class="upload-left">
          <div class="photo-drop" :class="{'has-photo': addForm.previewUrl}" @click="triggerAddFile" @dragover.prevent @drop.prevent="onAddDrop">
            <img v-if="addForm.previewUrl" :src="addForm.previewUrl" class="photo-preview" />
            <div v-else class="photo-hint"><el-icon size="32" color="#c0c4cc"><Upload /></el-icon><div style="margin-top:8px;font-size:13px;color:#909399">点击或拖拽上传照片</div></div>
          </div>
          <input ref="addFileRef" type="file" accept="image/*" style="display:none" @change="onAddFileChange" />
          <div v-if="addForm.embedding.length" class="embed-preview" style="margin-top:8px">
            <div class="embed-title">生成向量（8维）</div>
            <VecBar :vec="addForm.embedding" :dim-labels="dimLabels" />
          </div>
        </div>
        <div class="upload-right">
          <div style="display:flex;justify-content:flex-end;margin-bottom:10px">
            <el-button size="small" type="info" plain @click="randomFill"><el-icon><Refresh /></el-icon> 随机填写</el-button>
          </div>
          <el-form :model="addForm" label-width="68px" size="small">
            <el-form-item label="用户名"><el-input v-model="addForm.user_name" /></el-form-item>
            <el-form-item label="描述"><el-input v-model="addForm.description" type="textarea" :rows="3" /></el-form-item>
            <el-form-item label="标签">
              <el-select v-model="addForm.labels" multiple style="width:100%">
                <el-option v-for="lb in labels" :key="lb.label_name" :label="lb.label_name" :value="lb.label_name">
                  <span :style="{color:lb.color}">● </span>{{ lb.label_name }}
                </el-option>
              </el-select>
            </el-form-item>
            <el-form-item label="头像种子"><el-input v-model="addForm.avatar_style" /></el-form-item>
          </el-form>
        </div>
      </div>
      <template #footer>
        <el-button @click="uploadDialog = false">取消</el-button>
        <el-button type="primary" :loading="addLoading" :disabled="!addForm.photoFile || !addForm.user_name" @click="submitAdd">提交入库</el-button>
      </template>
    </el-dialog>

  </div>
</template>

<script setup>
import { ref, computed, onMounted, defineComponent, h, reactive } from 'vue'
import { ElMessage } from 'element-plus'
import { RefreshRight, Search, Plus, Upload, Picture, PriceTag, ChatLineRound, Refresh } from '@element-plus/icons-vue'
import { vectorApi } from '@/api'

// ── 向量柱状图组件 ──────────────────────────────────────────────
const VecBar = defineComponent({
  props: { vec: Array, dimLabels: Array },
  setup(props) {
    return () => {
      if (!props.vec?.length) return null
      const max = Math.max(...props.vec, 0.01)
      return h('div', { style: 'display:flex;gap:3px;align-items:flex-end;margin-top:6px' },
        props.vec.map((v, i) =>
          h('div', { key: i, style: 'display:flex;flex-direction:column;align-items:center;gap:2px' }, [
            h('div', { style: `width:18px;border-radius:2px 2px 0 0;height:${Math.round(v/max*40)}px;min-height:2px;background:${v/max>0.65?'#409eff':'#c6e2ff'};opacity:0.9` }),
            h('div', { style: 'font-size:9px;color:#909399;writing-mode:vertical-rl;height:36px;line-height:1.2;text-align:center' },
              (props.dimLabels?.[i] || `d${i}`).slice(0, 3))
          ])
        )
      )
    }
  }
})

// ── 常量 ────────────────────────────────────────────────────────
const MODES = [
  { key: 'vector', icon: '📷', title: '图片向量检索', desc: '上传照片 → cosine_distance 近似最近邻' },
  { key: 'scalar', icon: '🏷️', title: '标量标签检索', desc: '选择标签 → WHERE JSON 精确过滤' },
  { key: 'hybrid', icon: '🔀', title: '混合检索',    desc: '照片向量 + 标签过滤 + 描述文本协同' },
]
const MODE_BTN   = { vector: '向量检索', scalar: '标签检索', hybrid: '混合检索' }
const MODE_COLOR = { vector: 'primary', scalar: 'success', hybrid: 'warning' }
const MODE_LABEL = { vector: '向量检索', scalar: '标量过滤', hybrid: '混合检索' }

const RANDOM_NAMES = ['闪光霆','云端影','墨痕客','星尘灵','火焰凤','翡翠龙','幻影猫','风暴鹰','月光兔','晨曦虎']
const RANDOM_DESCS = ['阳光活泼，热爱探索新事物','沉稳睿智，擅长分析决策','幽默风趣，人缘极好','冒险进取，偏好高风险高收益','科技达人，深度数字用户']
const AVATAR_SEEDS = ['fox','cat','dog','lion','bear','tiger','wolf','panda','rabbit','owl']

// ── 状态 ────────────────────────────────────────────────────────
const users     = ref([])
const labels    = ref([])
const dimLabels = ref([])
const initing   = ref(false)
const searching = ref(false)
const sqlOpen   = ref(true)

const searchMode   = ref('hybrid')
const topK         = ref(5)
const labelFilters = ref([])
const description  = ref('')
const result       = ref(null)

// 查询照片状态
const queryPhoto = reactive({ previewUrl: '', embedding: [], photoFile: null, userId: null })
const fileInputRef = ref(null)

// 新增用户
const uploadDialog = ref(false)
const addLoading   = ref(false)
const addFileRef   = ref(null)
const addForm = reactive({ user_name: '', description: '', avatar_style: '', labels: [], photoFile: null, previewUrl: '', embedding: [] })

// ── 计算 ────────────────────────────────────────────────────────
const canSearch = computed(() => {
  if (searchMode.value === 'vector') return queryPhoto.embedding.length > 0
  if (searchMode.value === 'scalar') return labelFilters.value.length > 0
  return queryPhoto.embedding.length > 0 || labelFilters.value.length > 0 || description.value.trim().length > 0
})

const matchedKeywords = computed(() => {
  if (!description.value) return []
  const KWS = ['活跃','活泼','积极','热情','智慧','聪明','幽默','搞笑','冒险','勇敢','探索','科技','技术','运动','健身','艺术','创意','社交','友善','外向']
  return KWS.filter(kw => description.value.includes(kw))
})

// ── 工具函数 ────────────────────────────────────────────────────
function avatarUrl(style) {
  return `https://api.dicebear.com/7.x/bottts/svg?seed=${style}&backgroundColor=b6e3f4,ffd5dc,ffdfbf,c0aede,d1d4f9`
}
function scoreColor(sim) {
  if (sim >= 0.9) return '#67c23a'
  if (sim >= 0.7) return '#409eff'
  if (sim >= 0.5) return '#e6a23c'
  return '#f56c6c'
}
function toggleLabel(lb) {
  const i = labelFilters.value.indexOf(lb)
  if (i >= 0) labelFilters.value.splice(i, 1)
  else labelFilters.value.push(lb)
}
function clearResult() { result.value = null }

// ── 照片上传（查询用） ───────────────────────────────────────────
function triggerFile() { fileInputRef.value?.click() }
function onDrop(e) { const f = e.dataTransfer?.files?.[0]; if (f) handleQueryFile(f) }
function onFileChange(e) { const f = e.target?.files?.[0]; if (f) handleQueryFile(f) }
function handleQueryFile(file) {
  queryPhoto.photoFile = file
  queryPhoto.previewUrl = URL.createObjectURL(file)
  queryPhoto.userId = null
  // 前端预览向量（提交时后端重新生成）
  const seed = file.size + file.name.charCodeAt(0)
  queryPhoto.embedding = Array.from({ length: 8 }, (_, i) => {
    const v = Math.abs(Math.sin(seed * (i + 1) * 0.37))
    return Math.round((v > 0.55 ? 0.75 + v * 0.20 : 0.05 + v * 0.25) * 10000) / 10000
  })
}

// 点击用户库直接用作查询
function useUserAsQuery(u) {
  queryPhoto.previewUrl = avatarUrl(u.avatar_style)
  queryPhoto.embedding  = u.photo_embedding || []
  queryPhoto.userId     = u.user_id
  queryPhoto.photoFile  = null
  searchMode.value = 'vector'
}

// ── 检索 ────────────────────────────────────────────────────────
async function doSearch() {
  searching.value = true
  result.value = null
  try {
    if (queryPhoto.photoFile && searchMode.value !== 'scalar') {
      // 有真实照片文件 → multipart
      const fd = new FormData()
      fd.append('photo', queryPhoto.photoFile)
      fd.append('label_filters', JSON.stringify(searchMode.value !== 'vector' ? labelFilters.value : []))
      fd.append('description', searchMode.value === 'hybrid' ? description.value : '')
      fd.append('top_k', topK.value)
      result.value = await vectorApi.searchByPhoto(fd)
      queryPhoto.embedding = result.value.photo_embedding || queryPhoto.embedding
    } else {
      // 用现有向量 or 仅标签/描述
      result.value = await vectorApi.searchHybrid({
        query_vector:  (searchMode.value !== 'scalar' && queryPhoto.embedding.length) ? queryPhoto.embedding : null,
        label_filters: searchMode.value !== 'vector' ? labelFilters.value : [],
        description:   searchMode.value === 'hybrid' ? description.value : null,
        top_k:         topK.value,
      })
    }
    sqlOpen.value = true
  } finally { searching.value = false }
}

// ── 数据加载 ────────────────────────────────────────────────────
async function loadData() {
  try {
    const [u, lb, dl] = await Promise.all([vectorApi.users(), vectorApi.labels(), vectorApi.dimLabels()])
    users.value = u; labels.value = lb; dimLabels.value = dl
  } catch {}
}
async function initData() {
  initing.value = true
  try {
    const res = await vectorApi.init()
    ElMessage.success(`初始化完成：${res.users} 用户、${res.labels} 标签`)
    await loadData()
  } finally { initing.value = false }
}

// ── 新增用户 ────────────────────────────────────────────────────
function openUploadDialog() {
  Object.assign(addForm, { user_name: '', description: '', avatar_style: '', labels: [], photoFile: null, previewUrl: '', embedding: [] })
  uploadDialog.value = true
}
function triggerAddFile() { addFileRef.value?.click() }
function onAddDrop(e) { const f = e.dataTransfer?.files?.[0]; if (f) handleAddFile(f) }
function onAddFileChange(e) { const f = e.target?.files?.[0]; if (f) handleAddFile(f) }
function handleAddFile(file) {
  addForm.photoFile = file
  addForm.previewUrl = URL.createObjectURL(file)
  const seed = file.size + file.name.charCodeAt(0)
  addForm.embedding = Array.from({ length: 8 }, (_, i) => {
    const v = Math.abs(Math.sin(seed * (i + 1) * 0.37))
    return Math.round((v > 0.55 ? 0.75 + v * 0.20 : 0.05 + v * 0.25) * 10000) / 10000
  })
}
function randomFill() {
  const pick = a => a[Math.floor(Math.random() * a.length)]
  addForm.user_name   = pick(RANDOM_NAMES)
  addForm.description = pick(RANDOM_DESCS)
  addForm.avatar_style = pick(AVATAR_SEEDS)
  addForm.labels = labels.value.sort(() => Math.random() - 0.5).slice(0, Math.floor(Math.random() * 2) + 1).map(l => l.label_name)
}
async function submitAdd() {
  if (!addForm.photoFile || !addForm.user_name) return
  addLoading.value = true
  try {
    const fd = new FormData()
    fd.append('photo', addForm.photoFile)
    fd.append('user_name', addForm.user_name)
    fd.append('description', addForm.description)
    fd.append('avatar_style', addForm.avatar_style || addForm.user_name)
    fd.append('labels', JSON.stringify(addForm.labels))
    const res = await vectorApi.uploadUser(fd)
    addForm.embedding = res.embedding
    ElMessage.success(`用户 "${res.user_name}" 已入库（ID: ${res.user_id}）`)
    uploadDialog.value = false
    await loadData()
  } finally { addLoading.value = false }
}

onMounted(loadData)
</script>

<style scoped>
.vs-wrap { display: flex; flex-direction: column; gap: 16px; }

.hasp-banner {
  display: flex; align-items: center; justify-content: space-between; gap: 16px;
  background: linear-gradient(135deg, #f0f7ff 0%, #fff5f0 100%);
  border-left: 4px solid #409eff;
}
.banner-title { font-size: 15px; font-weight: 600; color: #1a1a1a; margin-bottom: 6px; }
.banner-desc  { font-size: 13px; color: #606266; line-height: 1.6; }
.banner-desc code { background: #f0f0f0; padding: 1px 5px; border-radius: 3px; font-size: 12px; }

.main-grid { display: grid; grid-template-columns: 240px 1fr; gap: 16px; align-items: start; }

/* 左侧用户库 */
.users-panel { max-height: calc(100vh - 200px); overflow-y: auto; padding: 12px; }
.panel-title { display: flex; align-items: center; font-size: 13px; font-weight: 600; color: #303133; margin-bottom: 8px; }
.user-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
.user-mini {
  padding: 8px; border-radius: 8px; border: 2px solid transparent;
  cursor: pointer; transition: all 0.2s; background: #fafafa; text-align: center;
}
.user-mini:hover { border-color: #c6e2ff; background: #f0f7ff; }
.user-mini.selected { border-color: #409eff; background: #ecf5ff; }
.mini-avatar { width: 44px; height: 44px; border-radius: 50%; background: #f0f0f0; }
.mini-name { font-size: 12px; font-weight: 600; color: #303133; margin: 3px 0 2px; }
.mini-labels { display: flex; flex-wrap: wrap; justify-content: center; gap: 2px; }
.empty-tip { text-align: center; padding: 20px 0; }

.label-chips { display: flex; flex-wrap: wrap; gap: 6px; }
.lchip {
  display: flex; align-items: center; gap: 4px;
  padding: 3px 8px; border-radius: 12px; border: 1.5px solid;
  font-size: 11px;
}
.lchip-name { font-weight: 600; }
.lchip-cnt  { opacity: 0.6; font-size: 10px; }

/* 右侧检索区 */
.right-col { display: flex; flex-direction: column; gap: 16px; }

.mode-tabs { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 8px; margin-bottom: 16px; }
.mode-tab {
  display: flex; align-items: flex-start; gap: 10px; padding: 10px 12px;
  border-radius: 8px; border: 2px solid #e4e7ed; cursor: pointer;
  transition: all 0.2s; background: #fafafa;
}
.mode-tab:hover { border-color: #c6e2ff; background: #f0f7ff; }
.mode-tab.active { border-color: #409eff; background: #ecf5ff; }
.mode-icon  { font-size: 20px; flex-shrink: 0; margin-top: 1px; }
.mode-title { font-size: 13px; font-weight: 600; color: #303133; }
.mode-desc  { font-size: 11px; color: #909399; margin-top: 2px; line-height: 1.4; }

.input-area { display: flex; flex-direction: column; gap: 16px; }
.input-block {}
.input-label {
  display: flex; align-items: center; gap: 6px;
  font-size: 13px; font-weight: 600; color: #303133; margin-bottom: 8px;
}
.input-hint { font-size: 11px; color: #909399; font-weight: 400; }

.photo-row { display: flex; gap: 12px; align-items: flex-start; }
.photo-drop {
  width: 110px; height: 110px; border: 2px dashed #dcdfe6; border-radius: 10px;
  cursor: pointer; display: flex; align-items: center; justify-content: center;
  overflow: hidden; background: #fafafa; flex-shrink: 0; transition: all 0.2s;
}
.photo-drop:hover { border-color: #409eff; background: #f0f7ff; }
.photo-drop.loaded { border-style: solid; border-color: #409eff; }
.photo-preview { width: 100%; height: 100%; object-fit: cover; }
.photo-hint { display: flex; flex-direction: column; align-items: center; }

.embed-preview {
  flex: 1; background: #f8f9fa; border-radius: 8px; padding: 10px 12px;
  border: 1px solid #f0f0f0; min-height: 90px; display: flex; flex-direction: column; justify-content: center;
}
.empty-embed { align-items: center; justify-content: center; font-size: 12px; color: #c0c4cc; }
.embed-title { font-size: 11px; color: #909399; margin-bottom: 4px; }
.embed-from  { margin-top: 6px; }

.tag-selector { display: flex; flex-wrap: wrap; gap: 8px; }
.tag-opt {
  padding: 5px 12px; border-radius: 16px; border: 1.5px solid #e4e7ed;
  cursor: pointer; font-size: 12px; transition: all 0.15s; font-weight: 600;
}
.tag-opt:hover { opacity: 0.8; transform: scale(1.02); }
.tag-opt.selected { background: #f0f7ff; }

.desc-keywords { display: flex; flex-wrap: wrap; gap: 4px; margin-top: 6px; }
.kw-tag { background: #f0f9eb; color: #67c23a; border: 1px solid #b3e19d; border-radius: 10px; padding: 2px 8px; font-size: 11px; }

.exec-row { display: flex; align-items: center; padding-top: 12px; border-top: 1px solid #f0f0f0; }

/* SQL */
.sql-card { padding: 12px 16px; }
.sql-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; cursor: pointer; }
.sql-code {
  background: #1e1e2e; color: #a8b3cf; padding: 14px 18px; border-radius: 8px;
  font-family: 'JetBrains Mono', Consolas, monospace; font-size: 12px;
  line-height: 1.8; white-space: pre; overflow-x: auto; margin: 0;
}

/* 结果 */
.result-list { display: flex; flex-direction: column; gap: 10px; }
.result-item {
  display: flex; gap: 12px; align-items: center;
  padding: 12px 14px; border-radius: 8px; background: #fafafa;
  border: 1px solid #f0f0f0; transition: box-shadow 0.2s;
}
.result-item:hover { box-shadow: 0 2px 8px rgba(0,0,0,0.08); background: #fff; }
.rank-badge {
  width: 26px; height: 26px; border-radius: 50%; flex-shrink: 0;
  display: flex; align-items: center; justify-content: center;
  font-size: 12px; font-weight: 700; color: #fff; background: #409eff;
}
.rank-badge.rank-1 { background: #f7c948; }
.rank-badge.rank-2 { background: #b0b0b0; }
.rank-badge.rank-3 { background: #cd7f32; }
.res-avatar { width: 46px; height: 46px; border-radius: 50%; flex-shrink: 0; background: #f0f0f0; }
.res-info   { flex: 1; min-width: 0; }
.res-name   { font-size: 14px; font-weight: 600; color: #1a1a1a; }
.res-desc   { font-size: 12px; color: #909399; margin: 2px 0; }
.res-score  { text-align: center; flex-shrink: 0; min-width: 72px; }
.score-val  { font-size: 20px; font-weight: 700; }
.score-label { font-size: 10px; color: #909399; }
.dist-val   { font-size: 10px; color: #c0c4cc; margin-top: 2px; }

/* 新增对话框 */
.upload-body { display: flex; gap: 20px; }
.upload-left { width: 200px; flex-shrink: 0; }
.upload-right { flex: 1; }
</style>
