<template>
  <div class="chart-recognition">
    <div class="card">
      <h2>📊 图表识别 & 向量检索</h2>

      <!-- 上传区域 -->
      <el-upload
        class="upload-area"
        drag
        action="/api/upload/image"
        :auto-upload="true"
        :on-success="handleUploadSuccess"
        :on-error="handleUploadError"
        accept=".svg,.png,.jpg,.jpeg,.gif,.webp"
        multiple
      >
        <el-icon class="el-icon--upload"><upload-filled /></el-icon>
        <div class="el-upload__text">
          拖拽或点击上传图表
          <em>（支持 SVG、PNG、JPG、GIF、WebP）</em>
        </div>
      </el-upload>

      <!-- 上传结果 -->
      <div v-if="uploadResult" class="result-box">
        <el-alert type="success" :closable="false" show-icon>
          <template #default>
            <strong>✅ 上传成功</strong>
            <p>文件: {{ uploadResult.filename }}</p>
            <p>格式: {{ uploadResult.format }} | 大小: {{ (uploadResult.size / 1024).toFixed(2) }} KB</p>
            <p>分辨率: {{ uploadResult.resolution[0] }} × {{ uploadResult.resolution[1] }}</p>
            <p>向量维度: {{ uploadResult.vector_dim }}</p>
          </template>
        </el-alert>
      </div>

      <!-- 分类结果 -->
      <div v-if="classifyResult" class="result-box">
        <el-alert :type="classifyResult.confidence > 0.7 ? 'success' : 'warning'" :closable="false" show-icon>
          <template #default>
            <strong>🎯 图表分类结果</strong>
            <p>类型: <el-tag>{{ classifyResult.chart_type }}</el-tag></p>
            <p>置信度: <el-progress :percentage="Math.round(classifyResult.confidence * 100)" :stroke-width="8" /></p>
            <p>{{ classifyResult.recommendation }}</p>
          </template>
        </el-alert>
      </div>

      <!-- 相似图表搜索 -->
      <div v-if="uploadResult" class="search-box">
        <el-button type="primary" @click="searchSimilar" :loading="searching">
          🔍 搜索相似图表
        </el-button>
        <el-switch v-model="enableVectorSearch" active-text="向量检索" inactive-text="关闭" style="margin-left: 12px" />
      </div>

      <!-- 相似结果 -->
      <div v-if="similarResults.length > 0" class="result-box">
        <h4>相似图表（Top 5）</h4>
        <el-row :gutter="16">
          <el-col v-for="(item, idx) in similarResults" :key="idx" :span="8">
            <div class="similar-card">
              <img :src="item.preview_url" style="width:100%; border-radius:4px; margin-bottom:8px" />
              <p><strong>{{ item.name }}</strong></p>
              <p style="color:#909399; font-size:12px">相似度: {{ (item.similarity * 100).toFixed(1) }}%</p>
            </div>
          </el-col>
        </el-row>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { ElMessage } from 'element-plus'
import axios from 'axios'

const uploadResult = ref(null)
const classifyResult = ref(null)
const similarResults = ref([])
const searching = ref(false)
const enableVectorSearch = ref(false)

const handleUploadSuccess = async (response) => {
  if (response.status === 'success') {
    uploadResult.value = response
    ElMessage.success(`✅ ${response.filename} 上传成功`)

    // 自动分类
    await classifyChart(response.filename)
  }
}

const handleUploadError = (error) => {
  ElMessage.error(`❌ 上传失败: ${error.message}`)
}

const classifyChart = async (filename) => {
  try {
    // 实际应该传文件，这里示意
    classifyResult.value = {
      filename,
      chart_type: "柱状图",
      confidence: 0.85,
      recommendation: "检测结果高置信度，可直接使用"
    }
  } catch (e) {
    ElMessage.error(`分类失败: ${e.message}`)
  }
}

const searchSimilar = async () => {
  if (!enableVectorSearch.value) {
    ElMessage.warning('请先启用向量检索')
    return
  }

  searching.value = true
  try {
    // 模拟向量搜索结果
    similarResults.value = [
      { name: '销售趋势图', similarity: 0.94, preview_url: 'data:image/svg+xml,<svg></svg>' },
      { name: '用户增长图', similarity: 0.87, preview_url: 'data:image/svg+xml,<svg></svg>' },
      { name: '收入对比图', similarity: 0.82, preview_url: 'data:image/svg+xml,<svg></svg>' },
    ]
    ElMessage.success('向量检索完成')
  } catch (e) {
    ElMessage.error(`检索失败: ${e.message}`)
  } finally {
    searching.value = false
  }
}
</script>

<style scoped>
.chart-recognition {
  padding: 20px;
  background: #f0f2f5;
  min-height: 100vh;
}

.card {
  background: white;
  border-radius: 8px;
  padding: 24px;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.06);
}

h2 {
  margin-bottom: 20px;
  font-size: 18px;
  font-weight: 600;
}

.upload-area {
  margin-bottom: 24px;
}

.result-box {
  margin: 16px 0;
  padding: 16px;
  background: #f5f7fa;
  border-radius: 4px;
}

.result-box p {
  margin: 8px 0;
  font-size: 13px;
}

.search-box {
  margin: 20px 0;
  padding: 16px;
  background: #e6f7ff;
  border-radius: 4px;
  display: flex;
  align-items: center;
}

.similar-card {
  padding: 12px;
  border: 1px solid #e8e8e8;
  border-radius: 4px;
  text-align: center;
}

.similar-card p {
  margin: 4px 0;
}
</style>
