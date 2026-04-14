<template>
  <div>
    <el-row :gutter="16">
      <!-- Doris 连接状态 -->
      <el-col :span="12">
        <div class="card">
          <div class="card-title">🗄️ Apache Doris 4.0 连接配置</div>
          <el-descriptions :column="1" border size="small">
            <el-descriptions-item label="Doris Host">{{ cfg.doris_host }}</el-descriptions-item>
            <el-descriptions-item label="MySQL 端口">{{ cfg.doris_port }}</el-descriptions-item>
            <el-descriptions-item label="数据库">{{ cfg.doris_database }}</el-descriptions-item>
            <el-descriptions-item label="HASP 状态">
              <el-tag :type="cfg.hasp_enabled ? 'success' : 'danger'">
                {{ cfg.hasp_enabled ? '✅ 已启用' : '❌ 未启用' }}
              </el-tag>
            </el-descriptions-item>
            <el-descriptions-item label="连接状态">
              <el-tag :type="dorisStatus === 'ok' ? 'success' : 'danger'">
                {{ dorisStatus === 'ok' ? '✅ 已连接' : '❌ 连接失败' }}
              </el-tag>
            </el-descriptions-item>
            <el-descriptions-item label="Doris 版本">{{ dorisVersion }}</el-descriptions-item>
          </el-descriptions>
          <el-button type="primary" style="margin-top:12px" @click="testDoris" :loading="testing">
            重新检测连接
          </el-button>
        </div>
      </el-col>

      <!-- AI Function 配置 -->
      <el-col :span="12">
        <div class="card">
          <div class="card-title">🤖 AI Function 配置</div>
          <el-descriptions :column="1" border size="small">
            <el-descriptions-item label="AI 提供商">{{ cfg.ai_provider }}</el-descriptions-item>
            <el-descriptions-item label="AI 模型">{{ cfg.ai_model }}</el-descriptions-item>
            <el-descriptions-item label="批次大小">{{ cfg.log_batch_size }} 条</el-descriptions-item>
            <el-descriptions-item label="刷新间隔">{{ cfg.log_flush_interval }} 秒</el-descriptions-item>
          </el-descriptions>
        </div>
      </el-col>
    </el-row>

    <!-- HASP 说明 -->
    <div class="card">
      <div class="card-title">⚡ Doris 4.0 HASP 能力说明</div>
      <el-row :gutter="16">
        <el-col :span="8" v-for="item in haspFeatures" :key="item.title">
          <el-card shadow="never" style="height:100%">
            <div style="font-size:24px;margin-bottom:8px">{{ item.icon }}</div>
            <div style="font-weight:600;margin-bottom:6px">{{ item.title }}</div>
            <div style="font-size:13px;color:#606266;line-height:1.6">{{ item.desc }}</div>
          </el-card>
        </el-col>
      </el-row>
    </div>

    <!-- FileBeat 配置 -->
    <div class="card">
      <div class="card-title">📡 FileBeat 配置说明</div>
      <pre class="config-block">{{ filebeatConfig }}</pre>
    </div>

    <!-- 启动说明 -->
    <div class="card">
      <div class="card-title">🚀 工程启动说明</div>
      <el-steps direction="vertical" :active="5" finish-status="success">
        <el-step v-for="step in startupSteps" :key="step.title"
                 :title="step.title" :description="step.desc" />
      </el-steps>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { systemApi } from '@/api'

const cfg = ref({})
const dorisStatus  = ref('checking')
const dorisVersion = ref('...')
const testing = ref(false)

const haspFeatures = [
  { icon: '🔄', title: 'Pipeline 执行引擎', desc: 'Doris 4.0 启用 Pipeline 模式，多核并行执行查询计划，吞吐量提升 3-5x' },
  { icon: '⚡', title: '向量化执行', desc: 'SIMD 指令集加速列式数据处理，复杂聚合查询性能提升 5-10x' },
  { icon: '🗂️', title: '行列混存（MOW）', desc: '用户宽表启用行列混存，Upsert 写入时合并，兼顾写入吞吐和点查性能' },
  { icon: '🔍', title: 'Runtime Filter', desc: '全局 Runtime Filter，Join 时动态过滤不相关数据，减少 Shuffle 数据量' },
  { icon: '📊', title: '物化视图自动加速', desc: '热点查询自动命中物化视图，无需修改 SQL，透明加速聚合分析' },
  { icon: '🤖', title: 'AI Function 内置', desc: 'Doris 4.0 内置 ai_completion() 函数，在数据库层直接调用 LLM，零数据传输' },
]

const filebeatConfig = `# filebeat/filebeat.yml 核心配置
filebeat.inputs:
  - type: log
    paths: ["/var/log/bank-app/*.log"]
    fields: {source: APP}

output.http:
  hosts: ["http://localhost:8000"]
  path: "/api/log/collect"   # CDP 后端接收端点
  method: POST
  batch_size: 100             # 批量发送，降低请求次数`

const startupSteps = [
  { title: '部署 Apache Doris 4.0', desc: '参考 Doris 官方文档部署 FE + BE，建议单机测试使用 docker-compose 一键启动' },
  { title: '初始化数据库', desc: 'mysql -h 127.0.0.1 -P 9030 -u root < sql/doris_ddl.sql' },
  { title: '导入测试数据', desc: 'mysql -h 127.0.0.1 -P 9030 -u root bank_cdp < test_data/init_data.sql' },
  { title: '启动后端', desc: 'cd backend && pip install -r requirements.txt && uvicorn app:app --reload' },
  { title: '启动前端', desc: 'cd frontend && npm install && npm run dev，访问 http://localhost:5173' },
  { title: '启动 FileBeat（可选）', desc: 'filebeat -c filebeat/filebeat.yml，或运行 bash filebeat/gen_test_logs.sh 生成测试日志' },
]

async function testDoris() {
  testing.value = true
  try {
    const res = await systemApi.health()
    dorisStatus.value  = res.status
    dorisVersion.value = res.doris_version
  } catch { dorisStatus.value = 'error' }
  finally { testing.value = false }
}

onMounted(async () => {
  try { cfg.value = await systemApi.config() } catch {}
  testDoris()
})
</script>

<style scoped>
.config-block {
  background: #1e1e1e; color: #d4d4d4; border-radius: 6px; padding: 16px;
  font-size: 13px; line-height: 1.7; font-family: 'Consolas', monospace;
  overflow-x: auto; white-space: pre;
}
</style>
