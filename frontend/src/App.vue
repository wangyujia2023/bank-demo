<template>
  <el-container class="app-wrap">
    <!-- 侧边栏 -->
    <el-aside width="188px" class="aside">
      <div class="logo">
        <el-icon size="18" color="#409eff"><DataBoard /></el-icon>
        <span>Doris 特性展示</span>
      </div>
      <el-menu
        :default-active="activeMenu"
        router
        background-color="#001529"
        text-color="#ffffffa0"
        active-text-color="#409eff"
      >
        <el-menu-item index="/dashboard">
          <el-icon><Odometer /></el-icon><span>首页大盘</span>
        </el-menu-item>
        <el-menu-item index="/management">
          <el-icon><DataAnalysis /></el-icon><span>经营管理大屏</span>
        </el-menu-item>

        <div class="menu-group-title">用户画像</div>
        <el-menu-item index="/user">
          <el-icon><User /></el-icon><span>宽表查询</span>
        </el-menu-item>
        <el-menu-item index="/segment">
          <el-icon><Grid /></el-icon><span>人群圈选</span>
        </el-menu-item>
        <el-menu-item index="/behavior">
          <el-icon><TrendCharts /></el-icon><span>行为分析</span>
        </el-menu-item>
        <el-menu-item index="/user-tag">
          <el-icon><Files /></el-icon><span>用户行为分析</span>
        </el-menu-item>
        <el-menu-item index="/log-classify">
          <el-icon><Cpu /></el-icon><span>AI 日志标签</span>
        </el-menu-item>

        <div class="menu-group-title">HASP 场景</div>
        <el-menu-item index="/vector">
          <el-icon><Coin /></el-icon><span>图片向量检索</span>
        </el-menu-item>
        <el-menu-item index="/satellite">
          <el-icon><Promotion /></el-icon><span>卫星数据分析</span>
        </el-menu-item>

        <div class="menu-group-title">数据能力</div>
        <el-menu-item index="/report">
          <el-icon><Document /></el-icon><span>银行报表</span>
        </el-menu-item>
        <el-menu-item index="/metrics">
          <el-icon><DataAnalysis /></el-icon><span>指标平台</span>
        </el-menu-item>
        <el-menu-item index="/observe">
          <el-icon><Monitor /></el-icon><span>日志可观测性</span>
        </el-menu-item>
        <el-menu-item index="/log-tag-stats">
          <el-icon><PriceTag /></el-icon><span>日志标签分析</span>
        </el-menu-item>
        <el-menu-item index="/trace">
          <el-icon><Share /></el-icon><span>链路追踪</span>
        </el-menu-item>
        <el-menu-item index="/benchmark">
          <el-icon><Histogram /></el-icon><span>高并发点查</span>
        </el-menu-item>

        <div class="menu-group-title">智能制造</div>
        <el-menu-item index="/manufacturing">
          <el-icon><SetUp /></el-icon><span>数字孪生沙盘</span>
        </el-menu-item>

        <div class="menu-group-title">证券场景</div>
        <el-menu-item index="/securities">
          <el-icon><WalletFilled /></el-icon><span>证券实时数仓</span>
        </el-menu-item>

        <div class="menu-group-title">基金场景</div>
        <el-menu-item index="/fund">
          <el-icon><TrendCharts /></el-icon><span>基金投研沙盘</span>
        </el-menu-item>
        <el-menu-item index="/news">
          <el-icon><Cpu /></el-icon><span>资讯 AI 分析</span>
        </el-menu-item>

        <div class="menu-group-title">系统</div>
        <el-menu-item index="/config">
          <el-icon><Setting /></el-icon><span>系统配置</span>
        </el-menu-item>
      </el-menu>

      <!-- Doris 状态指示 -->
      <div class="doris-status">
        <span class="dot" :class="dorisOk ? 'ok' : 'err'"></span>
        <span>Doris 4.0 {{ dorisOk ? '已连接' : '未连接' }}</span>
      </div>
    </el-aside>

    <!-- 主区域 -->
    <el-container>
      <el-header class="header">
        <span class="page-title">{{ currentTitle }}</span>
        <div class="header-right">
          <el-tag type="success" size="small" effect="plain">HASP 已启用</el-tag>
          <el-tag type="warning" size="small" effect="plain" style="margin-left:8px">AI Function 就绪</el-tag>
          <span class="username">数据分析师</span>
        </div>
      </el-header>
      <el-main class="main-content">
        <router-view />
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { systemApi } from '@/api'
import { Document, DataAnalysis, Monitor, Share, Histogram, Files, SetUp } from '@element-plus/icons-vue'

const route = useRoute()
const dorisOk = ref(false)
const activeMenu = computed(() => route.path)
const currentTitle = computed(() => route.meta?.title || 'CDP 平台')

onMounted(async () => {
  try {
    const res = await systemApi.health()
    dorisOk.value = res.status === 'ok'
  } catch { dorisOk.value = false }
})
</script>

<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, 'PingFang SC', 'Microsoft YaHei', sans-serif; }
.app-wrap { height: 100vh; overflow: hidden; }
.aside { background: #001529; display: flex; flex-direction: column; overflow: hidden; }
.logo {
  height: 48px; display: flex; align-items: center; gap: 8px;
  padding: 0 16px; color: #fff; font-size: 13px; font-weight: 600;
  border-bottom: 1px solid rgba(255,255,255,0.08); flex-shrink: 0; letter-spacing: .3px;
}
.el-menu { border-right: none; flex: 1; overflow-y: auto; }
/* 菜单项紧凑 */
.el-menu .el-menu-item {
  height: 36px !important;
  line-height: 36px !important;
  font-size: 12px !important;
  padding: 0 14px !important;
}
.el-menu .el-menu-item .el-icon { font-size: 14px !important; margin-right: 6px !important; }
/* 自定义分组标题 */
.menu-group-title {
  padding: 10px 14px 3px;
  font-size: 10px;
  color: rgba(255,255,255,0.28);
  letter-spacing: .8px;
  text-transform: uppercase;
}
.doris-status {
  padding: 10px 14px; font-size: 11px; color: rgba(255,255,255,0.4);
  display: flex; align-items: center; gap: 6px; border-top: 1px solid rgba(255,255,255,0.08);
}
.dot { width: 7px; height: 7px; border-radius: 50%; display: inline-block; }
.dot.ok { background: #67c23a; box-shadow: 0 0 6px #67c23a; }
.dot.err { background: #f56c6c; }
.header {
  background: #fff; border-bottom: 1px solid #e8e8e8;
  display: flex; align-items: center; justify-content: space-between;
  height: 56px !important; padding: 0 24px;
}
.page-title { font-size: 16px; font-weight: 600; color: #1a1a1a; }
.header-right { display: flex; align-items: center; gap: 12px; }
.username { font-size: 13px; color: #606266; }
.main-content { background: #f0f2f5; overflow-y: auto; padding: 20px; }
.card { background: #fff; border-radius: 8px; padding: 20px; margin-bottom: 16px; box-shadow: 0 1px 4px rgba(0,0,0,0.06); }
.card-title { font-size: 15px; font-weight: 600; color: #1a1a1a; margin-bottom: 16px; }
.stat-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 16px; }
.stat-card {
  background: #fff; border-radius: 8px; padding: 20px;
  box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
.stat-label { font-size: 13px; color: #909399; margin-bottom: 8px; }
.stat-value { font-size: 28px; font-weight: 700; color: #1a1a1a; }
.stat-sub { font-size: 12px; color: #909399; margin-top: 6px; }
</style>
