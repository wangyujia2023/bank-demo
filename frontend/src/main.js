import { createApp } from 'vue'
import { createPinia } from 'pinia'
import App from './App.vue'
import router from './router'
import './plugins/echarts'  // 全局 ECharts 配置（只加载一次）

const app = createApp(App)

app.use(createPinia())
app.use(router)
app.mount('#app')
