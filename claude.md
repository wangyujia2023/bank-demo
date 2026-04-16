# ⚡ Claude Code 极速协议

## 🎯 目标
精简Token，禁止废话，禁止跨目录搜索。

## 🏗️ 目录白名单
- 后端: `./backend/app/`, `./backend/api/` (忽略: venv, cache, logs)
- 前端: `./frontend/src/` (忽略: node_modules, dist, assets)

## ⚡ 行为准则
1. 响应: 禁止寒暄。直接输出结果。
2. 逻辑: 先grep定位，再read文件。严禁全盘读取。
3. 批处理: 多个修改一次性执行，减少确认轮次。
4. Python: 只读相关函数或类。
5. Vue: 只看逻辑，忽略style，除非涉及UI修复。

## 📝 格式
- 解释: 简短列表。
- 确认: 仅输出 Done: [文件名]。
