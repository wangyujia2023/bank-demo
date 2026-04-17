#!/bin/bash

# =================================================================
# Project: Bank-CDP-Doris4 全栈一键启动脚本
# Environment: Python 3.12 + Node.js
# =================================================================

# --- 路径配置 ---
PROJECT_DIR="/Users/yujia/Desktop/workspace/bank-cdp-doris4"
BACKEND_LOG="$PROJECT_DIR/backend.log"
FRONTEND_LOG="$PROJECT_DIR/frontend.log"

echo "🚀 启动 Bank-CDP 全栈服务..."

# --- 1. 后端启动逻辑 ---
echo "--- [1/2] 准备后端 (Python 3.12) ---"
cd "$PROJECT_DIR" || exit

# 清理 8000 端口
PID_8000=$(lsof -t -i:8000)
[ -n "$PID_8000" ] && kill -9 "$PID_8000" && echo "✅ 已清理旧后端进程"

# 激活环境并启动
source .venv/bin/activate
nohup uvicorn backend.app:app --host 0.0.0.0 --port 8000 --reload > "$BACKEND_LOG" 2>&1 &
echo "🔥 后端已在后台启动"

# --- 2. 前端启动逻辑 ---
echo "--- [2/2] 准备前端 (Node.js) ---"
# 注意：这里假设你的前端代码在项目根目录下的 /frontend 文件夹
# 如果路径不同（比如在 /web），请修改下方 cd 命令
cd "$PROJECT_DIR/frontend" || { echo "❌ 找不到前端目录"; exit 1; }

# 清理前端常见端口 (3000 或 5173)
PID_FRONT=$(lsof -t -i:3000,5173)
[ -n "$PID_FRONT" ] && kill -9 "$PID_FRONT" && echo "✅ 已清理旧前端进程"

# 启动前端 (通常是 npm run dev 或 npm start)
# 使用 nohup 保证终端关闭后不挂断
nohup npm run dev > "$FRONTEND_LOG" 2>&1 &

# --- 3. 结果检查 ---
sleep 3
echo "---------------------------------------"
echo "✨ 全栈启动任务已下发！"
echo "📍 后端 API: http://127.0.0.1:8000"
echo "📍 前端界面: http://127.0.0.1:5173 (请根据实际情况确认)"
echo "---------------------------------------"
echo "💡 提示："
echo "- 查看后端日志: tail -f $BACKEND_LOG"
echo "- 查看前端日志: tail -f $FRONTEND_LOG"