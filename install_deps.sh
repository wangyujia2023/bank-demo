#!/bin/bash

# 安装 Python 依赖
echo "📦 安装 Python 依赖..."
pip install -r requirements.txt

# macOS 特殊处理：安装 cairo（SVG 渲染依赖）
if [[ "$OSTYPE" == "darwin"* ]]; then
  echo "🍎 检测到 macOS，安装 cairo..."
  if command -v brew &> /dev/null; then
    brew install cairo
  else
    echo "⚠️  请先安装 Homebrew，或手动运行: brew install cairo"
  fi
fi

# Linux 特殊处理
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  echo "🐧 检测到 Linux，安装系统依赖..."
  sudo apt-get install -y libcairo2-dev pkg-config python3-dev
fi

echo "✅ 依赖安装完成！"
echo "▶️  启动后端: uvicorn backend.app:app --reload"
echo "▶️  启动前端: cd frontend && npm run dev"
