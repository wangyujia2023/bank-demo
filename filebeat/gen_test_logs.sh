#!/bin/bash
# 生成测试日志文件（模拟 FileBeat 采集场景）
mkdir -p /tmp/bank-test-logs
LOG_FILE=/tmp/bank-test-logs/bank_app.log
USER_IDS=(10001 10002 10003 10004 10005 10006 10007 10008)
ACTIONS=("LOGIN" "TRANSFER" "QUERY_BALANCE" "PURCHASE_FUND" "APPLY_LOAN" "LOGOUT")
REGIONS=("北京" "上海" "广州" "深圳" "杭州" "成都" "黑龙江")

for i in $(seq 1 50); do
  UID=${USER_IDS[$RANDOM % ${#USER_IDS[@]}]}
  ACT=${ACTIONS[$RANDOM % ${#ACTIONS[@]}]}
  REGION=${REGIONS[$RANDOM % ${#REGIONS[@]}]}
  AMOUNT=$((RANDOM * 100))
  TS=$(date -Iseconds)
  echo "{\"@timestamp\":\"$TS\",\"user_id\":$UID,\"action\":\"$ACT\",\"amount\":$AMOUNT,\"region\":\"$REGION\",\"message\":\"user_id=$UID action=$ACT amount=$AMOUNT region=$REGION result=SUCCESS\"}" >> $LOG_FILE
  sleep 0.1
done
echo "✅ 已生成 50 条测试日志: $LOG_FILE"
