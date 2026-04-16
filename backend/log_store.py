"""内存日志存储 - 存储系统请求日志"""
from collections import deque
from datetime import datetime
from typing import List, Dict

# 使用deque实现环形缓冲区，最多保存1000条日志
class RequestLogStore:
    def __init__(self, max_size: int = 1000):
        self.logs = deque(maxlen=max_size)
        self.max_size = max_size

    def add(self, log: Dict):
        """添加一条日志"""
        log['timestamp'] = datetime.now().isoformat()
        self.logs.append(log)

    def get_all(self) -> List[Dict]:
        """获取所有日志（最新的在前）"""
        return list(reversed(self.logs))

    def get_by_path(self, path: str) -> List[Dict]:
        """按路径筛选"""
        return [log for log in self.get_all() if path in log.get('path', '')]

    def get_by_status(self, status_code: int) -> List[Dict]:
        """按状态码筛选"""
        return [log for log in self.get_all() if log.get('status_code') == status_code]

    def get_errors(self) -> List[Dict]:
        """获取所有错误请求"""
        return [log for log in self.get_all() if log.get('status_code', 0) >= 400]

    def get_slow(self, threshold_ms: int = 1000) -> List[Dict]:
        """获取慢查询"""
        return [log for log in self.get_all() if log.get('duration_ms', 0) > threshold_ms]

    def get_by_trace(self, trace_id: str) -> List[Dict]:
        """按trace_id获取链路内所有日志"""
        return [log for log in self.get_all() if log.get('trace_id') == trace_id]

    def get_stats(self) -> Dict:
        """获取统计信息"""
        all_logs = list(self.logs)
        if not all_logs:
            return {
                'total': 0,
                'errors': 0,
                'slow': 0,
                'avg_duration_ms': 0,
                'top_paths': []
            }

        errors = sum(1 for log in all_logs if log.get('status_code', 0) >= 400)
        slow = sum(1 for log in all_logs if log.get('duration_ms', 0) > 1000)
        avg_duration = sum(log.get('duration_ms', 0) for log in all_logs) / len(all_logs) if all_logs else 0

        # 统计路径
        path_stats = {}
        for log in all_logs:
            path = log.get('path', 'unknown')
            if path not in path_stats:
                path_stats[path] = {'count': 0, 'total_duration': 0}
            path_stats[path]['count'] += 1
            path_stats[path]['total_duration'] += log.get('duration_ms', 0)

        top_paths = sorted(
            [{'path': k, 'count': v['count'], 'avg_duration': round(v['total_duration'] / v['count'], 2)}
             for k, v in path_stats.items()],
            key=lambda x: x['count'],
            reverse=True
        )[:10]

        return {
            'total': len(all_logs),
            'errors': errors,
            'slow': slow,
            'avg_duration_ms': round(avg_duration, 2),
            'top_paths': top_paths
        }


# 全局日志存储实例
log_store = RequestLogStore(max_size=2000)
