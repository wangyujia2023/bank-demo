from collections import deque
from datetime import datetime
from typing import List, Dict


class ServiceLogStore:
    def __init__(self, max_size: int = 2000):
        self.logs = deque(maxlen=max_size)

    def add(self, log: Dict):
        log['timestamp'] = datetime.now().isoformat()
        self.logs.append(log)

    def get_all(self) -> List[Dict]:
        return list(reversed(self.logs))

    def get_by_trace(self, trace_id: str) -> List[Dict]:
        return [l for l in self.get_all() if l.get('trace_id') == trace_id]

    def get_stats(self) -> Dict:
        all_logs = list(self.logs)
        if not all_logs:
            return {'total': 0, 'server_errors': 0, 'client_errors': 0, 'slow': 0,
                    'avg_duration_ms': 0, 'method_counts': [], 'status_counts': [], 'top_paths': []}
        server_errors = sum(1 for l in all_logs if l.get('status_code', 0) >= 500)
        client_errors = sum(1 for l in all_logs if 400 <= l.get('status_code', 0) < 500)
        slow = sum(1 for l in all_logs if l.get('duration_ms', 0) > 1000)
        avg_ms = round(sum(l.get('duration_ms', 0) for l in all_logs) / len(all_logs), 2)
        mc: Dict = {}
        for l in all_logs:
            m = l.get('method', 'UNKNOWN')
            mc[m] = mc.get(m, 0) + 1
        method_counts = [{'method': k, 'cnt': v} for k, v in sorted(mc.items(), key=lambda x: -x[1])]
        s2 = sum(1 for l in all_logs if 200 <= l.get('status_code', 0) < 300)
        status_counts = [{'label': '2xx', 'cnt': s2}, {'label': '4xx', 'cnt': client_errors}, {'label': '5xx', 'cnt': server_errors}]
        pc: Dict = {}
        for l in all_logs:
            p = l.get('path', '')
            if p not in pc:
                pc[p] = {'count': 0, 'total': 0}
            pc[p]['count'] += 1
            pc[p]['total'] += l.get('duration_ms', 0)
        top_paths = sorted(
            [{'path': k, 'count': v['count'], 'avg_duration': round(v['total'] / v['count'], 2)} for k, v in pc.items()],
            key=lambda x: -x['count']
        )[:10]
        return {'total': len(all_logs), 'server_errors': server_errors, 'client_errors': client_errors,
                'slow': slow, 'avg_duration_ms': avg_ms,
                'method_counts': method_counts, 'status_counts': status_counts, 'top_paths': top_paths}


class DBCallLogStore:
    def __init__(self, max_size: int = 5000):
        self.logs = deque(maxlen=max_size)

    def add(self, log: Dict):
        if 'call_time' not in log:
            log['call_time'] = datetime.now().isoformat()
        self.logs.append(log)

    def get_by_trace(self, trace_id: str) -> List[Dict]:
        return [l for l in self.logs if l.get('trace_id') == trace_id]

    def count_by_trace(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for l in self.logs:
            tid = l.get('trace_id', '')
            counts[tid] = counts.get(tid, 0) + 1
        return counts


log_store = ServiceLogStore(max_size=2000)
db_call_store = DBCallLogStore(max_size=5000)
