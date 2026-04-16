from backend.doris.connect import execute_query, execute_one, execute_write
import asyncio

INIT_SQL = """CREATE TABLE IF NOT EXISTS satellite_collect_data (
    id BIGINT, satellite_id VARCHAR(64), satellite_name VARCHAR(64),
    satellite_type VARCHAR(32), sensor_id VARCHAR(64), collect_time BIGINT,
    data_type VARCHAR(32), target_id VARCHAR(64), target_type VARCHAR(32),
    longitude DECIMAL(12,6), latitude DECIMAL(12,6), data_quality INT,
    data_size BIGINT, status VARCHAR(16), task_id VARCHAR(64), created_at BIGINT
) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 16
PROPERTIES ("replication_num" = "1")"""

DEMO_SQL = """INSERT INTO satellite_collect_data VALUES
(1,'SAT-001','尖兵-1','光学','SENSOR-01',1740000000000,'成像','TARGET-001','舰船',120.123456,30.123456,96,204800,'normal','TASK-001',1740000001000),
(2,'SAT-001','尖兵-1','光学','SENSOR-01',1740000100000,'成像','TARGET-002','车辆',120.124456,30.124456,95,194800,'normal','TASK-001',1740000101000),
(3,'SAT-002','尖兵-2','雷达','SENSOR-02',1740000200000,'信号','TARGET-003','机场',119.123456,31.123456,92,304800,'normal','TASK-002',1740000201000),
(4,'SAT-002','尖兵-2','雷达','SENSOR-02',1740000300000,'信号','TARGET-004','雷达站',119.124456,31.124456,90,284800,'warning','TASK-002',1740000301000),
(5,'SAT-003','尖兵-3','电子侦察','SENSOR-03',1740000400000,'轨道','TARGET-005','舰船',121.123456,29.123456,98,154800,'normal','TASK-003',1740000401000),
(6,'SAT-003','尖兵-3','电子侦察','SENSOR-03',1740000500000,'预警','TARGET-006','雷达站',121.124456,29.124456,88,124800,'warning','TASK-003',1740000501000),
(7,'SAT-004','资源-1','光学','SENSOR-04',1740000600000,'成像','TARGET-007','机场',118.123456,32.123456,91,234800,'normal','TASK-004',1740000601000),
(8,'SAT-004','资源-1','光学','SENSOR-04',1740000700000,'成像','TARGET-008','车辆',118.124456,32.124456,85,214800,'abnormal','TASK-004',1740000701000),
(9,'SAT-005','高分-1','雷达','SENSOR-05',1740000800000,'信号','TARGET-009','舰船',122.123456,28.123456,97,354800,'normal','TASK-005',1740000801000),
(10,'SAT-005','高分-1','雷达','SENSOR-05',1740000900000,'预警','TARGET-010','雷达站',122.124456,28.124456,93,334800,'warning','TASK-005',1740000901000),
(11,'SAT-001','尖兵-1','光学','SENSOR-01',1740001000000,'成像','TARGET-011','机场',120.125456,30.125456,94,204800,'normal','TASK-006',1740001001000),
(12,'SAT-002','尖兵-2','雷达','SENSOR-02',1740001100000,'轨道','TARGET-012','舰船',119.125456,31.125456,99,114800,'normal','TASK-006',1740001101000),
(13,'SAT-006','遥感-1','电子侦察','SENSOR-06',1740001200000,'信号','TARGET-013','车辆',117.123456,33.123456,87,264800,'abnormal','TASK-007',1740001201000),
(14,'SAT-006','遥感-1','电子侦察','SENSOR-06',1740001300000,'预警','TARGET-014','雷达站',117.124456,33.124456,82,244800,'warning','TASK-007',1740001301000),
(15,'SAT-007','高分-2','光学','SENSOR-07',1740001400000,'成像','TARGET-015','机场',116.123456,34.123456,96,184800,'normal','TASK-008',1740001401000),
(16,'SAT-007','高分-2','光学','SENSOR-07',1740001500000,'成像','TARGET-016','舰船',116.124456,34.124456,93,174800,'normal','TASK-008',1740001501000),
(17,'SAT-008','高分-3','雷达','SENSOR-08',1740001600000,'信号','TARGET-017','车辆',115.123456,35.123456,78,304800,'abnormal','TASK-009',1740001601000),
(18,'SAT-008','高分-3','雷达','SENSOR-08',1740001700000,'预警','TARGET-018','机场',115.124456,35.124456,91,294800,'warning','TASK-009',1740001701000),
(19,'SAT-003','尖兵-3','电子侦察','SENSOR-03',1740001800000,'轨道','TARGET-019','舰船',121.125456,29.125456,95,134800,'normal','TASK-010',1740001801000),
(20,'SAT-005','高分-1','雷达','SENSOR-05',1740001900000,'成像','TARGET-020','雷达站',122.125456,28.125456,89,344800,'normal','TASK-010',1740001901000)"""


class SatelliteService:

    async def init_table(self):
        await execute_write(INIT_SQL)
        cnt = await execute_one("SELECT COUNT(*) AS c FROM satellite_collect_data")
        if not cnt or cnt.get("c", 0) == 0:
            await execute_write(DEMO_SQL)
        return {"status": "ok"}

    async def overview(self):
        row = await execute_one(
            "SELECT COUNT(*) AS total,"
            "COUNT(DISTINCT satellite_id) AS satellites,"
            "SUM(CASE WHEN status='warning' THEN 1 ELSE 0 END) AS warnings,"
            "SUM(CASE WHEN status='abnormal' THEN 1 ELSE 0 END) AS abnormals "
            "FROM satellite_collect_data"
        )
        return row or {}

    async def charts(self):
        by_sat, by_type, by_target, by_quality = await asyncio.gather(
            execute_query("SELECT satellite_name,COUNT(*) AS cnt FROM satellite_collect_data GROUP BY satellite_name ORDER BY cnt DESC"),
            execute_query("SELECT data_type,COUNT(*) AS cnt FROM satellite_collect_data GROUP BY data_type ORDER BY cnt DESC"),
            execute_query("SELECT target_type,COUNT(*) AS cnt FROM satellite_collect_data GROUP BY target_type ORDER BY cnt DESC"),
            execute_query("SELECT satellite_name,ROUND(AVG(data_quality),1) AS avg_q FROM satellite_collect_data GROUP BY satellite_name ORDER BY avg_q DESC"),
        )
        return {"by_satellite": by_sat, "by_data_type": by_type, "by_target": by_target, "by_quality": by_quality}

    async def query(self, satellite_id=None, satellite_name=None, satellite_type=None,
                    data_type=None, target_type=None, quality_min=None, status=None,
                    page=1, size=20):
        where, args = "WHERE 1=1", []
        if satellite_id:
            where += " AND satellite_id=%s"; args.append(satellite_id)
        if satellite_name:
            where += " AND satellite_name LIKE %s"; args.append(f"%{satellite_name}%")
        if satellite_type:
            where += " AND satellite_type=%s"; args.append(satellite_type)
        if data_type:
            where += " AND data_type=%s"; args.append(data_type)
        if target_type:
            where += " AND target_type=%s"; args.append(target_type)
        if quality_min:
            where += " AND data_quality>=%s"; args.append(int(quality_min))
        if status:
            where += " AND status=%s"; args.append(status)
        offset = (page - 1) * size
        cnt = await execute_one(f"SELECT COUNT(*) AS c FROM satellite_collect_data {where}", tuple(args) or None)
        rows = await execute_query(
            f"SELECT id,satellite_name,satellite_type,data_type,target_type,"
            f"longitude,latitude,data_quality,status,collect_time,task_id "
            f"FROM satellite_collect_data {where} ORDER BY collect_time DESC LIMIT %s OFFSET %s",
            tuple(args + [size, offset])
        )
        return {"total": cnt.get("c", 0) if cnt else 0, "rows": rows}
