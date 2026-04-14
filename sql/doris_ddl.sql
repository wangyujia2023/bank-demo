-- ================================================================
-- 银行业 CDP 平台 - Apache Doris 4.0 建表语句
-- 所有表启用 HASP 特性：列存、向量化、Pipeline
-- ================================================================

CREATE DATABASE IF NOT EXISTS bank_cdp;
USE bank_cdp;

-- ================================================================
-- 1. 用户宽表 user_wide
--    UNIQUE KEY 模型，支持 Upsert，关联用户全量信息
--    HASP 适配：列存 + 分区 + 分桶
-- ================================================================
CREATE TABLE IF NOT EXISTS user_wide (
    user_id         BIGINT          NOT NULL    COMMENT '用户唯一ID',
    update_date     DATE            NOT NULL    COMMENT '数据更新日期（分区键）',

    -- 基础信息
    user_name       VARCHAR(64)                 COMMENT '用户姓名（脱敏：张*明）',
    id_card         VARCHAR(32)                 COMMENT '身份证号（脱敏：110***8881）',
    phone           VARCHAR(20)                 COMMENT '手机号（脱敏：138****8888）',
    gender          TINYINT                     COMMENT '性别：1男 2女 0未知',
    age             INT                         COMMENT '年龄',
    age_group       VARCHAR(16)                 COMMENT '年龄段：18-25/26-35/36-45/46-55/55+',
    city            VARCHAR(32)                 COMMENT '城市',
    province        VARCHAR(16)                 COMMENT '省份',
    education       TINYINT                     COMMENT '学历：1小学 2初中 3高中 4大专 5本科 6硕士 7博士',
    occupation      VARCHAR(32)                 COMMENT '职业',
    register_date   DATE                        COMMENT '注册日期',

    -- 资产信息
    asset_level     VARCHAR(16)                 COMMENT '资产等级：VIP私行/VIP钻石/VIP铂金/VIP黄金/普通',
    aum_total       DECIMAL(20,4)               COMMENT '总AUM资产（万元）',
    deposit_amount  DECIMAL(20,4)               COMMENT '存款余额（万元）',
    fund_amount     DECIMAL(20,4)               COMMENT '基金持有额（万元）',
    loan_amount     DECIMAL(20,4)               COMMENT '贷款余额（万元）',
    wm_amount       DECIMAL(20,4)               COMMENT '理财余额（万元）',
    insurance_amount DECIMAL(20,4)              COMMENT '保险保费（万元）',

    -- 业务信息
    has_credit_card TINYINT                     COMMENT '是否持有信用卡：1是 0否',
    has_debit_card  TINYINT                     COMMENT '是否持有借记卡：1是 0否',
    has_mortgage    TINYINT                     COMMENT '是否有房贷：1是 0否',
    product_count   INT                         COMMENT '持有产品总数',
    credit_score    INT                         COMMENT '综合信用评分 300-900',
    credit_grade    VARCHAR(8)                  COMMENT '信用等级：AAA/AA/A/B/C',
    risk_level      TINYINT                     COMMENT '风险等级：1保守 2稳健 3平衡 4进取 5激进',

    -- 渠道偏好
    preferred_channel VARCHAR(16)               COMMENT '偏好渠道：APP/网点/小程序/网银',
    app_login_30d   INT                         COMMENT 'APP近30天登录次数',
    app_last_login  DATE                        COMMENT 'APP最后登录日期',
    active_level    VARCHAR(16)                 COMMENT '活跃等级：高活/中活/低活/沉睡',

    -- 标签信息（含AI日志标签）
    lifecycle_stage VARCHAR(32)                 COMMENT '生命周期：新客/成长/成熟/沉睡/流失预警',
    churn_prob      DECIMAL(10,4)               COMMENT '流失概率（%，模型输出）',
    clv_score       DECIMAL(20,4)               COMMENT '客户终身价值分',
    log_tags        VARCHAR(512)                COMMENT 'AI日志标签（JSON数组）',
    anomaly_flag    TINYINT                     COMMENT '异常行为标记：1是 0否',

    -- 系统字段
    created_at      DATETIME                    COMMENT '创建时间',
    updated_at      DATETIME                    COMMENT '最后更新时间'
)
UNIQUE KEY(user_id, update_date)
COMMENT '用户宽表 - CDP核心标签表（Doris 4.0 HASP）'
PARTITION BY RANGE(update_date) (
    PARTITION p2024_q1 VALUES LESS THAN ('2024-04-01'),
    PARTITION p2024_q2 VALUES LESS THAN ('2024-07-01'),
    PARTITION p2024_q3 VALUES LESS THAN ('2024-10-01'),
    PARTITION p2024_q4 VALUES LESS THAN ('2025-01-01'),
    PARTITION p2025_q1 VALUES LESS THAN ('2025-04-01'),
    PARTITION p2025_q2 VALUES LESS THAN ('2025-07-01'),
    PARTITION p2025_q3 VALUES LESS THAN ('2025-10-01'),
    PARTITION p2025_q4 VALUES LESS THAN ('2026-01-01'),
    PARTITION p_future  VALUES LESS THAN MAXVALUE
)
DISTRIBUTED BY HASH(user_id) BUCKETS 64
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "enable_unique_key_merge_on_write" = "true",   -- Doris 4.0 MOW 写时合并
    "store_row_column" = "true",                    -- Doris 4.0 行列混存（HASP核心）
    "enable_single_replica_compaction" = "true",
    "compression" = "ZSTD",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "QUARTER",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "64"
);

-- ================================================================
-- 2. 用户标签表 user_tag
--    存储用户静态标签 + AI日志标签
--    支持 Bitmap 索引，加速圈选
-- ================================================================
CREATE TABLE IF NOT EXISTS user_tag (
    tag_date        DATE            NOT NULL    COMMENT '标签日期（分区键）',
    tag_category    VARCHAR(32)     NOT NULL    COMMENT '标签大类：BASIC/ASSET/BEHAVIOR/LOG_AI',
    tag_name        VARCHAR(64)     NOT NULL    COMMENT '标签名称',
    tag_value       VARCHAR(128)    NOT NULL    COMMENT '标签值',
    user_bitmap     BITMAP          NOT NULL    COMMENT '用户ID的Bitmap集合',
    user_count      BIGINT                      COMMENT '用户数量',
    updated_at      DATETIME                    COMMENT '更新时间'
)
AGGREGATE KEY(tag_date, tag_category, tag_name, tag_value)
COMMENT '用户标签Bitmap表 - 支持亿级人群快速圈选'
PARTITION BY RANGE(tag_date) (
    PARTITION p2025_q1 VALUES LESS THAN ('2025-04-01'),
    PARTITION p2025_q2 VALUES LESS THAN ('2025-07-01'),
    PARTITION p2025_q3 VALUES LESS THAN ('2025-10-01'),
    PARTITION p2025_q4 VALUES LESS THAN ('2026-01-01'),
    PARTITION p_future  VALUES LESS THAN MAXVALUE
)
DISTRIBUTED BY HASH(tag_name, tag_value) BUCKETS 32
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "compression" = "ZSTD"
);

-- ================================================================
-- 3. 用户行为表 user_behavior
--    DUPLICATE KEY，存储用户业务行为明细
-- ================================================================
CREATE TABLE IF NOT EXISTS user_behavior (
    event_id        BIGINT          NOT NULL    COMMENT '事件ID',
    user_id         BIGINT          NOT NULL    COMMENT '用户ID',
    event_date      DATE            NOT NULL    COMMENT '事件日期（分区键）',
    event_time      DATETIME        NOT NULL    COMMENT '事件时间',
    event_type      VARCHAR(64)     NOT NULL    COMMENT '事件类型',
    event_category  VARCHAR(32)                 COMMENT '事件分类：LOGIN/TRANSACTION/BROWSE/APPLY',
    channel         VARCHAR(32)                 COMMENT '渠道：APP/H5/WEB/BRANCH',
    product_code    VARCHAR(32)                 COMMENT '产品代码',
    amount          DECIMAL(20,4)               COMMENT '金额（元）',
    result_code     VARCHAR(16)                 COMMENT '结果：SUCCESS/FAIL/CANCEL',
    session_id      VARCHAR(128)                COMMENT '会话ID',
    device_type     VARCHAR(16)                 COMMENT '设备类型：iOS/Android/PC',
    ip_address      VARCHAR(64)                 COMMENT 'IP地址（脱敏）',
    extra_props     VARCHAR(2048)               COMMENT '扩展属性JSON'
)
DUPLICATE KEY(event_id, user_id, event_date)
COMMENT '用户行为事件明细表'
PARTITION BY RANGE(event_date) (
    PARTITION p2025_01 VALUES LESS THAN ('2025-02-01'),
    PARTITION p2025_02 VALUES LESS THAN ('2025-03-01'),
    PARTITION p2025_03 VALUES LESS THAN ('2025-04-01'),
    PARTITION p2025_04 VALUES LESS THAN ('2025-05-01'),
    PARTITION p2025_05 VALUES LESS THAN ('2025-06-01'),
    PARTITION p2025_06 VALUES LESS THAN ('2025-07-01'),
    PARTITION p_future  VALUES LESS THAN MAXVALUE
)
DISTRIBUTED BY HASH(user_id) BUCKETS 64
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "store_row_column" = "true",
    "compression" = "ZSTD",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "64",
    "dynamic_partition.history_partition_num" = "6"
);

-- ================================================================
-- 4. 人群圈选结果表 user_segment
--    存储圈选的人群包，基于 Bitmap 存储
-- ================================================================
CREATE TABLE IF NOT EXISTS user_segment (
    segment_id      BIGINT          NOT NULL    COMMENT '人群包ID',
    segment_name    VARCHAR(128)    NOT NULL    COMMENT '人群包名称',
    segment_desc    VARCHAR(512)                COMMENT '描述',
    rule_config     TEXT                        COMMENT '圈选规则JSON',
    segment_type    VARCHAR(16)                 COMMENT '类型：RULE/BITMAP/ML',
    snap_date       DATE                        COMMENT '快照日期',
    segment_bitmap  BITMAP                      COMMENT '人群用户Bitmap',
    user_count      BIGINT                      COMMENT '人群规模',
    status          TINYINT                     COMMENT '状态：1生效 0失效',
    created_by      VARCHAR(64)                 COMMENT '创建人',
    created_at      DATETIME                    COMMENT '创建时间',
    updated_at      DATETIME                    COMMENT '更新时间'
)
AGGREGATE KEY(segment_id, segment_name, segment_desc, rule_config,
              segment_type, snap_date, status, created_by, created_at, updated_at)
COMMENT '人群包表 - Bitmap存储'
DISTRIBUTED BY HASH(segment_id) BUCKETS 8
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);

-- ================================================================
-- 5. 日志原始表 user_log_raw
--    存储 FileBeat 采集的银行用户操作日志
--    HASP 适配：高效写入、实时查询
-- ================================================================
CREATE TABLE IF NOT EXISTS user_log_raw (
    log_id          BIGINT          NOT NULL    COMMENT '日志ID',
    log_date        DATE            NOT NULL    COMMENT '日志日期（分区键）',
    log_time        DATETIME        NOT NULL    COMMENT '日志时间',
    user_id         BIGINT                      COMMENT '用户ID（可为空：未登录用户）',
    session_id      VARCHAR(128)                COMMENT '会话ID',
    log_level       VARCHAR(16)                 COMMENT '日志级别：INFO/WARN/ERROR',
    log_source      VARCHAR(32)                 COMMENT '日志来源：APP/CORE_BANK/ATM/BRANCH',
    log_content     TEXT                        COMMENT '日志内容（原始）',
    operation_type  VARCHAR(64)                 COMMENT '操作类型（初步分类）',
    ip_address      VARCHAR(64)                 COMMENT 'IP地址',
    user_region     VARCHAR(64)                 COMMENT '用户历史所在地区',
    device_info     VARCHAR(256)                COMMENT '设备信息',
    response_code   VARCHAR(16)                 COMMENT '响应码',
    response_time   INT                         COMMENT '响应时长（毫秒）',
    filebeat_host   VARCHAR(64)                 COMMENT 'FileBeat采集节点',
    raw_json        TEXT                        COMMENT '原始JSON（FileBeat推送）',
    created_at      DATETIME                    COMMENT '入库时间'
)
DUPLICATE KEY(log_id, log_date)
COMMENT '日志原始表 - FileBeat采集（Doris 4.0 HASP高效写入）'
PARTITION BY RANGE(log_date) (
    PARTITION p2025_01 VALUES LESS THAN ('2025-02-01'),
    PARTITION p2025_02 VALUES LESS THAN ('2025-03-01'),
    PARTITION p2025_03 VALUES LESS THAN ('2025-04-01'),
    PARTITION p2025_04 VALUES LESS THAN ('2025-05-01'),
    PARTITION p2025_05 VALUES LESS THAN ('2025-06-01'),
    PARTITION p2025_06 VALUES LESS THAN ('2025-07-01'),
    PARTITION p_future  VALUES LESS THAN MAXVALUE
)
DISTRIBUTED BY HASH(log_id) BUCKETS 64
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "store_row_column" = "true",       -- HASP 行列混存，兼顾写入和查询
    "compression" = "ZSTD",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "64",
    "dynamic_partition.history_partition_num" = "3"
);

-- ================================================================
-- 6. 日志标签表 user_log_tag
--    存储 AI 打标签后的结构化结果
-- ================================================================
CREATE TABLE IF NOT EXISTS user_log_tag (
    log_id          BIGINT          NOT NULL    COMMENT '关联日志ID',
    log_date        DATE            NOT NULL    COMMENT '日志日期（分区键）',
    user_id         BIGINT                      COMMENT '用户ID',
    log_time        DATETIME                    COMMENT '日志时间',
    log_type        VARCHAR(32)                 COMMENT 'AI标签：登录日志/交易日志/查询日志/转账日志/异常日志',
    intent_tag      VARCHAR(64)                 COMMENT '操作意图：查询余额/转账汇款/购买理财/申请贷款/账户设置/其他',
    anomaly_tag     VARCHAR(32)                 COMMENT '异常标签：正常/异地登录/大额转账/频繁操作/可疑账户',
    risk_level      VARCHAR(16)                 COMMENT '风险等级：低风险/中风险/高风险',
    ai_raw_result   TEXT                        COMMENT 'AI原始返回结果（JSON）',
    tag_source      VARCHAR(16)                 COMMENT '标签来源：AI_AUTO/PYTHON_API/MANUAL',
    manual_label    VARCHAR(64)                 COMMENT '人工标注（覆盖AI结果）',
    created_at      DATETIME                    COMMENT '打标时间'
)
UNIQUE KEY(log_id, log_date)
COMMENT '日志AI标签表'
PARTITION BY RANGE(log_date) (
    PARTITION p2025_01 VALUES LESS THAN ('2025-02-01'),
    PARTITION p2025_02 VALUES LESS THAN ('2025-03-01'),
    PARTITION p2025_03 VALUES LESS THAN ('2025-04-01'),
    PARTITION p2025_04 VALUES LESS THAN ('2025-05-01'),
    PARTITION p2025_05 VALUES LESS THAN ('2025-06-01'),
    PARTITION p2025_06 VALUES LESS THAN ('2025-07-01'),
    PARTITION p_future  VALUES LESS THAN MAXVALUE
)
DISTRIBUTED BY HASH(log_id) BUCKETS 32
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "enable_unique_key_merge_on_write" = "true",
    "compression" = "ZSTD"
);

-- ================================================================
-- 7. 标签字典表 tag_dict
--    存储所有标签定义（静态标签 + AI日志标签）
-- ================================================================
CREATE TABLE IF NOT EXISTS tag_dict (
    tag_id          BIGINT          NOT NULL    COMMENT '标签ID',
    tag_category    VARCHAR(32)     NOT NULL    COMMENT '标签大类：BASIC/ASSET/BEHAVIOR/LOG_AI/CUSTOM',
    tag_name        VARCHAR(64)     NOT NULL    COMMENT '标签名称（英文key）',
    tag_label       VARCHAR(128)    NOT NULL    COMMENT '标签显示名（中文）',
    tag_desc        VARCHAR(512)                COMMENT '标签说明',
    value_type      VARCHAR(16)                 COMMENT '值类型：ENUM/RANGE/BOOLEAN/TEXT',
    value_options   TEXT                        COMMENT '枚举值选项（JSON数组）',
    source_table    VARCHAR(64)                 COMMENT '来源表',
    source_field    VARCHAR(64)                 COMMENT '来源字段',
    is_ai_tag       TINYINT                     COMMENT '是否AI标签：1是 0否',
    enable_bitmap   TINYINT                     COMMENT '是否支持Bitmap圈选：1是 0否',
    status          TINYINT                     COMMENT '状态：1启用 0停用',
    sort_order      INT                         COMMENT '排序',
    created_at      DATETIME                    COMMENT '创建时间'
)
UNIQUE KEY(tag_id)
COMMENT '标签字典表'
DISTRIBUTED BY HASH(tag_id) BUCKETS 4
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- ================================================================
-- Doris 4.0 AI Function 定义
-- 注册外部 AI 函数，供 SQL 直接调用
-- ================================================================
-- 注意：以下为 Doris 4.0 AI Function 注册语法示例
-- 实际使用需参考 Doris 4.0 官方文档适配您的部署

-- 方式一：使用 Doris 4.0 内置 ai_completion 函数（无需注册）
-- SELECT ai_completion('openai', 'gpt-4o-mini', 'sk-xxx', 'https://api.openai.com/v1',
--     CONCAT('分析日志：', log_content)) FROM user_log_raw LIMIT 10;

-- 方式二：创建自定义 AI 函数封装
CREATE OR REPLACE FUNCTION log_auto_tag(VARCHAR(4096))
RETURNS VARCHAR(512)
PROPERTIES (
    "type" = "Remote",
    "object_file" = "http://your-service/ai-tag-udf",
    "function_name" = "log_auto_tag",
    "input_type" = "varchar",
    "return_type" = "varchar"
);

-- ================================================================
-- 物化视图（HASP 加速热点查询）
-- ================================================================

-- 用户资产等级聚合（加速圈选估算）
CREATE MATERIALIZED VIEW mv_user_asset_dist
REFRESH ASYNC ON SCHEDULE EVERY 1 HOUR
AS
SELECT
    update_date,
    asset_level,
    active_level,
    lifecycle_stage,
    preferred_channel,
    COUNT(1)            AS user_count,
    SUM(aum_total)      AS total_aum,
    AVG(aum_total)      AS avg_aum,
    AVG(credit_score)   AS avg_credit,
    SUM(IF(anomaly_flag=1, 1, 0)) AS anomaly_count
FROM user_wide
WHERE update_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
GROUP BY update_date, asset_level, active_level, lifecycle_stage, preferred_channel;

-- 日志每日聚合（加速日志分析）
CREATE MATERIALIZED VIEW mv_log_daily_stat
REFRESH ASYNC ON SCHEDULE EVERY 30 MINUTE
AS
SELECT
    t.log_date,
    t.log_type,
    t.anomaly_tag,
    t.risk_level,
    COUNT(1)            AS log_count,
    COUNT(DISTINCT t.user_id) AS user_count,
    COUNT(IF(t.risk_level='高风险', 1, NULL)) AS high_risk_count
FROM user_log_tag t
WHERE t.log_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY t.log_date, t.log_type, t.anomaly_tag, t.risk_level;
