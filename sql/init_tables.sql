-- 用户画像表（Doris）
CREATE TABLE IF NOT EXISTS user_profiles (
  user_id VARCHAR(32) NOT NULL COMMENT '用户ID',
  user_name VARCHAR(128) COMMENT '用户名称',
  gender VARCHAR(1) COMMENT '性别(M/F/U)',
  age INT COMMENT '年龄',
  city VARCHAR(64) COMMENT '城市',
  phone VARCHAR(20) COMMENT '手机号',
  email VARCHAR(128) COMMENT '邮箱',
  balance DECIMAL(12, 2) COMMENT '账户余额',
  register_time DATETIME COMMENT '注册时间',
  tags VARCHAR(512) COMMENT '用户标签',
  created_at DATETIME NOT NULL DEFAULT NOW() COMMENT '创建时间',
  updated_at DATETIME NOT NULL DEFAULT NOW() COMMENT '更新时间'
)
COMMENT '银行CDP用户画像表'
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES (
  "replication_num" = "3",
  "light_schema_change" = "true"
);

-- 系统日志表（Doris）
CREATE TABLE IF NOT EXISTS bank_system_log (
  log_id VARCHAR(64) NOT NULL COMMENT '日志ID',
  log_date DATE COMMENT '日志日期',
  log_time DATETIME COMMENT '日志时间',
  user_id VARCHAR(32) COMMENT '用户ID',
  user_name VARCHAR(128) COMMENT '用户名称',
  log_source VARCHAR(32) COMMENT '日志来源',
  operation VARCHAR(32) COMMENT '操作类型',
  amount DECIMAL(12, 2) COMMENT '交易金额',
  risk_score DECIMAL(5, 2) COMMENT '风险评分',
  log_content TEXT COMMENT '日志内容',
  result_code VARCHAR(10) COMMENT '结果代码',
  created_at DATETIME NOT NULL DEFAULT NOW()
)
COMMENT '银行系统日志表'
DISTRIBUTED BY HASH(log_id) BUCKETS 32
PROPERTIES (
  "replication_num" = "3",
  "light_schema_change" = "true"
);

-- 日志标签化表（Doris）
CREATE TABLE IF NOT EXISTS bank_system_log_tagged (
  log_id VARCHAR(64) NOT NULL COMMENT '日志ID',
  log_date DATE COMMENT '日志日期',
  log_time DATETIME COMMENT '日志时间',
  user_id VARCHAR(32) COMMENT '用户ID',
  user_name VARCHAR(128) COMMENT '用户名称',
  log_source VARCHAR(32) COMMENT '日志来源',
  operation VARCHAR(32) COMMENT '操作类型',
  amount DECIMAL(12, 2) COMMENT '交易金额',
  risk_score DECIMAL(5, 2) COMMENT '风险评分',
  log_content TEXT COMMENT '日志内容',
  ai_tag VARCHAR(32) COMMENT 'AI标签',
  ai_tag_group VARCHAR(16) COMMENT '标签大类',
  is_exception INT COMMENT '是否异常(0/1)',
  is_risk INT COMMENT '是否风险(0/1)',
  classify_time DATETIME COMMENT '标签化时间',
  classify_method VARCHAR(32) COMMENT '标签方法(AI_CLASSIFY/FALLBACK)',
  log_content_short VARCHAR(256) COMMENT '日志摘要',
  created_at DATETIME NOT NULL DEFAULT NOW()
)
COMMENT '日志分类标签化表'
DISTRIBUTED BY HASH(log_id) BUCKETS 32
PROPERTIES (
  "replication_num" = "3",
  "light_schema_change" = "true"
);

-- 创建索引（可选）
ALTER TABLE user_profiles ADD INDEX idx_user_id (user_id);
ALTER TABLE user_profiles ADD INDEX idx_city (city);
ALTER TABLE bank_system_log ADD INDEX idx_user_id (user_id);
ALTER TABLE bank_system_log_tagged ADD INDEX idx_user_id (user_id);
ALTER TABLE bank_system_log_tagged ADD INDEX idx_ai_tag (ai_tag);
