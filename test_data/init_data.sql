-- ================================================================
-- 测试数据 - 用户数据
-- ================================================================
USE bank_cdp;

INSERT INTO user_wide (user_id, update_date, user_name, id_card, phone, gender, age, age_group,
    city, province, education, occupation, register_date, asset_level, aum_total, deposit_amount,
    fund_amount, loan_amount, wm_amount, insurance_amount, has_credit_card, has_debit_card,
    has_mortgage, product_count, credit_score, credit_grade, risk_level, preferred_channel,
    app_login_30d, app_last_login, active_level, lifecycle_stage, churn_prob, clv_score,
    log_tags, anomaly_flag, created_at, updated_at)
VALUES
(10001,'2025-04-12','张*明','110***8881','138****0001',1,45,'36-45','北京','北京',5,'金融从业',
 '2018-03-15','VIP私行',1280.50,520.00,380.00,0,350.00,30.50,1,1,0,8,820,'AAA',3,'APP',
 28,'2025-04-11','高活','成熟',0.05,95000.00,'["高净值","基金偏好"]',0,NOW(),NOW()),

(10002,'2025-04-12','李*华','310***2222','139****0002',2,32,'26-35','上海','上海',6,'IT工程师',
 '2021-06-20','VIP钻石',380.20,120.00,180.00,50.00,80.00,0,1,1,1,5,760,'AA',4,'APP',
 45,'2025-04-12','高活','成长',0.08,42000.00,'["理财偏好","高频交易"]',0,NOW(),NOW()),

(10003,'2025-04-12','王*芳','440***3333','136****0003',2,58,'46-55','广州','广东',4,'个体经营',
 '2019-11-08','VIP铂金',620.00,400.00,80.00,120.00,120.00,0,1,1,1,6,780,'AA',2,'网点',
 3,'2025-03-20','低活','沉睡',0.42,55000.00,'["贷款需求","异地登录"]',1,NOW(),NOW()),

(10004,'2025-04-12','赵*强','330***4444','135****0004',1,28,'26-35','杭州','浙江',5,'电商创业',
 '2024-01-10','VIP黄金',85.30,30.00,35.00,20.00,0,0,1,1,0,3,680,'A',5,'小程序',
 62,'2025-04-12','高活','新客',0.15,8500.00,'["新客","频繁操作"]',0,NOW(),NOW()),

(10005,'2025-04-12','陈*梅','510***5555','137****0005',2,41,'36-45','成都','四川',5,'医生',
 '2020-07-22','VIP钻石',445.80,200.00,150.00,0,95.80,0,1,1,0,5,810,'AAA',2,'APP',
 15,'2025-04-10','中活','成熟',0.18,48000.00,'["保险偏好","稳健型"]',0,NOW(),NOW()),

(10006,'2025-04-12','刘*伟','420***6666','133****0006',1,35,'26-35','武汉','湖北',6,'销售经理',
 '2022-09-05','普通',18.50,10.00,5.00,3.50,0,0,1,0,0,2,580,'B',3,'网银',
 8,'2025-04-08','低活','流失预警',0.68,1200.00,'[]',0,NOW(),NOW()),

(10007,'2025-04-12','孙*丽','610***7777','132****0007',2,50,'46-55','西安','陕西',4,'教师',
 '2017-04-18','VIP铂金',560.00,380.00,60.00,0,120.00,0,1,1,0,4,800,'AAA',1,'网点',
 2,'2025-03-01','低活','沉睡',0.55,60000.00,'["大额转账"]',1,NOW(),NOW()),

(10008,'2025-04-12','周*涛','120***8888','131****0008',1,38,'36-45','天津','天津',5,'政府职员',
 '2023-02-28','VIP黄金',125.60,80.00,30.00,0,15.60,0,1,1,0,3,720,'A',3,'APP',
 20,'2025-04-11','中活','成长',0.22,15000.00,'["稳健型"]',0,NOW(),NOW()),

(10009,'2025-04-12','吴*红','350***9999','130****0009',2,29,'26-35','福州','福建',6,'会计',
 '2024-03-15','VIP黄金',92.40,40.00,42.40,0,10.00,0,1,1,0,3,700,'A',4,'APP',
 35,'2025-04-12','高活','新客',0.10,9800.00,'["基金偏好"]',0,NOW(),NOW()),

(10010,'2025-04-12','郑*明','370***0010','129****0010',1,55,'46-55','济南','山东',3,'商人',
 '2016-08-12','VIP私行',2100.80,800.00,600.00,0,700.00,0.80,1,1,0,9,880,'AAA',4,'网点',
 5,'2025-04-09','中活','成熟',0.08,180000.00,'["高净值","贵宾"]',0,NOW(),NOW());

-- ================================================================
-- 测试数据 - 用户标签 Bitmap
-- ================================================================
INSERT INTO user_tag (tag_date, tag_category, tag_name, tag_value, user_bitmap, user_count, updated_at)
VALUES
-- 资产等级
('2025-04-12','ASSET','asset_level','VIP私行',    TO_BITMAP(10001)|TO_BITMAP(10010), 2, NOW()),
('2025-04-12','ASSET','asset_level','VIP钻石',    TO_BITMAP(10002)|TO_BITMAP(10005), 2, NOW()),
('2025-04-12','ASSET','asset_level','VIP铂金',    TO_BITMAP(10003)|TO_BITMAP(10007), 2, NOW()),
('2025-04-12','ASSET','asset_level','VIP黄金',    TO_BITMAP(10004)|TO_BITMAP(10008)|TO_BITMAP(10009), 3, NOW()),
('2025-04-12','ASSET','asset_level','普通',       TO_BITMAP(10006), 1, NOW()),
-- 活跃等级
('2025-04-12','BEHAVIOR','active_level','高活',   TO_BITMAP(10001)|TO_BITMAP(10002)|TO_BITMAP(10004)|TO_BITMAP(10009), 4, NOW()),
('2025-04-12','BEHAVIOR','active_level','中活',   TO_BITMAP(10005)|TO_BITMAP(10008)|TO_BITMAP(10010), 3, NOW()),
('2025-04-12','BEHAVIOR','active_level','低活',   TO_BITMAP(10003)|TO_BITMAP(10006)|TO_BITMAP(10007), 3, NOW()),
-- 生命周期
('2025-04-12','LIFECYCLE','lifecycle_stage','新客',       TO_BITMAP(10004)|TO_BITMAP(10009), 2, NOW()),
('2025-04-12','LIFECYCLE','lifecycle_stage','成长',       TO_BITMAP(10002)|TO_BITMAP(10008), 2, NOW()),
('2025-04-12','LIFECYCLE','lifecycle_stage','成熟',       TO_BITMAP(10001)|TO_BITMAP(10005)|TO_BITMAP(10010), 3, NOW()),
('2025-04-12','LIFECYCLE','lifecycle_stage','沉睡',       TO_BITMAP(10003)|TO_BITMAP(10007), 2, NOW()),
('2025-04-12','LIFECYCLE','lifecycle_stage','流失预警',   TO_BITMAP(10006), 1, NOW()),
-- 渠道偏好
('2025-04-12','CHANNEL','preferred_channel','APP',        TO_BITMAP(10001)|TO_BITMAP(10002)|TO_BITMAP(10005)|TO_BITMAP(10009), 4, NOW()),
('2025-04-12','CHANNEL','preferred_channel','网点',       TO_BITMAP(10003)|TO_BITMAP(10007)|TO_BITMAP(10010), 3, NOW()),
('2025-04-12','CHANNEL','preferred_channel','小程序',     TO_BITMAP(10004), 1, NOW()),
('2025-04-12','CHANNEL','preferred_channel','网银',       TO_BITMAP(10006)|TO_BITMAP(10008), 2, NOW()),
-- 异常标记
('2025-04-12','RISK','anomaly_flag','1',           TO_BITMAP(10003)|TO_BITMAP(10007), 2, NOW());

-- ================================================================
-- 测试数据 - 用户行为
-- ================================================================
INSERT INTO user_behavior (event_id, user_id, event_date, event_time, event_type, event_category,
    channel, product_code, amount, result_code, session_id, device_type)
VALUES
(1001,10001,'2025-04-12','2025-04-12 09:15:00','LOGIN','LOGIN','APP','',0,'SUCCESS','s001','iOS'),
(1002,10001,'2025-04-12','2025-04-12 09:16:00','BROWSE_PRODUCT','BROWSE','APP','FUND_001',0,'SUCCESS','s001','iOS'),
(1003,10001,'2025-04-12','2025-04-12 09:20:00','TRANSACTION','TRANSACTION','APP','FUND_001',50000,'SUCCESS','s001','iOS'),
(1004,10002,'2025-04-12','2025-04-12 10:00:00','LOGIN','LOGIN','APP','',0,'SUCCESS','s002','Android'),
(1005,10002,'2025-04-12','2025-04-12 10:05:00','TRANSACTION','TRANSACTION','APP','WM_002',100000,'SUCCESS','s002','Android'),
(1006,10003,'2025-04-11','2025-04-11 14:00:00','LOGIN','LOGIN','网点','',0,'SUCCESS','s003','PC'),
(1007,10004,'2025-04-12','2025-04-12 11:00:00','REGISTER','REGISTER','小程序','',0,'SUCCESS','s004','Android'),
(1008,10004,'2025-04-12','2025-04-12 11:05:00','LOGIN','LOGIN','小程序','',0,'SUCCESS','s004','Android'),
(1009,10004,'2025-04-12','2025-04-12 11:10:00','BROWSE_PRODUCT','BROWSE','小程序','LOAN_003',0,'SUCCESS','s004','Android'),
(1010,10005,'2025-04-12','2025-04-12 08:30:00','LOGIN','LOGIN','APP','',0,'SUCCESS','s005','iOS'),
(1011,10005,'2025-04-12','2025-04-12 08:35:00','BROWSE_PRODUCT','BROWSE','APP','INS_001',0,'SUCCESS','s005','iOS'),
(1012,10001,'2025-04-11','2025-04-11 15:00:00','LOGIN','LOGIN','APP','',0,'SUCCESS','s006','iOS'),
(1013,10001,'2025-04-11','2025-04-11 15:05:00','TRANSACTION','TRANSACTION','APP','FUND_002',30000,'SUCCESS','s006','iOS'),
(1014,10002,'2025-04-11','2025-04-11 16:00:00','LOGIN','LOGIN','APP','',0,'SUCCESS','s007','Android'),
(1015,10002,'2025-04-11','2025-04-11 16:30:00','APPLY','APPLY','APP','LOAN_001',0,'SUCCESS','s007','Android'),
(1016,10008,'2025-04-12','2025-04-12 09:00:00','LOGIN','LOGIN','APP','',0,'SUCCESS','s008','Android'),
(1017,10009,'2025-04-12','2025-04-12 10:30:00','REGISTER','REGISTER','APP','',0,'SUCCESS','s009','iOS'),
(1018,10009,'2025-04-12','2025-04-12 10:35:00','LOGIN','LOGIN','APP','',0,'SUCCESS','s009','iOS'),
(1019,10009,'2025-04-12','2025-04-12 10:40:00','BROWSE_PRODUCT','BROWSE','APP','FUND_003',0,'SUCCESS','s009','iOS'),
(1020,10009,'2025-04-12','2025-04-12 10:45:00','APPLY','APPLY','APP','FUND_003',0,'SUCCESS','s009','iOS'),
(1021,10009,'2025-04-12','2025-04-12 10:50:00','TRANSACTION','TRANSACTION','APP','FUND_003',20000,'SUCCESS','s009','iOS');

-- ================================================================
-- 测试数据 - 日志
-- ================================================================
INSERT INTO user_log_raw (log_id, log_date, log_time, user_id, session_id, log_level,
    log_source, log_content, operation_type, ip_address, user_region, created_at)
VALUES
(9001,'2025-04-12','2025-04-12 09:15:00',10001,'s001','INFO','APP',
 'user_id=10001 action=LOGIN device=iOS ip=1.2.3.4 result=SUCCESS','LOGIN','1.2.3.4','北京',NOW()),

(9002,'2025-04-12','2025-04-12 09:20:00',10001,'s001','INFO','APP',
 'user_id=10001 action=TRANSFER amount=50000 to_account=6217****1234 result=SUCCESS','TRANSFER','1.2.3.4','北京',NOW()),

(9003,'2025-04-12','2025-04-12 10:00:00',10002,'s002','INFO','APP',
 'user_id=10002 action=LOGIN device=Android ip=5.6.7.8 result=SUCCESS','LOGIN','5.6.7.8','上海',NOW()),

(9004,'2025-04-12','2025-04-12 14:00:00',10003,'s003','WARN','CORE_BANK',
 'user_id=10003 action=LOGIN ip=221.1.2.3 region=广州 device=PC 异地登录检测：历史地区=广州 当前IP归属=黑龙江','LOGIN','221.1.2.3','黑龙江',NOW()),

(9005,'2025-04-12','2025-04-12 14:05:00',10003,'s003','WARN','CORE_BANK',
 'user_id=10003 action=TRANSFER amount=800000 to_account=9988****5678 大额转账预警 风控系统触发复核','TRANSFER','221.1.2.3','黑龙江',NOW()),

(9006,'2025-04-12','2025-04-12 11:00:00',10004,'s004','INFO','小程序',
 'user_id=10004 action=REGISTER channel=小程序 referral=推广活动A','LOGIN','10.1.1.1','杭州',NOW()),

(9007,'2025-04-12','2025-04-12 11:10:00',10004,'s004','INFO','小程序',
 'user_id=10004 action=QUERY product=贷款产品 amount_range=10万-50万','QUERY','10.1.1.1','杭州',NOW()),

(9008,'2025-04-12','2025-04-12 08:30:00',10005,'s005','INFO','APP',
 'user_id=10005 action=LOGIN device=iOS result=SUCCESS','LOGIN','20.3.4.5','成都',NOW()),

(9009,'2025-04-12','2025-04-12 22:00:00',10007,'s010','ERROR','CORE_BANK',
 'user_id=10007 action=TRANSFER amount=500000 频繁操作预警：30分钟内第5次转账 风控拦截','TRANSFER','110.2.3.4','西安',NOW()),

(9010,'2025-04-12','2025-04-12 09:00:00',10008,'s008','INFO','APP',
 'user_id=10008 action=LOGIN action=BROWSE_PRODUCT product=理财产品 result=SUCCESS','LOGIN','30.4.5.6','天津',NOW());

-- ================================================================
-- 测试数据 - 日志AI标签
-- ================================================================
INSERT INTO user_log_tag (log_id, log_date, user_id, log_time, log_type, intent_tag,
    anomaly_tag, risk_level, ai_raw_result, tag_source, created_at)
VALUES
(9001,'2025-04-12',10001,'2025-04-12 09:15:00','登录日志','账户设置','正常','低风险',
 '{"log_type":"登录日志","intent":"账户设置","anomaly_type":"正常","risk_score":0.05}','PYTHON_API',NOW()),

(9002,'2025-04-12',10001,'2025-04-12 09:20:00','转账日志','转账汇款','正常','低风险',
 '{"log_type":"转账日志","intent":"转账汇款","anomaly_type":"正常","risk_score":0.1}','PYTHON_API',NOW()),

(9003,'2025-04-12',10002,'2025-04-12 10:00:00','登录日志','账户设置','正常','低风险',
 '{"log_type":"登录日志","intent":"账户设置","anomaly_type":"正常","risk_score":0.05}','PYTHON_API',NOW()),

(9004,'2025-04-12',10003,'2025-04-12 14:00:00','异常日志','账户设置','异地登录','高风险',
 '{"log_type":"异常日志","intent":"账户设置","anomaly_type":"异地登录","risk_score":0.88}','PYTHON_API',NOW()),

(9005,'2025-04-12',10003,'2025-04-12 14:05:00','交易日志','转账汇款','大额转账','高风险',
 '{"log_type":"交易日志","intent":"转账汇款","anomaly_type":"大额转账","risk_score":0.92}','PYTHON_API',NOW()),

(9006,'2025-04-12',10004,'2025-04-12 11:00:00','登录日志','账户设置','正常','低风险',
 '{"log_type":"登录日志","intent":"账户设置","anomaly_type":"正常","risk_score":0.05}','PYTHON_API',NOW()),

(9007,'2025-04-12',10004,'2025-04-12 11:10:00','查询日志','申请贷款','正常','低风险',
 '{"log_type":"查询日志","intent":"申请贷款","anomaly_type":"正常","risk_score":0.08}','PYTHON_API',NOW()),

(9008,'2025-04-12',10005,'2025-04-12 08:30:00','登录日志','账户设置','正常','低风险',
 '{"log_type":"登录日志","intent":"账户设置","anomaly_type":"正常","risk_score":0.03}','PYTHON_API',NOW()),

(9009,'2025-04-12',10007,'2025-04-12 22:00:00','异常日志','转账汇款','频繁操作','高风险',
 '{"log_type":"异常日志","intent":"转账汇款","anomaly_type":"频繁操作","risk_score":0.95}','PYTHON_API',NOW()),

(9010,'2025-04-12',10008,'2025-04-12 09:00:00','登录日志','购买理财','正常','低风险',
 '{"log_type":"登录日志","intent":"购买理财","anomaly_type":"正常","risk_score":0.06}','PYTHON_API',NOW());

-- ================================================================
-- 测试数据 - 标签字典
-- ================================================================
INSERT INTO tag_dict (tag_id, tag_category, tag_name, tag_label, tag_desc, value_type,
    value_options, source_table, source_field, is_ai_tag, enable_bitmap, status, sort_order, created_at)
VALUES
(1,'ASSET','asset_level','资产等级','客户AUM资产等级分类','ENUM',
 '["VIP私行","VIP钻石","VIP铂金","VIP黄金","普通"]','user_wide','asset_level',0,1,1,1,NOW()),
(2,'BEHAVIOR','active_level','活跃等级','客户APP使用活跃程度','ENUM',
 '["高活","中活","低活","沉睡"]','user_wide','active_level',0,1,1,2,NOW()),
(3,'LIFECYCLE','lifecycle_stage','生命周期','客户生命周期阶段','ENUM',
 '["新客","成长","成熟","沉睡","流失预警"]','user_wide','lifecycle_stage',0,1,1,3,NOW()),
(4,'CHANNEL','preferred_channel','偏好渠道','客户最常使用的服务渠道','ENUM',
 '["APP","网点","小程序","网银"]','user_wide','preferred_channel',0,1,1,4,NOW()),
(5,'RISK','anomaly_flag','异常标记','是否有异常行为记录','BOOLEAN',
 '["0","1"]','user_wide','anomaly_flag',0,1,1,5,NOW()),
(6,'LOG_AI','log_type','日志类型','AI识别的日志操作类型','ENUM',
 '["登录日志","交易日志","查询日志","转账日志","异常日志"]','user_log_tag','log_type',1,0,1,6,NOW()),
(7,'LOG_AI','intent_tag','操作意图','AI识别的用户操作意图','ENUM',
 '["查询余额","转账汇款","购买理财","申请贷款","账户设置","其他"]','user_log_tag','intent_tag',1,0,1,7,NOW()),
(8,'LOG_AI','anomaly_tag','异常标签','AI识别的异常行为类型','ENUM',
 '["正常","异地登录","大额转账","频繁操作","可疑账户"]','user_log_tag','anomaly_tag',1,1,1,8,NOW()),
(9,'LOG_AI','risk_level','风险等级','AI综合评估的风险等级','ENUM',
 '["低风险","中风险","高风险"]','user_log_tag','risk_level',1,1,1,9,NOW());
