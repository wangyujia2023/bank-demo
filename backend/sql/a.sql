-- 场景2: 客户画像 (Bitmap)
CREATE TABLE IF NOT EXISTS bank.t_customer_tags (
  tag_id  BIGINT,
  tag_name     VARCHAR(32),
  tag_bitmap   BITMAP BITMAP_UNION,
  update_time  DATETIME REPLACE
) AGGREGATE KEY(tag_id, tag_name)
DISTRIBUTED BY HASH(tag_id) BUCKETS 16
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS bank.user_tag_wide (
    customer_id        BIGINT COMMENT '用户ID',
    update_time        DATETIME COMMENT '更新时间',

    -- ==================== 1. 基础属性 ====================
    male               TINYINT DEFAULT '0' COMMENT '男性',
    female             TINYINT DEFAULT '0' COMMENT '女性',
    age_under_20       TINYINT DEFAULT '0' COMMENT '年龄<20',
    age_20_25          TINYINT DEFAULT '0' COMMENT '年龄20-25',
    age_26_30          TINYINT DEFAULT '0' COMMENT '年龄26-30',
    age_31_35          TINYINT DEFAULT '0' COMMENT '年龄31-35',
    age_36_40          TINYINT DEFAULT '0' COMMENT '年龄36-40',
    age_41_45          TINYINT DEFAULT '0' COMMENT '年龄41-45',
    age_46_50          TINYINT DEFAULT '0' COMMENT '年龄46-50',
    age_51_55          TINYINT DEFAULT '0' COMMENT '年龄51-55',
    age_56_60          TINYINT DEFAULT '0' COMMENT '年龄56-60',
    age_over_60        TINYINT DEFAULT '0' COMMENT '年龄>60',
    married            TINYINT DEFAULT '0' COMMENT '已婚',
    unmarried          TINYINT DEFAULT '0' COMMENT '未婚',
    divorced           TINYINT DEFAULT '0' COMMENT '离异',
    has_child          TINYINT DEFAULT '0' COMMENT '有子女',
    no_child           TINYINT DEFAULT '0' COMMENT '无子女',
    has_house          TINYINT DEFAULT '0' COMMENT '有房',
    no_house           TINYINT DEFAULT '0' COMMENT '无房',
    has_car            TINYINT DEFAULT '0' COMMENT '有车',
    no_car             TINYINT DEFAULT '0' COMMENT '无车',
    local_hukou        TINYINT DEFAULT '0' COMMENT '本地户籍',
    foreign_hukou      TINYINT DEFAULT '0' COMMENT '外地户籍',
    has_social_security TINYINT DEFAULT '0' COMMENT '缴纳社保',
    has_fund           TINYINT DEFAULT '0' COMMENT '缴纳公积金',
    education_bachelor TINYINT DEFAULT '0' COMMENT '本科及以上',
    education_college  TINYINT DEFAULT '0' COMMENT '大专',
    education_high     TINYINT DEFAULT '0' COMMENT '高中及以下',

    -- ==================== 2. 资产类 ====================
    income_under_5k    TINYINT DEFAULT '0' COMMENT '月收入<5k',
    income_5k_1w       TINYINT DEFAULT '0' COMMENT '月收入5k-1w',
    income_1w_2w       TINYINT DEFAULT '0' COMMENT '月收入1w-2w',
    income_2w_5w       TINYINT DEFAULT '0' COMMENT '月收入2w-5w',
    income_over_5w     TINYINT DEFAULT '0' COMMENT '月收入>5w',
    asset_under_10w    TINYINT DEFAULT '0' COMMENT '资产<10w',
    asset_10w_50w      TINYINT DEFAULT '0' COMMENT '资产10w-50w',
    asset_50w_100w     TINYINT DEFAULT '0' COMMENT '资产50w-100w',
    asset_100w_500w    TINYINT DEFAULT '0' COMMENT '资产100w-500w',
    asset_500w_1000w   TINYINT DEFAULT '0' COMMENT '资产500w-1000w',
    asset_over_1000w   TINYINT DEFAULT '0' COMMENT '资产>1000w',
    high_net_worth     TINYINT DEFAULT '0' COMMENT '高净值用户',
    ultra_high_net     TINYINT DEFAULT '0' COMMENT '超高净值',
    potential_client   TINYINT DEFAULT '0' COMMENT '潜力客户',
    normal_client      TINYINT DEFAULT '0' COMMENT '普通客户',
    low_asset          TINYINT DEFAULT '0' COMMENT '低资产用户',

    -- ==================== 3. 产品持有 ====================
    has_financial      TINYINT DEFAULT '0' COMMENT '持有理财',
    has_fund_product   TINYINT DEFAULT '0' COMMENT '持有基金',
    has_stock          TINYINT DEFAULT '0' COMMENT '持有股票',
    has_insurance      TINYINT DEFAULT '0' COMMENT '持有保险',
    has_bonds          TINYINT DEFAULT '0' COMMENT '持有国债',
    has_gold           TINYINT DEFAULT '0' COMMENT '持有贵金属',
    has_loan           TINYINT DEFAULT '0' COMMENT '有贷款',
    has_housing_loan   TINYINT DEFAULT '0' COMMENT '有房贷',
    has_car_loan       TINYINT DEFAULT '0' COMMENT '有车贷',
    has_consumer_loan  TINYINT DEFAULT '0' COMMENT '有消费贷',

    -- ==================== 4. 行为标签 ====================
    active_month_1_3   TINYINT DEFAULT '0' COMMENT '月登录1-3次',
    active_month_4_8   TINYINT DEFAULT '0' COMMENT '月登录4-8次',
    active_month_over_9 TINYINT DEFAULT '0' COMMENT '月登录≥9次',
    active_7d          TINYINT DEFAULT '0' COMMENT '7日活跃',
    active_30d         TINYINT DEFAULT '0' COMMENT '30日活跃',
    inactive_7d        TINYINT DEFAULT '0' COMMENT '7日不活跃',
    inactive_30d       TINYINT DEFAULT '0' COMMENT '30日不活跃',
    trans_monthly      TINYINT DEFAULT '0' COMMENT '月均交易',
    trans_high_freq    TINYINT DEFAULT '0' COMMENT '高交易频率',
    big_transactor     TINYINT DEFAULT '0' COMMENT '大额交易用户',
    mobile_user        TINYINT DEFAULT '0' COMMENT '手机银行用户',
    web_user           TINYINT DEFAULT '0' COMMENT '网银用户',

    -- ==================== 5. 营销标签 ====================
    click_ads          TINYINT DEFAULT '0' COMMENT '点击广告',
    apply_loan         TINYINT DEFAULT '0' COMMENT '申请贷款',
    buy_financing      TINYINT DEFAULT '0' COMMENT '购买理财',
    transfer_in        TINYINT DEFAULT '0' COMMENT '资金转入',
    transfer_out       TINYINT DEFAULT '0' COMMENT '资金转出',
    high_response      TINYINT DEFAULT '0' COMMENT '高响应率',
    low_response       TINYINT DEFAULT '0' COMMENT '低响应率',
    marketing_sensitive TINYINT DEFAULT '0' COMMENT '营销敏感'

)
DUPLICATE KEY(customer_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "1"
);