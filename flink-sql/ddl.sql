-- ddl.sql

-- 这些代码应该在flinksql里面运行！！！

-- =============================================================================
--  1. 创建 Kafka 源表 (Source Table)
-- =============================================================================
-- 这张表代表了从 Kafka 'scenic_visitor_topic' topic流入的数据流
CREATE TABLE kafka_visitor_events (
    `event_time` TIMESTAMP(3),  -- 事件时间，我们用它来做时间相关的计算，精度为毫秒
    `area_id`    STRING,         -- 区域ID，例如 "gate_a"
    `action`     STRING,         -- 动作，"in" 或 "out"
    `user_id`    STRING,         -- 游客ID
    -- 定义 Watermark，这是 Flink 处理事件时间的核心。
    -- 它告诉 Flink 数据流的时间戳是 `event_time` 字段，并允许最大 5 秒的乱序。
    -- 这意味着 Flink 会等待 5 秒，以确保迟到的数据也能被正确处理。
    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'scenic_visitor_topic',
    'properties.bootstrap.servers' = '172.23.79.129:9092', -- Kafka Broker 地址
    'properties.group.id' = 'visitor_guard_consumer', -- 消费者组ID，可以自定义
    'scan.startup.mode' = 'latest-offset',            -- 从最新的消息开始消费
    'json.timestamp-format.standard' = 'ISO-8601',
    'format' = 'json'                                 -- 数据格式为 JSON
);


-- =============================================================================
--  2. 创建 Print 目标表 (Sink Table) 用于调试
-- =============================================================================
-- 这张表不是一个真实的存储，它会把流计算的“结果”实时打印在 Flink SQL 客户端的命令行窗口中。
-- 这对于我们验证逻辑是否正确非常有用。
-- CREATE TABLE print_sink (
--     `area_id`         STRING,       -- 区域ID
--     `current_visitors` BIGINT,      -- 当前游客数
--     `window_end`      TIMESTAMP(3)  -- 窗口结束时间，用于观察结果更新
-- ) WITH (
--     'connector' = 'print'
-- );

-- print完全不好用，我这边没有显示，就不要那个表了

--  2. 创建 MySQL 目标表 (Sink Table)
-- =============================================================================
CREATE TABLE mysql_sink_area_stats (
    `area_id`         VARCHAR(255) NOT NULL,
    `current_visitors` BIGINT,
    PRIMARY KEY (`area_id`) NOT ENFORCED -- 告诉 Flink area_id 是主键
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://172.23.79.129:3306/bigdata?serverTimezone=UTC', 
    'table-name' = 'area_realtime_stats',
    'username' = 'remote_user',
    'password' = 'Admin@123'
);


-- =============================================================================
--  3. 创建 MySQL 目标表 (Sink) 用于存储每分钟出入流量
-- =============================================================================

CREATE TABLE mysql_sink_traffic_stats (
    `area_id`         VARCHAR(255) NOT NULL,
    `window_end`      TIMESTAMP(3) NOT NULL,
    `in_count`        BIGINT,
    `out_count`       BIGINT,
    PRIMARY KEY (`area_id`, `window_end`) NOT ENFORCED,
    WATERMARK FOR `window_end` AS `window_end` - INTERVAL '1' SECOND  -- 可选：定义水印
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://172.23.79.129:3306/bigdata?serverTimezone=UTC',
    'table-name' = 'area_traffic_per_minute',
    'username' = 'remote_user',
    'password' = 'Admin@123'
);

-- =============================================================================
--  4. 创建 MySQL 维表 (Source) 用于读取预警阈值
-- =============================================================================
-- 这是一张维表 (Dimension Table)，它的数据是静态的，用于关联查询
CREATE TABLE mysql_dim_thresholds (
    `area_id` VARCHAR(255) NOT NULL,
    `comfort_threshold` INT,
    `warning_threshold` INT,
    `alert_threshold` INT,
    PRIMARY KEY (area_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://172.23.79.129:3306/bigdata?serverTimezone=UTC',
    'table-name' = 'area_thresholds',
    'username' = 'remote_user', -- 替换为你的用户名
    'password' = 'Admin@123'    -- 替换为你的密码
);

-- =============================================================================
--  5. 创建 MySQL 目标表 (Sink) 用于存储预警日志
-- =============================================================================
-- CREATE TABLE mysql_sink_alert_log (
--     `alert_time` TIMESTAMP(3),
--     `area_id` VARCHAR(255),
--     `current_visitors` BIGINT,
--     `alert_level` VARCHAR(50),
--     `alert_message` VARCHAR(512)
-- ) WITH (
--     'connector' = 'jdbc',
--     'url' = 'jdbc:mysql://172.23.79.129:3306/bigdata?serverTimezone=UTC',
--     'table-name' = 'alert_log',
--     'username' = 'remote_user', -- 替换为你的用户名
--     'password' = 'Admin@123'    -- 替换为你的密码
-- );

CREATE TABLE mysql_sink_current_alert (
    `area_id` VARCHAR(255) NOT NULL,
    `last_alert_time` TIMESTAMP(3),
    `current_visitors` BIGINT,
    `alert_level` VARCHAR(50),
    `alert_message` VARCHAR(512),
    PRIMARY KEY (area_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://172.23.79.129:3306/bigdata?serverTimezone=UTC',
    'table-name' = 'area_current_alert', -- 指向我们新建的表
    'username' = 'remote_user',
    'password' = 'Admin@123'
);

-- 计算实时游客人数，和之前一样
CREATE OR REPLACE TEMPORARY VIEW visitors_aggregation_view AS
SELECT
    area_id,
    GREATEST(CAST(0 AS BIGINT), SUM(CASE `action` WHEN 'in' THEN 1 ELSE -1 END)) as current_visitors
FROM
    kafka_visitor_events
GROUP BY
    area_id;

CREATE OR REPLACE TEMPORARY VIEW visitors_with_hardcoded_thresholds AS
SELECT
    area_id,
    current_visitors,
    -- 使用 CASE WHEN 硬编码每个区域的警告阈值
    CASE area_id
        WHEN '好莱坞大道' THEN 100
        WHEN '哈利波特的魔法世界' THEN 180
        WHEN '小黄人乐园' THEN 120
        WHEN '侏罗纪世界大冒险' THEN 250
        WHEN '变形金刚基地' THEN 220
        ELSE 99999 -- 为未知的区域设置一个超大值，避免误报
    END AS warning_threshold,
    -- 使用 CASE WHEN 硬编码每个区域的警戒阈值
    CASE area_id
        WHEN '好莱坞大道' THEN 120
        WHEN '哈利波特的魔法世界' THEN 200
        WHEN '小黄人乐园' THEN 150
        WHEN '侏罗纪世界大冒险' THEN 300
        WHEN '变形金刚基地' THEN 250
        ELSE 999999
    END AS alert_threshold
FROM
    visitors_aggregation_view;