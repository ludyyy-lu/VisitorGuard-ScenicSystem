-- 这段代码的作用是告诉 Flink：
-- 有一个名为 kafka_visitor_events 的表，它连接到 Kafka 的 scenic_visitor_topic 主题。
-- 这个表的数据是 JSON 格式，并且 Flink 应该如何解析它的字段。
-- 定义一个名为 print_sink 的表，它会将结果直接打印到 Flink SQL 客户端的控制台。
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
    'url' = 'jdbc:mysql://172.23.79.129:3306/bigdata', 
    'table-name' = 'area_realtime_stats',
    'username' = 'remote_user',
    'password' = 'Admin@123'
);