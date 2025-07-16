-- 这是我们项目的核心计算逻辑。它的作用是：
-- 从 kafka_visitor_events 表读取数据。
-- 按 area_id 分组。
-- 实时计算每个 area_id 的当前游客数。计算方法是：进入的人数(in)记为 +1，离开的人数(out)记为 -1，然后把它们累加起来。
-- 将计算结果持续地插入到我们的 print_sink 表中。

-- etl.sql

-- =============================================================================
--  核心计算逻辑：实时统计各区域当前游客数
-- =============================================================================
-- INSERT INTO print_sink
-- SELECT
--     area_id,
--     -- 这是核心计算逻辑：
--     -- 使用 CASE WHEN 语句，如果 action 是 'in'，则计数为 1；否则为 -1。
--     -- 然后使用 SUM() 对每个区域的所有事件进行累加，得到当前实时的人数。
--     SUM(CASE `action` WHEN 'in' THEN 1 ELSE -1 END) AS current_visitors,
--     -- 'TUMBLE_END' 是 Flink 提供的一个函数，用于显示当前计算结果所属的时间窗口的结束时间。
--     -- 这里我们用它来观察数据的更新。我们设置一个10秒的滚动窗口来触发更新和打印。
--     TUMBLE_END(event_time, INTERVAL '10' SECOND) as window_end
-- FROM
--     kafka_visitor_events
-- GROUP BY
--     -- 按区域ID和时间窗口进行分组
--     area_id,
--     TUMBLE(event_time, INTERVAL '10' SECOND);



-- etl.sql

-- 将计算结果持续地 UPSERT 到 MySQL 表中
-- 这个不带窗口的查询会持续地计算每个 area_id 的最新总和，并将结果更新到 MySQL 表中。
-- 这个版本会出现人数为负数的情况
INSERT INTO mysql_sink_area_stats
SELECT
    area_id,
    SUM(CASE `action` WHEN 'in' THEN 1 ELSE -1 END) AS current_visitors
FROM
    kafka_visitor_events
GROUP BY
    area_id;


-- etl.sql (已修复负数问题的版本)

-- =============================================================================
--  第一个计算逻辑：实时统计各区域当前游客数 (已修复负数问题)
-- =================================G============================================
INSERT INTO mysql_sink_area_stats
SELECT
    area_id,
    GREATEST(CAST(0 AS BIGINT), SUM(CASE `action` WHEN 'in' THEN 1 ELSE -1 END)) AS current_visitors
FROM
    kafka_visitor_events
GROUP BY
    area_id;


-- =============================================================================
--  第二个计算逻辑：按分钟窗口统计各区域的出入人次
-- =============================================================================
INSERT INTO mysql_sink_traffic_stats
SELECT
    area_id,
    TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
    -- 使用 COUNT 结合 CASE WHEN 来分别统计 'in' 和 'out' 的数量
    COUNT(CASE `action` WHEN 'in' THEN 1 ELSE NULL END) AS in_count,
    COUNT(CASE `action` WHEN 'out' THEN 1 ELSE NULL END) AS out_count
FROM
    kafka_visitor_events
GROUP BY
    -- 按区域ID和1分钟的滚动窗口进行分组
    area_id,
    TUMBLE(event_time, INTERVAL '1' MINUTE);


-- =============================================================================
--  第三个计算逻辑：游客饱和度预警引擎
-- =============================================================================
-- 为了能进行关联查询，我们先创建一个基于实时人数流的视图 (View)
CREATE TEMPORARY VIEW realtime_visitors_view AS
SELECT
    area_id,
    GREATEST(CAST(0 AS BIGINT), SUM(CASE `action` WHEN 'in' THEN 1 ELSE -1 END)) AS current_visitors
FROM
    kafka_visitor_events
GROUP BY
    area_id;

INSERT INTO mysql_sink_current_alert
SELECT
    -- 我们需要调整输出的列，以匹配新表的结构
    area_id, -- 主键列
    LOCALTIMESTAMP AS last_alert_time, -- 更新时间
    current_visitors,
    CASE
        WHEN current_visitors > alert_threshold THEN '红色警戒'
        ELSE '黄色拥堵'
    END AS alert_level,
    CONCAT(
        '【', CASE WHEN current_visitors > alert_threshold THEN '红色警戒' ELSE '黄色拥堵' END, '】区域: ', area_id,
        ', 当前人数: ', CAST(current_visitors AS STRING),
        ', 已超过阈值: ', CASE WHEN current_visitors > alert_threshold THEN CAST(alert_threshold AS STRING) ELSE CAST(warning_threshold AS STRING) END,
        '人, 请相关人员注意！'
    ) AS alert_message
FROM
    visitors_with_hardcoded_thresholds
WHERE
    current_visitors > warning_threshold;


-- =============================================================================
--  第四个计算逻辑：使用 MATCH_RECOGNIZE 计算游客逗留时长 (最终语法修正版)
-- =============================================================================
INSERT INTO mysql_sink_stay_duration
SELECT
    user_id,
    area_id,
    entry_time,
    exit_time,
    TIMESTAMPDIFF(MINUTE, entry_time, exit_time) AS duration_minutes
FROM
    kafka_visitor_events
    MATCH_RECOGNIZE (
        PARTITION BY user_id, area_id
        ORDER BY event_time
        MEASURES
            a.event_time AS entry_time,
            b.event_time AS exit_time
        ONE ROW PER MATCH
        AFTER MATCH SKIP TO NEXT ROW
        -- 【语法修正】PATTERN 只定义模式的结构
        PATTERN (a b)
        -- 【语法修正】使用独立的 DEFINE 子句来为模式变量赋予条件
        DEFINE
            a AS a.action = 'in',
            b AS b.action = 'out'
    );