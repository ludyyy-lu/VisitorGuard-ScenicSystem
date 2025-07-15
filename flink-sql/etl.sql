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
INSERT INTO mysql_sink_area_stats
SELECT
    area_id,
    SUM(CASE `action` WHEN 'in' THEN 1 ELSE -1 END) AS current_visitors
FROM
    kafka_visitor_events
GROUP BY
    area_id;