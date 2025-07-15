
-- 创建一张表，名为 area_realtime_stats，用来存储每个区域的实时游客数量。
CREATE TABLE area_realtime_stats (
    area_id VARCHAR(255) NOT NULL,    -- 区域ID，例如 "好莱坞大道"
    current_visitors BIGINT,         -- 当前游客数
    PRIMARY KEY (area_id)             -- 【非常重要】将 area_id 设置为主键
);



CREATE TABLE area_traffic_per_minute (
    area_id VARCHAR(255) NOT NULL,
    window_end TIMESTAMP(3) NOT NULL,  -- 统计窗口的结束时间
    in_count BIGINT,                   -- 该窗口内进入的人次
    out_count BIGINT,                  -- 该窗口内离开的人次
    PRIMARY KEY (area_id, window_end)  -- 使用区域ID和窗口时间作为联合主键
);