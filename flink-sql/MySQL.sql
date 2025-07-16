
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


-- 创建预警规则表
CREATE TABLE area_thresholds (
    area_id VARCHAR(255) NOT NULL PRIMARY KEY,
    -- '舒适'状态下的最大人数
    comfort_threshold INT,
    -- 超过这个值就进入'拥挤'状态
    warning_threshold INT,
    -- 超过这个值就进入'警戒'状态，需要立即干预
    alert_threshold INT
);
-- 插入预警规则数据
INSERT INTO area_thresholds (area_id, comfort_threshold, warning_threshold, alert_threshold) VALUES
('好莱坞大道', 80, 100, 120),
('哈利波特的魔法世界', 150, 180, 200),
('小黄人乐园', 100, 120, 150),
('侏罗纪世界大冒险', 200, 250, 300),
('变形金刚基地', 180, 220, 250); 

-- 创建预警日志表
CREATE TABLE alert_log (
    alert_time DATETIME(3) NOT NULL,
    area_id VARCHAR(255) NOT NULL,
    current_visitors BIGINT,
    alert_level VARCHAR(50), -- 例如: '拥挤预警', '警戒预警'
    alert_message VARCHAR(512)
);

-- 创建一个名为 area_current_alert 的新表，它代表**“每个区域当前的最新报警状态”**。这张表有主键，可以被 Flink 更新。
CREATE TABLE area_current_alert (
    area_id VARCHAR(255) NOT NULL PRIMARY KEY, -- area_id 作为主键
    last_alert_time DATETIME(3),              -- 最近一次报警的时间
    current_visitors BIGINT,
    alert_level VARCHAR(50),
    alert_message VARCHAR(512)
);