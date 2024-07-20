CREATE DATABASE IF NOT EXISTS travel CHARSET UTF8;
USE travel;

CREATE TABLE IF NOT EXISTS t_heat (
    -- id           BIGINT PRIMARY KEY AUTO_INCREMENT,  -- id
    longitude       DOUBLE,                             -- 经度
    latitude        DOUBLE,                             -- 纬度
    scenic          VARCHAR(20),                        -- 景点
    sec_moment      VARCHAR(15),                        -- 具体时刻
    sec_quantity    BIGINT,                             -- 具体数量
    PRIMARY KEY(scenic, sec_moment)                     -- 设置联合主键
);

CREATE TABLE IF NOT EXISTS t_scenic (
    -- id           BIGINT PRIMARY KEY AUTO_INCREMENT,  -- id
    scenic          VARCHAR(20),                        -- 景点
    min_moment      VARCHAR(15),                        -- 具体时刻
    min_quantity    BIGINT,                             -- 具体数量
    PRIMARY KEY(scenic, min_moment)                     -- 设置联合主键
);

CREATE TABLE IF NOT EXISTS t_offset (
    k_topic       VARCHAR(50)   NOT NULL,           -- 设置 Topic
    k_groupid     VARCHAR (50)  NOT NULL,           -- 设置 groupid
    k_partition   INT           NOT NULL,           -- 设置 Partition
    k_offset      BIGINT        NOT NULL,           -- 设置 offset
    PRIMARY KEY(k_topic, k_groupid, k_partition)    -- 设置联合主键
);