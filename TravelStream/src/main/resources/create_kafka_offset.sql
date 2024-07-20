USE
CREATE TABLE t_offset (
    k_topic       VARCHAR(50)   NOT NULL,           -- 设置 Topic
    k_groupid     VARCHAR (50)  NOT NULL,           -- 设置 groupid
    k_partition   INT           NOT NULL,           -- 设置 Partition
    k_offset      BIGINT        NOT NULL,           -- 设置 offset
    PRIMARY KEY(k_topic, k_groupid, k_partition)    -- 设置联合主键
);