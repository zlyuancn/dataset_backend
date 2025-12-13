CREATE TABLE `dataset`
(
    `dataset_id`     int unsigned     NOT NULL AUTO_INCREMENT COMMENT '数据集id',
    `dataset_name`   varchar(32)      NOT NULL COMMENT '数据集名',
    `remark`         varchar(1024)    NOT NULL DEFAULT '' COMMENT '备注',
    `dataset_extend` varchar(8192)    NOT NULL DEFAULT '' COMMENT '数据集扩展数据',
    `chunk_meta`     text             NOT NULL COMMENT '所有chunk元数据',
    `value_total`    bigint unsigned  NOT NULL DEFAULT 0 COMMENT 'value 总数',
    `create_time`    datetime         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`    datetime         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `processed_time` datetime         NULL COMMENT '加工完成时间',
    `op_source`      varchar(32)      NOT NULL DEFAULT '' COMMENT '最后操作来源',
    `op_user_id`     varchar(32)      NOT NULL DEFAULT '' COMMENT '最后操作用户id',
    `op_user_name`   varchar(32)      NOT NULL DEFAULT '' COMMENT '最后操作用户名',
    `op_remark`      varchar(1024)    NOT NULL DEFAULT '' COMMENT '最后操作备注',
    `status`         tinyint unsigned NOT NULL DEFAULT 0 COMMENT '任务状态 0=已创建 2=运行中 3=已完成 4=正在停止 5=已停止 6=已删除',
    `activate_time`  datetime         NOT NULL DEFAULT '1000-01-01 00:00:00' comment '最后激活时间',
    PRIMARY KEY (`dataset_id`),
    KEY `idx_create_time` (`create_time` DESC),
    KEY `idx_status_create_time` (`status`, `create_time` DESC),
    KEY `idx_dataset_name` (`dataset_name`(8), `create_time` DESC),
    KEY `idx_op_user_id` (`op_user_id`(8), `create_time` DESC),
    KEY `idx_op_user_name` (`op_user_name`(8), `create_time` DESC),
    KEY `idx_activate_time_status_index` (`activate_time` desc, `status`) comment '恢复器扫描处理中的数据集时索引覆盖'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci COMMENT ='数据集';

CREATE TABLE `dataset_history`
(
    `id`             int unsigned     NOT NULL AUTO_INCREMENT,
    `dataset_id`     bigint unsigned  NOT NULL COMMENT '数据集id',
    `dataset_name`   varchar(32)      NOT NULL COMMENT '数据集名',
    `remark`         varchar(1024)    NOT NULL DEFAULT '' COMMENT '备注',
    `dataset_extend` varchar(8192)    NOT NULL DEFAULT '' COMMENT '数据集扩展数据',
    `create_time`    datetime         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `op_source`      varchar(32)      NOT NULL DEFAULT '' COMMENT '最后操作来源',
    `op_user_id`     varchar(32)      NOT NULL DEFAULT '' COMMENT '最后操作用户id',
    `op_user_name`   varchar(32)      NOT NULL DEFAULT '' COMMENT '最后操作用户名',
    `op_remark`      varchar(1024)    NOT NULL DEFAULT '' COMMENT '最后操作备注',
    `op_cmd`         tinyint unsigned NOT NULL DEFAULT 0 COMMENT '操作指令',
    `status`         tinyint unsigned NOT NULL DEFAULT 0 COMMENT '任务状态 0=已创建 2=运行中 3=已完成 4=正在停止 5=已停止 6=已删除',
    PRIMARY KEY (`id`),
    KEY `idx_create_time` (`dataset_id`, `create_time` DESC),
    KEY `idx_op_user_id` (`op_user_id`(8), `create_time` DESC),
    KEY `idx_op_user_name` (`op_user_name`(8), `create_time` DESC)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci COMMENT ='数据集操作历史';
