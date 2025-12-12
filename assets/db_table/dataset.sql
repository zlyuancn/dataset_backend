CREATE TABLE `dataset`
(
    `dataset_id`         bigint unsigned  NOT NULL AUTO_INCREMENT COMMENT '数据集id',
    `dataset_name`       varchar(32)      NOT NULL COMMENT '数据集名',
    `remark`             varchar(1024)    NOT NULL DEFAULT '' COMMENT '备注',
    `data_source`        tinyint unsigned NOT NULL DEFAULT 0 COMMENT '数据源',
    `data_source_extend` varchar(4096)    NOT NULL DEFAULT '' COMMENT '数据源扩展数据',
    `data_total`         bigint unsigned  NOT NULL DEFAULT 0 COMMENT '数据总数',
    `dataset_meta`       varchar(1024)    NOT NULL DEFAULT '' COMMENT '数据集元数据',
    `delim`              varchar(128)     NOT NULL DEFAULT '' COMMENT '分隔符',
    `chunk_meta`         text             NOT NULL COMMENT '所有chunk元数据',
    `create_time`        datetime         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`        datetime         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `processed_time`     datetime         NULL COMMENT '加工完成时间',
    `op_source`          varchar(32)      NOT NULL DEFAULT '' COMMENT '最后操作来源',
    `op_user_id`         varchar(32)      NOT NULL DEFAULT '' COMMENT '最后操作用户id',
    `op_user_name`       varchar(32)      NOT NULL DEFAULT '' COMMENT '最后操作用户名',
    `op_remark`          varchar(1024)    NOT NULL DEFAULT '' COMMENT '最后操作备注',
    `status`             tinyint unsigned NOT NULL DEFAULT 0 COMMENT '状态 0=正常 1=隐藏',
    PRIMARY KEY (`dataset_id`),
    KEY `dataset_create_time` (`create_time` DESC),
    KEY `dataset_status_create_time` (`status`, `create_time` DESC)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci COMMENT ='数据集';
