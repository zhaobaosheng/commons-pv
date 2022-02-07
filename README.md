# commons-pv
异步消息处理组件

JdbcMessageQueue mysql表结构
CREATE TABLE `msg_queue_store` (
  `id` varchar(40) NOT NULL COMMENT '消息标识',
  `queue_no` varchar(20) COMMENT '队列编号',
  `hash_key` varchar(100) COMMENT '保证顺序消息',
  `content` varchar(2000) COMMENT '消息内容',
  `create_time` datetime DEFAULT NULL COMMENT '消息创建时间',
  `exec_finishtime` datetime DEFAULT NULL COMMENT '消息执行时间',
  `exec_count` decimal(2,0) DEFAULT NULL COMMENT '执行次数',
  `msg_state` decimal(1,0) DEFAULT NULL COMMENT '消息状态 0未执行 1执行中 2执行失败 3错误消息',
  PRIMARY KEY (`id`)
);

CREATE TABLE `msg_queue_log` (
  `id` varchar(40) NOT NULL COMMENT '消息标识',
  `hash_key` varchar(100) COMMENT '保证顺序消息值',
  `content` varchar(2000) COMMENT '消息内容',
  `create_time` datetime DEFAULT NULL COMMENT '消息创建时间',
  `dispose_unit_tag` varchar(100) COMMENT '处理单元标识',
  `msg_source` decimal(1,0) DEFAULT NULL COMMENT '消息来源 1添加失败的消息 2拒绝执行的消息',
  PRIMARY KEY (`id`)
);