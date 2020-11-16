CREATE EXTERNAL TABLE IF NOT EXISTS original_action_log
(
    remote_addr   STRING,
    request_time  STRING,
    log_version   STRING,
    start_time    bigint,
    end_time      bigint,
    region        STRING,
    product_id    STRING,
    hardware      STRING,
    os            STRING,
    soft          STRING,
    active_user   STRING,
    active_org    STRING,
    active_type   int,
    passive_obj   STRING,
    passive_type  STRING,
    from_prod     STRING,
    from_pos      STRING,
    company       string,
    action_title  STRING,
    action_type   int,
    request       STRING,
    request_param STRING,
    group_type    int,
    group_id      STRING,
    result_flag   int,
    result        STRING
)
    partitioned by (put_date STRING,idx string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~'
    STORED AS TEXTFILE
    LOCATION '/pep_cloud/ulog/ods/original_action_log';


    CREATE EXTERNAL TABLE IF NOT EXISTS original_action_log_web
(
    remote_addr   STRING,
    request_time  STRING,
    log_version   STRING,
    start_time    bigint,
    end_time      bigint,
    region        STRING,
    product_id    STRING,
    hardware      STRING,
    os            STRING,
    soft          STRING,
    active_user   STRING,
    active_org    STRING,
    active_type   int,
    passive_obj   STRING,
    passive_type  STRING,
    from_prod     STRING,
    from_pos      STRING,
    company       string,
    action_title  STRING,
    action_type   int,
    request       STRING,
    request_param STRING,
    group_type    int,
    group_id      STRING,
    result_flag   int,
    result        STRING
)
    partitioned by (put_date STRING,idx string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~'
    STORED AS TEXTFILE
    LOCATION '/pep_cloud/ulog/ods/original_action_log_web';

    CREATE EXTERNAL TABLE IF NOT EXISTS original_action_server_log
(
    id            STRING,
    remote_addr   STRING,
    request_time  STRING,
    log_version   STRING,
    start_time    bigint,
    end_time      bigint,
    region        STRING,
    product_id    STRING,
    hardware      STRING,
    os            STRING,
    soft          STRING,
    active_user   STRING,
    active_org    STRING,
    active_type   int,
    passive_obj   STRING,
    passive_type  STRING,
    from_prod     STRING,
    from_pos      STRING,
    company       string,
    action_title  STRING,
    action_type   int,
    request       STRING,
    request_param STRING,
    group_type    int,
    group_id      STRING,
    result_flag   int,
    result        STRING
)
    partitioned by (put_date STRING,company_idx string,idx string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~'
    STORED AS TEXTFILE
    LOCATION '/pep_cloud/ulog/ods/original_action_server_log';

CREATE EXTERNAL TABLE IF NOT EXISTS action_log_ot
(
    id            STRING,
    remote_addr   STRING,
    request_time  STRING,
    log_version   STRING,
    start_time    bigint,
    end_time      bigint,
    region        STRING,
    product_id    STRING,
    hardware      STRING,
    os            STRING,
    soft          STRING,
    active_user   STRING,
    active_org    STRING,
    active_type   int,
    passive_obj   STRING,
    passive_type  STRING,
    from_prod     STRING,
    from_pos      STRING,
    company       string,
    action_title  STRING,
    action_type   int,
    request       STRING,
    request_param STRING,
    group_type    int,
    group_id      STRING,
    result_flag   int,
    result        STRING
)
    partitioned by (put_date int)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~'
    STORED AS TEXTFILE
    LOCATION '/pep_cloud/ulog/ods/action_log_ot';


CREATE EXTERNAL TABLE IF NOT EXISTS action_log(
id STRING,
remote_addr STRING,
request_time STRING,
log_version STRING,
start_time bigint,
end_time bigint,
region STRING,
product_id STRING,
hardware STRING ,
os STRING ,
soft STRING ,
active_user STRING ,
active_org STRING ,
active_type int,
passive_obj STRING ,
passive_type STRING,
from_prod STRING,
from_pos STRING ,
company string,
action_title STRING ,
action_type int,
request STRING,
request_param STRING ,
group_type int,
group_id STRING,
result_flag int,
result STRING
)
partitioned by (put_date int,idx string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '~'
STORED AS TEXTFILE
LOCATION '/pep_cloud/ulog/ods/action_log';

CREATE TABLE `action_do_log`
(
    `id`            string,    //
    `remote_addr`   string,    //ip地址
    `country`       string,    //国家
    `province`      string,    //省
    `city`          string,    //市
    `location`      string,    //地区
    `request_time`  string,    //数据上报时间
    `log_version`   string,    //日志版本
    `start_time`    bigint,    //开始时间
    `end_time`      bigint,    //结束时间
    `region`        string,    //地区
    `product_id`    string,    //产品ID
    `os`            string,    //操作系统信息
    `soft`          string,    //软件信息
    `hardware`      string,    //硬件信息
    `device_id`     string,    //设备ID
    `active_user`   string,    //用户ID
    `active_org`    string,    //
    `active_type`   int,       //
    `passive_obj`   string,    //事件主体
    `passive_type`  string,    //
    `from_prod`     string,    //依赖模块
    `from_pos`      string,    //产品位置
    `company`       string,    //渠道
    `action_title`  string,    //上报事件类型
    `action_type`   int,       //
    `request`       string,    //请求地址
    `request_param` string,    //请求参数
    `group_type`    int,       //
    `group_id`      string,    //会话ID
    `result_flag`   int,       //事件上报结果标志
    `result`        string,    //事件上报结果内容
    `num`           int        //
)
    partitioned by (put_date string)
    STORED AS parquet;

CREATE TABLE `action_do_log_dc`
(
    `id`            string,
    `remote_addr`   string,
    `country`       string,
    `province`      string,
    `city`          string,
    `location`      string,
    `request_time`  string,
    `log_version`   string,
    `start_time`    bigint,
    `end_time`      bigint,
    `region`        string,
    `product_id`    string,
    `os`            string,
    `soft`          string,
    `hardware`      string,
    `device_id`     string,
    `active_user`   string,
    `active_org`    string,
    `active_type`   int,
    `passive_obj`   string,
    `passive_type`  string,
    `from_prod`     string,
    `from_pos`      string,
    `company`       string,
    `action_title`  string,
    `action_type`   int,
    `request`       string,
    `request_param` string,
    `group_type`    int,
    `group_id`      string,
    `result_flag`   int,
    `result`        string,
    `num`           int
)
    partitioned by (put_date int)
    STORED AS parquet;

create table if not exists ods.ods_order_detail
(
    id            bigint,
    app_id        string,
    app_order_id  string,
    product_id    string,
    product_name  string,
    quantity      bigint,
    type          int,
    code          string,
    start_time    string,
    end_time      string,
    beans         double,
    material_code string,
    material_name string
) partitioned by (year string,month string,day string)
    stored as textfile;


create table if not exists ods.ods_order_info
(
    id                bigint,
    app_id            string,
    app_order_id      string,
    user_id           string,
    user_name         string,
    sale_channel_id   int,
    sale_channel_name string,
    state             int,
    create_time       string,
    delete_time       string,
    discount          double,
    pay_channel       int,
    pay_time          string,
    pay_tradeno       string,
    remark            string,
    beans             double
) partitioned by (year string,month string,day string)
    stored as textfile;


create external table if not exists ods.ods_order_detail_without_partition
(
    id            bigint,
    app_id        string,
    app_order_id  string,
    product_id    string,
    product_name  string,
    quantity      bigint,
    type          int,
    code          string,
    start_time    string,
    end_time      string,
    beans         double,
    material_code string,
    material_name string
) stored as textfile location '/pep_cloud/order/ods_order_detail_without_partition';


create external table if not exists ods.ods_order_info_without_partition
(
    id                bigint,
    app_id            string,
    app_order_id      string,
    user_id           string,
    user_name         string,
    sale_channel_id   int,
    sale_channel_name string,
    state             int,
    create_time       string,
    delete_time       string,
    discount          double,
    pay_channel       int,
    pay_time          string,
    pay_tradeno       string,
    remark            string,
    beans             double
) stored as textfile location '/pep_cloud/order/ods_order_info_without_partition';


/*
    创建订单明细表的宽表
 */
create external table if not exists ods.ods_order_details_width
(
    order_id          bigint comment '订单表主键Id',
    detail_id         bigint comment '详情表主键Id',
    app_id            bigint comment '字典表外键',
    app_order_id      bigint comment '订单与详情连接键',
    product_id        bigint comment '产品Id',
    product_name      string comment '产品名',
    quantity          int comment '产品数量',
    type              string comment '产品类型',
    code              string comment '教育编码',
    user_id           string comment '用户Id',
    sale_channel_id   bigint comment '销售渠道Id',
    sale_channel_name string comment '销售渠道名称',
    state             string comment '订单状态',
    create_time       string comment '订单生成时间',
    del_time          string comment '订单删除时间',
    start_time        string comment '服务开始时间',
    end_time          string comment '服务结束时间',
    pay_time          string comment '支付时间',
    discount          string comment '折扣',
    beans             double comment '学豆支付',
    material_code     string comment '物料号',
    material_name     string comment '物料名',
    pay_channel       string comment '支付渠道',
    pay_tradeno       string comment '支付编码',
    remark            string comment '评论'
) partitioned by (year string,month string,day string)
    stored as parquet location '/hive/warehouse/ods.db/ods_order_details_width';

