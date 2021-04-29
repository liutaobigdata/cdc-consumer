##  kafka 消费来自 postgres cdc 的数据

##  配置文件参数说明 
- target_jdbc_url : 目标数据库jdbc url 
- target_user:  目标数据库有写权限的用户
- target_password:目标数据库密码
- kafka_hosts: kafka broker地址 
- kafka_topic: topic 名称
- kafka_consumer_group: 消费者组name 
- kafka_consumer_num:消费者数量 推荐和kafka partition 分区数保持一致
- kafka_ssl_location: 公网环境下连接kafka需要使用ssl 
- environment: 部署环境 生产环境为 prod 
- dingTalk_token:预警钉钉token 
- check_connection_rate: 检查目标数据库连接是否可用的周期
- target_db_type: 目标数据库类型 0.1版本仅支持mysql和postgres 取值为 mysql || postgres