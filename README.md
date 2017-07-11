Datax RedisWriter

1 快速介绍

Rediswriter 插件利用 Redis协议的java客户端Jedis进行redis-cluter/redis/codis的写操作。针对数据更新的需求，通过配置业务主键的方式也可以实现。

2 实现原理

RedisWriter通过Datax框架获取Reader生成的数据，然后将Datax支持的类型通过逐一判断转换成Redis支持的类型。 目前支持上游的odpsreader/mysqlreader 可选择指定的若干列做为key组(无分隔符),可选择指定的若干列座位value值(value值可以为json或者带分隔符的字符串)

3 功能说明

该示例从ODPS读一份数据到Redis。
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "odpsreader", 
                    "parameter": {
                        "accessId": "", 
                        "accessKey": "", 
                        "column": ["col1","col2","col3","col4"], 
                        "odpsServer": "http://service.odps.aliyun.com/api", 
                        "packageAuthorizedProject": "", 
                        "partition": ["dt=yyyyMMdd"], 
                        "project": "", 
                        "splitMode": "record", 
                        "table": ""
                    }
                }, 
                "writer": {
                    "name": "rediswriter", 
                    "parameter": {
					    "clusterMode": "redis",
                        "address": "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006", 
                        "password": "", 
                        "keyColumn": ["col1","col2"], 
						"valueColumn": ["col3","col4"], 
                        "valueMode": "json", 
                        "writeMode": "insert"
                    }
                }
            }
        ], 
       "setting": {
            "speed": { "channel": 1, "byte": 1048576, "record": 1000}
       }
    }
}


4 参数说明

clusterMode: redis集群模式，当前只支持redis-cluster,后续会更新codis/redis单节点版本【必填】
address： cluster模式127.0.0.1:7001，....【必填】
password：redis-cluster模式为空(jsdiscluster暂不支持密码)
keyColumn: reader plugin中那些列作为组合key(目前支持列之间添加拼接符号)，key的顺序必须跟reader中的顺序一致【必填】
valueColumn： reader plugin中那些列作为组合value(目前支持json格式，未来开放带分隔符的字符串模式)，key的顺序必须跟reader中的顺序一致【必填】
valueMode：value组的格式json/delsrt【必填】
writeMode：写模式insert(暂无意义，可以考虑将pipeline模式和普通插入模式进行设置)
prejob/lastjob:(未来考虑开放，在执行job完成前后，对redis插入一条记录)
5 类型转换

redis key 所有类型字段均转换为字符串 redis value 所有类型转换为json/delstr串存入。

DataX 内部类型	Redis 数据类型
Long	int, Long
Double	double
String	string, array
Date	date
Boolean	boolean
Bytes	不支持
6 集成到DataX3.0

(1)访问dataX3.0 github地址，下载源码 https://github.com/alibaba/DataX

(2)修改源码DataX-master目录下的pom.xml，在modules标签中添加 rediswriter
(3)修改源码DataX-master目录下的package.xml，在 fileSets标签中添加 rediswriter/target/datax/ **/. datax

(4)repository下载后的代码重命名为 rediswriter 并copyDataX-master根目录下

(5)参考如下地址进行源码编译 https://github.com/alibaba/DataX/wiki/compile-datax

(6)打包完成后，可以将如下目录直接放置在生产上，进行热插拔部署 。。/datax/plugin/writer/rediswriter cd ../datax/bin python datax.py -r odpsreader -w rediswriter
若能生产模板文件，则拉起任务 python datax.py odpsToRedis.json

7 性能报告
