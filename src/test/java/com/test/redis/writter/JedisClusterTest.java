package com.test.redis.writter;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.JedisCluster;

import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConf;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisRecord;
import com.alibaba.datax.plugin.writer.rediswriter.util.GsonParser;
import com.alibaba.datax.plugin.writer.rediswriter.util.JedisUtil;

public class JedisClusterTest {

    public static void main(String[] args) {
    	
    	RedisConf conf = new RedisConf();
    	conf.setAddress("10.139.36.118:7001,10.139.36.118:7002,10.139.36.118:7003,"
    			+ "10.139.36.118:7004,10.139.36.118:7005,10.139.36.118:7006"); 	
    	System.out.println(GsonParser.confToJson(conf));
    	
    	JedisCluster cluster = JedisUtil.initJedisClusterClient(conf);
    	System.out.println(cluster.get("zengli"));
    	
    	
    	List<RedisRecord> data = new ArrayList<RedisRecord>();
    	
    	for (int i = 0; i < 1000; i++) {
    		RedisRecord record = new RedisRecord();
    		record.setReKey("zengli"+i);
    		record.setReValue("zengli"+i);
    		data.add(record);
		}
    	JedisUtil.doJedisClusterBatchWriteDemo(cluster, data);
    	
    }
	

	
	
	
	
}
