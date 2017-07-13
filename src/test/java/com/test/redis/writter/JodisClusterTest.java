package com.test.redis.writter;

import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConf;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import io.codis.jodis.RoundRobinJedisPool;

public class JodisClusterTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String zkAddress ="10.139.36.118:2181";
		//String address ="10.139.36.118:2181,.....";
		
		String zkProxy="/jodis/codis-demo";
		
		
	    RedisConf conf = new RedisConf();
	    
	    conf.setAddress(zkAddress);
	    conf.setZkProxyDir(zkProxy);
		
		JedisPoolConfig pool = new JedisPoolConfig();
		//最小空闲连接数
		pool.setMinIdle(conf.getMinIdle());
		//最大空闲连接数
		pool.setMaxIdle(conf.getMaxIdle());
		//最大连接数
		pool.setMaxTotal(conf.getMaxTotal());
		pool.setTestOnBorrow(conf.isTestOnBorrow());
		pool.setTestOnReturn(conf.isTestOnReturn());
		pool.setTestWhileIdle(conf.isTestWhileIdle());
		pool.setMaxWaitMillis(conf.getMaxWaitMillis());
		pool.setMinEvictableIdleTimeMillis(conf.getMinEvictableIdleTimeMillis());
		pool.setSoftMinEvictableIdleTimeMillis(conf.getSoftMinEvictableIdleTimeMillis());
		pool.setTimeBetweenEvictionRunsMillis(1000);

		
		RoundRobinJedisPool rrPool = RoundRobinJedisPool.create().poolConfig(pool).
				curatorClient(zkAddress, 30000).zkProxyDir(zkProxy).build();			
		
		Jedis jedis = rrPool.getResource();

		System.out.println(jedis.set("zengli", "zengli"));
		
		System.out.println(jedis.get("zengli"));
		
		
		Pipeline pipe = jedis.pipelined();
		for (int i = 0; i < 1000; i++) {
			pipe.set("zengli"+i, "zengli"+i);
		}
		pipe.sync();
		
		System.out.println(jedis.get("zengli999"));
		
		
		jedis.close();
		
		rrPool.close();
		
	}

}
