package com.alibaba.datax.plugin.writer.rediswriter.util;

import java.io.IOException;
import java.util.List;

import io.codis.jodis.RoundRobinJedisPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConf;
import com.alibaba.datax.plugin.writer.rediswriter.RedisError;

public class JodisUtil {

	private static final Logger LOG = LoggerFactory.getLogger(JodisUtil.class);
	
	/**
	 * inital redis connection by  jodis codis client
	 * @param conf redis param object
	 * @return
	 */
	public static RoundRobinJedisPool initJodisRoundRobinPool(RedisConf conf) {
		
		//pool config
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMinIdle(conf.getMinIdle());
		poolConfig.setMaxIdle(conf.getMaxIdle());
		poolConfig.setMaxTotal(conf.getMaxTotal());
		poolConfig.setMaxWaitMillis(conf.getMaxWaitMillis());
		poolConfig.setMinEvictableIdleTimeMillis(conf.getMinEvictableIdleTimeMillis());
		poolConfig.setSoftMinEvictableIdleTimeMillis(conf.getSoftMinEvictableIdleTimeMillis());
		poolConfig.setTimeBetweenEvictionRunsMillis(conf.getTimeBetweenEvictionRunsMillis());
		poolConfig.setTestOnBorrow(conf.isTestOnBorrow());
		poolConfig.setTestOnReturn(conf.isTestOnReturn());
		poolConfig.setTestWhileIdle(conf.isTestWhileIdle());

		String zkAddr =conf.getAddress();
		String zkProxyDir = conf.getZkProxyDir();
        int zkSessionTimeoutMs = 3000;
	        
        try {
        	RoundRobinJedisPool pool = RoundRobinJedisPool.create().poolConfig(poolConfig).
    				curatorClient(zkAddr, zkSessionTimeoutMs).zkProxyDir(zkProxyDir).build();
        	return pool;	
		} catch (Exception e) {
			// TODO: handle exception
			throw DataXException.asDataXException(RedisError.JODIS_POOLJEDISGET_FAIL,
					"jodis roundrobin pool inital failed", e);
		}

	}
	
	
	/**
	 *  get jedis intance from jodis roundrobin pool
	 * @param pool
	 * @return
	 */
	public static Jedis initJodisCodisClient(RoundRobinJedisPool pool){
	
		try {
			Jedis jedis = pool.getResource();	
			return jedis;
		} catch (Exception e) {
			// TODO: handle exception
			throw DataXException.asDataXException(RedisError.JODIS_POOLJEDISGET_FAIL,
					"get jedis instance from  jodis roundrobin pool failed", e);
		}	
	}
	
	
	
	/**
	 * inital redis connection by  jodis instance
	 * @param conf redis param object
	 * @return
	 */
	public static void closeJedis(Jedis jedis) {
		
		if(jedis!=null){
			jedis.close();
		}
		
	}
	

	/**
	 * inital redis connection by  jodis roundRollbin pool
	 * @param conf redis param object
	 * @return
	 */
	public static void closeJodisPool(RoundRobinJedisPool jodisPool) {
		
		if(jodisPool!=null){
			jodisPool.close();
		}	
	}
	
	/**
	 * 
	 * @param jedis
	 * @param conf
	 * @param writerBuffer
	 */
	public static void doJodisCodisBatchWrite(Jedis jedis,RedisConf conf,List<Record> data){
		
		//get write param
		List<String> keysColumns = conf.getRedisKeyColumns();
		List<String> valueColumns = conf.getRedisValueColumns();
		String valueMode = conf.getValueMode();
		
		//pipeline
		Pipeline pipeline = jedis.pipelined(); 
		
		try {
			for (int i = 0; i < data.size(); i++) {
				//LOG.info("LOG ALL REORD INFO:"+data.get(i).toString());
				String redKey = CommonUtil.getRedisKey(data.get(i),keysColumns);
				String redValue = CommonUtil.getRedisValue(data.get(i),
						valueMode,keysColumns,valueColumns);
				pipeline.set(redKey, redValue);
			}
			pipeline.sync();
			
		} catch (Exception e) {
			// TODO: handle exception
			throw DataXException.asDataXException(RedisError.JEDIS_PIPESYNC_FAIL,
					"jedis unknown exception", e);
		}finally{
			try {
				pipeline.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
}
