package com.alibaba.datax.plugin.writer.rediswriter;

import io.codis.jodis.RoundRobinJedisPool;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConf;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConst;
import com.alibaba.datax.plugin.writer.rediswriter.util.GsonParser;
import com.alibaba.datax.plugin.writer.rediswriter.util.JedisUtil;
import com.alibaba.datax.plugin.writer.rediswriter.util.JodisUtil;

public class RedisWriterSlaveProxy {

	private static final Logger LOG = LoggerFactory.getLogger(RedisWriterSlaveProxy.class);

	private RedisConf conf;
	
	private JedisCluster jedisCluster;
	
    private RoundRobinJedisPool jodisPool;
	
	private Jedis jedis;
	
	private int batchSize = 0;


	/**
	 * init task job resources
	 * 
	 * @param config
	 */
	public void init(Configuration config) {
        LOG.info("Initaling redis write task!");
		conf = GsonParser.jsonToConf(config.getString(RedisKey.KEY_REDIS_CONF));
		
		  //inital test connection by cluster_mode 
        if(RedisConst.CLUSTER_REDIS.equals(conf.getClusterMode())){
        	this.jedisCluster =JedisUtil.initJedisClusterClient(conf);
        }
        if(RedisConst.CLUSTER_CODIS.equals(conf.getClusterMode())){
        	this.jodisPool = JodisUtil.initJodisRoundRobinPool(conf);
        	this.jedis = JodisUtil.initJodisCodisClient(jodisPool);
        }  
        
		this.batchSize = conf.getPipeBatchSize();
		if(this.batchSize==0){
			this.batchSize = RedisConst.PIPELINE_BATCHSIZE;
		}
	}

	/**
	 * close task job resources
	 */
	public void close() {
		
    	//close connection by cluster_mode 
        if(RedisConst.CLUSTER_REDIS.equals(conf.getClusterMode())){
        	JedisUtil.close(this.jedisCluster);
        }
        if(RedisConst.CLUSTER_CODIS.equals(conf.getClusterMode())){
        	JodisUtil.closeJedis(jedis);
        	JodisUtil.closeJodisPool(jodisPool);
        }  
	}

	/**
	 * write data
	 * @param recordReceiver
	 * @param collector
	 */
	public void write(RecordReceiver lineReceiver,TaskPluginCollector collector) {
		// check param
		 if(RedisConst.CLUSTER_REDIS.equals(conf.getClusterMode())){
			if (conf == null || jedisCluster == null || batchSize == 0) {
				throw DataXException.asDataXException(
						RedisError.ILLEGAL_WRITEVPRARM, "redis write task 参数校验失败");
			}
	     }
	     if(RedisConst.CLUSTER_CODIS.equals(conf.getClusterMode())){
				if (conf == null || jodisPool == null ||jedis==null ||batchSize == 0) {
					throw DataXException.asDataXException(
							RedisError.ILLEGAL_WRITEVPRARM, "redis write task 参数校验失败");
				}
	     }  
		
		List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
		Record record = null;
				
		//handle when data transfers 
		while((record = lineReceiver.getFromReader()) != null){
			writerBuffer.add(record);
			if(writerBuffer.size() >= this.batchSize){
								
				if (RedisConst.CLUSTER_REDIS.equals(conf.getClusterMode())){
					JedisUtil.doJedisClusterBatchWrite(jedisCluster,conf, writerBuffer);
				}
				if (RedisConst.CLUSTER_CODIS.equals(conf.getClusterMode())) {
					JodisUtil.doJodisCodisBatchWrite(jedis, conf, writerBuffer);
				}

				writerBuffer.clear();
			}
		}
		//handle when data stops transfer 
		if(!writerBuffer.isEmpty()) {
			
			if (RedisConst.CLUSTER_REDIS.equals(conf.getClusterMode())){
				JedisUtil.doJedisClusterBatchWrite(jedisCluster,conf, writerBuffer);
			}
			if (RedisConst.CLUSTER_CODIS.equals(conf.getClusterMode())) {
				JodisUtil.doJodisCodisBatchWrite(jedis, conf, writerBuffer);
			}
            writerBuffer.clear();
        }
	}

}
