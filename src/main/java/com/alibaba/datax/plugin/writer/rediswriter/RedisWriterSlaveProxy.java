package com.alibaba.datax.plugin.writer.rediswriter;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCluster;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConf;
import com.alibaba.datax.plugin.writer.rediswriter.util.GsonParser;
import com.alibaba.datax.plugin.writer.rediswriter.util.JedisUtil;

public class RedisWriterSlaveProxy {

	private static final Logger LOG = LoggerFactory.getLogger(RedisWriterSlaveProxy.class);

	private RedisConf conf;
	private JedisCluster cluster;

	private int batchSize = 0;

	private static int BATCH_SIZE = 1000;

	/**
	 * init task job resources
	 * 
	 * @param config
	 */
	public void init(Configuration config) {
        LOG.info("Initaling redis write task!");
		conf = GsonParser.jsonToConf(config.getString(RedisKey.KEY_REDIS_CONF));
		cluster = JedisUtil.initJedisClusterClient(conf);
		this.batchSize = BATCH_SIZE;
	}

	/**
	 * close task job resources
	 */
	public void close() {
		// 关闭连接
		JedisUtil.clusterClose(this.cluster);
	}

	/**
	 * write data
	 * @param recordReceiver
	 * @param collector
	 */
	public void write(RecordReceiver lineReceiver,TaskPluginCollector collector) {
		// check param
		if (conf == null || cluster == null || batchSize == 0) {
			throw DataXException.asDataXException(
					RedisError.ILLEGAL_WRITEVPRARM, "redis write task 参数校验失败");
		}
		
		List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
		Record record = null;
				
		//handle when data transfers 
		while((record = lineReceiver.getFromReader()) != null){
			writerBuffer.add(record);
			if(writerBuffer.size() >= this.batchSize){
				//add a router redis-cluster/redis/
				JedisUtil.doJedisClusterBatchWrite(cluster,conf, writerBuffer);
				writerBuffer.clear();
			}
		}
		//handle when data stops transfer 
		if(!writerBuffer.isEmpty()) {
			JedisUtil.doJedisClusterBatchWrite(cluster,conf,writerBuffer);
            writerBuffer.clear();
        }
	}

}
