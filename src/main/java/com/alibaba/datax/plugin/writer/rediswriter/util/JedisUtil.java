package com.alibaba.datax.plugin.writer.rediswriter.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConf;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConst;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisRecord;
import com.alibaba.datax.plugin.writer.rediswriter.RedisError;
import com.alibaba.fastjson.JSONObject;

public class JedisUtil {

	private static final Logger LOG = LoggerFactory.getLogger(JedisUtil.class);
	
	/**
	 * inital redis connection by  jedisCluster
	 * @param conf redis param object
	 * @return
	 */
	public static JedisCluster initJedisClusterClient(RedisConf conf) {		
		try {
			// get config
			List<String> addressList = Arrays.asList(conf.getAddress().split(
					RedisConst.DEL_COMMA));
			// set cluster hosts
			HashSet<HostAndPort> nodes = new HashSet<HostAndPort>();
			for (int i = 0; i < addressList.size(); i++) {
				String[] hostAndPort = addressList.get(i).split(
						RedisConst.DEL_COLON);
				//LOG.info(hostAndPort[0]+"--"+hostAndPort[1]);
				nodes.add(new HostAndPort(hostAndPort[0], Integer.valueOf(hostAndPort[1])));
			}

			JedisCluster cluster = null;
			if(StringUtils.isEmpty(conf.getPassword())){
				cluster = new JedisCluster(nodes);
			}
			//cluster auth mode init(jedis unsupported)
			else {	
				//cluster = new JedisCluster(nodes, 15000, 1000, 1000, "123456",null);
				cluster = new JedisCluster(nodes);
			}
			return cluster;
			
		} catch (NumberFormatException e) {
			throw DataXException.asDataXException(RedisError.ILLEGAL_ADDRESS,
					"illegle redis address", e);
		} catch (JedisConnectionException e) {
			throw DataXException.asDataXException(
					RedisError.JEDIS_CONNECT_TIMEOUT, "jesdis connect time out", e);
		} catch (Exception e) {
			throw DataXException.asDataXException(RedisError.JEDIS_UNKOWN,
					"jedis unknown exception", e);
		}
	}
	
	
	
	/**
	 * close jedis cluster
	 * @param jedisCluster
	 */
	public static void close(JedisCluster jedisCluster) {
		try {
			if (jedisCluster != null) {
				jedisCluster.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	

	/**
	 * pipline batch write by redis-cluster mode
	 * @param cluster  jedis cluster
	 * @param conf  param object
	 * @param data  
	 */
	public static void doJedisClusterBatchWrite(JedisCluster cluster,RedisConf conf,List<Record> data){
	
		//get write param
		List<String> keysColumns = conf.getRedisKeyColumns();
		List<String> valueColumns = conf.getRedisValueColumns();
		String valueMode = conf.getValueMode();
		
		//create jedis cluster pipline
		JedisClusterPipeline jcp = JedisClusterPipeline.pipelined(cluster);
		jcp.refreshCluster();
		
		try {
			for (int i = 0; i < data.size(); i++) {
				
				//LOG.info("LOG ALL REORD INFO:"+data.get(i).toString());
				String redKey = CommonUtil.getRedisKey(data.get(i),keysColumns);
				String redValue = CommonUtil.getRedisValue(data.get(i),
						valueMode,keysColumns,valueColumns);
     			jcp.set(redKey, redValue);
			}
			jcp.sync();
		}catch(Exception e){
			
			throw DataXException.asDataXException(RedisError.JEDIS_CLUSTER_PIPESYNC_FAIL,
					"jedis unknown exception", e);
			
		}finally{
			jcp.close();
		}	
	}
	
	/**
	 * pipline batch write by jedis cluster mode demo
	 * @param cluster
	 */
	public static void doJedisClusterBatchWriteDemo(JedisCluster cluster,List<RedisRecord> data){
		
		JedisClusterPipeline jcp = JedisClusterPipeline.pipelined(cluster);
		jcp.refreshCluster();
		try {
			for (int i = 0; i < data.size(); i++) {
				String reKey = data.get(i).getReKey();
				String reVlue = data.get(i).getReValue();
				jcp.set(reKey, reVlue);
			}
			jcp.sync();
		} finally{
			jcp.close();
		}	
	}

	
	
}
