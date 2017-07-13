package com.alibaba.datax.plugin.writer.rediswriter;

import io.codis.jodis.RoundRobinJedisPool;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConf;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConst;
import com.alibaba.datax.plugin.writer.rediswriter.util.CommonUtil;
import com.alibaba.datax.plugin.writer.rediswriter.util.GsonParser;
import com.alibaba.datax.plugin.writer.rediswriter.util.JedisUtil;
import com.alibaba.datax.plugin.writer.rediswriter.util.JodisUtil;

public class RedisWriterMasterProxy {
    
    private static final Logger LOG = LoggerFactory.getLogger(RedisWriterMasterProxy.class);
   
    private RedisConf conf = new RedisConf();
    
    private JedisCluster jedisCluster;
    
    private RoundRobinJedisPool jodisPool;
    
    private Jedis jedis;
    
    /**
     * init rediswritter job
     * 
     * @param currentConfig 
     * @param peerConfig
     * @param pluginName
     */
    public void init(Configuration curPluginConfig, Configuration peerPluginConfig, String pluginName ) {
        //necessery check
    	CommonUtil.checkNecessaryConfig(curPluginConfig);	
    	CommonUtil.checkPeerPlugin(pluginName);
    		
    	//json check & set config
    	conf.setClusterMode(curPluginConfig.getString(RedisKey.KEY_CLUSTERMODE));
    	conf.setAddress(CommonUtil.checkAddress(curPluginConfig.getString(
    			RedisKey.KEY_CLUSTERMODE), curPluginConfig.getString(RedisKey.KEY_ADDRESS)));
        conf.setPassword(curPluginConfig.getString(RedisKey.KEY_PASSWORD));
    	conf.setZkProxyDir(CommonUtil.checkZkProxy(curPluginConfig.getString(
    			RedisKey.KEY_CLUSTERMODE), curPluginConfig.getString(RedisKey.KEY_ZKPROXYDIR)));
        conf.setRedisKeyColumns(CommonUtil.checkKeyColumns(
        		curPluginConfig.getList(RedisKey.KEY_KEYCOLUMN,String.class), 
        		peerPluginConfig.getList(RedisKey.KEY_COLUMN,String.class)));
        conf.setRedisValueColumns(CommonUtil.checkValueColumns(
        		curPluginConfig.getList(RedisKey.KEY_VALUECOLUMN,String.class), 
        		peerPluginConfig.getList(RedisKey.KEY_COLUMN,String.class)));
        conf.setValueMode(CommonUtil.checkValueMode(
        		curPluginConfig.getString(RedisKey.KEY_VALUEMODE)));
        conf.setPipeBatchSize(CommonUtil.checkPipeBatchsize(
        		curPluginConfig.getString(RedisKey.KEY_PIPELINE_BATCHSIZE)));
    	conf.setKeyType(RedisConst.REDIS_KVTYPE);
    	conf.setValueType(RedisConst.REDIS_KVTYPE);
    	
    	//roundRodin pool config(jedis pool)
    	conf.setMinIdle(1);
    	conf.setMaxIdle(3);
    	conf.setMaxTotal(5);
    	conf.setTimeBetweenEvictionRunsMillis(3000);
    	conf.setMinEvictableIdleTimeMillis(1800000);
    	conf.setSoftMinEvictableIdleTimeMillis(10000);
    	conf.setTestOnBorrow(true);
    	conf.setTestOnReturn(false);
    	conf.setTestWhileIdle(true);
    	
        //inital test connection by cluster_mode 
        if(RedisConst.CLUSTER_REDIS.equals(conf.getClusterMode())){
        	this.jedisCluster =JedisUtil.initJedisClusterClient(conf);
        }
        if(RedisConst.CLUSTER_CODIS.equals(conf.getClusterMode())){
        	this.jodisPool = JodisUtil.initJodisRoundRobinPool(conf);
        	this.jedis = JodisUtil.initJodisCodisClient(jodisPool);
        }  
        
    }
    
    /**
     * Job-Task spliet 
     * 1.send task configurations
     * 2.confirm/overwrite configuration 
     * @param mandatoryNumber
     * @return
     */
    public List<Configuration> split(int mandatoryNumber){
        LOG.info("Begin split and MandatoryNumber : {}", mandatoryNumber);
        List<Configuration> configurations = new ArrayList<Configuration>();
        for (int i = 0; i < mandatoryNumber; i++) {
        	//将封装好的RedisConf添加到全局的configuration    
            Configuration configuration = Configuration.newDefault();
            configuration.set(RedisKey.KEY_REDIS_CONF, GsonParser.confToJson(this.conf));
            configurations.add(configuration);
        }
        LOG.info("End split.");
        assert(mandatoryNumber == configurations.size());
        return configurations;
    }
    
    
    /**
     *  release datax job resources
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
     *  handle datax job pre-job-task
     *  such as data-trans-batch-date
     *
     */
    public void prepare(){
    	//do somthing 
    	
    }
    
    
}
