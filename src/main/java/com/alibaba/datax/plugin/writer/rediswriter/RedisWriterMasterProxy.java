package com.alibaba.datax.plugin.writer.rediswriter;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCluster;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConf;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConst;
import com.alibaba.datax.plugin.writer.rediswriter.util.GsonParser;
import com.alibaba.datax.plugin.writer.rediswriter.util.JedisUtil;

public class RedisWriterMasterProxy {
    
    private static final Logger LOG = LoggerFactory.getLogger(RedisWriterMasterProxy.class);
   
    private RedisConf conf = new RedisConf();
    
    private JedisCluster jedCluster;
    
    /**
     * init rediswritter job
     * 
     * @param currentConfig 
     * @param peerConfig
     * @param pluginName
     */
    public void init(Configuration curPluginConfig, Configuration peerPluginConfig, String pluginName ) {
        //necessery check
    	JedisUtil.checkNecessaryConfig(curPluginConfig);	
    	JedisUtil.checkPeerPlugin(pluginName);
    		
    	//json check & set config
    	conf.setClusterMode(curPluginConfig.getString(RedisKey.KEY_CLUSTERMODE));
    	conf.setAddress(JedisUtil.checkAddress(curPluginConfig.getString(
    			RedisKey.KEY_CLUSTERMODE), curPluginConfig.getString(RedisKey.KEY_ADDRESS)));
        conf.setPassword(curPluginConfig.getString(RedisKey.KEY_PASSWORD));
        
        conf.setRedisKeyColumns(JedisUtil.checkKeyColumns(
        		curPluginConfig.getList(RedisKey.KEY_KEYCOLUMN,String.class), 
        		peerPluginConfig.getList(RedisKey.KEY_COLUMN,String.class)));
        
        conf.setRedisValueColumns(JedisUtil.checkValueColumns(
        		curPluginConfig.getList(RedisKey.KEY_VALUECOLUMN,String.class), 
        		peerPluginConfig.getList(RedisKey.KEY_COLUMN,String.class)));

        conf.setValueMode(JedisUtil.checkValueMode(curPluginConfig.getString(RedisKey.KEY_VALUEMODE)));
        conf.setWriteMode(JedisUtil.checkWriteMode(curPluginConfig.getString(RedisKey.KEY_WRITEMODE)));
        
        //job默认参数设置
        conf.setClusterMode(RedisConst.CLUSTER_REDIS);
    	conf.setKeyType(RedisConst.REDIS_KVTYPE);
    	conf.setValueType(RedisConst.REDIS_KVTYPE);
    	
    	//jedis连接池默认参数(可以将下列参数做成限制项,并做限制项校验)
    	conf.setJedisMaxActive(3); 
    	conf.setJedisMaxIdle(1);   
    	conf.setJedisMinIdle(1); 	
    	conf.setJedisMaxWait(3000); 
    	conf.setJedisTimeBetweenEvictionRunsMillis(3000);
    	conf.setJedisMinEvictableIdleTimeMillis(1800000);
    	conf.setJedisSoftMinEvictableIdleTimeMillis(10000); 	
    	conf.setJedisTestOnBorrow(true);
        conf.setJedisTestOnReturn(true);
        conf.setJedisTestWhileIdle(true);
    	
        //启动jedis(连接测试地址是否争正确)
    	this.jedCluster =JedisUtil.initJedisClusterClient(conf);
          
    }
    
    /**
     * Job-Task切分 
     * 1.为task分发configuration(可覆盖)
     * 2.确认/覆写task数量
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
     *  释放job级别资源
     */
    public void close() {
    	//关闭测试连接
    	JedisUtil.clusterClose(this.jedCluster);
    }
    
    
    /**
     *  进行job级别前置任务
     *  可以进行前置任务，比如查询批量同步的redis key
     */
    public void prepare(){
    	//do somthing 
    	
    }
    
    
}
