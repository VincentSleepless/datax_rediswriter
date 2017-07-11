package com.alibaba.datax.plugin.writer.rediswriter.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConf;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConst;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisRecord;
import com.alibaba.datax.plugin.writer.rediswriter.RedisError;
import com.alibaba.datax.plugin.writer.rediswriter.RedisKey;
import com.alibaba.datax.plugin.writer.rediswriter.RedisWriterMasterProxy;
import com.alibaba.fastjson.JSONObject;


public class JedisUtil {

	
    private static final Logger LOG = LoggerFactory.getLogger(RedisWriterMasterProxy.class);
	
	/**
	 * checke necessary config
	 * @param originalConfig 转换后的json配置参数
	 */
    public static void checkNecessaryConfig(Configuration originalConfig) {
    	
    	//字符参数校验(密码选填)
    	originalConfig.getNecessaryValue(RedisKey.KEY_ADDRESS,RedisError.REQUIRED_VALUE);
    	originalConfig.getNecessaryValue(RedisKey.KEY_CLUSTERMODE,RedisError.REQUIRED_VALUE);
        //originalConfig.getNecessaryValue(RedisKey.KEY_PASSWORD,RedisError.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(RedisKey.KEY_VALUEMODE,RedisError.REQUIRED_VALUE);
        //写模式暂不做必须要性校验
        //originalConfig.getNecessaryValue(ReidsKey.KEY_WRITEMODE,RedisError.REQUIRED_VALUE);

        //数据参数校验
        //检查redis key column组
        if (null == originalConfig.getList(RedisKey.KEY_KEYCOLUMN) ||
                originalConfig.getList(RedisKey.KEY_KEYCOLUMN, String.class).isEmpty()) {
            throw DataXException.asDataXException(RedisError.REQUIRED_VALUE, "您未配置写入redis-key列组信息. " +
                    "正确的配置方式是给datax的 keyColumn项配置上redis-key的组合列信息,用英文逗号分隔 例如: "+
            		" \"keyColumn\": [\"col1\",\"col2\"].");
        }
        //检查redis value column组
        if (null == originalConfig.getList(RedisKey.KEY_VALUECOLUMN) ||
                originalConfig.getList(RedisKey.KEY_VALUECOLUMN, String.class).isEmpty()) {
            throw DataXException.asDataXException(RedisError.REQUIRED_VALUE, "您未配置写入redis-value列组信息. " +
                    "正确的配置方式是给datax的 valueColumn项配置上redis-value的组合列信息,用英文逗号分隔 例如: "+
            		" \"ValueColumn\": [\"col1\",\"col2\"].");
        }
    }
	
	/**
	 * check address
	 * @param clusterMode 集群模式 redis/codis (目前默认设置为redis)
	 * @param address 集群地址
	 * @return
	 */
	public static String checkAddress(String clusterMode,String address){
    	//校验redis-cluster模式和地址
    	if(RedisConst.CLUSTER_REDIS.equals(clusterMode)){
    		//去除最后一个分隔符
        	if(String.valueOf(address.charAt(address.length()-1)).equals(RedisConst.DEL_COMMA)){
        		address = address.substring(0,address.length()-1);
        	}
    		
    		//可扩展为多个，需要修改json模板、必要输入校验、jedisconf
    		List<String> hostAddress = Arrays.asList(address.split(RedisConst.DEL_COMMA));
    		//进行redis-cluster集群校验		
    		if(!isHostPortPattern(hostAddress)){
    			 throw DataXException.asDataXException(RedisError.INVALID_ADDRESS,
    					 "您配置的reids-cluster不符合校验规范,正确的address配置模式是:"+
    	                 " \"address\": [\"127.0.0.1:3333\",\"127.0.0.1:3334\"].");
    		}
    		return address;		
    	}
    	//校验redis-cluster模式和地址
    	else if(RedisConst.CLUSTER_CODIS.equals(clusterMode)){
    		
    		//do something to check
    		//。。。。。。
    		//return address;	
    		
	   		 throw DataXException.asDataXException(RedisError.INVALID_CLUSTERMODE,
						 "您配置的reids-cluster-mode不符合校验规范,暂不支持codis");
    
    	}else{
    		 throw DataXException.asDataXException(RedisError.INVALID_CLUSTERMODE,
					 "您配置的reids-cluster-mode不符合校验规范,正确的clusterMode配置模式是:"+
	                 " \"clusterMode\": \"redis/codis\",");
    	}
    }
    
    /**
     * check str is a host:port 
     * @param addressList
     * @return
     */
    private static boolean isHostPortPattern(List<String> addressList) {
        for(Object address : addressList) {
            String regex = "(\\S+):([0-9]+)";
            if(!((String)address).matches(regex)) {
                return false;
            }
        }
        return true;
    }
    
    
    
    /**
     * check peer plugin supportable
     * @param pluginName
     * @return
     */
    public static void checkPeerPlugin(String pluginName){
    	
    	if(RedisConst.PLUGIN_READER_MYSQL.equals(pluginName)||
    			RedisConst.PLUGIN_READER_ODPS.equals(pluginName)){
    		return;
    	}
    	
    	 throw DataXException.asDataXException(RedisError.UNSUPPORT_PEER_PLUGIN, 
    			"redis-writter-plugin不支持您配置的reader-plugin");
    }
       
    
    /**
     * check key columns 
     * 
     * is a subList by peer columns
     * 
     * @param keyColums
     * @param peerPluginConfigColumns
     * @return
     */
	public static List<String> checkKeyColumns(List<String> keyColumns,
    		List<String> peerPluginConfigColumns){
   
    	if(peerPluginConfigColumns.size()==0){
        	throw DataXException.asDataXException(RedisError.COLUMN_VALUE_IS_NULL,
				"redis writer对手方plugin colums字段没有配置");
    	}
        if (1 == peerPluginConfigColumns.size()
                && "*".equals(peerPluginConfigColumns.get(0))) {
            LOG.warn("这是一条警告信息，您配置的 ODPS 读取的列为*，这是不推荐的行为，"
            		+ "因为当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错. 建议您把所有需要抽取的列都配置上. ");
            return keyColumns;
        }
        
    	if(!peerPluginConfigColumns.containsAll(keyColumns)){
    		throw DataXException.asDataXException(RedisError.COLUMN_NOT_CONTAINS,
    				"redis writer对手方plugin colums字段没有配置");
    	}
    
    	return keyColumns;
    }

    
    /**
     * check value columns 
     * 
     * peer plugin do sechma column check job
     * is a subList by peer columns
     * 
     * @param keyColums
     * @param peerPluginConfigColumns
     * @return
     */
    public static List<String> checkValueColumns(List<String> valueColumns,
    		List<String> peerPluginConfigColumns){
   
    	if(peerPluginConfigColumns.size()==0){
        	throw DataXException.asDataXException(RedisError.INVALID_CLUSTERMODE,
				"redis writer对手方plugin colums字段没有配置");
    	}
        if (1 == peerPluginConfigColumns.size()
                && "*".equals(peerPluginConfigColumns.get(0))) {
            LOG.warn("这是一条警告信息，您配置的 ODPS 读取的列为*，这是不推荐的行为，"
            		+ "因为当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错. 建议您把所有需要抽取的列都配置上. ");
            return valueColumns;
        }
        
    	if(!peerPluginConfigColumns.containsAll(valueColumns)){
    		throw DataXException.asDataXException(RedisError.INVALID_CLUSTERMODE,
    				"redis writer对手方plugin colums字段没有配置");
    	}
    
    	return valueColumns;
    }
    

    
    /**
     *  check value-mode json/del-str
     * @param valueMode
     * @return
     */
    public static String checkValueMode(String valueMode){
    	
    	//if(!valueMode.equals(RedisConst.)||!valueMode.equals(anObject))
    	//目前只支持json
    	if(!valueMode.equals(RedisConst.VALUEMODE_JSON)){
    		throw DataXException.asDataXException(RedisError.INVALID_VALUEMODE,
					 "您配置的reids-value-mode不符合校验规范,正确的valueMode配置模式是:"+
	                 " \"valueMode\": \"json\",");
    	}    	
    	
    	return valueMode;
    	
    }
    
    
    /**
     *  check write-mode   pipeline
     * @param writeMode
     * @return
     */
	public static String checkWriteMode(String writeMode) {

		if (writeMode.equals(RedisConst.WRITEMODE_PIPELINE)) {
			return writeMode;
		}else {
			throw DataXException.asDataXException(RedisError.INVALID_ADDRESS,
					"您配置的reids-write-mode不符合校验规范,正确的writeMode配置模式是:"
							+ " \"writeMode\": \"pipeline\",");
		}
	}
    
	/**
	 * 校验pipeline write batch -size
	 * @param pipeBatchsize
	 * @return
	 */
	public static int checkPipeBatchsize(String pipeBatchsize) {

		try {
			if (StringUtils.isEmpty(pipeBatchsize)) {
				return RedisConst.PIPELINE_BATCHSIZE;
			}
			int batchSize = Integer.valueOf(pipeBatchsize);
			return batchSize;
		} catch (Exception e) {
			// TODO: handle exception
			throw DataXException.asDataXException(RedisError.INVALID_ADDRESS,
					"您配置的reids-write-mode不符合校验规范,正确的writeMode配置模式是:"
							+ " \"writeMode\": \"pipeline\",");
		}
	}
	
	
	
	/**
	 * inital redis connection by  jedisCluster
	 * @param conf redis param object
	 * @return
	 */
	public static JedisCluster initJedisClusterClient(RedisConf conf) {		
		try {
			// 获取配置参数
			List<String> addressList = Arrays.asList(conf.getAddress().split(
					RedisConst.DEL_COMMA));
			// 设置集群地址
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
			}else {	
				//cluster = new JedisCluster(nodes, 15000, 1000, 1000, "123456",null);
				cluster = new JedisCluster(nodes);
			}
			return cluster;
			
		} catch (NumberFormatException e) {
			throw DataXException.asDataXException(RedisError.ILLEGAL_ADDRESS,
					"不合法redis地址", e);
		} catch (JedisConnectionException e) {
			throw DataXException.asDataXException(
					RedisError.JEDIS_CONNECT_TIMEOUT, "jesdis集群连接超时", e);
		} catch (Exception e) {
			throw DataXException.asDataXException(RedisError.JEDIS_UNKOWN,
					"jedis未知异常", e);
		}
	}
	
	
	/**
	 * inital redis connection by single-redis/codis
	 * @param conf
	 * @return
	 */
	public static Jedis initJedisClient(RedisConf conf) {	
		return null ;
	}
	
	

	
	/**
	 * close jedis cluster
	 * @param jedisCluster
	 */
	public static void clusterClose(JedisCluster jedisCluster) {
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
				String redKey = getRedisKey(data.get(i),keysColumns);
				String redValue = getRedisValue(data.get(i),
						valueMode,keysColumns,valueColumns);
     			jcp.set(redKey, redValue);
			}
			jcp.sync();
		} finally{
			jcp.close();
		}	
	}
	

	/**
	 *  pipline batch write by single-redis/codis mode
	 * @param cluster
	 * @param conf
	 * @param data
	 */
	public static void doJedisBatchWrite(Jedis cluster,RedisConf conf,List<Record> data){
		
		//do something
		

		
		
	}
	
	
	/**
	 * get redis key by columns name
	 * 
	 * @param data
	 *            single record
	 * @param keysColumn
	 *            columns name
	 * @return
	 */
	private static String getRedisKey(Record record, List<String> keyColumns) {

		String redisKey = "";
		int redKeyCount = keyColumns.size();
		for (int i = 0; i < redKeyCount; i++) {
			Column col = record.getColumn(i);
				
			if (col.getRawData() == null) {
				//LOG.error("COLUMN TYPE :" +col.getType()+" COLUMN INFO :" +col.getByteSize());
				//throw new IllegalArgumentException(String.format(RedisError.COLUMN_VALUE_IS_NULL
				//       .toString(),col.asString(),col.getType().toString()));
			}
			
			redisKey = redisKey+ColumnConversion.columnConvertStr(col);
		}

		return redisKey;
	}
	
	
	/**
	 * get redis value by columns name
	 * @param record           single reord
	 * @param valueType        json/del-str
	 * @param keyColumns       key columns name
	 * @param valueColumns     value columns name
	 * @return
	 */
	private static String getRedisValue(Record record, String valueType, 
			List<String> keyColumns,List<String> valueColumns){
		
		String redisValue  = "";
		//only json type supported
		if(RedisConst.VALUEMODE_JSON.equals(valueType)){
			redisValue = getJsonValue(record,keyColumns,valueColumns);
		}else{
			redisValue = getJsonValue(record,keyColumns,valueColumns);
		}
		return redisValue;	
	}
	

	/**
	 * get json type value str
	 * @param record
	 * @param keyColumns
	 * @param valueColumns
	 * @return
	 */
	private static String getJsonValue(Record record,List<String> keyColumns,List<String> valueColumns){
		// caculate column index
		int redKeyCount = keyColumns.size();
		int redValueCount = valueColumns.size();
		// create json
		JSONObject json = new JSONObject();

		for (int i = 0; i < redValueCount; i++) {
			Column col = record.getColumn(redKeyCount + i);
			
			//empty data ??? 
			if (col.getRawData() == null) {
				//LOG.error("COLUMN TYPE :" +col.getType()+" COLUMN INFO :" +col.getByteSize());
				//throw new IllegalArgumentException(String.format(RedisError.COLUMN_VALUE_IS_NULL
				//      .toString(),col.asString(),col.getType().toString()));
			}

			json = ColumnConversion.columnToReidsValue(json,valueColumns.get(i),col);
		}
		String redisValue = json.toString();
		return redisValue;
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
