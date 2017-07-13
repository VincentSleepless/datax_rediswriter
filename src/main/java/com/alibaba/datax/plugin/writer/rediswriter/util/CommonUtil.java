package com.alibaba.datax.plugin.writer.rediswriter.util;


import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConst;
import com.alibaba.datax.plugin.writer.rediswriter.RedisError;
import com.alibaba.datax.plugin.writer.rediswriter.RedisKey;
import com.alibaba.fastjson.JSONObject;

public class CommonUtil {

	
    private static final Logger LOG = LoggerFactory.getLogger(CommonUtil.class);
	
	/**
	 * checke necessary config
	 * @param originalConfig 转换后的json配置参数
	 */
    public static void checkNecessaryConfig(Configuration originalConfig) {
    	
    	//check string config below
    	originalConfig.getNecessaryValue(RedisKey.KEY_ADDRESS,RedisError.REQUIRED_VALUE);
    	originalConfig.getNecessaryValue(RedisKey.KEY_CLUSTERMODE,RedisError.REQUIRED_VALUE);
    	//if clusterMode = codis check zkproxy
    	if(RedisConst.CLUSTER_CODIS.equals(originalConfig.get(RedisKey.KEY_CLUSTERMODE))){
    		originalConfig.getNecessaryValue(RedisKey.KEY_ZKPROXYDIR,RedisError.REQUIRED_VALUE);
    	}
        //originalConfig.getNecessaryValue(RedisKey.KEY_PASSWORD,RedisError.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(RedisKey.KEY_VALUEMODE,RedisError.REQUIRED_VALUE);
        
      
        //check arry config below
        if (null == originalConfig.getList(RedisKey.KEY_KEYCOLUMN) ||
                originalConfig.getList(RedisKey.KEY_KEYCOLUMN, String.class).isEmpty()) {
            throw DataXException.asDataXException(RedisError.REQUIRED_VALUE, "您未配置写入redis-key列组信息. " +
                    "正确的配置方式是给datax的 keyColumn项配置上redis-key的组合列信息,用英文逗号分隔 例如: "+
            		" \"keyColumn\": [\"col1\",\"col2\"].");
        }
        if (null == originalConfig.getList(RedisKey.KEY_VALUECOLUMN) ||
                originalConfig.getList(RedisKey.KEY_VALUECOLUMN, String.class).isEmpty()) {
            throw DataXException.asDataXException(RedisError.REQUIRED_VALUE, "您未配置写入redis-value列组信息. " +
                    "正确的配置方式是给datax的 valueColumn项配置上redis-value的组合列信息,用英文逗号分隔 例如: "+
            		" \"ValueColumn\": [\"col1\",\"col2\"].");
        }
    }
	
	/**
	 * check address
	 * @param clusterMode  redis/codis
	 * @param address      cluster-addresses
	 * @return
	 */
	public static String checkAddress(String clusterMode,String address){
    	//check address & cluster mode
    	if(RedisConst.CLUSTER_REDIS.equals(clusterMode)||
    			RedisConst.CLUSTER_CODIS.equals(clusterMode)){
    		//delete last del-prex (if last char is ",")
        	if(String.valueOf(address.charAt(address.length()-1)).equals(RedisConst.DEL_COMMA)){
        		address = address.substring(0,address.length()-1);
        	}
    		
    		List<String> hostAddress = Arrays.asList(address.split(RedisConst.DEL_COMMA));
    		//check host address	
    		if(!isHostPortPattern(hostAddress)){
    			 throw DataXException.asDataXException(RedisError.INVALID_ADDRESS,
    					 "您配置的address不符合校验规范,正确的address配置应该为:"+
    	                 " \"address\": [\"127.0.0.1:3333\",\"127.0.0.1:3334\"].");
    		}
    		return address;		
    	}else{
    		 throw DataXException.asDataXException(RedisError.INVALID_CLUSTERMODE,
					 "您配置的reids-cluster-mode不符合校验规范,正确的clusterMode配置应该为:"+
	                 " \"clusterMode\": \"redis/codis\",");
    	}
    }
    

	/**
	 * check zkProxy is legal in cluster mode
	 * 
	 * @param zkProxy
	 * @return 
	 */
	public static String checkZkProxy(String clusterMode,String zkProxy){
				
		if(RedisConst.CLUSTER_REDIS.equals(clusterMode)){
			return "";
		}
		
		if(RedisConst.CLUSTER_CODIS.equals(clusterMode) ){
			
			if(!isValidZkProxy(zkProxy)||StringUtils.isEmpty(zkProxy)){
				throw DataXException.asDataXException(RedisError.INVALID_ZKPROXY,
						 "您配置的zkProxy不符合校验规范,在codis-cluster模式下zkProxy配置应该为:"+
		                 " \"zkProxy\": \"/jodis/codis-demo\",");
			}
		}	
		
		return zkProxy;	
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
     *  check str is a valid zkPorxy
     *  0-9 a-z A-Z _ - / 
     *  //is not allowed
     * @param zkProxy
     * @return
     */
    private static boolean isValidZkProxy(String zkProxy){
    	
    	String regex = "^((?![/?]{2,})[a-zA-Z0-9-_/])*$";   
    	System.out.println(zkProxy);
    	if(!zkProxy.matches(regex)){
    		return false;
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
        	throw DataXException.asDataXException(RedisError.COLUMN_NOT_CONTAINS,
				"redis writer对手方plugin colums字段没有配置");
    	}
        if (1 == peerPluginConfigColumns.size()
                && "*".equals(peerPluginConfigColumns.get(0))) {
            LOG.warn("这是一条警告信息，您配置的 ODPS 读取的列为*，这是不推荐的行为，"
            		+ "因为当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错. 建议您把所有需要抽取的列都配置上. ");
            return valueColumns;
        }
        
    	if(!peerPluginConfigColumns.containsAll(valueColumns)){
    		throw DataXException.asDataXException(RedisError.COLUMN_NOT_CONTAINS,
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
					 "您配置的reids-value-mode不符合校验规范,正确的valueMode配置应该为:"+
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
					"您配置的reids-write-mode不符合校验规范,正确的writeMode配置应该为:"
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
			throw DataXException.asDataXException(RedisError.ILLEGAL_PIPE_BATCHSIZE,
					"您配置的pipeBatchsize不符合校验规范,正确的pipeBatchsize配置应该为:"
							+ " \"pipeBatchsize\": \"20\",");
		}
	}
	
	
	/**
	 * get redis key by columns name
	 * 
	 * @param data        single record
	 * @param keysColumn  columns name
	 * @return
	 */
	public static String getRedisKey(Record record, List<String> keyColumns) {

		String redisKey = "";
		int redKeyCount = keyColumns.size();
		for (int i = 0; i < redKeyCount; i++) {
			Column col = record.getColumn(i);
			
			//empty data handle
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
	public static String getRedisValue(Record record, String valueType, 
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
	private  static String getJsonValue(Record record,List<String> keyColumns,List<String> valueColumns){
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
	
	
	
}
