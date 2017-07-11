package com.alibaba.datax.plugin.writer.rediswrite.model;

import java.util.List;

public class RedisConf {
	
	//集群模式(暂不开放配置) redis-cluster/codis-cluster
	private String clusterMode;
	
	//redis cluster 给出负载之后的地址  host:port
	//codis cluster 给出ha-proxy-url
	private String address;
	
	private String password;
	
	//reids key类型(默认只支持String)
	private String keyType;
	//redis value类型(默认只支持String)
	private String valueType;
	//redis value模式(分隔符字符串/json字符串) (del-str/json)
	private String valueMode;
	//redis 写模式(insert/upsert)
	private String writeMode;
	//redis pipiline模式
	private int pipeBatchSize;
	
	//redis key/column由那些上游传入的record的那些列组成
	private List<String> redisKeyColumns;
	private List<String> redisValueColumns;
	

	//一些默认项目比如超时时间等参数
	//....
	
	
	//连接池参数暂不放出来
	//最大连接数
	private int jedisMaxActive;
	//最大空闲连接数
	private int jedisMaxIdle;
	//初始化连接
	private int jedisMinIdle;
	//最大等待时间(毫秒)
    private int jedisMaxWait;
    //释放连接的扫描间隔(毫秒 --3000)
    private int jedisTimeBetweenEvictionRunsMillis;
    //连接最小空闲时间(毫秒--1800000)
    private int jedisMinEvictableIdleTimeMillis;
    //连接空闲多久后释放, 当空闲时间>该值 且 空闲连接>最大空闲连接数 时直接释放(毫秒--10000)
    private int jedisSoftMinEvictableIdleTimeMillis;
    
    //对拿到的connection进行validateObject校验
    private boolean jedisTestOnBorrow;
    //对返回的connection进行validateObject校验
    private boolean jedisTestOnReturn;
    //定时对线程池中空闲的链接进行validateObject校验
    private boolean jedisTestWhileIdle;

	
    private RestrictConf restrictConf;

    //redis限制项目
    public class RestrictConf {
//        private int requestTotalSizeLimition = 1024 * 1024;
//       
//
//        public int getRequestTotalSizeLimition() {
//            return requestTotalSizeLimition;
//        }
//        public void setRequestTotalSizeLimition(int requestTotalSizeLimition) {
//            this.requestTotalSizeLimition = requestTotalSizeLimition;
//        }

    }

	public String getClusterMode() {
		return clusterMode;
	}

	public void setClusterMode(String clusterMode) {
		this.clusterMode = clusterMode;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getKeyType() {
		return keyType;
	}

	public void setKeyType(String keyType) {
		this.keyType = keyType;
	}

	public String getValueType() {
		return valueType;
	}

	public void setValueType(String valueType) {
		this.valueType = valueType;
	}

	public String getValueMode() {
		return valueMode;
	}

	public void setValueMode(String valueMode) {
		this.valueMode = valueMode;
	}

	public String getWriteMode() {
		return writeMode;
	}

	public void setWriteMode(String writeMode) {
		this.writeMode = writeMode;
	}

	public List<String> getRedisKeyColumns() {
		return redisKeyColumns;
	}

	public void setRedisKeyColumns(List<String> redisKeyColumns) {
		this.redisKeyColumns = redisKeyColumns;
	}

	public List<String> getRedisValueColumns() {
		return redisValueColumns;
	}

	public void setRedisValueColumns(List<String> redisValueColumns) {
		this.redisValueColumns = redisValueColumns;
	}

	public int getJedisMaxActive() {
		return jedisMaxActive;
	}

	public void setJedisMaxActive(int jedisMaxActive) {
		this.jedisMaxActive = jedisMaxActive;
	}

	public int getJedisMaxIdle() {
		return jedisMaxIdle;
	}

	public void setJedisMaxIdle(int jedisMaxIdle) {
		this.jedisMaxIdle = jedisMaxIdle;
	}

	public int getJedisMinIdle() {
		return jedisMinIdle;
	}

	public void setJedisMinIdle(int jedisMinIdle) {
		this.jedisMinIdle = jedisMinIdle;
	}

	public int getJedisMaxWait() {
		return jedisMaxWait;
	}

	public void setJedisMaxWait(int jedisMaxWait) {
		this.jedisMaxWait = jedisMaxWait;
	}

	public int getJedisTimeBetweenEvictionRunsMillis() {
		return jedisTimeBetweenEvictionRunsMillis;
	}

	public void setJedisTimeBetweenEvictionRunsMillis(
			int jedisTimeBetweenEvictionRunsMillis) {
		this.jedisTimeBetweenEvictionRunsMillis = jedisTimeBetweenEvictionRunsMillis;
	}

	public int getJedisMinEvictableIdleTimeMillis() {
		return jedisMinEvictableIdleTimeMillis;
	}

	public void setJedisMinEvictableIdleTimeMillis(
			int jedisMinEvictableIdleTimeMillis) {
		this.jedisMinEvictableIdleTimeMillis = jedisMinEvictableIdleTimeMillis;
	}

	public int getJedisSoftMinEvictableIdleTimeMillis() {
		return jedisSoftMinEvictableIdleTimeMillis;
	}

	public void setJedisSoftMinEvictableIdleTimeMillis(
			int jedisSoftMinEvictableIdleTimeMillis) {
		this.jedisSoftMinEvictableIdleTimeMillis = jedisSoftMinEvictableIdleTimeMillis;
	}

	public boolean isJedisTestOnBorrow() {
		return jedisTestOnBorrow;
	}

	public void setJedisTestOnBorrow(boolean jedisTestOnBorrow) {
		this.jedisTestOnBorrow = jedisTestOnBorrow;
	}

	public boolean isJedisTestOnReturn() {
		return jedisTestOnReturn;
	}

	public void setJedisTestOnReturn(boolean jedisTestOnReturn) {
		this.jedisTestOnReturn = jedisTestOnReturn;
	}

	public boolean isJedisTestWhileIdle() {
		return jedisTestWhileIdle;
	}

	public void setJedisTestWhileIdle(boolean jedisTestWhileIdle) {
		this.jedisTestWhileIdle = jedisTestWhileIdle;
	}

	public RestrictConf getRestrictConf() {
		return restrictConf;
	}

	public void setRestrictConf(RestrictConf restrictConf) {
		this.restrictConf = restrictConf;
	}

	public int getPipeBatchSize() {
		return pipeBatchSize;
	}

	public void setPipeBatchSize(int pipeBatchSize) {
		this.pipeBatchSize = pipeBatchSize;
	}

	
	
	
    //do somthing
	// override to string 
	
}