package com.alibaba.datax.plugin.writer.rediswrite.model;

import java.util.List;

public class RedisConf {
	
	//cluster-mode redis/codis
	private String clusterMode;
	//redis cluster host1:port1,host2:port2......
	private String address;
	//master auth
	private String password;
	//codis cluster jodis zk address
	private String zkProxyDir;
	//reids key type(default only String)
	private String keyType;
	//redis value type(default only String)
	private String valueType;
	//redis value mode(del-str/json)
	private String valueMode;
	//redis pipeline batch size sync
	private int pipeBatchSize;
	//redis key/column is combined by which colums array
	private List<String> redisKeyColumns;
	private List<String> redisValueColumns;
	
	//jodis round robin pool config
    private int minIdle =1;
	
	private int maxIdle =3;
	
	private int maxTotal=5;
	
	private int maxWaitMillis = 3000;

	private int minEvictableIdleTimeMillis =3000;
	
	private int softMinEvictableIdleTimeMillis = 10000;
	
	private int timeBetweenEvictionRunsMillis = 1800000 ;
	
	private boolean testOnBorrow = true ;
	
	private boolean testOnReturn =false ;
	
	private boolean testWhileIdle =true;
        
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
	
	public String getZkProxyDir() {
		return zkProxyDir;
	}

	public void setZkProxyDir(String zkProxyDir) {
		this.zkProxyDir = zkProxyDir;
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


	public int getPipeBatchSize() {
		return pipeBatchSize;
	}

	public void setPipeBatchSize(int pipeBatchSize) {
		this.pipeBatchSize = pipeBatchSize;
	}

	public int getSoftMinEvictableIdleTimeMillis() {
		return softMinEvictableIdleTimeMillis;
	}

	public void setSoftMinEvictableIdleTimeMillis(int softMinEvictableIdleTimeMillis) {
		this.softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;
	}

	public int getTimeBetweenEvictionRunsMillis() {
		return timeBetweenEvictionRunsMillis;
	}

	public void setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
		this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
	}

	public int getMinIdle() {
		return minIdle;
	}

	public void setMinIdle(int minIdle) {
		this.minIdle = minIdle;
	}

	public int getMaxIdle() {
		return maxIdle;
	}

	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public int getMaxTotal() {
		return maxTotal;
	}

	public void setMaxTotal(int maxTotal) {
		this.maxTotal = maxTotal;
	}

	public int getMaxWaitMillis() {
		return maxWaitMillis;
	}

	public void setMaxWaitMillis(int maxWaitMillis) {
		this.maxWaitMillis = maxWaitMillis;
	}

	public int getMinEvictableIdleTimeMillis() {
		return minEvictableIdleTimeMillis;
	}

	public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
		this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
	}

	public boolean isTestOnBorrow() {
		return testOnBorrow;
	}

	public void setTestOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
	}

	public boolean isTestOnReturn() {
		return testOnReturn;
	}

	public void setTestOnReturn(boolean testOnReturn) {
		this.testOnReturn = testOnReturn;
	}

	public boolean isTestWhileIdle() {
		return testWhileIdle;
	}

	public void setTestWhileIdle(boolean testWhileIdle) {
		this.testWhileIdle = testWhileIdle;
	}
	
	
    //do somthing
	// override to string 
	
}