package com.alibaba.datax.plugin.writer.rediswriter;

public class RedisKey {

	//peer key const
	public final static String KEY_COLUMN="column";
	
	// current key const
	public final static String KEY_CLUSTERMODE = "clusterMode";
	public final static String KEY_ADDRESS = "address";
	public final static String KEY_ZKPROXYDIR = "zkProxyDir";
	public final static String KEY_PASSWORD ="password";
	public final static String KEY_KEYTYPE = "keyType";
	public final static String KEY_KEYCOLUMN = "keyColumn";
    public final static String KEY_VALUETYPE = "valueType";
    public final static String KEY_VALUECOLUMN = "valueColumn";
    public final static String KEY_VALUEMODE = "valueMode";
    public final static String KEY_WRITEMODE = "writeMode";
    public final static String KEY_PIPELINE_BATCHSIZE = "pipeBatchSize";
			
	// Column(Key Column/Value Column)
    public final static String KEY_COLUMNTYPE = "type";
    public final static String KEY_COLUMNNAME = "name";

	// Column(Key Column/Value Column)
    public final static String KEY_ADDRESSHOST = "host";
    public final static String KEY_ADDRESSPORT = "port";
    
    
    //Configuration中的参数
    public final static String KEY_REDIS_CONF = "KEY_REDIS_CONF";
	
}
