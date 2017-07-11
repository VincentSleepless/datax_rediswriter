package com.alibaba.datax.plugin.writer.rediswrite.model;

public class RedisConst {
	
    // Writer support type
    public final static String TYPE_STRING  = "STRING";
    public final static String TYPE_INTEGER = "INT";
    public final static String TYPE_DOUBLE  = "DOUBLE";
	
	// redis cluster mode
    public final static String CLUSTER_REDIS= "redis";
    public final static String CLUSTER_CODIS= "codis";
    
    // redis value mode
    public final static String VALUEMODE_JSON= "json";
    public final static String VALUEMODE_DELSTR= "delstr";
    
    // redis write mode
    public final static String WRITEMODE_PIPELINE= "pipeline";
    //pipe batch size
    public final static int PIPELINE_BATCHSIZE=2000;
    
	// redis key/value type
    public final static String REDIS_KVTYPE ="String";
    
    // project delimit
    public final static String DEL_COMMA =",";
    public final static String DEL_COLON = ":";
    
    
    //plugin name
    public final static String PLUGIN_READER_MYSQL="mysqlreader";
    public final static String PLUGIN_READER_ODPS="odpsreader";
    
}
