package com.alibaba.datax.plugin.writer.rediswriter;
import com.alibaba.datax.common.spi.ErrorCode;


public enum RedisError implements ErrorCode {

	//error list
	REQUIRED_VALUE("RedisWriterError-01", "error necessery config item required value."),
	INVALID_ADDRESS("RedisWriterError-02","error config item:redis address."),
	INVALID_CLUSTERMODE("RedisWriterError-03","error config item:redis cluster-mode"),
	INVALID_VALUEMODE("RedisWriterError-04","error config item:redis value-mode"),
	INVALID_WRITEMODE("RedisWriterError-05","error config item:redis write-mode"),
	
	
	ILLEGAL_WRITEVPRARM("RedisWriterError-11","redis write task param check fail"),
	ILLEGAL_ADDRESS("RedisWriterError-12","error config item:redis address"),
	ILLEGAL_PEERCOLUMN("RedisWriterError-13","redis writer peer plugin colums item required value。"),
	COLUMN_NOT_CONTAINS("RedisWriterError-14","redis writer key-colums is not one ofthe peer-plugin-column。"),
	UNSUPPORT_PEER_PLUGIN("RedisWriterError-15","redis unsupported peer plugin"),
	COLUMN_CONVERT("RedisWriterError-16","redis key/value columns convert error"),
	COLUMN_VALUE_IS_NULL("RedisWriterError-17","redis key/value columns value is null"),
	JEDIS_CONNECT_TIMEOUT("RedisWriterError-70","Jedis连接超时"),
	JEDIS_UNKOWN("RedisWriterError-71","jedis连接未知异常"),
	
	
	SPLIT_ERROR("RedisWriterError-80","redis job split 错误"),
	WRITE_ERROR("RedisWriterError-81","redis task write 错误"),
    UNKNOWN("RedisWriterError-99","该错误表示插件的内部错误，表示系统没有处理到的异常");
	
	
	
	
	
    private final String code;
    private final String description;

    private RedisError(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
