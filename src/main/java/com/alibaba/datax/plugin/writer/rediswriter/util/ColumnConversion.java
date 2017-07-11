package com.alibaba.datax.plugin.writer.rediswriter.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.rediswriter.RedisError;
import com.alibaba.fastjson.JSONObject;



/**
 * 备注：datax提供的转换机制有如下限制,如下规则是不能转换的
 * 1. bool   -> binary
 * 2. binary -> long, double, bool
 * 3. double -> bool, binary
 * 4. long   -> binary
 * 
 * 5. 默认支持的类型为 INT LONG  DOUBLE STRING BOOL DATE NULL
 * 6. BAD  BYTES 不支持
 * 
 */
public class ColumnConversion {
	
    private static final Logger LOG = LoggerFactory.getLogger(ColumnConversion.class);
	
    public static String columnConvertStr(Column col) {
    	
        try {
            switch (col.getType()) {
            case STRING:
                return col.asString();
            case INT:
                return col.asString();
            case DOUBLE:
                return col.asString();
            case DATE:
                return col.asString();
            case BOOL:
                return col.asString();
            case NULL:
            	return "";
            case LONG:
            	return col.asString();
            default:    	
            	LOG.info("COLUMN TYPE :" +col.getType()+" COLUMN INFO :" +col.getByteSize());
                throw new IllegalArgumentException(String.format(
                		RedisError.COLUMN_CONVERT.toString(),col.getType(),col.asString()));
            }
        } catch (DataXException e) {
            throw new IllegalArgumentException(String.format(
            		RedisError.COLUMN_CONVERT.toString(),col.asString()));
        }
    }
    
    
 public static JSONObject columnToReidsValue(JSONObject json,String name,Column col) {
    	
        try {
            switch (col.getType()) {
            case STRING:	
            	json.put(name, col.asString());
                return json;
            case INT:
            	json.put(name, col.asLong());
                return json;
            case DOUBLE:
            	json.put(name, col.asDouble());
                return json;
            case DATE:
            	json.put(name, col.asDate());
                return json;
            case BOOL:
            	json.put(name, col.asBoolean());
                return json;
            case NULL:
            	json.put(name, "");
                return json;
            case LONG:
            	json.put(name, col.asLong());
            	return json;
            default:    	
            	LOG.error("UNSUPPORTED COLUMN TYPE :" +col.getType() + 
            			" COLUMN SIZE :" +col.getByteSize() + 
            			" COLUMN VALUE :" +col.getRawData().toString());
                throw new IllegalArgumentException(String.format(
                		RedisError.COLUMN_CONVERT.toString(),col.getType(),col.asString()));
            }
        } catch (DataXException e) {
            throw new IllegalArgumentException(String.format(
            		RedisError.COLUMN_CONVERT.toString(),col.asString()));
        }
    }
    
    
}
