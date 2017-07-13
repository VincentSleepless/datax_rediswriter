package com.test.redis.writter;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.datax.plugin.writer.rediswriter.util.CommonUtil;

public class ConfigCheckTest {

    public static void main(String[] args) {
    	
    	System.out.println(StringUtils.isEmpty(""));
    	System.out.println(StringUtils.isEmpty(null));
    	
    	//System.out.println(CommonUtil.checkAddress("redis", "127.0.0.1:8080"));
    	//System.out.println(CommonUtil.checkAddress("redis", "127.0.0.1:8080,"));
    	//System.out.println(CommonUtil.checkAddress("redis", "127.0.0.1:8080,dada"));
    	//System.out.println(CommonUtil.checkAddress("redis", "127.0.0.1:8080,127.0.0.2:8088"));
    	//System.out.println(CommonUtil.checkAddress("codis", "127.0.0.1:8080/proxy/url"));
    	System.out.println(CommonUtil.checkAddress("codis", "10.139.36.118:2181"));
    	
    	
    	System.out.println(CommonUtil.checkZkProxy("codis", "/jodis/codis-demo"));
       	System.out.println(CommonUtil.checkZkProxy("codis", "/jodis/codis_demo"));      	
       	//System.out.println(CommonUtil.checkZkProxy("codis", "/jodis//codis-demo"));
       	//System.out.println(CommonUtil.checkZkProxy("codis", null));
    	//System.out.println(CommonUtil.checkZkProxy("codis", ""));
    	//System.out.println(CommonUtil.checkZkProxy("redis", "/jodis//codis-demo"));
    	System.out.println(CommonUtil.checkZkProxy("redis", ""));
    	
    	System.out.println(CommonUtil.checkWriteMode("pipeline"));
    	//System.out.println(CommonUtil.checkWriteMode("upsert"));
    	
    	System.out.println(CommonUtil.checkValueMode("json"));
        //System.out.println(CommonUtil.checkValueMode("delstr"));
    	
    }
	
	
}
