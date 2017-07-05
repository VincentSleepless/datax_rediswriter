package reidswriter;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.datax.plugin.writer.rediswriter.util.JedisUtil;

public class ConfigCheckTest {

    public static void main(String[] args) {
    	
    	
    	System.out.println(StringUtils.isEmpty(""));
    	System.out.println(StringUtils.isEmpty(null));
    	System.out.println(JedisUtil.checkAddress("redis", "127.0.0.1:8080"));
    	System.out.println(JedisUtil.checkAddress("redis", "127.0.0.1:8080,"));
    	System.out.println(JedisUtil.checkAddress("redis", "127.0.0.1:8080,dada"));
    	System.out.println(JedisUtil.checkAddress("redis", "127.0.0.1:8080,127.0.0.2:8088"));
    	System.out.println(JedisUtil.checkAddress("codis", "127.0.0.1:8080/proxy/url"));
    	System.out.println(JedisUtil.checkWriteMode("upsert"));
    	System.out.println(JedisUtil.checkWriteMode("insert"));
    	System.out.println(JedisUtil.checkWriteMode("inaaasert"));
    	System.out.println(JedisUtil.checkValueMode("json"));
        System.out.println(JedisUtil.checkValueMode("upsert"));
    	
    }
	
	
}
