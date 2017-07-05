package reidswriter;

import redis.clients.jedis.JedisCluster;

import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConf;
import com.alibaba.datax.plugin.writer.rediswriter.util.GsonParser;
import com.alibaba.datax.plugin.writer.rediswriter.util.JedisUtil;
import com.alibaba.fastjson.JSONObject;


public class JsonTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		JSONObject json = new JSONObject();
		json.put("name", "zhangsan");
		json.put("age","11");
		json.put("address","show me your way");
		System.out.println(json.toString());
		
		
		RedisConf conf = new RedisConf();
    	conf.setAddress("10.139.36.118:7001,10.139.36.118:7002,10.139.36.118:7003,"
    			+ "10.139.36.118:7004,10.139.36.118:7005,10.139.36.118:7006"); 	
    	System.out.println(GsonParser.confToJson(conf));
		

    	JedisCluster cluster = JedisUtil.initJedisClusterClient(conf);
    	System.out.println(cluster.get("zengli"));
    	System.out.println(cluster.set("jsonTest",json.toJSONString()));
    	System.out.println(cluster.get("jsonTest"));
    	
    	

	}

}
