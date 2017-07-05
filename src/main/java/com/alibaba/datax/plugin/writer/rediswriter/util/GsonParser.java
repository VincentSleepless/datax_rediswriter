package com.alibaba.datax.plugin.writer.rediswriter.util;

import com.alibaba.datax.plugin.writer.rediswrite.model.RedisConf;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonParser {
    
    private static Gson gsonBuilder() {
        return new GsonBuilder()
        .create();
    }

    public static String confToJson (RedisConf conf) {
        Gson g = gsonBuilder();
        return g.toJson(conf);
    }

    public static RedisConf jsonToConf (String jsonStr) {
        Gson g = gsonBuilder();
        return g.fromJson(jsonStr, RedisConf.class);
    }

}
