package com.hdfs.cn.utils;

import org.eclipse.jetty.util.ajax.JSON;

/**
 * @Author: Mr.L
 * @Date: 2020/10/15 11:19
 **/
public class JsonUtil {
    public static <T extends Object> T fromObject(String jsonStr,Class<T> clazz){
        T parse = (T) JSON.parse(jsonStr);
        return parse;
    }
}
