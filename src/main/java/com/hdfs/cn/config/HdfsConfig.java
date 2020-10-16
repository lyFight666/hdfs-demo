package com.hdfs.cn.config;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import javax.annotation.Resource;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author: Mr.L
 * @Date: 2020/10/15 10:58
 **/

@Configuration
public class HdfsConfig {
    private static final Logger logger = LoggerFactory.getLogger(HdfsConfig.class);

    private String hdfsPath = "hdfs://47.93.237.213:8020";
    private String hdfsName = "hdfsuser";
    /**
     * hadoop hdfs 配置参数对象
     * @return
     */
    @Bean
    public org.apache.hadoop.conf.Configuration  getConfiguration(){
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("fs.defaultFS", hdfsPath);
        // 缓存Filesystem实例，否则会导致某个datanode关闭连接后抛出异常"java.io.IOException: Filesystem closed"
//        configuration.setBoolean("fs.hdfs.impl.disable.cache",true);
        return configuration;
    }
    /**
     * hadoop filesystem 文件系统
     * @return
     */
    @Bean
    @Scope(scopeName = "prototype")
    public FileSystem getFileSystem(){
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.newInstance(new URI(hdfsPath), getConfiguration(), hdfsName);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error(e.getMessage());
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            logger.error(e.getMessage());
        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            logger.error(e.getMessage());
        }
        return fileSystem;
    }

}
