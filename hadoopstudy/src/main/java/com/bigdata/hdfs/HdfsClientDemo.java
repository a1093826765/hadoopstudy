package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;


public class HdfsClientDemo {

    FileSystem fs=null;
    @Before
    public void init() throws Exception {
        Configuration conf=new Configuration();
//        conf.set("fs.defaultFs","hdfs://172.16.106.128:9000");
        //副本数由客户端的参数dfs.replication决定（优先级： conf.set >  自定义配置文件 > jar包中的hdfs-default.xml）
        conf.set("dfs.replication","4");
        //拿到一个文件系统操作的客户端实例对象(传入一个uri和用户身份)
        fs=FileSystem.get(new URI("hdfs://172.16.106.128:9000"),conf,"hadoop");
    }

    //上传文件
    @Test
    public void testUpload() throws Exception {
        fs.copyFromLocalFile(new Path("/Users/november/Downloads/WechatIMG1.jpeg"),new Path("/macUpload/WechatIMG4.jpeg"));
        fs.close();
    }

    //下载文件
    @Test
    public void testDownload() throws Exception{
        fs.copyToLocalFile(new Path("/macUpload/WechatIMG3.jpeg"),new Path("/Users/november/Downloads"));
        fs.close();
    }

}
