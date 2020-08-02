package com.bigdata.hdfs;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * 用流的方式来操作hdfs上的文件
 * 可以实现读取指定偏移量范围的数据
 */
public class HdfsStaremAccess {
    FileSystem fs=null;
    Configuration conf=null;
    @Before
    public void init() throws Exception {
        conf=new Configuration();
//        conf.set("fs.defaultFs","hdfs://172.16.106.128:9000");
        //副本数由客户端的参数dfs.replication决定（优先级： conf.set >  自定义配置文件 > jar包中的hdfs-default.xml）
        conf.set("dfs.replication","4");
        //拿到一个文件系统操作的客户端实例对象(传入一个uri和用户身份)
        fs=FileSystem.get(new URI("hdfs://172.16.106.128:9000"),conf,"hadoop");
    }

    /**
     * 通过流的方式上传文件
     * @throws Exception
     */
    @Test
    public void testUpload() throws Exception{
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/testMkdir/mysql-connector-java-8.0.21.jar"), true);
        FileInputStream fileInputStream = new FileInputStream("/Users/november/Desktop/file/mysql-connector-java-8.0.21.jar");
        IOUtils.copy(fileInputStream, fsDataOutputStream);
    }

    /**
     * 通过流的方式下载文件
     * @throws Exception
     */
    @Test
    public void testDownload() throws Exception{
        FSDataInputStream fsDataInputStream = fs.open(new Path("/testMkdir/mysql-connector-java-8.0.21.jar"));
        FileOutputStream fileOutputStream = new FileOutputStream("/Users/november/Desktop/file/test.jar");
        IOUtils.copy(fsDataInputStream,fileOutputStream);
    }

    /**
     * 通过流的方式切片下载文件
     * @throws Exception
     */
    @Test
    public void testRandomAccess() throws Exception{
        FSDataInputStream fsDataInputStream = fs.open(new Path("/testMkdir/mysql-connector-java-8.0.21.jar"));
        //在第12字节开始读起
        fsDataInputStream.seek(12);
        FileOutputStream fileOutputStream=new FileOutputStream("/Users/november/Desktop/file/test2.jar");
        IOUtils.copy(fsDataInputStream,fileOutputStream);
    }

    /**
     * 查看文件内容
     * @throws Exception
     */
    @Test
    public void testCat() throws Exception{
        FSDataInputStream fsDataInputStream=fs.open(new Path("/testMkdir/jdk.txt"));
        IOUtils.copy(fsDataInputStream,System.out);
        fs.close();
    }
}
