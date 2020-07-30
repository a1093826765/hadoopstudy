package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Iterator;
import java.util.Map;

/**
 * hdfs的基本操作
 */

public class HdfsClientDemo {

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
     * Configuration配置的默认值（hadoop配置的默认值）
     * @throws Exception
     */
    @Test
    public void testConf(){
        Iterator<Map.Entry<String, String>> integer=conf.iterator();
        while(integer.hasNext()){
            Map.Entry<String ,String> entry=integer.next();
            System.out.println(entry.getKey()+"值："+entry.getValue());
        }
    }

    /**
     * 上传文件
     * @throws Exception
     */
    @Test
    public void testUpload() throws Exception {
        fs.copyFromLocalFile(new Path("/Users/november/Downloads/WechatIMG1.jpeg"),new Path("/macUpload/WechatIMG4.jpeg"));
        fs.close();
    }


    /**
     * 下载文件
     * @throws Exception
     */
    @Test
    public void testDownload() throws Exception{
        fs.copyToLocalFile(new Path("/macUpload/WechatIMG3.jpeg"),new Path("/Users/november/Downloads"));
        fs.close();
    }


    /**
     * 创建文件夹
     * @throws Exception
     */
    @Test
    public void testMkdir() throws Exception{
        boolean mkdirs=fs.mkdirs(new Path("/testMkdir/test2"),new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
        System.out.println("创建是否成功"+mkdirs);
        fs.close();
    }


    /**
     * 删除目录
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception{
        fs.delete(new Path("/testMkdir/test2"),true);
        fs.close();
    }

    /**
     * 递归查询出指定目录下的所有文件（包含子目录中的文件）
     * @throws Exception
     */
    @Test
    public void testFindAll() throws Exception{
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/testMkdir"), true);
        while(remoteIterator.hasNext()){
            LocatedFileStatus locatedFileStatus = remoteIterator.next();
            System.out.println("数据块大小："+locatedFileStatus.getBlockSize());
            System.out.println("owner："+locatedFileStatus.getOwner());
            System.out.println("副本数量："+locatedFileStatus.getReplication());
            System.out.println("路径："+locatedFileStatus.getPath());
            System.out.println("文件名："+locatedFileStatus.getPath().getName());
            System.out.println("权限："+locatedFileStatus.getPermission());
            System.out.println("============================================");
            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            for(BlockLocation b:blockLocations){
                String[] names = b.getNames();
                for(String n:names){
                    System.out.println("块名称"+n);
                }
                System.out.println("块起始偏移量："+b.getOffset());
                System.out.println("块长度"+b.getLength());
                //块所在的datanode节点
                String[] hosts = b.getHosts();
                for(String str:hosts){
                    System.out.println("块主机"+str);
                }
            }
        }
        fs.close();
    }

    /**
     * 查询指定目录下的所有文件和文件夹（不包含子目录）
     * @throws Exception
     */
    @Test
    public void testFind() throws Exception{
        FileStatus[] fileStatuses = fs.listStatus(new Path("/testMkdir"));
        for(FileStatus file:fileStatuses){
            System.out.println("数据块大小："+file.getBlockSize());
            System.out.println("owner："+file.getOwner());
            System.out.println("副本数量："+file.getReplication());
            System.out.println("路径："+file.getPath());
            System.out.println("文件名："+file.getPath().getName());
            System.out.println("权限："+file.getPermission());
            System.out.println("是否是文件夹："+file.isDirectory());
            System.out.println("是否是文件："+file.isFile());
            System.out.println("============================================");
        }
    }
}
