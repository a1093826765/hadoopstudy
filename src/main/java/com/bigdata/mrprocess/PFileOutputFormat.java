package com.bigdata.mrprocess;

import com.bigdata.mr.logenhance.LogEnhanceOutputFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *  maptask或者reducetask在最终输出时，先调用OutputFormat的getRecordWriter方法拿到一个RecordWriter
 *  然后再调用RecordWriter的write(k,v)方法将数据写出
 */
public class PFileOutputFormat extends FileOutputFormat<Text, PBeanSort> {

    //最终输出时，会调用此方法拿到RecordWriter
    //第十八步，当空参数构造后，map执行完毕，获取RecordWriter
    @Override
    public RecordWriter<Text, PBeanSort> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        System.out.println("===>>PFileOutputFormat -- getRecordWriter");
        FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());

        Path enhancePath = new Path("/Users/november/Desktop/file/test/aaa.txt");
        // 多个输出地址，可以自己添加
        // Path tocrawlPath = new Path("/Users/november/Desktop/file/test/url.dat");

        FSDataOutputStream enhancedOs = fs.create(enhancePath);
        // 多个输出地址，可以自己添加
        // FSDataOutputStream tocrawlOs = fs.create(tocrawlPath);
        // 这里可以写一个自定义类，继承RecordWriter类自定义方法
        return new PRecordWriter(enhancedOs);
    }
}
