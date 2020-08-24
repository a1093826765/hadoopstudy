package com.bigdata.mrprocess;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


import java.io.IOException;

public class PFileInputFormat extends FileInputFormat<LongWritable, Text> {

    //创建一个createRecordReader,这个类会从切片中读取每一条数据，如果设置了不切片，则读取整个文件
    //第二步，执行一次
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        System.out.println("===>>FileInputFormat -- createRecordReader");
        //这里可以写一个继承inputFormat的类自定义方法
        PRecordReader pRecordReader=new PRecordReader();
        pRecordReader.initialize(inputSplit,taskAttemptContext);
        return pRecordReader;
    }


    //设置是否进行切片，返回True or false
    //第一步，都有多少个文件就执行多少次，直到把所有文件读完
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        System.out.println("===>>FileInputFormat -- isSplitable");
        return super.isSplitable(context, filename);
    }
}
