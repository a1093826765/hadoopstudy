package com.bigdata.mrprocess;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

//
public class PInputFormat extends InputFormat<LongWritable, Text> {

    //获取切片
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        System.out.println("===>>InputFormat -- getSplits");
        return null;
    }

    //创建一个createRecordReader,这个类会从切片中读取每一条数据
    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        System.out.println("===>>InputFormat -- createRecordReader");
        //这里可以写一个继承inputFormat的类自定义方法
        PRecordReader pRecordReader=new PRecordReader();
        pRecordReader.initialize(inputSplit,taskAttemptContext);
        return pRecordReader;
    }


}
