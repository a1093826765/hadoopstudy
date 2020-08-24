package com.bigdata.mrprocess;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class PRecordWriter extends RecordWriter<Text,PBeanSort> {
    FSDataOutputStream enhancedOs = null;

    // 用于加载数据到此类,用于给予inputFormat初始化使用
    public PRecordWriter(FSDataOutputStream enhancedOs) {
        super();
        this.enhancedOs = enhancedOs;
        System.out.println("===>>PRecordWriter -- PRecordWriter");
    }

    //最终输出
    @Override
    public void write(Text text, PBeanSort pBeanSort) throws IOException, InterruptedException {
        System.out.println("===>>PRecordWriter -- write");
        String result = text.toString();
        enhancedOs.write((result+"\t"+pBeanSort).getBytes());
    }

    //清理此类，类似io
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        System.out.println("===>>PRecordWriter -- close");
    }
}
