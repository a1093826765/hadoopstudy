package com.bigdata.mrprocess;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class PRecordWriter extends RecordWriter<Text,PBeanSort> {
    FSDataOutputStream enhancedOs = null;

    // 用于加载数据到此类,用于给予inputFormat初始化使用
    // 第十九步，PFileOutputFormat进行调用初始化
    public PRecordWriter(FSDataOutputStream enhancedOs) {
        super();
        this.enhancedOs = enhancedOs;
        System.out.println("===>>PRecordWriter -- PRecordWriter");
    }

    //最终输出
    //第二十五步，从reduce逻辑层拿到数据后，向指定位置输出
    @Override
    public void write(Text text, PBeanSort pBeanSort) throws IOException, InterruptedException {
        System.out.println("===>>PRecordWriter -- write");
        String result = text.toString();
        System.out.println("===>>最终输出 -- "+result+"\t"+pBeanSort.toString());
        enhancedOs.write((result+"\t"+pBeanSort.toString()).getBytes());
    }

    //清理此类，类似io
    //第二十七步，reduce执行完，输出执行完后，清理此类
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        System.out.println("===>>PRecordWriter -- close");
        //这里注意一定要关闭，不然则没有输出结果
        if(enhancedOs!=null) {
            enhancedOs.close();
        }
    }
}
