package com.bigdata.mrprocess;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 *  * RecordReader的核心工作逻辑：
 *  * 通过nextKeyValue()方法去读取数据构造将返回的key   value
 *  * 通过getCurrentKey 和 getCurrentValue来返回上面构造好的key和value
 */
public class PRecordReader extends RecordReader<LongWritable,Text> {
    private boolean processed = false;

    //用于加载数据到此类,用于给予inputFormat初始化使用
    //第三步，createRecordReader调用，进行初始化
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        System.out.println("===>>PRecordReader -- initialize");
    }

    //读取文件，给key和value赋值
    //第六步，当map初始化后调用，进行给key和value赋值
    //第十二步，map逻辑层执行后，序列化后进行赋值
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        System.out.println("===>>PRecordReader -- nextKeyValue");
        if(!processed){
            processed=true;
            return true;
        }
        return false;
    }

    //RecordReader通过此方法获取key给map
    //第八步，当赋值后，获取当前进度，map获取key
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        System.out.println("===>>PRecordReader -- getCurrentKey");
        return new LongWritable(123);
    }

    //RecordReader通过此方法获取value给map
    //第九步，map获取key后，则获取value
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        System.out.println("===>>PRecordReader -- getCurrentValue");
        return new Text("456");
    }


    //返回当前进度
    //第七步，当给key和value赋值后，返回当前进度
    //第十三步，序列化赋值后，返回当前进度
    @Override
    public float getProgress() throws IOException, InterruptedException {
        System.out.println("===>>PRecordReader -- getProgress");
        return 0;
    }

    //清理此类，类型io
    //第十五步，map清理后，再此类清理
    @Override
    public void close() throws IOException {
        System.out.println("===>>PRecordReader -- close");
    }
}
