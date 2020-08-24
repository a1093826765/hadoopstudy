package com.bigdata.mrprocess;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class PReduce extends Reducer<Text,PBeanSort, Text,PBeanSort> {

    //reduce初始化
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("===>>PReduce -- setup");
        super.setup(context);
    }

    //reduce逻辑层
    @Override
    protected void reduce(Text key, Iterable<PBeanSort> values, Context context) throws IOException, InterruptedException {
        System.out.println("===>>PReduce -- reduce");
        super.reduce(key, values, context);
    }

    //清理reduce,类似ip
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("===>>PReduce -- cleanup");
        super.cleanup(context);
    }

    //执行reduce,此方法会调用 setup-->reduce-->cleanup
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        System.out.println("===>>PReduce -- run");
        super.run(context);
    }
}
