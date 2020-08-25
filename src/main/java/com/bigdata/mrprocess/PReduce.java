package com.bigdata.mrprocess;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class PReduce extends Reducer<Text,PBeanSort, Text,PBeanSort> {

    //reduce初始化
    //第二十一步，reduce初始化
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("===>>PReduce -- setup");
        super.setup(context);
    }

    //reduce逻辑层
    //第二十四步，进行反序列化后，拿到数据后调用逻辑层
    @Override
    protected void reduce(Text key, Iterable<PBeanSort> values, Context context) throws IOException, InterruptedException {
        System.out.println("===>>PReduce -- reduce");
        for(PBeanSort bean:values){
            System.out.println("===>>reduce数据 -- "+bean.getId()+"\t"+bean.getUserName()+"\t"+bean.getPassowrd());
            context.write(key,bean);
        }
    }

    //清理reduce,类似ip
    //第二十六步，逻辑层和输出执行完后，清理reduce
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("===>>PReduce -- cleanup");
        super.cleanup(context);
    }

    //执行reduce,此方法会调用 setup-->reduce-->cleanup
    //第二十步，获取完RecordWriter后，开始执行reduce
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        System.out.println("===>>PReduce -- run");
        super.run(context);
    }
}
