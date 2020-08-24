package com.bigdata.mrprocess;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//有多少个文件，mapper就执行多少次
public class PMapper extends Mapper<LongWritable, Text,Text,PBeanSort> {
    Text tx=new Text();
    PBeanSort pBeanSort=new PBeanSort(1,"map","123456");

    //map初始化
    //第五步，mapper初始化
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("===>>Mapper -- setup");
        super.setup(context);
    }

    //map逻辑层
    //第十步，当recordReader获取key和value后调取map逻辑层
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("===>>Mapper -- map");
        tx.set("map");
        context.write(tx,pBeanSort);
    }

    //开始执行mapper,此方法会调用 setup-->map-->cleanup
    //第四步，读取完文件后，进入mapper开始执行逻辑
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        System.out.println("===>>Mapper -- run");
        super.run(context);
    }

    //用户清理map,类似io等
    //第十四步，map逻辑层调用后，bean类赋值后，清理map
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("===>>Mapper -- cleanup");
        super.cleanup(context);
    }
}
