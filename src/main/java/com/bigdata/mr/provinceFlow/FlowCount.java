package com.bigdata.mr.provinceFlow;

import com.bigdata.mr.flowsum.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 把mapper和reduer都写在一个类中
 *
 * 此程序是通过文本数据拿到所需要的数据并进行分区
 */
public class FlowCount {
    static class FlowMapper extends Mapper<LongWritable,Text,Text, com.bigdata.mr.flowsum.FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //将内容变成String
            String line=value.toString();
            //切分字段
            String[] split = line.split(" ");
            //取出手机号码
            String phone=split[1];
            //取出上行流量，下行流量
            long upFlow=Long.valueOf(split[split.length-3]);
            long dFlow=Long.valueOf(split[split.length-2]);
            context.write(new Text(phone),new com.bigdata.mr.flowsum.FlowBean(upFlow,dFlow));
        }
    }

    static class FlowReduer extends Reducer<Text, com.bigdata.mr.flowsum.FlowBean,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<com.bigdata.mr.flowsum.FlowBean> values, Context context) throws IOException, InterruptedException {
            long upFlow=0;
            long dFlow=0;
            for(com.bigdata.mr.flowsum.FlowBean flowBean:values){
                upFlow+=flowBean.getUpFlow();
                dFlow+=flowBean.getdFlow();
            }
            String res=upFlow+"\t"+dFlow+"\t"+(upFlow+dFlow);
            context.write(key,new Text(res));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //指定本程序jar所在路径
        job.setJarByClass(FlowCount.class);

        //指定本业务job使用的mapper和reducer的业务类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduer.class);

        //指定mapper输出数据的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定我们自定义的数据分区器
        job.setPartitionerClass(ProvincePartitioner.class);
        //同时指定相应“分区”数量的reducetask
        //分区数量是根据partitioner中的数量而定，只能是1或者大于等于分区数量
        //当为1的时候则会把所有结果写入一个文件 -- 当为分区数量时，则会分别写入对应的文件 -- 当大于分区数量时，则会产生空文件
        job.setNumReduceTasks(3);

        //指定最终输出数据的kv类型（有reducer就是reducer的输出，没有reducer就是mapper的输出）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //指定job输入的原始文件目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //指定job输出结果的所在目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //将job中配置的相关配置，以及job所用的java类所在的jar包，提交给yarn运行
//        job.submit();//此方法并不知道程序的运行情况
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
