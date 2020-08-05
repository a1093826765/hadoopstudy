package com.bigdata.mr.flowsum;

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

public class FlowCountSort {

    static class FlowCountSortMapper extends Mapper<LongWritable,Text,FlowBean,Text> {
          FlowBean flowBean=new FlowBean();
          Text text=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] split = line.split("\t");
            long upFlow=Long.parseLong(split[1]);
            long dFlow=Long.parseLong(split[2]);
            flowBean.setAll(upFlow,dFlow);
            text.set(split[0]);
            context.write(flowBean,text);
        }

    }

    static class FlowCountSortReduer extends Reducer<FlowBean,Text,Text,FlowBean>{

        @Override
        protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(),key);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //指定本程序jar所在路径
        job.setJarByClass(FlowCountSort.class);

        //指定本业务job使用的mapper和reducer的业务类
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReduer.class);

        //指定mapper输出数据的类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出数据的kv类型（有reducer就是reducer的输出，没有reducer就是mapper的输出）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

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
