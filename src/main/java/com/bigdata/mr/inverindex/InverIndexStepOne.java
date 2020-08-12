package com.bigdata.mr.inverindex;

import com.bigdata.mr.rjoin.InfoBean;
import com.bigdata.mr.rjoin.RJoin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InverIndexStepOne {

    static class InverIndexStepOneMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        Text tk=new Text();
        IntWritable iv=new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] split = line.split(" ");
            FileSplit inputSplit = (FileSplit) context.getInputSplit();//获取切片（根据需要分析的数据定义类型，这里需要分析的是文件）
            String fileName = inputSplit.getPath().getName();
            for(String word:split){
                tk.set(word+"--"+fileName);
                context.write(tk,iv);
            }
        }
    }

    static class InverIndexStepOneReduce extends Reducer<Text,IntWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long count=0;
            for(IntWritable value:values){
                count+=value.get();
            }
            context.write(key,new LongWritable(count));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        //如果在linux上执行，已经配置hadoop则不需要配置
//        configuration.set("mapreduce.framework.name","yarn");
//        configuration.set("yarn.resoucemanager.hostname","bigdata1");
        Job job = Job.getInstance(configuration);

        //指定本程序jar所在路径
//        job.setJar("/Users/november/IdeaProjects/hadoopstudy/out/artifacts/hadoopstudy_jar/hadoopstudy.jar");
        job.setJarByClass(InverIndexStepOne.class);

        //指定本业务job使用的mapper和reducer的业务类
        job.setMapperClass(InverIndexStepOneMapper.class);
        job.setReducerClass(InverIndexStepOneReduce.class);

        //指定mapper输出数据的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        //指定最终输出数据的kv类型（有reducer就是reducer的输出，没有reducer就是mapper的输出）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //指定job输入的原始文件目录
        FileInputFormat.setInputPaths(job,new Path("/Users/november/Desktop/file/test/input"));
        //指定job输出结果的所在目录
        FileOutputFormat.setOutputPath(job,new Path("/Users/november/Desktop/file/test/out2"));

        //将job中配置的相关配置，以及job所用的java类所在的jar包，提交给yarn运行
//        job.submit();//此方法并不知道程序的运行情况
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
