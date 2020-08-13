package com.bigdata.mr.fensi;

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

public class SharedFriendsStepTwo {

        static class SharedFriendsStepTwoMapper extends Mapper<LongWritable, Text,Text,Text> {
            Text tx=new Text();
            Text tx2=new Text();
            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line=value.toString();
                String[] split = line.split("\t");
                tx.set(split[0]);
                tx2.set(split[1]);
                context.write(tx,tx2);
            }
        }

        static class SharedFriendStepOneReduce extends Reducer<Text,Text,Text,Text> {
            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                StringBuffer stringBuffer=new StringBuffer();
                for(Text value:values){
                    stringBuffer.append(value).append(" ");
                }
                context.write(key,new Text(stringBuffer.toString()));
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
            job.setJarByClass(com.bigdata.mr.fensi.SharedFriendsStepTwo.class);

            //指定本业务job使用的mapper和reducer的业务类
            job.setMapperClass(com.bigdata.mr.fensi.SharedFriendsStepTwo.SharedFriendsStepTwoMapper.class);
            job.setReducerClass(com.bigdata.mr.fensi.SharedFriendsStepTwo.SharedFriendStepOneReduce.class);

            //指定mapper输出数据的类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);


            //指定最终输出数据的kv类型（有reducer就是reducer的输出，没有reducer就是mapper的输出）
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //指定job输入的原始文件目录
            FileInputFormat.setInputPaths(job,new Path("/Users/november/Desktop/file/test/out4/"));
            //指定job输出结果的所在目录
            FileOutputFormat.setOutputPath(job,new Path("/Users/november/Desktop/file/test/out5"));

            //将job中配置的相关配置，以及job所用的java类所在的jar包，提交给yarn运行
//        job.submit();//此方法并不知道程序的运行情况
            boolean res = job.waitForCompletion(true);
            System.exit(res?0:1);
        }
    

}
