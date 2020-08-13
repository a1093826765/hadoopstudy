package com.bigdata.mr.fensi;

import com.bigdata.mr.inverindex.InverIndexStepOne;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class SharedFriendsStepOne {
    static class SharedFriendsStepOneMapper extends Mapper<LongWritable, Text,Text,Text>{
        Text tx=new Text();
        Text tx2=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] user_friends=line.split(":");
            String[] friends=user_friends[1].split(",");
            for(String friend:friends){
                tx.set(friend);
                tx2.set(user_friends[0]);
                context.write(tx,tx2);
            }
        }
    }

    static class SharedFriendStepOneReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> userList=new ArrayList<String>();
            for(Text value:values){
                userList.add(value.toString());
            }
            for(int i=0;i<userList.size()-1;i++){
                for(int j=i+1;j<userList.size();j++){
                    if(userList.get(j).charAt(0)<userList.get(i).charAt(0)){
                        context.write(new Text(userList.get(j) + "-" + userList.get(i)), key);
                    }else {
                        context.write(new Text(userList.get(i) + "-" + userList.get(j)), key);
                    }
                }
            }
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
        job.setJarByClass(SharedFriendsStepOne.class);

        //指定本业务job使用的mapper和reducer的业务类
        job.setMapperClass(SharedFriendsStepOneMapper.class);
        job.setReducerClass(SharedFriendStepOneReduce.class);

        //指定mapper输出数据的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        //指定最终输出数据的kv类型（有reducer就是reducer的输出，没有reducer就是mapper的输出）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //指定job输入的原始文件目录
        FileInputFormat.setInputPaths(job,new Path("/Users/november/Desktop/file/test/input/qq.txt"));
        //指定job输出结果的所在目录
        FileOutputFormat.setOutputPath(job,new Path("/Users/november/Desktop/file/test/out4"));

        //将job中配置的相关配置，以及job所用的java类所在的jar包，提交给yarn运行
//        job.submit();//此方法并不知道程序的运行情况
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
