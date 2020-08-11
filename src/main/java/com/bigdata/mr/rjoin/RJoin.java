package com.bigdata.mr.rjoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class RJoin {
    static class RJoinMapper extends Mapper<LongWritable, Text,Text,InfoBean>{
        InfoBean infoBean=new InfoBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line=value.toString();

            FileSplit inputSplit = (FileSplit) context.getInputSplit();//获取切片（根据需要分析的数据定义类型，这里需要分析的是文件）
            String name = inputSplit.getPath().getName();//获取文件名
            String[] split = line.split(",");
            //通过文件名判断是哪种数据
            if(name.startsWith("order")){//文件名是否包含此字段
                //由于需要执行序列化方法，不能给予空值，所以给予一个默认值
                infoBean.set(Integer.parseInt(split[0]),split[1],split[2],Integer.parseInt(split[3]),"",0,0,"0");

            }else{
                //由于需要执行序列化方法，不能给予空值，所以给予一个默认值
                infoBean.set(0,"",split[0],0,split[1],Integer.parseInt(split[2]),Integer.parseInt(split[3]),"1");

            }
            context.write(new Text(infoBean.getP_id()),infoBean);
        }
    }


    static class RJoinReducer extends Reducer<Text,InfoBean,Text,InfoBean>{
        InfoBean keyBean=new InfoBean();
        //用于储存该id下关联的表内容
        ArrayList<InfoBean> infoBeanArrayList=new ArrayList<InfoBean>();

        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {

           for(InfoBean bean:values){
                if(bean.getFlag().equals("1")){
                    try {
                        BeanUtils.copyProperties(keyBean,bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }else {
                    InfoBean orderBean=new InfoBean();
                    try {
                        BeanUtils.copyProperties(orderBean,bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    infoBeanArrayList.add(orderBean);
                }
            }

            //拼接两类结果输出
            for(InfoBean bean:infoBeanArrayList){
                bean.setP_name(keyBean.getP_name());
                bean.setCategory_id(keyBean.getCategory_id());
                bean.setPrice(keyBean.getPrice());
                context.write(new Text(bean.getP_id()),bean);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //如果在linux上执行，已经配置hadoop则不需要配置
//        configuration.set("mapreduce.framework.name","yarn");
//        configuration.set("yarn.resoucemanager.hostname","bigdata1");
        Job job = Job.getInstance(configuration);

        //指定本程序jar所在路径
        job.setJar("/Users/november/IdeaProjects/hadoopstudy/out/artifacts/hadoopstudy_jar/hadoopstudy.jar");
//        job.setJarByClass(RJoin.class);

        //指定本业务job使用的mapper和reducer的业务类
        job.setMapperClass(RJoinMapper.class);
        job.setReducerClass(RJoinReducer.class);

        //指定mapper输出数据的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);


        //指定最终输出数据的kv类型（有reducer就是reducer的输出，没有reducer就是mapper的输出）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoBean.class);

        //指定job输入的原始文件目录
        FileInputFormat.setInputPaths(job,new Path("/test/input"));
        //指定job输出结果的所在目录
        FileOutputFormat.setOutputPath(job,new Path("/test/out"));

        //将job中配置的相关配置，以及job所用的java类所在的jar包，提交给yarn运行
//        job.submit();//此方法并不知道程序的运行情况
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
