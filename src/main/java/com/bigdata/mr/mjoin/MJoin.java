package com.bigdata.mr.mjoin;

import com.bigdata.mr.rjoin.InfoBean;
import com.bigdata.mr.rjoin.RJoin;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * 解决reduce数据倾斜问题，使用map join
 */
public class MJoin {

    static class MJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
        Map<String,String> pdInfoMap=new HashMap<String,String>();
        Text tk=new Text();
        /**
         *是在map处理数据之前调用一次，可以用于初始化工作
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //用hashMap加载产品信息表
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("goods.txt")));
            String line;
            //读每一行，只要不为空
            while(StringUtils.isNotEmpty(line=bufferedReader.readLine())){
                String[] split = line.split(",");
                pdInfoMap.put(split[0],split[1]);
            }
        }

        //已经有完整的产品信息表，所以在map已经可以实现join了
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line =value.toString();
            String[] split = line.split(",");
            String pdName=pdInfoMap.get(split[2]);
            tk.set(line+","+pdName);
            context.write(tk,NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //如果在linux上执行，已经配置hadoop则不需要配置
//        configuration.set("mapreduce.framework.name","yarn");
//        configuration.set("yarn.resoucemanager.hostname","bigdata1");
        Job job = Job.getInstance(configuration);

        //指定本程序jar所在路径
//        job.setJar("/Users/november/IdeaProjects/hadoopstudy/out/artifacts/hadoopstudy_jar/hadoopstudy.jar");
        job.setJarByClass(MJoin.class);

        //指定本业务job使用的mapper和reducer的业务类
        job.setMapperClass(MJoinMapper.class);

        //指定mapper输出数据的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //map端join的逻辑不需要reduce,指定reduce数量为0
        job.setNumReduceTasks(0);


        //跑本地需要把配置文件去掉
        //指定job输入的原始文件目录
        FileInputFormat.setInputPaths(job,new Path("/Users/november/Desktop/file/test/order.txt"));
        //指定job输出结果的所在目录
        FileOutputFormat.setOutputPath(job,new Path("/Users/november/Desktop/file/test/out"));

        //指定一个文件缓存到所有的maptask节点工作目录
//        job.addArchiveToClassPath(new Path(""));//缓存第三方jar包到运行节点classpath中
//        job.addFileToClassPath(new Path(""));//缓存普通文件到运行节点classpath中
//        job.addCacheArchive(new URI(""));//缓存压缩包文件到运行节点的工作目录中
//        job.addCacheFile(new URI(""));//缓存普通文件到运行节点的工作目录
        //把产品表缓存到工作目录中
        job.addCacheFile(new URI("file:///Users/november/Desktop/file/test/goods.txt"));


        //将job中配置的相关配置，以及job所用的java类所在的jar包，提交给yarn运行
//        job.submit();//此方法并不知道程序的运行情况
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
