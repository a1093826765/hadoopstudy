package com.bigdata.mrprocess;

import com.bigdata.combinefile.SmallFilesToSequenceFileConverter;
import com.bigdata.combinefile.WholeFileInputFormat;
import com.bigdata.mr.logenhance.LogEnhanceOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//这里 extends Configured implements Tool 就不需要把job写进main方法，可以直接写到run方法里
public class PMain  extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        args=new String[]{"/Users/november/Desktop/file/test/wholeinput","/Users/november/Desktop/file/test/out2"};
        int exitCode = ToolRunner.run(new PMain(),
                args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] strings) throws Exception {

        System.out.println("===>>PMain -- run");
        Configuration configuration = new Configuration();
        //如果在linux上执行，已经配置hadoop则不需要配置
//        configuration.set("mapreduce.framework.name","yarn");
//        configuration.set("yarn.resoucemanager.hostname","bigdata1");
        Job job = Job.getInstance(configuration);

        //指定本程序jar所在路径
        job.setJarByClass(PMain.class);

        //指定本业务job使用的mapper和reducer的业务类
        job.setMapperClass(PMapper.class);
        job.setReducerClass(PReduce.class);

        //指定mapper输出数据的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PBeanSort.class);

        //如果不设置，默认使用TextInputFormatClass.class
        //多个小文件分区使用，可以优化速度，用过合并多个小文件的分区方法
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);
//        CombineTextInputFormat.setMinInputSplitSize(job,2097152);

        //指定最终输出数据的kv类型（有reducer就是reducer的输出，没有reducer就是mapper的输出）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PBeanSort.class);

        //在此设置自定义的WritableComparator类
        job.setGroupingComparatorClass(PWritableComparator.class);
        //在此设置自定义的partitioner类
        job.setPartitionerClass(PPartitioner.class);

        // 要控制不同的内容写往不同的目标路径，可以采用自定义outputFormat的方法
        job.setOutputFormatClass(PFileOutputFormat.class);

        // 设置inputFormat
        job.setInputFormatClass(PFileInputFormat.class);

        //设置分区数量
        job.setNumReduceTasks(1);

        //指定job输入的原始文件目录
        FileInputFormat.setInputPaths(job,new Path(strings[0]));
        //指定job输出结果的所在目录，必须输出一个_success文件，所以在此还需要设置输出path
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));

        //将job中配置的相关配置，以及job所用的java类所在的jar包，提交给yarn运行
//        job.submit();//此方法并不知道程序的运行情况
        return  job.waitForCompletion(true) ? 0 : 1;
    }
}
