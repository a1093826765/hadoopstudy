package com.bigdata.mr.wcdemo;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN:默认情况下，是mr框架所读到的一行文本的起始偏量，Long,
 *
 * VALUEIN:默认情况下，是mr框架所读到的一行本文的内容，String,
 *         同上，所以用Text
 *
 * KEYOUT:是用户自定义逻辑处理完成后输出的数据中的key，自定义类型
 * VALLUEOUT:是用户自定义逻辑处理完成后输出数据中的value，自定义类型
 *
 * 在hadoop中有自己更精简的序列化接口，对应的类型有对应的序列化
 * Long 》》 LongWriteable
 * String 》》 Text
 * int 》》 IntWritable
 * boolean 》》 BooleanWritable
 * double 》》 DoubleWritable
 * byte 》》 BytesWritable
 */
public class  WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 重写业务逻辑map()方法
     * maptask对每一行数据调用一次我们的自定义的map()方法
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //将maptask传给我们的文本内容转化为String
        String line=value.toString();
        //根据空格将这一行切分成单词
        String[] words=line.split(" ");
        //将单词输出为<单词，1>
        for(String word:words){
            //将单词作为key，将次数作为value，以便于后续的数据分发，可以根据单词分发，以便于相同单词会到相同的reduce task
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
