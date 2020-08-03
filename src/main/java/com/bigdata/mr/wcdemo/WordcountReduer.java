package com.bigdata.mr.wcdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


/**
 * KEYIN,VLUEIN 对应 mapper输出的KEYOUT,VALUEOUT的类型队形
 *
 * KEYOUT,VALUEOUT 是自定义reduce逻辑处理完成后输出的数据类型
 */
public class WordcountReduer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * 重写业务逻辑reduce()方法,每一组会调用一次这个方法
     * 入参key,是一组相同单词kv对的key
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;

        //两种循环的写法
        /*
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()){
            count+=iterator.next().get();
        }
        */


        for(IntWritable value:values){
            count+=value.get();
        }

        context.write(key,new IntWritable(count));
    }
}
