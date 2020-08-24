package com.bigdata.mrprocess;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * map执行后就根据分区放到对应的位置
 * 用于分区，设置分区数量等于reduce task数量一致
 */
public class PPartitioner extends Partitioner<Text,PBeanSort> {
    // 根据不同的返回值，放到不同的分区里，每个分区会生成一个文件
    // 这里的返回值数量必须和job.setNumReduceTasks()设置的数量一致
    @Override
    public int getPartition(Text text, PBeanSort pBeanSort, int i) {
        System.out.println("===>>PPartitioner -- getPartition");
        return 1;
    }
}
