package com.bigdata.mrprocess;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PPartitioner extends Partitioner<Text,Text> {
    @Override
    public int getPartition(Text text, Text text2, int i) {
        return 0;
    }
}
