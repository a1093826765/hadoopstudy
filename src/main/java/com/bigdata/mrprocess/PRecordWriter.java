package com.bigdata.mrprocess;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class PRecordWriter extends RecordWriter<Text,Text> {
    @Override
    public void write(Text text, Text text2) throws IOException, InterruptedException {

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }
}
