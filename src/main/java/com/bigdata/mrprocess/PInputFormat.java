package com.bigdata.mrprocess;

import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

public class PInputFormat extends InputFormat {
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }
}
