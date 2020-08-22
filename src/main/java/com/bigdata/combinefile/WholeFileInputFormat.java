package com.bigdata.combinefile;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

//继承FileInputFormat，需要设置返回，这里是返回NullWritable和BytesWritable，返回给map
public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable>{

	//这里是设置能否被切片，这里表示不切片
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	//创建一个createRecordReader,这个类会从切片中读取每一条数据，这里因为设置了不切片，所以这里是读取整个文件
	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		//WholeFileRecordReader继承RecordReader
		WholeFileRecordReader reader = new WholeFileRecordReader();
		reader.initialize(split, context);
		return reader;
	}

}
