package com.cloudwick.sandeep.TextPairCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CustomRecordReader extends RecordReader<Text, Text>{

	private Text key=new Text();
	private Text value=new Text();
	
	long start;
	long end;
	long pos;
	FSDataInputStream fileIn;
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	if(fileIn!=null)
		fileIn.close();	
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit inpSplit, TaskAttemptContext con) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSplit split=(FileSplit)inpSplit;
		start=split.getStart();
		end=start+split.getLength();
		pos=start;
		Path file = split.getPath(); 
		Configuration conf=con.getConfiguration();
		FileSystem fs=file.getFileSystem(conf);
		fileIn=fs.open(split.getPath());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

}
