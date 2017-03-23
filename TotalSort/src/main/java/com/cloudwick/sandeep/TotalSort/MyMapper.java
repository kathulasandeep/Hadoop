package com.cloudwick.sandeep.TotalSort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable>{
	public void map(LongWritable offset, Text val,Context con) throws IOException, InterruptedException
	{
		con.write(new LongWritable(Long.parseLong(val.toString())),NullWritable.get());
	}

}
