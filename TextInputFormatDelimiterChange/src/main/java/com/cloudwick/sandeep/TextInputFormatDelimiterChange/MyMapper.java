package com.cloudwick.sandeep.TextInputFormatDelimiterChange;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	public void map(LongWritable offset, Text val, Context con) throws IOException, InterruptedException
	{
		con.write(val, NullWritable.get());
	}

}
