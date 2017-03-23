package com.cloudwick.sandeep.Statistics;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class MyMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, NullWritable, FloatWritable>{

	FloatWritable value=new FloatWritable();
	
	public void map(LongWritable k, Text val, Context con) throws IOException, InterruptedException
	{
		value.set(Float.parseFloat(val.toString()));
		con.write(NullWritable.get(), value);
	}
	

}
