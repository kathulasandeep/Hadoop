package com.cloudwick.sandeep.SecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<CustomWritable, NullWritable, IntWritable, IntWritable>{
	IntWritable key=new IntWritable();
	IntWritable value=new IntWritable();
	String message,id,location;
	public void reduce(CustomWritable key, Iterable<NullWritable> val, Context con) throws IOException, InterruptedException
	{
		for(NullWritable v:val)
		{
			this.key.set(Integer.parseInt(key.getNaturalKey().toString()));
			value.set(Integer.parseInt(key.getSecondaryKey().toString()));
			con.write(this.key, value);
		}
		
		
	}
}