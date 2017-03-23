package com.cloudwick.sandeep.TextPairCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyCombiner extends Reducer<CustomWritable, IntWritable, CustomWritable, IntWritable>{
	private int sum;
	private IntWritable value=new IntWritable();
	public void reduce(CustomWritable key, Iterable<IntWritable> vals, Context con) throws IOException, InterruptedException
	{
		sum=0;
		for(IntWritable val:vals)
		{
			sum+=val.get();
		}
		value.set(sum);
		con.write(key, value);
	}	
}
