package com.cloudwick.sandeep.IPMapping;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	private IntWritable count=new IntWritable(1);
	public void reduce(Text k, Iterable<IntWritable> v, Context con) throws IOException, InterruptedException
	{
		int sum=0;
		for(IntWritable i:v)
			sum+=i.get();
		count.set(sum);
		con.write(k, count);
	}

}
