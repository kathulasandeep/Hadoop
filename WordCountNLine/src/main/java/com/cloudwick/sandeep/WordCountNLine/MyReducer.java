package com.cloudwick.sandeep.WordCountNLine;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	private IntWritable count=new IntWritable(1);
	public void reduce(Text word, Iterable<IntWritable> val, Context con) throws IOException, InterruptedException
	{
		int sum=0;
		for(IntWritable i:val)
			sum+=i.get();
		count.set(sum);
		con.write(word, count);
	}

}
