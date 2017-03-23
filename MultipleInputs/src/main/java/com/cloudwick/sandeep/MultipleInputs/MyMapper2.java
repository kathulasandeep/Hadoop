package com.cloudwick.sandeep.MultipleInputs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper2 extends Mapper<LongWritable, Text, LongWritable, Text>{
	
	public void map(LongWritable l, Text v, Context con) throws IOException, InterruptedException
	{
		con.write(l, v);
	}

}
