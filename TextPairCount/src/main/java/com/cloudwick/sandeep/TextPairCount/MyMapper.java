package com.cloudwick.sandeep.TextPairCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, CustomWritable, IntWritable>{
	CustomWritable cust;
	Text state=new Text();
	Text city=new Text();
	IntWritable one=new IntWritable(1);
	
	@Override
	public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] pair=line.toString().split(":");
		state.set(pair[0]);
		city.set(pair[1]);
		cust=new CustomWritable();
		cust.setState(state);
		cust.setCity(city);
		context.write(cust, one);
		cust=null;
	}

}
