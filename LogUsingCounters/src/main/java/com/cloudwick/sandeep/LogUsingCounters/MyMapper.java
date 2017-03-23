package com.cloudwick.sandeep.LogUsingCounters;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	public void map(LongWritable offset, Text code, Context con){
		String str=code.toString();
		if(str.contains("200"))
			con.getCounter(Codes.TwoHundred).increment(1);
		else if(str.contains("404"))
			con.getCounter(Codes.FourHundredAndFour).increment(1);
		else if(str.contains("503"))
			con.getCounter(Codes.FiveHundredAndThree).increment(1);
		
	}

}
