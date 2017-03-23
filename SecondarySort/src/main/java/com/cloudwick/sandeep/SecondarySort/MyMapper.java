package com.cloudwick.sandeep.SecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper extends Mapper<LongWritable, Text, CustomWritable, NullWritable>{
	
	CustomWritable cust=new CustomWritable();
	Text t1=new Text();
	Text t2=new Text();
	IntWritable i=new IntWritable();
	public void map(LongWritable offset, Text line, Context con) throws IOException, InterruptedException{
		String[] l=line.toString().split(",");
		t1.set(l[0]);
		t2.set(l[1]);
		cust.setNaturalKey(t1);
		cust.setSecondaryKey(t2);
		con.write(cust,NullWritable.get());		
	}

}
