package com.cloudwick.sandeep.MRTask5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudwick.sandeep.MRTask5.CustomWritable;

public class MyMapper extends Mapper<LongWritable, Text, CustomWritable, Text>{
	
	CustomWritable cust=new CustomWritable();
	Text t1=new Text();
	Text t2=new Text();
	IntWritable i=new IntWritable();
	public void map(LongWritable offset, Text line, Context con) throws IOException, InterruptedException{
		String[] l=line.toString().split(",");
		if(l.length==3)
		{
			t1.set(l[2]);
			t2.set(l[0]+"---"+l[1]);
			i.set(1);	
			//System.out.println("33");
		}
		else if(l.length==2)
		{
			t1.set(l[0]);
			t2.set(l[1]);
			i.set(2);	
			//System.out.println("22");
		}
		else
			System.exit(1);
		cust.setNaturalKey(t1);
		cust.setSecondaryKey(i);
		con.write(cust, t2);		
	}

}
