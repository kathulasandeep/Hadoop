package com.cloudwick.sandeep.FriendsOfFriends;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	Text key=new Text();
	IntWritable value=new IntWritable();
	public void map(LongWritable k, Text line, Context con) throws IOException, InterruptedException
	{
		String l[]=line.toString().split("\t");
		String from=l[0];
		String t[]=l[1].split(",");
		for(String val:t)
		{
			key.set(from+"---"+val);
			value.set(-1);
			con.write(key, value);
		}
		if(t.length>1)
		{
			for(int i=0;i<t.length-1;i++)
			{
				for(int j=i+1;j<t.length;j++)
				{
					key.set(t[i]+"---"+t[j]);
					value.set(Integer.parseInt(from));
					con.write(key, value);
					
				}
			}
		}
	}

}
