package com.cloudwick.hadoop.WordCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private Text word=new Text();
	private IntWritable count=new IntWritable(1);
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
	{
		StringTokenizer st=new StringTokenizer(value.toString());
		while(st.hasMoreTokens())
		{
			word.set(st.nextToken());
			con.write(word, count);
			
		}
	}

}
