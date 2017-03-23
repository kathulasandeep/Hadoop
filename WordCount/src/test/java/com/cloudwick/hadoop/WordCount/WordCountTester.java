package com.cloudwick.hadoop.WordCount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountTester {
	
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	 @Before
     public void setUp() {
          WordCountMapper mapper = new WordCountMapper();
          mapDriver = MapDriver.newMapDriver(mapper);
          WordCountReducer reducer=new WordCountReducer();
          reduceDriver=ReduceDriver.newReduceDriver(reducer);
          mapReduceDriver=MapReduceDriver.newMapReduceDriver(mapper, reducer);
 }
	 @Test
	 public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("abc\tdef"))
		.withOutput(new Text("abc"), new IntWritable(1))
		.withOutput(new Text("def"), new IntWritable(1))
		.runTest();
	 }
		
	@Test
	public void testReducer() throws IOException {
		List<IntWritable> val=new ArrayList<IntWritable>();
		val.add(new IntWritable(1));
		val.add(new IntWritable(1));
		reduceDriver.withInput(new Text("abc"), val)
		.withOutput(new Text("abc"), new IntWritable(2))
		.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException
	{
		mapReduceDriver.withInput(new LongWritable(), new Text("abc\tdef\ndef\tabc"))
		.withOutput(new Text("abc"), new IntWritable(2))
		.withOutput(new Text("def"), new IntWritable(2))
		.runTest();
	}
	

}
