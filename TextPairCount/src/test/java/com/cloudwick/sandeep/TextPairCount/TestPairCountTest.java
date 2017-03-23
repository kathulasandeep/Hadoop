package com.cloudwick.sandeep.TextPairCount;

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


public class TestPairCountTest {
	
	MapDriver<LongWritable, Text, CustomWritable, IntWritable> mapDriver;
	ReduceDriver<CustomWritable, IntWritable, CustomWritable, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, CustomWritable, IntWritable, CustomWritable, IntWritable> mapReduceDriver;
	
	 @Before
     public void setUp() {
          MyMapper mapper = new MyMapper();
          mapDriver = MapDriver.newMapDriver(mapper);
          MyReducer reducer=new MyReducer();
          reduceDriver=ReduceDriver.newReduceDriver(reducer);
          mapReduceDriver=MapReduceDriver.newMapReduceDriver(mapper, reducer);
 }
	 @Test
	 public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("abc:def"))
		.withOutput(new CustomWritable("abc","def"), new IntWritable(1))
		.runTest();
	 }
		
	@Test
	public void testReducer() throws IOException {
		List<IntWritable> val=new ArrayList<IntWritable>();
		val.add(new IntWritable(1));
		val.add(new IntWritable(1));
		reduceDriver.withInput(new CustomWritable("abc","def"), val)
		.withOutput(new CustomWritable("abc","def"), new IntWritable(2))
		.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException
	{
		mapReduceDriver.withInput(new LongWritable(), new Text("abc:def"))
		.withOutput(new CustomWritable("abc","def"), new IntWritable(1))
		.runTest();
	}
	


}
