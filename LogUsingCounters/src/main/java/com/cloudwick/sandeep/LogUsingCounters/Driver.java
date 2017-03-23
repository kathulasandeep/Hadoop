package com.cloudwick.sandeep.LogUsingCounters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Driver extends Configured implements Tool {
	
	public static void main(String args[]) throws Exception
	{
		int res=ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res);
	}

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=getConf();
		Job job=Job.getInstance(conf);
		job.setJarByClass(Driver.class);
		job.setJobName("Log count using counters");
		job.setMapperClass(MyMapper.class);
		job.setInputFormatClass(MyInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("input.data"/*args[0]*/));
		FileOutputFormat.setOutputPath(job, new Path("output"/*args[1]*/));
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		int x=job.waitForCompletion(true)?0:1 ;
		Counters counters = job.getCounters();
		for (CounterGroup group : counters) {
			  for (Counter counter : group) {
			    System.out.println("  - " + counter.getDisplayName() + ": " + counter.getName() + ": "+counter.getValue());
			  }
			}
		
		return x;
		
	}

}
enum Codes { TwoHundred,
FourHundredAndFour,
FiveHundredAndThree
}