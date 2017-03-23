package com.cloudwick.sandeep.TextInputFormatDelimiterChange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Driver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res=ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=getConf();
		conf.set("textinputformat.record.delimiter", "$");
		Job job=Job.getInstance(conf);
		job.setJarByClass(Driver.class);
		job.setJobName(" Change TextInputFormat delimiter to $");
		job.setMapperClass(MyMapper.class);
		FileInputFormat.addInputPath(job, new Path("input"/*args[0]*/));
		FileOutputFormat.setOutputPath(job, new Path("output"/*args[1]*/));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		return job.waitForCompletion(true)?0:1 ;
	}

}
