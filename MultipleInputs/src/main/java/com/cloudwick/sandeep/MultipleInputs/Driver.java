package com.cloudwick.sandeep.MultipleInputs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
		Job job=Job.getInstance(conf);
		job.setJarByClass(Driver.class);
		job.setJobName("Multiple inputs with multiple paths");
		MultipleInputs.addInputPath(job, new Path("inp1"), TextInputFormat.class, MyMapper1.class);
		MultipleInputs.addInputPath(job, new Path("inp2"), TextInputFormat.class, MyMapper2.class);
		FileOutputFormat.setOutputPath(job, new Path("output"/*args[1]*/));
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		return job.waitForCompletion(true)?0:1 ;
	}

}