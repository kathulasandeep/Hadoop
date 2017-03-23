package com.cloudwick.sandeep.TotalSort;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new Configuration(), new Driver(), args);

	}

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job=Job.getInstance(getConf());
		job.setJarByClass(Driver.class);
		job.setJobName("Total sort");
		job.setMapperClass(MyMapper.class);
		FileInputFormat.addInputPath(job, new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		job.setNumReduceTasks(5);
		job.setInputFormatClass(TextInputFormat.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		InputSampler.Sampler<LongWritable,NullWritable> sampler = new InputSampler.RandomSampler<LongWritable,NullWritable>(0.5, 5, 1);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("intermediate"));
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		InputSampler.writePartitionFile(job, sampler);
		
		return job.waitForCompletion(true)?0:1;
	}

}
