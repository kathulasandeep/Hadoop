package com.cloudwick.sandeep.IPMapping;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


@SuppressWarnings("deprecation")
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
		job.setJobName("IP Mapping");
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setInputFormatClass(MyInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("input.data"/*args[0]*/));
		FileOutputFormat.setOutputPath(job, new Path("output"/*args[1]*/));
		DistributedCache.addCacheFile(new URI("GeoIP.dat"), job.getConfiguration());
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true)?0:1 ;
	
		
	}

}