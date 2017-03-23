package com.cloudwick.sandeep.IPMapping;


import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.maxmind.geoip.LookupService;

@SuppressWarnings("deprecation")
public class MyMapper extends Mapper<LongWritable, CustomWritable, Text, IntWritable>{
	
	LookupService cl;
	private IntWritable o=new IntWritable(1);
	@Override
	protected void setup(Mapper<LongWritable, CustomWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		Path[] p=DistributedCache.getLocalCacheFiles(context.getConfiguration());
		String dbfile=p[0].getName().toString();
		System.out.println(dbfile);
		cl = new LookupService(dbfile,LookupService.GEOIP_MEMORY_CACHE);
		String y=cl.getCountry("66.178.86.193").getName();
		System.out.println(y);
	}
	public void map(LongWritable l, CustomWritable c, Context con) throws IOException, InterruptedException
	{
		//String x=c.getIp().toString()+"-"+c.getUserId().toString()+"-"+c.getTimeStamp().toString()+"-"+c.getRequestType().toString()+"-"+c.getResponceCode().toString();
		String x=cl.getCountry(c.getIp().toString()).getName();
		//String y=cl.getCountry(c.getIp().toString()).getName();
		con.write(new Text(x), o);
	}

}
