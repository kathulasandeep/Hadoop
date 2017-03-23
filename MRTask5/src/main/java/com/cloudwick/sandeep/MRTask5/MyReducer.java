package com.cloudwick.sandeep.MRTask5;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.cloudwick.sandeep.MRTask5.CustomWritable;

public class MyReducer extends Reducer<CustomWritable, Text, Text, Text>{
	Text key=new Text();
	Text value=new Text();
	String message,id,location;
	public void reduce(CustomWritable key, Iterable<Text> val, Context con) throws IOException, InterruptedException
	{
		
		Iterator<Text>itr=val.iterator();
		String a[]=itr.next().toString().split("---");
		message=a[1];
		id=a[0];
		this.key.set(id);
		//this.key.set(key.getNaturalKey()+"---"+key.getSecondaryKey());
		location=val.iterator().next().toString();
		value.set(message+"\t"+location);
		con.write(this.key, value);
		
		
	}
}