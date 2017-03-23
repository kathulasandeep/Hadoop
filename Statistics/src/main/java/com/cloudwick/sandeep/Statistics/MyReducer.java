package com.cloudwick.sandeep.Statistics;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends org.apache.hadoop.mapreduce.Reducer<NullWritable,FloatWritable, Text, FloatWritable>{

	float min=Float.MAX_VALUE,max=Float.MIN_VALUE,sum;
	int count;
	Text key=new Text();
	FloatWritable value=new FloatWritable();
	public void reduce(NullWritable k, Iterable<FloatWritable> val, Context con)
	{
		Iterator<FloatWritable> itr= val.iterator();
		while(itr.hasNext())
		{
			float value=itr.next().get();
			count+=1;
			if(value>max)
				max=value;
			if(value<min)
				min=value;
			sum+=value;
		}
	}
	@Override
	protected void cleanup(Reducer<NullWritable, FloatWritable, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		key.set("min");
		value.set(min);
		context.write(key, value);
		key.set("max");
		value.set(max);
		context.write(key, value);
		key.set("count");
		value.set(count);
		context.write(key, value);
		key.set("sum");
		value.set(sum);
		context.write(key, value);
		key.set("avg");
		value.set(sum/count);
		context.write(key, value);	
	}
}
