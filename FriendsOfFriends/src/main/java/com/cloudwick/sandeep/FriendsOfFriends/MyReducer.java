package com.cloudwick.sandeep.FriendsOfFriends;

import java.io.IOException;
import java.util.ArrayList;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, IntWritable, IntWritable>{
	List <Integer> mutual=new ArrayList<Integer>();
	List<MyClass> fin=new ArrayList<MyClass>();
	IntWritable key=new IntWritable();
	IntWritable value=new IntWritable();
	

	@Override
	public void reduce(Text k, Iterable<IntWritable> val, Context con) throws IOException, InterruptedException
	{
		mutual.clear();
		Iterator<IntWritable> itr=val.iterator();
		while(itr.hasNext())
		{
			int friend=itr.next().get();
			if(friend==-1)
			{
				mutual.clear();
				break;
			}
			else
			{
				mutual.add(friend);
			}
		}
		//Collections.sort(list2);
		if(!mutual.isEmpty())
		{
			String[]a=k.toString().split("---");
			MyClass obj=new MyClass(mutual.size(),new Text(a[0]+"---"+a[1]));
			fin.add(obj);
		}
	}
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		Collections.sort(fin, new Comparator<MyClass>() {

			public int compare(MyClass o1, MyClass o2) {
				// TODO Auto-generated method stub
				return -(o1.x-o2.x);
			}
		});
		for(MyClass my:fin)
		{
			
			String[]a=my.y.toString().split("---");
			key.set(Integer.parseInt(a[0]));
			value.set(Integer.parseInt(a[1]));
			context.write(key, value);
			context.write(value, key);
		}
		
	}
}

class MyClass{
	int x;
	Text y;
	MyClass(int x,Text y)
	{
		this.x=x;
		this.y=y;
	}
	/**
	 * @return the x
	 */
	public int getX() {
		return x;
	}

	/**
	 * @param x the x to set
	 */
	public void setX(int x) {
		this.x = x;
	}
	
	
	/**
	 * @return the y
	 */
	public Text getY() {
		return y;
	}
	/**
	 * @param y the y to set
	 */
	public void setY(Text y) {
		this.y = y;
	}
	
}
