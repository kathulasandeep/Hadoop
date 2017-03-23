package com.cloudwick.sandeep.MRTask5;



import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator{
	
	public GroupComparator(){
		super(CustomWritable.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b)
	{
		CustomWritable x=(CustomWritable)a;
		CustomWritable y=(CustomWritable)b;
		
		return x.getNaturalKey().compareTo(y.getNaturalKey());
		
	}
}