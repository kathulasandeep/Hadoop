package com.cloudwick.sandeep.MRTask5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.cloudwick.sandeep.MRTask5.CustomWritable;

public class MyPartitioner extends Partitioner<CustomWritable, Text>{

	@Override
	public int getPartition(CustomWritable arg0, Text arg1, int numReducers) {
		// TODO Auto-generated method stub
		return Math.abs(arg0.getNaturalKey().hashCode())%numReducers;
	}

}
