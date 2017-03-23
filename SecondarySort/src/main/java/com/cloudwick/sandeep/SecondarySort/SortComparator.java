package com.cloudwick.sandeep.SecondarySort;

import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator{
	
	public SortComparator(){
		super(CustomWritable.class,true);
	}
	
	public int compare(CustomWritable a, CustomWritable b)
	{
		
		int comp=a.getNaturalKey().compareTo(b.getNaturalKey());
		 if(comp==0)
			 comp=a.getSecondaryKey().compareTo(b.getSecondaryKey());
		return comp;	 
	}

}
