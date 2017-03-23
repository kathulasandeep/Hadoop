package com.cloudwick.sandeep.SecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CustomWritable implements Writable,WritableComparable<CustomWritable>{
	private Text naturalKey=new Text();
	private Text secondaryKey=new Text();
	public int hashCode() {
		return naturalKey.hashCode() * 163 + secondaryKey.hashCode();
	}
	
	public boolean equals(Object o)
	{ 
		if (o instanceof CustomWritable) 
		{
			CustomWritable tp = (CustomWritable) o;
			return naturalKey.equals(tp.naturalKey) && secondaryKey.equals(tp.secondaryKey); 
		}
		return false; 
	}
	
	
	public int compareTo(CustomWritable o) {
		// TODO Auto-generated method stub
		int compare=naturalKey.compareTo(o.naturalKey);
		if(compare!=0)
			return compare;
		return secondaryKey.compareTo(o.secondaryKey);
	
	}

	public void readFields(DataInput arg) throws IOException {
		// TODO Auto-generated method stub
		naturalKey.readFields(arg);
		secondaryKey.readFields(arg);
		
	}

	public void write(DataOutput arg) throws IOException {
		// TODO Auto-generated method stub
		naturalKey.write(arg);
		secondaryKey.write(arg);
		
	}

	/**
	 * @return the naturalKey
	 */
	public Text getNaturalKey() {
		return naturalKey;
	}

	/**
	 * @param naturalKey the naturalKey to set
	 */
	public void setNaturalKey(Text naturalKey) {
		this.naturalKey = naturalKey;
	}

	/**
	 * @return the secondaryKey
	 */
	public Text getSecondaryKey() {
		return secondaryKey;
	}

	/**
	 * @param secondaryKey the secondaryKey to set
	 */
	public void setSecondaryKey(Text secondaryKey) {
		this.secondaryKey = secondaryKey;
	}

}
