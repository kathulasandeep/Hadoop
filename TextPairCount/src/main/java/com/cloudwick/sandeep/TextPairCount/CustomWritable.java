package com.cloudwick.sandeep.TextPairCount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class CustomWritable implements Writable, WritableComparable<CustomWritable>{

	private Text state=new Text();
	private Text city=new Text();
	/**
	 * @return the state
	 */
	public CustomWritable()
	{
		
	}
	
	public CustomWritable(String state, String city)
	{
		this.state.set(state);
		this.city.set(city);
	}
	
	public Text getState() {
		return state;
	}

	/**
	 * @param state the state to set
	 */
	public void setState(Text state) {
		this.state = state;
	}

	/**
	 * @return the city
	 */
	public Text getCity() {
		return city;
	}

	/**
	 * @param city the city to set
	 */
	public void setCity(Text city) {
		this.city = city;
	}

	public int compareTo(CustomWritable o) {
		// TODO Auto-generated method stub
		int x=this.state.compareTo(o.state);
		if(x==0)
			x=this.city.compareTo(o.city);
		return x;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		state.readFields(in);
		city.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		state.write(out);
		city.write(out);
	}
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return state.hashCode() * 163 + city.hashCode();
	}
	
	@Override
	public boolean equals(Object o)
	{ 
		if (o instanceof CustomWritable) 
		{
			CustomWritable tp = (CustomWritable) o;
			return state.equals(tp.state) && city.equals(tp.city); 
		}
		return false; 
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return state+"-"+city;
	}

}
