package com.cloudwick.sandeep.IPMapping;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CustomWritable implements Writable{

	private Text ip,userId,timeStamp, requestType, responceCode;
	/**
	 * @return the ip
	 */
	public Text getIp() {
		return ip;
	}

	/**
	 * @param ip the ip to set
	 */
	public void setIp(Text ip) {
		this.ip = ip;
	}

	/**
	 * @return the userId
	 */
	public Text getUserId() {
		return userId;
	}

	/**
	 * @param userId the userId to set
	 */
	public void setUserId(Text userId) {
		this.userId = userId;
	}

	/**
	 * @return the timeStamp
	 */
	public Text getTimeStamp() {
		return timeStamp;
	}

	/**
	 * @param timeStamp the timeStamp to set
	 */
	public void setTimeStamp(Text timeStamp) {
		this.timeStamp = timeStamp;
	}

	/**
	 * @return the requestType
	 */
	public Text getRequestType() {
		return requestType;
	}

	/**
	 * @param requestType the requestType to set
	 */
	public void setRequestType(Text requestType) {
		this.requestType = requestType;
	}

	/**
	 * @return the responceCode
	 */
	public Text getResponceCode() {
		return responceCode;
	}

	/**
	 * @param responceCode the responceCode to set
	 */
	public void setResponceCode(Text responceCode) {
		this.responceCode = responceCode;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		ip.write(out);
		userId.write(out);
		timeStamp.write(out);
		requestType.write(out);
		responceCode.write(out);
		
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		ip.readFields(in);
		userId.readFields(in);
		timeStamp.readFields(in);
		requestType.readFields(in);
		responceCode.readFields(in);
	}

}
