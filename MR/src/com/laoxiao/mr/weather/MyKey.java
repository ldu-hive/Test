package com.laoxiao.mr.weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 如果自定义对象用于value，只要实现Writable接口
 * 如果自定义的对象用户key ，需要实现WritableComparable
 * @author root
 *
 */
public class MyKey implements WritableComparable{

	private int year;
	private int month;
	private double hot;
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public int getMonth() {
		return month;
	}
	public void setMonth(int month) {
		this.month = month;
	}
	public double getHot() {
		return hot;
	}
	public void setHot(double hot) {
		this.hot = hot;
	}
	
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeDouble(hot);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.year=in.readInt();
		this.month=in.readInt();
		this.hot=in.readDouble();
	}
	
	public int compareTo(Object o) {
		return this==o?0:-1;
	}
	
}
