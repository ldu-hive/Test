package com.laoxiao.mr.friend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class User implements WritableComparable<User> {

	private String user;
	private String other;
	private int count;
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getOther() {
		return other;
	}
	public void setOther(String other) {
		this.other = other;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(user);
		out.writeUTF(other);
		out.writeInt(count);
	}
	
	public void readFields(DataInput in) throws IOException {
		this.user =in.readUTF();
		this.other =in.readUTF();
		this.count=in.readInt();
	}
	
	public int compareTo(User o) {
		int r = this.getUser().compareTo(o.getUser());
		if(r==0){
			return -Integer.compare(this.getCount(), o.getCount());
		}
		return r;
	}
	
}
