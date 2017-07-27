package com.laoxiao.mr.weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MyPartioner extends HashPartitioner<MyKey,Text>{

	public int getPartition(MyKey key, Text value, int numReduceTasks) {
		return (key.getYear()-1949)%numReduceTasks;
	}
}
