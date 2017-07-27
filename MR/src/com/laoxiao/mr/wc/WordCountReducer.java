package com.laoxiao.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	/*
	 * 该方法每一组调用一次。
	 */
	protected void reduce(Text key, Iterable<IntWritable> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		int sum =0;
		for(IntWritable i :arg1){
			sum=sum+i.get();
		}
		arg2.write(key, new IntWritable(sum));
	}
}
