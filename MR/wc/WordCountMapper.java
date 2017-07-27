package com.laoxiao.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author root
 * 定义map任务输入和输出数据类型。
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	/**
	 * map方法是一行数据调用一次。每一次调用传入一行数据。该行数据的下标位为key。内容为value
	 */
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		 String[] words =value.toString().split(" ");
		 for (int i = 0; i < words.length; i++) {
			String w = words[i];
			Text outkey =new Text(w);
			IntWritable outvalue=new IntWritable(1);
			context.write(outkey, outvalue);
		}
	}
}
