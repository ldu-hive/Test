package com.laoxiao.mr.weather;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {

	public static void main(String[] args) {
		Configuration config =new Configuration();
		config.set("fs.defaultFS", "hdfs://node6:9000");
//		config.set("mapred.jar", "C:\\Users\\Administrator\\Desktop\\wc.jar");
		config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		try {
			FileSystem fs =FileSystem.get(config);
			Job job =Job.getInstance(config);
			job.setJobName("wc");
			job.setJarByClass(RunJob.class);//job的入口类
			
			job.setMapperClass(WeatherMapper.class);
			job.setReducerClass(WeatherReduer.class);
//			
			job.setMapOutputKeyClass(MyKey.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setPartitionerClass(MyPartioner.class);
			job.setSortComparatorClass(MySort.class);
			job.setGroupingComparatorClass(MyGroup.class);
			
			job.setNumReduceTasks(3);
			
			//设置一个map任务输入数据的格式
			job.setInputFormatClass(KeyValueTextInputFormat.class);//KeyValueTextInputFormat 把第一个隔开符的左边为key，右边为value
			
//			job.setCombinerClass(WordCountReducer.class);
			//定义job任务输入数据目录和输出结果目录
			//把wc.txt上传到hdfs目录中/usr/intput/wc.txt
			//输出结果数据放到/usr/output/wc
			
			FileInputFormat.addInputPath(job, new Path("/usr/input/weather"));
			
			//输出结果数据目录不能存在，job执行时自动创建的。如果在执行时目录已经存在，则job执行失败。
			Path output =new Path("/usr/output/weather");
			if(fs.exists(output)){
				fs.delete(output, true);
			}
			FileOutputFormat.setOutputPath(job,output );
			
			boolean f= job.waitForCompletion(true);
			if(f){
				System.out.println("job执行成功！");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	static class WeatherMapper extends Mapper<Text, Text, MyKey, Text>{
		
		protected void map(Text key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String datestring =key.toString();
			try {
				Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(datestring);
				Calendar c =Calendar.getInstance();
				c.setTime(date);
				int year =c.get(Calendar.YEAR);
				int month =c.get(Calendar.MONTH);
				double hot =Double.parseDouble(value.toString().substring(0, value.toString().length()-1));
				MyKey outkey =new MyKey();
				outkey.setHot(hot);
				outkey.setMonth(month);
				outkey.setYear(year);
				context.write(outkey, new Text(key.toString()+"\t"+value.toString()));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	static class WeatherReduer extends Reducer<MyKey, Text, NullWritable, Text>{
		protected void reduce(MyKey arg0, Iterable<Text> arg1,
				Context arg2)
				throws IOException, InterruptedException {
			int i=0;
			for(Text t :arg1){
				if(i<3){
					arg2.write(NullWritable.get(), t);
				}else{
					break;
				}
				i++;
			}
		}
	}
}
