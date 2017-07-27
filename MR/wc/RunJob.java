package com.laoxiao.mr.wc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {

	public static void main(String[] args) {
		Configuration config =new Configuration();
//		config.set("fs.defaultFS", "hdfs://node5:9000");
		config.set("mapred.jar", "C:\\Users\\Administrator\\Desktop\\wc.jar");
		try {
			FileSystem fs =FileSystem.get(config);
			Job job =Job.getInstance(config);
			job.setJobName("wc");
			job.setJarByClass(RunJob.class);//job的入口类
			
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
//			job.setCombinerClass(WordCountReducer.class);
			//定义job任务输入数据目录和输出结果目录
			//把wc.txt上传到hdfs目录中/usr/intput/wc.txt
			//输出结果数据放到/usr/output/wc
			
			FileInputFormat.addInputPath(job, new Path("/usr/input/wc.txt"));
			
			//输出结果数据目录不能存在，job执行时自动创建的。如果在执行时目录已经存在，则job执行失败。
			Path output =new Path("/usr/output/wc");
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
}
