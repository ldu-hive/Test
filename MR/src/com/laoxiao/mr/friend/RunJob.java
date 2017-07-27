package com.laoxiao.mr.friend;


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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class RunJob {

	public static void main(String[] args) {
		Configuration config =new Configuration();
		config.set("fs.defaultFS", "hdfs://node6:9000");
//		config.set("mapred.jar", "C:\\Users\\Administrator\\Desktop\\wc.jar");
		config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		try {
			FileSystem fs =FileSystem.get(config);
			Job job =Job.getInstance(config);
			job.setJobName("f1");
			job.setJarByClass(RunJob.class);//job的入口类
			
			job.setMapperClass(Mapper1.class);
			job.setReducerClass(Reducer1.class);
////			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
//			
//			job.setPartitionerClass(MyPartioner.class);
//			job.setSortComparatorClass(MySort.class);
//			job.setGroupingComparatorClass(MyGroup.class);
			
//			job.setNumReduceTasks(3);
			
			//设置一个map任务输入数据的格式
			job.setInputFormatClass(KeyValueTextInputFormat.class);//KeyValueTextInputFormat 把第一个隔开符的左边为key，右边为value
			
//			job.setCombinerClass(WordCountReducer.class);
			//定义job任务输入数据目录和输出结果目录
			//把wc.txt上传到hdfs目录中/usr/intput/wc.txt
			//输出结果数据放到/usr/output/wc
			
			FileInputFormat.addInputPath(job, new Path("/usr/input/friend"));
			
			//输出结果数据目录不能存在，job执行时自动创建的。如果在执行时目录已经存在，则job执行失败。
			Path output =new Path("/usr/output/f1");
			if(fs.exists(output)){
				fs.delete(output, true);
			}
			FileOutputFormat.setOutputPath(job,output );
			
			boolean f= job.waitForCompletion(true);
			if(f){
				System.out.println("第一个job执行成功！");
				job =Job.getInstance(config);
				job.setJobName("f2");
				job.setJarByClass(RunJob.class);//job的入口类
				
				job.setMapperClass(Mapper2.class);
				job.setReducerClass(Reducer2.class);
////				
				job.setMapOutputKeyClass(User.class);
				job.setMapOutputValueClass(Text.class);
				
				job.setGroupingComparatorClass(Group1.class);
				
				job.setInputFormatClass(KeyValueTextInputFormat.class);//KeyValueTextInputFormat 把第一个隔开符的左边为key，右边为value
				
//				job.setCombinerClass(WordCountReducer.class);
				//定义job任务输入数据目录和输出结果目录
				//把wc.txt上传到hdfs目录中/usr/intput/wc.txt
				//输出结果数据放到/usr/output/wc
				
				FileInputFormat.addInputPath(job, new Path("/usr/output/f1"));
				
				//输出结果数据目录不能存在，job执行时自动创建的。如果在执行时目录已经存在，则job执行失败。
				output =new Path("/usr/output/f2");
				if(fs.exists(output)){
					fs.delete(output, true);
				}
				FileOutputFormat.setOutputPath(job,output );
				
				f= job.waitForCompletion(true);
				if(f){
					System.out.println("第二个job执行成功！");
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	static class Mapper1 extends Mapper<Text, Text, Text, IntWritable>{
		
		/**
		 * 找到所有的fof：user1:user2   zs:ww   ww:zs
		 */
		protected void map(Text key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String user=key.toString();
			String[] friends =StringUtils.split(value.toString(), '\t');
			for (int i = 0; i < friends.length; i++) {
				String f1 = friends[i];
				String userAndFriend= f1.compareTo(user)>0 ?f1+":"+user:user+":"+f1;
				context.write(new Text(userAndFriend), new IntWritable(0)); //输出的是直接好友关系  比如：zs:ls  0
				for (int j = i+1; j < friends.length; j++) {
					String f2 = friends[j];
					String fof= f1.compareTo(f2)>0 ?f1+":"+f2:f2+":"+f1;
					context.write(new Text(fof), new IntWritable(1));//输出的是fof关系      比如：zs:ls   1
				}
			}
			
		}
	}
	
	static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable>{
		protected void reduce(Text arg0, Iterable<IntWritable> arg1,
				Context arg2)
				throws IOException, InterruptedException {
			int sum =0;
			boolean f=true;
			for(IntWritable i :arg1){
				if(i.get()==0){//如果存在一个value=0的情况。一定在map时候有直接好友存在。删除这一组（不要）
					f=false;
					break;
				}else{
					sum=sum+i.get();
				}
			}
			if(f){
				arg2.write(arg0, new IntWritable(sum));
			}
		}
	}
	
	static class Mapper2 extends Mapper<Text, Text, User, Text>{
		
		protected void map(Text key, Text value,
				Context context)
						throws IOException, InterruptedException {
			String f1 =key.toString().split(":")[0];
			String f2 =key.toString().split(":")[1];
			int count =Integer.parseInt(value.toString());
			
			User u1 =new User();
			u1.setUser(f1);
			u1.setOther(f2);
			u1.setCount(count);
			context.write(u1, new Text(f2+":"+count));
			
			User u2 =new User();
			u2.setUser(f2);
			u2.setOther(f1);
			u2.setCount(count);
			context.write(u2, new Text(f1+":"+count));
		}
	}
	
	static class Reducer2 extends Reducer<User, Text, Text, Text>{
		protected void reduce(User arg0, Iterable<Text> arg1,
				Context arg2)
						throws IOException, InterruptedException {
			StringBuilder sb =new StringBuilder();
			for(Text t:arg1){
				sb.append(t.toString()).append(",");
			}
			arg2.write(new Text(arg0.getUser()), new Text(sb.substring(0, sb.length()-1)));
		}
	}
	
	static class Group1 extends WritableComparator{
		public Group1(){
			super(User.class,true);
		}
		
		public int compare(WritableComparable a, WritableComparable b) {
			User u1 =(User) a;
			User u2=(User) b;
			return u1.getUser().compareTo(u2.getUser());
		}
	}
}
