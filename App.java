package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * App
 */
public class App {

	public static void main(String[] args) throws Exception {
		

		// 创建配置对象
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000/");
		conf.set("mapreduce.framework.name", "yarn");
		//conf.set("mapred.job.tracker","localhost:8021");
		conf.set("mapred.jar", "/home/acer/Untitled.jar");
		FileSystem fs = FileSystem.get(conf);
		
		// 创建job对象
		Job job = Job.getInstance(conf);
		job.setJarByClass(App.class);

		job.setJobName("2nd sort");
		
		FileInputFormat.addInputPath(job, new Path("/user/hadoop/se.seq"));
		FileOutputFormat.setOutputPath(job, new Path("/user/hadoop/out"));
		
		//输入格式(seqFile)
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(CompKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(WCMaper.class);
		job.setReducerClass(WCReducer.class);
		
		//设置分区类
		
		job.setPartitionerClass(YearPartitioner.class);
		//设置分组对比器
		job.setGroupingComparatorClass(YearGroupComparator.class);
		job.setSortComparatorClass(CompKeyComparator.class);
		job.waitForCompletion(true);
		return;
	}
}
