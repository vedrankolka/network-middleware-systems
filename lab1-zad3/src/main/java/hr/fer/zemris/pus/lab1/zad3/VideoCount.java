package hr.fer.zemris.pus.lab1.zad3;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class VideoCount {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("Usage: VideoCount <input path> <output path>");
			System.exit(-1);
		}
		
		JobConf job1 = new JobConf(VideoCount.class);
		job1.setJobName("Video count 1/2");
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("temp"));
		job1.setMapperClass(VideoCountMap.class);
		job1.setReducerClass(VideoCountReduce.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		JobClient.runJob(job1);
		
		JobConf job2 = new JobConf(VideoCount.class);
		job2.setJobName("Video count 2/2");
		FileInputFormat.addInputPath(job2, new Path("temp/part-00000"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.setMapperClass(VideoCountIdentityMap.class);
		job2.setReducerClass(VideoCountIdentityReduce.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		// add custom sorting of the keys
		job2.setOutputKeyComparatorClass(CustomTextComparator.class);
		JobClient.runJob(job2);
	}
}
