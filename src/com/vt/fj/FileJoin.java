package com.vt.fj;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class FileJoin {

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length < 2) {
			System.err.println("Usage: FileJoin <inp>[inp...] <out>");
			System.exit(2);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "File Join");
		job.setJarByClass(FileJoin.class);
		job.setMapperClass(FileJoinMapper.class);
		job.setCombinerClass(FileJoinReducer.class);
		job.setReducerClass(FileJoinReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		for(int i = 0; i < otherArgs.length -1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
