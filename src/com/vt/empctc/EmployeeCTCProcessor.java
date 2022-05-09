package com.vt.empctc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class EmployeeCTCProcessor {
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length < 2) {
			System.err.println("Usage: Employee CTC Analysis <inp>[inp...] <out>");
			System.exit(2);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "File Join");
		job.setJarByClass(EmployeeCTCProcessor.class);
		job.setMapperClass(EmployeeCTCMapper.class);
		job.setPartitionerClass(EmployeeCTCPartitioner.class);
		job.setReducerClass(EmployeeCTCReducer.class);
		
		job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		for(int i = 0; i < otherArgs.length -1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
